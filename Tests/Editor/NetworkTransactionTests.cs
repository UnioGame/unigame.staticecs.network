namespace UniGame.StaticEcs.Network.Tests
{
    using FFS.Libraries.StaticEcs;
    using FFS.Libraries.StaticPack;
    using NUnit.Framework;

    public sealed class NetworkTransactionTests
    {
        [SetUp]
        public void SetUp()
        {
            World<TransactionWorld>.Create(WorldConfig.Default());
            World<TransactionWorld>.Types()
                .Event<TransactionCommand>()
                .Event<NetworkCommandAcceptedEvent<TransactionCommand>>()
                .Event<NetworkCommandRejectedEvent<TransactionCommand>>();
            World<TransactionWorld>.Initialize();
        }

        [TearDown]
        public void TearDown() => World<TransactionWorld>.Destroy();

        [Test]
        public void TransactionIsExactOnceAndReceiptPrecedesSnapshot()
        {
            using var harness = new Harness(11);
            var receiver = World<TransactionWorld>
                .RegisterEventReceiver<NetworkCommandAcceptedEvent<TransactionCommand>>();
            Assert.That(harness.Client.SubmitTransaction(
                    new TransactionCommand { Value = 7 }, out var id),
                Is.EqualTo(NetworkCommandResult.Queued));

            Assert.That(harness.ServerTransport.TryReceive(out var packet), Is.True);
            harness.ClientTransport.TrySend(packet.Retain());
            harness.ClientTransport.TrySend(packet);
            harness.Server.Receive();
            harness.Server.BeginTick();
            var accepted = 0;
            foreach (var item in receiver)
            {
                accepted++;
                Assert.That(item.Value.Context.Delivery,
                    Is.EqualTo(NetworkCommandDelivery.Transaction));
                Assert.That(item.Value.Context.TransactionId, Is.EqualTo(id));
            }
            Assert.That(accepted, Is.EqualTo(1));
            Assert.That(harness.Server.CompleteTransaction(1, 11, id), Is.True);
            harness.Server.CompleteTick();

            Assert.That(harness.ClientTransport.TryReceive(out var first), Is.True);
            Assert.That(NetworkPacket.TryDecode(first, out var header, out _), Is.True);
            Assert.That(header.Kind, Is.EqualTo(PacketKind.TransactionReceipt));
            harness.ServerTransport.TrySend(first);
            harness.Client.Process();
            Assert.That(harness.Client.TryDequeueTransactionResult(out var result), Is.True);
            Assert.That(result.TransactionId, Is.EqualTo(id));
            Assert.That(result.Status, Is.EqualTo(NetworkTransactionStatus.Applied));
            Assert.That(result.ApplicationTick, Is.EqualTo(1));
            World<TransactionWorld>.DeleteEventReceiver(ref receiver);
        }

        [Test]
        public void TransactionReportsPolicyGameplayAndUnhandledOutcomes()
        {
            using var harness = new Harness(12);
            AssertStatus(harness, -1, NetworkTransactionStatus.PolicyRejected,
                complete: null);
            AssertStatus(harness, 1, NetworkTransactionStatus.Unhandled,
                complete: null);
            AssertStatus(harness, 2, NetworkTransactionStatus.GameplayRejected,
                complete: NetworkTransactionStatus.GameplayRejected);
        }

        [Test]
        public void TransactionCommandsNeverEnterInputBatch()
        {
            using var harness = new Harness(15);
            var command = new TransactionCommand { Value = 1 };
            Assert.That(harness.Client.QueueCommand(in command, 1, out var sequence),
                Is.EqualTo(NetworkCommandResult.SchemaMismatch));
            Assert.That(sequence, Is.Zero);
            Assert.That(harness.ServerTransport.TryReceive(out _), Is.False);
        }

        [Test]
        public void ServerBoundsPendingTransactionsAndRejectsOverflow()
        {
            using var harness = new Harness(16);
            for (var i = 0; i < 256; i++)
                Assert.That(harness.Client.SubmitTransaction(
                        new TransactionCommand { Value = i }, out _),
                    Is.EqualTo(NetworkCommandResult.Queued));
            harness.Server.Receive();
            Assert.That(harness.Server.PendingTransactionCount, Is.EqualTo(256));

            using var pool = new NetworkBufferPool(1L << 20);
            var payloadBytes = new byte[NetworkTransactionWire.CommandHeaderSize + 4];
            Write64(payloadBytes, 0, 257);
            Write32(payloadBytes, 8, 1);
            payloadBytes[12] = 0;
            Write32(payloadBytes, 16, 1);
            using var payload = pool.Copy(payloadBytes);
            var header = new PacketHeader
            {
                Kind = PacketKind.TransactionCommand,
                Flags = PacketFlags.ReliableOrdered,
                SessionEpoch = harness.Epoch,
                PacketSequence = 258,
                SchemaFingerprint = harness.SchemaFingerprint,
            };
            Assert.That(NetworkPacket.TryEncode(pool, header, payload.Span,
                    out var packet), Is.True);
            Assert.That(harness.ClientTransport.TrySend(packet), Is.True);
            harness.Server.Receive();
            Assert.That(harness.Server.PendingTransactionCount, Is.EqualTo(256));
            harness.Server.BeginTick();
            harness.Server.CompleteTick();

            var foundOverflowReceipt = false;
            while (harness.ClientTransport.TryReceive(out var incoming))
            {
                try
                {
                    if (!NetworkPacket.TryDecode(incoming, out var incomingHeader,
                            out var incomingPayload) ||
                        incomingHeader.Kind != PacketKind.TransactionReceipt ||
                        !NetworkTransactionWire.TryReadReceipt(incomingPayload.Span,
                            out var id, out var status, out _))
                        continue;
                    if (id.Value == 257)
                    {
                        foundOverflowReceipt = true;
                        Assert.That(status,
                            Is.EqualTo(NetworkTransactionStatus.PolicyRejected));
                    }
                }
                finally
                {
                    incoming.Dispose();
                }
            }
            Assert.That(foundOverflowReceipt, Is.True);
        }

        [Test]
        public void StalledReliableReceiptsStayBoundedAndDrainBeforeSnapshots()
        {
            using var harness = new Harness(17, stallServerSends: true);
            harness.ServerGate.BlockSends = true;
            for (var i = 0; i < 256; i++)
                Assert.That(harness.Client.SubmitTransaction(
                        new TransactionCommand { Value = i }, out _),
                    Is.EqualTo(NetworkCommandResult.Queued));

            harness.Server.Receive();
            Assert.That(harness.Client.PendingTransactionCount, Is.EqualTo(256));
            Assert.That(harness.Server.PendingTransactionCount, Is.EqualTo(256));
            for (var i = 0; i < 8; i++)
            {
                harness.Server.BeginTick();
                harness.Server.CompleteTick();
            }

            // A blocked reliable transport must retain, rather than duplicate,
            // the 256 terminal receipts and must not produce snapshots behind them.
            Assert.That(harness.Server.PendingTransactionCount, Is.EqualTo(256));

            harness.ServerGate.BlockSends = false;
            harness.Server.BeginTick();
            harness.Server.CompleteTick();
            harness.Client.Process();

            Assert.That(harness.Server.PendingTransactionCount, Is.Zero);
            Assert.That(harness.Client.PendingTransactionCount, Is.Zero);
            Assert.That(harness.Client.PendingTransactionResultCount,
                Is.EqualTo(256));
        }

        private static void Write32(byte[] destination, int offset, uint value)
        {
            destination[offset] = (byte)value;
            destination[offset + 1] = (byte)(value >> 8);
            destination[offset + 2] = (byte)(value >> 16);
            destination[offset + 3] = (byte)(value >> 24);
        }

        private static void Write64(byte[] destination, int offset, ulong value)
        {
            Write32(destination, offset, (uint)value);
            Write32(destination, offset + 4, (uint)(value >> 32));
        }

        [Test]
        public void TransactionBoundsAndDisconnectAreTerminal()
        {
            using var harness = new Harness(13);
            NetworkTransactionId first = default;
            for (var i = 0; i < 256; i++)
            {
                Assert.That(harness.Client.SubmitTransaction(
                        new TransactionCommand { Value = i }, out var id),
                    Is.EqualTo(NetworkCommandResult.Queued));
                if (i == 0) first = id;
            }
            Assert.That(harness.Client.SubmitTransaction(
                    new TransactionCommand { Value = 257 }, out _),
                Is.EqualTo(NetworkCommandResult.LimitExceeded));
            harness.Client.Disconnect();
            // The ECS owner may release/dispose the endpoint before its next
            // projection pass. Terminal values must remain dequeueable then.
            harness.Client.Dispose();
            Assert.That(harness.Client.TryDequeueTransactionResult(out var lost),
                Is.True);
            Assert.That(lost.TransactionId, Is.EqualTo(first));
            Assert.That(lost.Status, Is.EqualTo(NetworkTransactionStatus.SessionLost));

            using var oversized = new Harness(14);
            Assert.That(oversized.Client.SubmitTransaction(
                    new OversizedTransactionCommand { Count =
                        ProtocolLimits.MaxCommandBytes + 1 }, out _),
                Is.EqualTo(NetworkCommandResult.LimitExceeded));
            Assert.That(oversized.ServerTransport.TryReceive(out _), Is.False);
        }

        private static void AssertStatus(Harness harness, int value,
            NetworkTransactionStatus expected,
            NetworkTransactionStatus? complete)
        {
            Assert.That(harness.Client.SubmitTransaction(
                    new TransactionCommand { Value = value }, out var id),
                Is.EqualTo(NetworkCommandResult.Queued));
            harness.Server.Receive();
            harness.Server.BeginTick();
            if (complete.HasValue)
                Assert.That(harness.Server.CompleteTransaction(1,
                    harness.Epoch, id, complete.Value), Is.True);
            harness.Server.CompleteTick();
            harness.Client.Process();
            Assert.That(harness.Client.TryDequeueTransactionResult(out var result),
                Is.True);
            Assert.That(result.Status, Is.EqualTo(expected));
        }

        private sealed class Harness : System.IDisposable
        {
            internal Harness(uint epoch, bool stallServerSends = false)
            {
                Epoch = epoch;
                var clientFactory = NetworkCompilerSupport
                    .Create<TransactionWorld>();
                clientFactory.Command<TransactionCommand>(new NetworkTypeId(1));
                clientFactory.Command<OversizedTransactionCommand>(
                    new NetworkTypeId(2));
                var serverFactory = NetworkCompilerSupport
                    .Create<TransactionWorld>();
                serverFactory.Command<TransactionCommand, TransactionPolicy>(
                    new NetworkTypeId(1));
                serverFactory.Command<OversizedTransactionCommand,
                    OversizedTransactionPolicy>(new NetworkTypeId(2));
                MemoryNetworkTransport.CreatePair(new ConnectionId(epoch),
                    out var clientTransport, out var serverTransport);
                ClientTransport = clientTransport;
                ServerTransport = serverTransport;
                ServerGate = stallServerSends
                    ? new ToggleSendTransport(serverTransport)
                    : null;
                var serverSchema = serverFactory.Freeze();
                SchemaFingerprint = serverSchema.Fingerprint;
                Server = new NetworkServer<TransactionWorld>(
                    serverSchema, (scope, entity) => true);
                Server.AddConnection(ServerGate ??
                    (INetworkTransport)ServerTransport, 1, epoch, default);
                Client = new NetworkClient<TransactionWorld>(ClientTransport,
                    clientFactory.Freeze());
                Assert.That(Client.BeginHandshake(), Is.True);
                Server.Receive();
                Client.Process();
                Assert.That(Client.Session.State,
                    Is.EqualTo(NetworkSessionState.Established));
            }

            internal uint Epoch { get; }
            internal SchemaFingerprint SchemaFingerprint { get; }
            internal MemoryNetworkTransport ClientTransport { get; }
            internal MemoryNetworkTransport ServerTransport { get; }
            internal ToggleSendTransport ServerGate { get; }
            internal NetworkClient<TransactionWorld> Client { get; }
            internal NetworkServer<TransactionWorld> Server { get; }

            public void Dispose()
            {
                Client.Dispose();
                Server.Dispose();
                ClientTransport.Dispose();
                if (ServerGate == null)
                    ServerTransport.Dispose();
                else
                    ServerGate.Dispose();
            }
        }

        private sealed class ToggleSendTransport : INetworkTransport
        {
            private readonly INetworkTransport _inner;

            internal ToggleSendTransport(INetworkTransport inner)
            {
                _inner = inner;
            }

            internal bool BlockSends { get; set; }

            public ConnectionId Connection => _inner.Connection;
            public int MaxReliablePayloadBytes =>
                _inner.MaxReliablePayloadBytes;
            public int MaxUnreliablePayloadBytes =>
                _inner.MaxUnreliablePayloadBytes;

            public bool TrySend(NetworkBufferLease packet)
            {
                if (BlockSends)
                {
                    packet?.Dispose();
                    return false;
                }
                return _inner.TrySend(packet);
            }

            public bool TryReceive(out NetworkBufferLease packet) =>
                _inner.TryReceive(out packet);

            public void Dispose() => _inner.Dispose();
        }

        public struct TransactionWorld : IWorldType { }

        public struct TransactionCommand : IEvent, INetworkTransactionCommand
        {
            public int Value;
            public void Write(ref BinaryPackWriter writer) => writer.WriteInt(Value);
            public void Read(ref BinaryPackReader reader, byte version) =>
                Value = reader.ReadInt();
        }

        public struct OversizedTransactionCommand : IEvent,
            INetworkTransactionCommand
        {
            public int Count;
            public void Write(ref BinaryPackWriter writer)
            {
                for (var i = 0; i < Count; i++) writer.WriteByte(1);
            }
            public void Read(ref BinaryPackReader reader, byte version) { }
        }

        public struct TransactionPolicy :
            INetworkCommandPolicy<TransactionWorld, TransactionCommand>
        {
            public bool Authorize(in NetworkCommandContext context,
                in TransactionCommand command) => command.Value >= 0;
        }

        public struct OversizedTransactionPolicy :
            INetworkCommandPolicy<TransactionWorld, OversizedTransactionCommand>
        {
            public bool Authorize(in NetworkCommandContext context,
                in OversizedTransactionCommand command) => true;
        }
    }
}
