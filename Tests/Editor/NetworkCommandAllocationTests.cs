using System;
using System.Globalization;
using FFS.Libraries.StaticEcs;
using FFS.Libraries.StaticPack;
using NUnit.Framework;

namespace UniGame.StaticEcs.Network.Tests
{
    /// <summary>
    /// Editor-only, deterministic managed-allocation diagnostics for the hot server command path.
    /// <para>
    /// Every measurement warms the relevant JIT, closed generic, ArrayPool, buffer-pool and
    /// coordinator high-water state first, then snapshots
    /// <see cref="GC.GetAllocatedBytesForCurrentThread"/> immediately around one tight loop and
    /// prints one compact line after the window. Assertions only validate harness invariants,
    /// decoded values, exact consumption, lease ownership cleanup and the empty-loop floor. They
    /// never assert an unknown current allocation slope as pass/fail.
    /// </para>
    /// <para>
    /// The end-to-end probe drives one realistic <see cref="NetworkServer{TWorld}"/> fixture. The
    /// handshake, every command packet, the schema, the sessions, the server command policy and all
    /// payload storage are constructed before the windows. Receive and BeginTick dispatch are then
    /// measured in separate windows so that command decode/queue allocations and session
    /// dispatch/policy-event-wrap/tick-dispatch allocations are reported independently, with the
    /// decoded command count and dispatch order verified after the windows.
    /// </para>
    /// </summary>
    [TestFixture]
    internal sealed class NetworkCommandAllocationTests
    {
        private const int ControlOperations = 4096;
        private const int ReadOperations = 4096;
        private const int ScratchOperations = 4096;
        private const int LeaseOperations = 4096;
        private const int SendEventOperations = 400;
        private const int CoordinatorOperations = 16;
        private const int CoordinatorWarmup = 16;
        private const int ServerOperations = 512;
        private const uint AllocationTypeId = 150;

        [Test]
        public void EmptyMeasuredLoopAllocatesNothing()
        {
            var sink = 0;
            var start = GC.GetAllocatedBytesForCurrentThread();
            for (var i = 0; i < ControlOperations; i++)
                sink += i;
            var bytes = GC.GetAllocatedBytesForCurrentThread() - start;

            Assert.That(sink, Is.GreaterThan(0), "control loop must execute");
            Assert.That(bytes, Is.Zero, "empty measured loop must not allocate");
            Report("control.empty_loop", ControlOperations, bytes);
        }

        [Test]
        public void CommandInvokerReadAllocationDistinguishesArrayPoolFromSerializer()
        {
            var buffer = new byte[64];
            const int offset = 7;
            const int length = sizeof(int);
            const int expected = unchecked((int)0x5A17C0DE);
            Assert.That(offset, Is.GreaterThan(0), "probe must exercise a non-zero input offset");
            Assert.That(offset + length, Is.LessThanOrEqualTo(buffer.Length));
            var writer = BinaryPackWriter.Create(buffer, (uint)offset);
            writer.WriteInt(expected);

            // Warm JIT, the closed generic instantiation, ArrayPool and BinaryPackReader.
            for (var i = 0; i < 32; i++)
                Assert.That(ReadInvokerProbe.ReadExposed(buffer, offset, length, 0).Value,
                    Is.EqualTo(expected));

            // Exact-consumption validation must reject a truncated payload.
            Assert.That(() => ReadInvokerProbe.ReadExposed(buffer, offset, length - 1, 0),
                Throws.Exception);

            long checksum = 0;
            var readStart = GC.GetAllocatedBytesForCurrentThread();
            for (var i = 0; i < ReadOperations; i++)
                checksum += ReadInvokerProbe.ReadExposed(buffer, offset, length, 0).Value;
            var readBytes = GC.GetAllocatedBytesForCurrentThread() - readStart;
            Assert.That(checksum, Is.EqualTo((long)expected * ReadOperations),
                "every protected Read must decode the exact payload at the non-zero offset");

            var scratch = new byte[64];
            checksum = 0;
            var scratchStart = GC.GetAllocatedBytesForCurrentThread();
            for (var i = 0; i < ScratchOperations; i++)
                checksum += ReadWithPreallocatedScratch(scratch, buffer, offset, length, 0).Value;
            var scratchBytes = GC.GetAllocatedBytesForCurrentThread() - scratchStart;
            Assert.That(checksum, Is.EqualTo((long)expected * ScratchOperations),
                "controlled scratch path must decode the same payload");
            Assert.That(readBytes, Is.GreaterThanOrEqualTo(0));
            Assert.That(scratchBytes, Is.GreaterThanOrEqualTo(0));

            Report("command.invoker_read_arraypool", ReadOperations, readBytes);
            Report("command.invoker_read_scratch", ScratchOperations, scratchBytes);
        }

        [Test]
        public void NetworkBufferLeaseRetainSliceAllocationIsMeasured()
        {
            using var pool = new NetworkBufferPool(1 << 16);
            var root = pool.Copy(new byte[64]);
            try
            {
                for (var i = 0; i < 32; i++)
                {
                    var warm = root.RetainSlice(8, 16);
                    Assert.That(warm.Length, Is.EqualTo(16));
                    warm.Dispose();
                }
                Assert.That(pool.CaptureDiagnostics().OutstandingLeases, Is.EqualTo(1));

                var start = GC.GetAllocatedBytesForCurrentThread();
                for (var i = 0; i < LeaseOperations; i++)
                {
                    var slice = root.RetainSlice(8, 16);
                    slice.Dispose();
                }
                var bytes = GC.GetAllocatedBytesForCurrentThread() - start;

                Assert.That(root.Length, Is.EqualTo(64));
                Assert.That(pool.CaptureDiagnostics().OutstandingLeases, Is.EqualTo(1));
                Assert.That(bytes, Is.GreaterThanOrEqualTo(0));
                Report("buffer.retain_slice", LeaseOperations, bytes);
            }
            finally
            {
                root.Dispose();
            }
            Assert.That(pool.CaptureDiagnostics().OutstandingLeases, Is.Zero);
        }

        [Test]
        public void StaticEcsSendEventAllocationIsMeasuredInProvisionedPage()
        {
            var worldCreated = false;
            var receiverRegistered = false;
            var receiver = default(EventReceiver<EventAllocationWorld, AllocationPing>);
            try
            {
                World<EventAllocationWorld>.Create(WorldConfig.Default());
                worldCreated = true;
                World<EventAllocationWorld>.Types().Event<AllocationPing>();
                World<EventAllocationWorld>.Initialize();
                receiver = World<EventAllocationWorld>
                    .RegisterEventReceiver<AllocationPing>();
                receiverRegistered = true;

                // Provision page 0 and drain it. The page stays allocated because index 0 is not
                // the 512th slot, so the measured sends below reuse the provisioned page.
                World<EventAllocationWorld>.SendEvent(new AllocationPing { Value = -1 });
                Assert.That(DrainPings(receiver), Is.EqualTo(1));

                var start = GC.GetAllocatedBytesForCurrentThread();
                for (var i = 0; i < SendEventOperations; i++)
                    World<EventAllocationWorld>.SendEvent(new AllocationPing { Value = i + 1 });
                var bytes = GC.GetAllocatedBytesForCurrentThread() - start;

                Assert.That(DrainPings(receiver), Is.EqualTo(SendEventOperations),
                    "all sends must stay inside one provisioned page");
                Assert.That(bytes, Is.GreaterThanOrEqualTo(0));
                Report("static_ecs.send_event", SendEventOperations, bytes);
            }
            finally
            {
                if (receiverRegistered)
                    World<EventAllocationWorld>.DeleteEventReceiver(ref receiver);
                if (worldCreated)
                    World<EventAllocationWorld>.Destroy();
            }
        }

        [Test]
        public void CoordinatorQueueHeapAllocationIsMeasuredAtHighWater()
        {
            var worldCreated = false;
            NetworkServerCoordinator<CoordinatorAllocationWorld> coordinator = null;
            var warmEnvelopes = new NetworkCommandEnvelope[CoordinatorWarmup];
            var warmTransferred = new bool[CoordinatorWarmup];
            var envelopes = new NetworkCommandEnvelope[CoordinatorOperations];
            var transferred = new bool[CoordinatorOperations];
            using var pool = new NetworkBufferPool(1 << 20);
            try
            {
                World<CoordinatorAllocationWorld>.Create(WorldConfig.Default());
                worldCreated = true;
                World<CoordinatorAllocationWorld>.Types()
                    .Event<NetworkCommandAcceptedEvent<AllocationCommand>>()
                    .Event<NetworkCommandRejectedEvent<AllocationCommand>>();
                World<CoordinatorAllocationWorld>.Initialize();

                var clientSchema = CoordinatorSchema(false);
                var serverSchema = CoordinatorSchema(true);
                var connection = new ConnectionId(1);
                var client = new NetworkSession<CoordinatorAllocationWorld>(connection,
                    NetworkRole.Client, clientSchema, pool);
                var server = new NetworkSession<CoordinatorAllocationWorld>(connection,
                    NetworkRole.Server, serverSchema, pool);
                Assert.That(client.Admit(serverSchema.Fingerprint, 1, 1, new ScopeId(1)),
                    Is.EqualTo(NetworkAdmissionResult.Accepted));
                Assert.That(server.Admit(clientSchema.Fingerprint, 1, 1, new ScopeId(1)),
                    Is.EqualTo(NetworkAdmissionResult.Accepted));

                coordinator = new NetworkServerCoordinator<CoordinatorAllocationWorld>();
                coordinator.Add(server);

                // Warm the heap to the measured high-water and populate the internal maps so the
                // measured pushes neither grow the backing array nor insert new dictionary keys.
                for (var i = 0; i < CoordinatorWarmup; i++)
                {
                    var tick = (uint)(i + 1);
                    Assert.That(client.CreateCommand(new AllocationCommand { Value = i }, tick,
                        out warmEnvelopes[i]), Is.EqualTo(NetworkCommandResult.Queued));
                    warmTransferred[i] = coordinator.Queue(warmEnvelopes[i], tick) ==
                        NetworkCommandResult.Queued;
                    Assert.That(warmTransferred[i], Is.True);
                }
                Assert.That(coordinator.Dispatch((uint)CoordinatorWarmup).Total,
                    Is.EqualTo(CoordinatorWarmup));
                Assert.That(coordinator.PendingCommandCount, Is.Zero);

                for (var i = 0; i < CoordinatorOperations; i++)
                    Assert.That(client.CreateCommand(new AllocationCommand { Value = 100 + i }, 1,
                        out envelopes[i]), Is.EqualTo(NetworkCommandResult.Queued));

                var start = GC.GetAllocatedBytesForCurrentThread();
                for (var i = 0; i < CoordinatorOperations; i++)
                    transferred[i] = coordinator.Queue(envelopes[i], 1) ==
                        NetworkCommandResult.Queued;
                var bytes = GC.GetAllocatedBytesForCurrentThread() - start;

                Assert.That(coordinator.PendingCommandCount,
                    Is.EqualTo(CoordinatorOperations));
                Assert.That(coordinator.PendingCommandsHighWater,
                    Is.GreaterThanOrEqualTo(CoordinatorOperations));
                Assert.That(bytes, Is.GreaterThanOrEqualTo(0));
                Report("coordinator.queue_heap", CoordinatorOperations, bytes);
            }
            finally
            {
                try
                {
                    coordinator?.Clear();
                }
                finally
                {
                    for (var i = 0; i < warmTransferred.Length; i++)
                        if (!warmTransferred[i])
                            warmEnvelopes[i].Dispose();
                    for (var i = 0; i < transferred.Length; i++)
                        if (!transferred[i])
                            envelopes[i].Dispose();
                    if (worldCreated)
                        World<CoordinatorAllocationWorld>.Destroy();
                }
            }
            Assert.That(pool.CaptureDiagnostics().OutstandingLeases, Is.Zero);
        }

        [Test]
        public void ServerReceiveAndTickDispatchAllocationIsMeasured()
        {
            var worldCreated = false;
            var receiverRegistered = false;
            var receiver = default(EventReceiver<ServerAllocationWorld,
                NetworkCommandAcceptedEvent<AllocationCommand>>);
            using var pool = new NetworkBufferPool(1 << 20);
            NetworkServer<ServerAllocationWorld> server = null;
            MemoryNetworkTransport clientTransport = null;
            MemoryNetworkTransport serverTransport = null;
            var warmPackets = new NetworkBufferLease[ServerOperations];
            var warmValues = new int[ServerOperations];
            var measuredPackets = new NetworkBufferLease[ServerOperations];
            var measuredValues = new int[ServerOperations];
            try
            {
                World<ServerAllocationWorld>.Create(WorldConfig.Default());
                worldCreated = true;
                World<ServerAllocationWorld>.Types()
                    .Event<AllocationCommand>()
                    .Event<NetworkCommandAcceptedEvent<AllocationCommand>>()
                    .Event<NetworkCommandRejectedEvent<AllocationCommand>>();
                World<ServerAllocationWorld>.Initialize();
                receiver = World<ServerAllocationWorld>
                    .RegisterEventReceiver<NetworkCommandAcceptedEvent<AllocationCommand>>();
                receiverRegistered = true;

                var schema = ServerSchema();
                MemoryNetworkTransport.CreatePair(new ConnectionId(201),
                    out clientTransport, out serverTransport);
                server = new NetworkServer<ServerAllocationWorld>(schema,
                    static (_, _) => false, bufferPool: pool);
                var session = server.AddConnection(serverTransport, 7, 1, new ScopeId(1));

                // Complete the handshake before any measurement window.
                SendPacket(clientTransport, pool, HelloHeader(schema.Fingerprint),
                    ReadOnlySpan<byte>.Empty);
                server.Receive();
                Assert.That(session.State, Is.EqualTo(NetworkSessionState.Established));

                // Encode every packet, payload and policy binding before the windows. Warm and
                // measured streams use disjoint sequences so duplicates cannot mask decode work.
                for (var i = 0; i < ServerOperations; i++)
                {
                    warmValues[i] = -1 - i;
                    measuredValues[i] = i;
                    warmPackets[i] = EncodeCommandPacket(pool, schema.Fingerprint, 1,
                        checked((uint)(i + 1)), checked((uint)(i + 1)), 1, warmValues[i]);
                    measuredPackets[i] = EncodeCommandPacket(pool, schema.Fingerprint, 1,
                        checked((uint)(ServerOperations + i + 1)),
                        checked((uint)(ServerOperations + i + 1)), 1, measuredValues[i]);
                }

                // Warm receive, decode/queue, session dispatch, policy event wrapping, the event
                // page pool, the coordinator heap and its dictionary keys to the measured sizes.
                for (var i = 0; i < ServerOperations; i++)
                {
                    Assert.That(clientTransport.TrySend(warmPackets[i]), Is.True);
                    server.Receive();
                }
                Assert.That(server.CaptureMemoryDiagnostics().PendingCommands,
                    Is.EqualTo(ServerOperations));
                Assert.That(server.BeginTick(), Is.EqualTo(1u));
                server.CompleteTick();
                Assert.That(DrainAccepted(receiver, warmValues), Is.EqualTo(ServerOperations));

                // Phase one: framed receive, decode, validation and ordered queueing only.
                var receiveStart = GC.GetAllocatedBytesForCurrentThread();
                for (var i = 0; i < ServerOperations; i++)
                {
                    clientTransport.TrySend(measuredPackets[i]);
                    server.Receive();
                }
                var receiveBytes = GC.GetAllocatedBytesForCurrentThread() - receiveStart;

                // Phase two: session dispatch, policy/event wrapping and tick dispatch only.
                var dispatchStart = GC.GetAllocatedBytesForCurrentThread();
                var dispatchedTick = server.BeginTick();
                var dispatchBytes = GC.GetAllocatedBytesForCurrentThread() - dispatchStart;
                server.CompleteTick();

                Assert.That(dispatchedTick, Is.EqualTo(2u));
                Assert.That(server.CaptureMemoryDiagnostics().PendingCommands, Is.Zero,
                    "every received command must be dispatched exactly once");
                Assert.That(DrainAccepted(receiver, measuredValues),
                    Is.EqualTo(ServerOperations),
                    "dispatch order must match command sequence order exactly");
                Assert.That(receiveBytes, Is.GreaterThanOrEqualTo(0));
                Assert.That(dispatchBytes, Is.GreaterThanOrEqualTo(0));
                Report("server.receive", ServerOperations, receiveBytes);
                Report("server.tick_dispatch", ServerOperations, dispatchBytes);
            }
            finally
            {
                server?.Dispose();
                clientTransport?.Dispose();
                serverTransport?.Dispose();
                if (receiverRegistered)
                    World<ServerAllocationWorld>.DeleteEventReceiver(ref receiver);
                if (worldCreated)
                    World<ServerAllocationWorld>.Destroy();
            }
            Assert.That(pool.CaptureDiagnostics().OutstandingLeases, Is.Zero);
        }

        private static AllocationCommand ReadWithPreallocatedScratch(byte[] scratch,
            byte[] buffer, int offset, int length, byte version)
        {
            Buffer.BlockCopy(buffer, offset, scratch, 0, length);
            var reader = new BinaryPackReader(scratch, (uint)length, 0);
            var value = default(AllocationCommand);
            value.Read(ref reader, version);
            if (reader.Position != (uint)length)
                throw new InvalidOperationException(
                    "Command hook did not consume the exact payload.");
            return value;
        }

        private static int DrainPings(
            EventReceiver<EventAllocationWorld, AllocationPing> receiver)
        {
            var count = 0;
            foreach (World<EventAllocationWorld>.Event<AllocationPing> item in receiver)
                count++;
            return count;
        }

        private static int DrainAccepted(
            EventReceiver<ServerAllocationWorld,
                NetworkCommandAcceptedEvent<AllocationCommand>> receiver,
            int[] expected)
        {
            var count = 0;
            foreach (World<ServerAllocationWorld>
                         .Event<NetworkCommandAcceptedEvent<AllocationCommand>> item in receiver)
            {
                if (count < expected.Length)
                    Assert.That(item.Value.Command.Value, Is.EqualTo(expected[count]),
                        "accepted command values must dispatch in exact sequence order");
                count++;
            }
            return count;
        }

        private static NetworkSchema<CoordinatorAllocationWorld> CoordinatorSchema(bool server)
        {
            var factory = NetworkCompilerSupport.Create<CoordinatorAllocationWorld>();
            if (server)
                factory.Command<AllocationCommand, RejectAllocationPolicy>(new NetworkTypeId(77));
            else
                factory.Command<AllocationCommand>(new NetworkTypeId(77));
            return factory.Freeze();
        }

        private static NetworkSchema<ServerAllocationWorld> ServerSchema()
        {
            var factory = NetworkCompilerSupport.Create<ServerAllocationWorld>();
            factory.Command<AllocationCommand, AllowServerAllocationPolicy>(
                new NetworkTypeId(AllocationTypeId));
            return factory.Freeze();
        }

        private static PacketHeader HelloHeader(SchemaFingerprint fingerprint) => new PacketHeader
        {
            Kind = PacketKind.Hello,
            Flags = PacketFlags.ReliableOrdered,
            Compression = NetworkCompression.None,
            SessionEpoch = 0,
            PacketSequence = 1,
            SchemaFingerprint = fingerprint,
        };

        private static NetworkBufferLease EncodeCommandPacket(NetworkBufferPool pool,
            SchemaFingerprint fingerprint, uint epoch, uint packetSequence,
            uint commandSequence, uint targetTick, int value)
        {
            var payload = new byte[1 + 17 + sizeof(int)];
            payload[0] = 1;
            Write32(payload, 1, commandSequence);
            Write32(payload, 5, targetTick);
            Write32(payload, 9, AllocationTypeId);
            payload[13] = 0;
            Write32(payload, 14, sizeof(int));
            Write32(payload, 18, unchecked((uint)value));
            var header = new PacketHeader
            {
                Kind = PacketKind.CommandBatch,
                Flags = PacketFlags.UnreliableSequenced,
                Compression = NetworkCompression.None,
                SessionEpoch = epoch,
                PacketSequence = packetSequence,
                ServerTick = targetTick,
                SchemaFingerprint = fingerprint,
            };
            Assert.That(NetworkPacket.TryEncode(pool, header, payload, out var packet), Is.True);
            return packet;
        }

        private static void SendPacket(INetworkTransport transport,
            NetworkBufferPool pool, in PacketHeader header, ReadOnlySpan<byte> payload)
        {
            Assert.That(NetworkPacket.TryEncode(pool, header, payload, out var packet), Is.True);
            Assert.That(transport.TrySend(packet), Is.True);
        }

        private static void Write32(Span<byte> destination, int offset, uint value)
        {
            destination[offset] = (byte)value;
            destination[offset + 1] = (byte)(value >> 8);
            destination[offset + 2] = (byte)(value >> 16);
            destination[offset + 3] = (byte)(value >> 24);
        }

        private static void Report(string name, int iterations, long bytes)
        {
            var perOperation = iterations == 0 ? 0d : (double)bytes / iterations;
            TestContext.Progress.WriteLine(
                "alloc " + name +
                " iterations=" + iterations.ToString(CultureInfo.InvariantCulture) +
                " bytes=" + bytes.ToString(CultureInfo.InvariantCulture) +
                " bytesPerOp=" + perOperation.ToString("F4", CultureInfo.InvariantCulture));
        }

        internal sealed class ReadInvokerProbe :
            CommandNetworkInvoker<AllocationWorld, AllocationCommand>
        {
            public static AllocationCommand ReadExposed(byte[] buffer, int offset, int length,
                byte version) => Read(buffer, offset, length, version);
        }

        internal struct AllocationWorld : IWorldType { }

        private struct EventAllocationWorld : IWorldType { }

        private struct CoordinatorAllocationWorld : IWorldType { }

        private struct ServerAllocationWorld : IWorldType { }

        internal struct AllocationCommand : IEvent, INetworkCommand
        {
            public int Value;

            public void Write(ref BinaryPackWriter writer) => writer.WriteInt(Value);

            public void Read(ref BinaryPackReader reader, byte version) =>
                Value = reader.ReadInt();
        }

        private struct AllocationPing : IEvent
        {
            public int Value;
        }

        private struct RejectAllocationPolicy :
            INetworkCommandPolicy<CoordinatorAllocationWorld, AllocationCommand>
        {
            public bool Authorize(in NetworkCommandContext context,
                in AllocationCommand command) => false;
        }

        private struct AllowServerAllocationPolicy :
            INetworkCommandPolicy<ServerAllocationWorld, AllocationCommand>
        {
            public bool Authorize(in NetworkCommandContext context,
                in AllocationCommand command) => true;
        }
    }
}
