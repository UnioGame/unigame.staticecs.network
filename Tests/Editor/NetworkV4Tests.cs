using System;
using System.Collections.Generic;
using System.IO;
using FFS.Libraries.StaticEcs;
using FFS.Libraries.StaticPack;
using NUnit.Framework;

namespace UniGame.StaticEcs.Network.Tests
{
    public sealed class NetworkV4Tests
    {
        private static readonly NetworkBufferPool Buffers = new NetworkBufferPool(64L << 20);

        [Test]
        public void TypeIdsAndPacketHeaderAreCanonicalV4AndRejectV3()
        {
            var id = NetworkCompilerSupport.TypeId("SourceGenerator.Tests", "Demo.Position");
            Assert.That(id.Value, Is.EqualTo(4089044646u));
            Assert.That(NetworkCompilerSupport.TypeId("game.shared", "Demo.Position").Value, Is.EqualTo(1933934308u));
            var header = new PacketHeader
            {
                Kind = PacketKind.FullSnapshot,
                Flags = PacketFlags.ReliableOrdered,
                Compression = NetworkCompression.None,
                ServerTick = 42,
                PayloadLength = 3,
                SchemaFingerprint = new SchemaFingerprint(1, 2),
                SimulationFingerprint = 3,
                ContentFingerprint = 4,
                PayloadHash = 7
            };
            var bytes = new byte[PacketHeader.Size];
            Assert.That(header.TryWrite(bytes), Is.True);
            Assert.That(PacketHeader.TryRead(bytes, out var decoded), Is.True);
            Assert.That(PacketHeader.Version, Is.EqualTo(4));
            Assert.That(decoded.SchemaFingerprint, Is.EqualTo(header.SchemaFingerprint));
            Assert.That(decoded.SimulationFingerprint,
                Is.EqualTo(header.SimulationFingerprint));
            Assert.That(decoded.ContentFingerprint, Is.EqualTo(header.ContentFingerprint));
            bytes[4] = 3;
            bytes[5] = 0;
            Assert.That(PacketHeader.TryRead(bytes, out _), Is.False);
            Assert.That(header.TryWrite(bytes), Is.True);
            bytes[10] = 1;
            Assert.That(PacketHeader.TryRead(bytes, out _), Is.False);
            header.Kind = PacketKind.Hello;
            Assert.That(NetworkPacket.TryEncode(Buffers, header, new byte[] { 1, 2, 3 }, out var packet), Is.True);
            var corruptBytes = packet.Memory.ToArray();
            packet.Dispose();
            corruptBytes[corruptBytes.Length - 1] ^= 1;
            packet = Buffers.Copy(corruptBytes);
            Assert.That(NetworkPacket.TryDecode(packet, header.SchemaFingerprint, out _, out _), Is.False);
            packet.Dispose();
        }

        [Test]
        public void SimulationConfigProducesStableDistinctFingerprints()
        {
            var config = NetworkSimulationPresets.Create(
                NetworkSimulationPreset.Immediate);
            var first = new NetworkSimulationConfigResource(in config);
            var second = new NetworkSimulationConfigResource(20, 64, 2, 3, 2f);
            var changed = new NetworkSimulationConfigResource(30, 64, 2, 3, 2f);

            Assert.That(first.Config.TicksPerSecond, Is.EqualTo(20));
            Assert.That(first.Fingerprint, Is.EqualTo(second.Fingerprint));
            Assert.That(changed.Fingerprint, Is.Not.EqualTo(first.Fingerprint));
        }

        [Test]
        public void SchemaFingerprintIsOrderIndependentAndRejectsCollisions()
        {
            var first = NetworkCompilerSupport.Create<TestWorld>();
            first.Tag<TestTag>(new NetworkTypeId(2));
            first.Component<TestComponent>(new NetworkTypeId(1));
            var second = NetworkCompilerSupport.Create<TestWorld>();
            second.Component<TestComponent>(new NetworkTypeId(1));
            second.Tag<TestTag>(new NetworkTypeId(2));
            Assert.That(first.Freeze().Fingerprint, Is.EqualTo(second.Freeze().Fingerprint));
            var duplicate = NetworkCompilerSupport.Create<TestWorld>();
            duplicate.Tag<TestTag>(new NetworkTypeId(1));
            Assert.Throws<InvalidOperationException>(() => duplicate.Component<TestComponent>(new NetworkTypeId(1)));
        }

        [Test]
        public void TwoClientsKeepQueuesAndPacketsIsolated()
        {
            using var mock = new TwoClientNetworkMock();
            Assert.That(mock.ClientA.TrySend(Lease(new byte[] { 1 })), Is.True);
            Assert.That(mock.ClientB.TrySend(Lease(new byte[] { 2 })), Is.True);
            Assert.That(mock.ServerA.TryReceive(out var first), Is.True);
            Assert.That(mock.ServerB.TryReceive(out var second), Is.True);
            CollectionAssert.AreEqual(new byte[] { 1 }, first.Memory.ToArray());
            CollectionAssert.AreEqual(new byte[] { 2 }, second.Memory.ToArray());
            first.Dispose();
            second.Dispose();
        }

        [Test]
        public void RedundantCommandsAreDeduplicatedAndAcknowledgedOnAdverseLink()
        {
            World<InputWorld>.Create(WorldConfig.Default());
            var types = World<InputWorld>.Types();
            types.Event<TestInput>();
            types.Event<NetworkCommandAcceptedEvent<TestInput>>();
            types.Event<NetworkCommandRejectedEvent<TestInput>>();
            World<InputWorld>.Initialize();
            var receiver = World<InputWorld>
                .RegisterEventReceiver<NetworkCommandAcceptedEvent<TestInput>>();
            try
            {
                var clientFactory = NetworkCompilerSupport.Create<InputWorld>();
                clientFactory.Command<TestInput>(new NetworkTypeId(91));
                var serverFactory = NetworkCompilerSupport.Create<InputWorld>();
                serverFactory.Command<TestInput, AllowInputPolicy>(
                    new NetworkTypeId(91));
                var immediate = NetworkSimulationPresets.Create(
                    NetworkSimulationPreset.Immediate);
                using var simulator = new NetworkSimulator(new ConnectionId(90),
                    in immediate);
                var server = new NetworkServer<InputWorld>(serverFactory.Freeze(),
                    static (_, _) => false);
                server.AddConnection(simulator.Server, 7, 11, new ScopeId(1));
                var client = new NetworkClient<InputWorld>(simulator.Client,
                    clientFactory.Freeze(), new ScopeId(1), commandRedundancy: 3);

                Assert.That(client.BeginHandshake(), Is.True);
                simulator.Advance(0);
                server.Receive();
                server.Tick(_ => { });
                simulator.Advance(0);
                client.Process();
                Assert.That(client.Session.State,
                    Is.EqualTo(NetworkSessionState.Established));

                var adverse = NetworkSimulationPresets.Create(
                    NetworkSimulationPreset.Unstable);
                adverse.Seed = 771;
                adverse.LossProbability = 0.1f;
                adverse.DuplicateProbability = 0.1f;
                adverse.ReorderProbability = 0.1f;
                simulator.ApplyConfig(in adverse);

                var sequences = new HashSet<uint>();
                uint latestTick = 0;
                uint latestSequence = 0;
                for (var index = 0; index < 106; index++)
                {
                    var input = new TestInput { Value = index < 100 ? index + 1 : 0 };
                    Assert.That(client.SendCommand(in input, server.ServerTick + 4),
                        Is.EqualTo(NetworkCommandResult.Queued));
                    simulator.Advance(50);
                    server.Receive();
                    server.Tick(_ => { });
                    CollectAcceptedInputs(receiver, sequences, ref latestTick,
                        ref latestSequence);
                    World<InputWorld>.Tick();
                    simulator.Advance(50);
                    client.Process();
                }
                for (var index = 0; index < 8; index++)
                {
                    simulator.Advance(50);
                    server.Receive();
                    server.Tick(_ => { });
                    CollectAcceptedInputs(receiver, sequences, ref latestTick,
                        ref latestSequence);
                    World<InputWorld>.Tick();
                    simulator.Advance(50);
                    client.Process();
                }

                Assert.That(sequences.Count, Is.GreaterThanOrEqualTo(95));
                Assert.That(client.ServerProcessedCommandTick, Is.EqualTo(latestTick));
                Assert.That(client.ServerProcessedCommandSequence,
                    Is.EqualTo(latestSequence));
                var stats = simulator.CaptureStats();
                Assert.That(stats.ClientToServer.LostPackets, Is.GreaterThan(0));
                Assert.That(stats.ClientToServer.DuplicatePackets, Is.GreaterThan(0));
                Assert.That(stats.ClientToServer.ReorderedPackets, Is.GreaterThan(0));
            }
            finally
            {
                World<InputWorld>.DeleteEventReceiver(ref receiver);
                World<InputWorld>.Destroy();
            }
        }

        private static void CollectAcceptedInputs(
            EventReceiver<InputWorld, NetworkCommandAcceptedEvent<TestInput>> receiver,
            HashSet<uint> sequences,
            ref uint latestTick,
            ref uint latestSequence)
        {
            foreach (World<InputWorld>.Event<NetworkCommandAcceptedEvent<TestInput>> item
                     in receiver)
            {
                Assert.That(sequences.Add(item.Value.Context.Sequence), Is.True,
                    "Redundant or duplicated input was applied twice.");
                latestTick = Math.Max(latestTick, item.Value.Context.TargetTick);
                latestSequence = Math.Max(latestSequence,
                    item.Value.Context.Sequence);
            }
        }

        [Test]
        public void CommandQueueRejectsOverflowBeforeSendingPartialBatch()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            var receiver = World<AuthorityWorld>
                .RegisterEventReceiver<NetworkCommandAcceptedEvent<TestCommand>>();
            try
            {
                MemoryNetworkTransport.CreatePair(new ConnectionId(92),
                    out var clientTransport, out var serverTransport);
                using (clientTransport)
                using (serverTransport)
                {
                    var server = new NetworkServer<AuthorityWorld>(
                        Schema<AuthorityWorld>(true), static (_, _) => false);
                    server.AddConnection(serverTransport, 7, 12, new ScopeId(1));
                    var client = new NetworkClient<ClientAWorld>(clientTransport,
                        Schema<ClientAWorld>(false), new ScopeId(1));
                    Assert.That(client.BeginHandshake(), Is.True);
                    server.Receive();
                    server.Tick(_ => { });
                    client.Process();
                    server.Receive();

                    for (var i = 0; i < ProtocolLimits.MaxCommandsPerBatch; i++)
                    {
                        var command = new TestCommand { Value = i };
                        Assert.That(client.QueueCommand(in command, 2, out _),
                            Is.EqualTo(NetworkCommandResult.Queued));
                    }
                    var overflow = new TestCommand { Value = -1 };
                    Assert.That(client.QueueCommand(in overflow, 2, out _),
                        Is.EqualTo(NetworkCommandResult.LimitExceeded));
                    Assert.That(serverTransport.TryReceive(out _), Is.False,
                        "Queueing must not send a partial command batch.");

                    Assert.That(client.FlushCommands(2),
                        Is.EqualTo(NetworkCommandResult.Queued));
                    server.Receive();
                    server.Tick(_ => { });
                    var count = 0;
                    foreach (World<AuthorityWorld>
                                 .Event<NetworkCommandAcceptedEvent<TestCommand>> _ in receiver)
                        count++;
                    Assert.That(count, Is.EqualTo(ProtocolLimits.MaxCommandsPerBatch));
                }
            }
            finally
            {
                World<AuthorityWorld>.DeleteEventReceiver(ref receiver);
                World<AuthorityWorld>.Destroy();
                World<ClientAWorld>.Destroy();
            }
        }

        [Test]
        public void CommandBatchRepeatsCurrentAndThreePreviousTicks()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            try
            {
                MemoryNetworkTransport.CreatePair(new ConnectionId(94),
                    out var clientTransport, out var serverTransport);
                using (clientTransport)
                using (serverTransport)
                {
                    var server = new NetworkServer<AuthorityWorld>(
                        Schema<AuthorityWorld>(true), static (_, _) => false);
                    server.AddConnection(serverTransport, 7, 14, new ScopeId(1));
                    var clientSchema = Schema<ClientAWorld>(false);
                    var client = new NetworkClient<ClientAWorld>(clientTransport,
                        clientSchema, new ScopeId(1), commandRedundancy: 3);
                    Assert.That(client.BeginHandshake(), Is.True);
                    server.Receive();
                    server.Tick(_ => { });
                    client.Process();
                    server.Receive();

                    NetworkBufferLease latestPacket = null;
                    for (uint tick = 2; tick <= 6; tick++)
                    {
                        var command = new TestCommand { Value = (int)tick };
                        Assert.That(client.QueueCommand(in command, tick, out _),
                            Is.EqualTo(NetworkCommandResult.Queued));
                        Assert.That(client.FlushCommands(tick),
                            Is.EqualTo(NetworkCommandResult.Queued));
                        Assert.That(serverTransport.TryReceive(out latestPacket), Is.True);
                    }

                    Assert.That(NetworkPacket.TryDecode(latestPacket,
                        clientSchema.Fingerprint, out _, out var payload), Is.True);
                    Assert.That(payload.Span[0], Is.EqualTo(4));
                    var ticks = new List<uint>();
                    int offset = 1;
                    for (var i = 0; i < payload.Span[0]; i++)
                    {
                        ticks.Add(Read32(payload.Span, offset + 4));
                        int commandBytes = checked((int)Read32(
                            payload.Span, offset + 13));
                        offset += 17 + commandBytes;
                    }
                    CollectionAssert.AreEqual(new uint[] { 3, 4, 5, 6 }, ticks);
                }
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
                World<ClientAWorld>.Destroy();
            }
        }

        [Test]
        public void MalformedAndOversizedCommandBatchesNeverQueueValidPrefix()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            var receiver = World<AuthorityWorld>
                .RegisterEventReceiver<NetworkCommandAcceptedEvent<TestCommand>>();
            try
            {
                MemoryNetworkTransport.CreatePair(new ConnectionId(93),
                    out var clientTransport, out var serverTransport);
                using (clientTransport)
                using (serverTransport)
                {
                    var serverSchema = Schema<AuthorityWorld>(true);
                    var clientSchema = Schema<ClientAWorld>(false);
                    var server = new NetworkServer<AuthorityWorld>(serverSchema,
                        static (_, _) => false);
                    server.AddConnection(serverTransport, 7, 13, new ScopeId(1));
                    var client = new NetworkClient<ClientAWorld>(clientTransport,
                        clientSchema, new ScopeId(1));
                    Assert.That(client.BeginHandshake(), Is.True);
                    server.Receive();
                    server.Tick(_ => { });
                    client.Process();
                    server.Receive();

                    var command = new TestCommand { Value = 7 };
                    Assert.That(client.QueueCommand(in command, 2, out _),
                        Is.EqualTo(NetworkCommandResult.Queued));
                    Assert.That(client.FlushCommands(2),
                        Is.EqualTo(NetworkCommandResult.Queued));
                    Assert.That(serverTransport.TryReceive(out var validPacket), Is.True);
                    Assert.That(NetworkPacket.TryDecode(validPacket, clientSchema.Fingerprint,
                        out var header, out var validPayload), Is.True);

                    var malformedPayload = new byte[validPayload.Length + 5];
                    validPayload.Span.CopyTo(malformedPayload);
                    malformedPayload[0] = 2;
                    Assert.That(NetworkPacket.TryEncode(Buffers, header, malformedPayload,
                        out var malformedPacket), Is.True);
                    Assert.That(clientTransport.TrySend(malformedPacket), Is.True);
                    server.Receive();

                    var oversizedPayload = new byte[18];
                    oversizedPayload[0] = 1;
                    Write32(oversizedPayload, 1, 2);
                    Write32(oversizedPayload, 5, 2);
                    Write32(oversizedPayload, 9, 10);
                    oversizedPayload[13] = 0;
                    Write32(oversizedPayload, 14,
                        ProtocolLimits.MaxCommandBytes + 1u);
                    header.PacketSequence = 2;
                    Assert.That(NetworkPacket.TryEncode(Buffers, header, oversizedPayload,
                        out var oversizedPacket), Is.True);
                    Assert.That(clientTransport.TrySend(oversizedPacket), Is.True);
                    server.Receive();
                    server.Tick(_ => { });

                    var count = 0;
                    foreach (World<AuthorityWorld>
                                 .Event<NetworkCommandAcceptedEvent<TestCommand>> _ in receiver)
                        count++;
                    Assert.That(count, Is.Zero);
                }
            }
            finally
            {
                World<AuthorityWorld>.DeleteEventReceiver(ref receiver);
                World<AuthorityWorld>.Destroy();
                World<ClientAWorld>.Destroy();
            }
        }

        [Test]
        public void RemoteDisconnectClearsClientReplicasAndHistory()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            try
            {
                var authoritySchema = Schema<AuthorityWorld>(true);
                var clientSchema = Schema<ClientAWorld>(false);
                MemoryNetworkTransport.CreatePair(new ConnectionId(41), out var clientTransport,
                    out var serverTransport);
                using (clientTransport)
                using (serverTransport)
                {
                    var server = new NetworkServer<AuthorityWorld>(authoritySchema,
                        (scope, entity) => true);
                    server.AddConnection(serverTransport, 4, 9, new ScopeId(1));
                    var client = new NetworkClient<ClientAWorld>(clientTransport, clientSchema,
                        new ScopeId(1));
                    var authority = World<AuthorityWorld>.NewEntity<TestEntity>();
                    authority.Set(new TestComponent { Value = 5 });

                    client.BeginHandshake();
                    server.Receive();
                    server.Tick(_ => { });
                    client.Process();
                    Assert.That(client.History.Count, Is.EqualTo(1));
                    Assert.That(World<ClientAWorld>.Query(default(EntityIs<TestEntity>))
                        .EntitiesCount(), Is.EqualTo(1));

                    var disconnect = Packet(PacketKind.Disconnect, 9, 3);
                    disconnect.SchemaFingerprint = clientSchema.Fingerprint;
                    Assert.That(NetworkPacket.TryEncode(Buffers, disconnect, ReadOnlySpan<byte>.Empty,
                        out var packet), Is.True);
                    Assert.That(serverTransport.TrySend(packet), Is.True);
                    client.Process();

                    Assert.That(client.Session.State, Is.EqualTo(NetworkSessionState.Closed));
                    Assert.That(client.History.Count, Is.Zero);
                    Assert.That(World<ClientAWorld>.Query(default(EntityIs<TestEntity>))
                        .EntitiesCount(), Is.Zero);
                }
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
                World<ClientAWorld>.Destroy();
            }
        }

        [Test]
        public void HistoryEvictsOldestAndScopeCaptureIsSharedByReference()
        {
            var history = new NetworkHistory<string>(2);
            history.Store(2, "two"); history.Store(1, "one"); history.Store(3, "three");
            Assert.That(history.TryGet(1, out _), Is.False);
            var bytes = new NetworkHistory<byte[]>(4, 3, value => value.Length);
            bytes.Store(1, new byte[] { 1, 2 }); bytes.Store(2, new byte[] { 3, 4 });
            Assert.That(bytes.TryGet(1, out _), Is.False);
            Assert.That(bytes.Bytes, Is.EqualTo(2));
            var coordinator = new NetworkServerCoordinator<TestWorld>(2);
            var capture = new NetworkSnapshot(7, default, new ScopeId(9),
                Lease(new byte[] { 1 }), 0, 0);
            coordinator.StoreCapture(new ScopeId(9), capture);
            Assert.That(coordinator.TryGetCapture(new ScopeId(9), 7, out var retained), Is.True);
            Assert.That(retained, Is.SameAs(capture));
            Assert.That(coordinator.TryGetCapture(new ScopeId(10), 7, out _), Is.False);
        }

        [Test]
        public void FramedTwoClientPipelineHandshakesOrdersCommandsAndReplicatesCreateUpdateDespawn()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            CreateReplicationWorld<ClientBWorld>(false);
            var receiver = World<AuthorityWorld>.RegisterEventReceiver<NetworkCommandAcceptedEvent<TestCommand>>();
            try
            {
                var authoritySchema = Schema<AuthorityWorld>(true);
                var clientASchema = Schema<ClientAWorld>(false);
                var clientBSchema = Schema<ClientBWorld>(false);
                using var mock = new TwoClientNetworkMock();
                var peerObserver = new TestPeerObserver();
                var server = new NetworkServer<AuthorityWorld>(authoritySchema, (scope, entity) => true,
                    4, 1024 * 1024, peerObserver: peerObserver);
                server.AddConnection(mock.ServerA, 2, 11, new ScopeId(7));
                Assert.Throws<InvalidOperationException>(() =>
                    server.AddConnection(mock.ServerB, 2, 12, new ScopeId(7)));
                server.AddConnection(mock.ServerB, 1, 12, new ScopeId(7));
                var clientA = new NetworkClient<ClientAWorld>(mock.ClientA, clientASchema, new ScopeId(7));
                var clientB = new NetworkClient<ClientBWorld>(mock.ClientB, clientBSchema, new ScopeId(7));
                var authority = World<AuthorityWorld>.NewEntity<TestEntity>();
                authority.Set(new TestComponent { Value = 1 });
                authority.Set(new NetworkOwnerComponent { PeerId = 2 });
                var gid = authority.GID;

                Assert.That(clientA.BeginHandshake(), Is.True);
                Assert.That(clientB.BeginHandshake(), Is.True);
                server.Receive(); server.Tick(_ => { });
                clientA.Process(); clientB.Process();
                Assert.That(clientA.Session.State, Is.EqualTo(NetworkSessionState.Established));
                Assert.That(clientB.Session.State, Is.EqualTo(NetworkSessionState.Established));
                Assert.That(peerObserver.AdmittedPeers.Count, Is.EqualTo(2));
                Assert.That(peerObserver.AdmittedPeers[0].PeerId, Is.EqualTo(2));
                Assert.That(peerObserver.AdmittedPeers[1].PeerId, Is.EqualTo(1));
                Assert.That(gid.TryUnpack<ClientAWorld>(out var replicaA), Is.True);
                Assert.That(gid.TryUnpack<ClientBWorld>(out var replicaB), Is.True);
                Assert.That(replicaA.Read<TestComponent>().Value, Is.EqualTo(1));
                Assert.That(replicaB.Read<TestComponent>().Value, Is.EqualTo(1));
                Assert.That(replicaA.Read<NetworkOwnerComponent>().PeerId, Is.EqualTo(2));
                Assert.That(replicaB.Read<NetworkOwnerComponent>().PeerId, Is.EqualTo(2));

                authority.Set(new TestComponent { Value = 2 });
                replicaA.Set(new NetworkOwnerComponent { PeerId = 999 });
                Assert.That(clientA.SendCommand(new TestCommand { Value = 20 }, 2), Is.EqualTo(NetworkCommandResult.Queued));
                Assert.That(clientB.SendCommand(new TestCommand { Value = 10 }, 2), Is.EqualTo(NetworkCommandResult.Queued));
                server.Receive(); server.Tick(_ => { });
                clientA.Process(); clientB.Process();
                Assert.That(replicaA.Read<TestComponent>().Value, Is.EqualTo(2));
                Assert.That(replicaB.Read<TestComponent>().Value, Is.EqualTo(2));
                Assert.That(authority.Read<NetworkOwnerComponent>().PeerId, Is.EqualTo(2), "client-local metadata must not affect authority state");
                Assert.That(replicaA.Read<NetworkOwnerComponent>().PeerId, Is.EqualTo(2), "the next snapshot restores server-assigned display metadata");
                authority.Disable<TestComponent>();
                Assert.That(authority.HasDisabled<TestComponent>(), Is.True);
                server.Receive(); server.Tick(_ => { }); clientA.Process(); clientB.Process();
                Assert.That(replicaA.HasDisabled<TestComponent>(), Is.True);
                Assert.That(replicaB.HasDisabled<TestComponent>(), Is.True);
                var index = 0;
                foreach (var item in receiver)
                {
                    Assert.That(item.Value.Command.Value, Is.EqualTo(index == 0 ? 10 : 20));
                    Assert.That(item.Value.Context.PeerId, Is.EqualTo(index == 0 ? 1 : 2), "server authority comes from admitted session context");
                    index++;
                }
                Assert.That(index, Is.EqualTo(2));
                Assert.That(clientA.AcknowledgedSnapshotTick, Is.EqualTo(3));
                Assert.That(clientA.ServerProcessedCommandSequence, Is.EqualTo(1));

                Assert.That(mock.ServerA.TrySend(Lease(new byte[] { 1, 2, 3 })), Is.True);
                clientA.Process();
                Assert.That(clientA.TryConsumeRecoveryTransition(out var recovery),
                    Is.True);
                Assert.That(recovery.Phase,
                    Is.EqualTo(NetworkRecoveryPhase.AwaitingKeyframe));
                Assert.That(recovery.Reason, Is.EqualTo(NetworkRecoveryReason.SnapshotRejected));

                authority.Destroy();
                server.Receive(); server.Tick(_ => { });
                clientA.Process(); clientB.Process();
                Assert.That(gid.TryUnpack<ClientAWorld>(out _), Is.False);
                Assert.That(gid.TryUnpack<ClientBWorld>(out _), Is.False);

                Assert.That(server.RemoveConnection(new ConnectionId(1)), Is.True);
                Assert.That(peerObserver.DisconnectedPeers.Count, Is.EqualTo(1));
                Assert.That(peerObserver.DisconnectedPeers[0].PeerId, Is.EqualTo(2));
                Assert.That(server.RemoveConnection(new ConnectionId(1)), Is.False);
                Assert.That(peerObserver.DisconnectedPeers.Count, Is.EqualTo(1));
                CreateReplicationWorld<ReconnectWorld>(false);
                MemoryNetworkTransport.CreatePair(new ConnectionId(1), out var reconnectTransport, out var reconnectServer);
                using (reconnectTransport) using (reconnectServer)
                {
                    server.AddConnection(reconnectServer, 2, 22, new ScopeId(7));
                    var reconnect = new NetworkClient<ReconnectWorld>(reconnectTransport, Schema<ReconnectWorld>(false), new ScopeId(7));
                    reconnect.BeginHandshake(); server.Receive(); server.Tick(_ => { }); reconnect.Process();
                    Assert.That(reconnect.Session.State, Is.EqualTo(NetworkSessionState.Established));
                    Assert.That(reconnect.Session.Epoch, Is.EqualTo(22));
                }
            }
            finally
            {
                World<AuthorityWorld>.DeleteEventReceiver(ref receiver);
                World<AuthorityWorld>.Destroy(); World<ClientAWorld>.Destroy(); World<ClientBWorld>.Destroy();
                if (World<ReconnectWorld>.Status == WorldStatus.Initialized) World<ReconnectWorld>.Destroy();
            }
        }

        private sealed class TestPeerObserver : INetworkPeerObserver
        {
            internal readonly List<NetworkPeerData> AdmittedPeers = new List<NetworkPeerData>();
            internal readonly List<NetworkPeerData> DisconnectedPeers = new List<NetworkPeerData>();

            public void Admitted(in NetworkPeerData peer) => AdmittedPeers.Add(peer);

            public void Disconnected(in NetworkPeerData peer) => DisconnectedPeers.Add(peer);
        }

        [Test]
        public void SchemaMismatchHandshakeClosesBothEndpoints()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var authoritySchema = Schema<AuthorityWorld>(true);
                var mismatchFactory = NetworkCompilerSupport.Create<MismatchWorld>();
                mismatchFactory.Entity<TestEntity>(new NetworkTypeId(1)); mismatchFactory.DisableableComponent<TestComponent>(new NetworkTypeId(2), 1); mismatchFactory.Tag<TestTag>(new NetworkTypeId(3)); mismatchFactory.Command<TestCommand>(new NetworkTypeId(10));
                var mismatchSchema = mismatchFactory.Freeze();
                MemoryNetworkTransport.CreatePair(new ConnectionId(99), out var clientTransport, out var serverTransport);
                using (clientTransport) using (serverTransport)
                {
                    var server = new NetworkServer<AuthorityWorld>(authoritySchema, (scope, entity) => true);
                    var serverSession = server.AddConnection(serverTransport, 9, 4, new ScopeId(1));
                    var client = new NetworkClient<MismatchWorld>(clientTransport, mismatchSchema, new ScopeId(1));
                    client.BeginHandshake(); server.Receive(); server.Tick(_ => { }); client.Process();
                    Assert.That(serverSession.State, Is.EqualTo(NetworkSessionState.Closed));
                    Assert.That(client.Session.State, Is.EqualTo(NetworkSessionState.Closed));
                    Assert.That(client.TryConsumeRecoveryTransition(out _), Is.False);
                }
            }
            finally { World<AuthorityWorld>.Destroy(); }
        }

        [Test]
        public void IncompatibleClientPacketTerminatesInsteadOfRequestingResync()
        {
            CreateReplicationWorld<ClientAWorld>(false);
            try
            {
                var schema = Schema<ClientAWorld>(false);
                MemoryNetworkTransport.CreatePair(new ConnectionId(98),
                    out var clientTransport, out var serverTransport);
                using (clientTransport)
                using (serverTransport)
                using (var client = new NetworkClient<ClientAWorld>(clientTransport,
                           schema, new ScopeId(1)))
                {
                    Assert.That(client.Session.Admit(schema.Fingerprint, 1, 1,
                        new ScopeId(1)), Is.EqualTo(NetworkAdmissionResult.Accepted));
                    var packetHeader = Packet(PacketKind.FullSnapshot, 1, 1);
                    packetHeader.ServerTick = 1;
                    packetHeader.SchemaFingerprint = default;
                    Assert.That(packetHeader.SchemaFingerprint,
                        Is.Not.EqualTo(schema.Fingerprint));
                    Assert.That(NetworkPacket.TryEncode(Buffers, packetHeader,
                        ReadOnlySpan<byte>.Empty, out var packet), Is.True);
                    Assert.That(serverTransport.TrySend(packet), Is.True);

                    client.Process();

                    Assert.That(client.Session.State,
                        Is.EqualTo(NetworkSessionState.Closed));
                    Assert.That(client.TryConsumeRecoveryTransition(out var recovery),
                        Is.True);
                    Assert.That(recovery.Phase,
                        Is.EqualTo(NetworkRecoveryPhase.DisconnectRequired));
                    Assert.That(recovery.Reason,
                        Is.EqualTo(NetworkRecoveryReason.ProtocolIncompatible));
                }
            }
            finally { World<ClientAWorld>.Destroy(); }
        }

        [Test]
        public void ProtocolVersionMismatchTerminatesClientInsteadOfRequestingResync()
        {
            CreateReplicationWorld<ClientAWorld>(false);
            try
            {
                var schema = Schema<ClientAWorld>(false);
                MemoryNetworkTransport.CreatePair(new ConnectionId(97),
                    out var clientTransport, out var serverTransport);
                using (clientTransport)
                using (serverTransport)
                using (var client = new NetworkClient<ClientAWorld>(clientTransport,
                           schema, new ScopeId(1)))
                {
                    Assert.That(client.Session.Admit(schema.Fingerprint, 1, 1,
                        new ScopeId(1)), Is.EqualTo(NetworkAdmissionResult.Accepted));
                    var packetHeader = Packet(PacketKind.FullSnapshot, 1, 1);
                    packetHeader.ServerTick = 1;
                    packetHeader.SchemaFingerprint = schema.Fingerprint;
                    Assert.That(NetworkPacket.TryEncode(Buffers, packetHeader,
                        ReadOnlySpan<byte>.Empty, out var packet), Is.True);
                    var bytes = packet.Memory.ToArray();
                    packet.Dispose();
                    bytes[4] = 3;
                    bytes[5] = 0;
                    Write32(bytes, 80, 0);
                    Write32(bytes, 80, Crc32(bytes));
                    Assert.That(serverTransport.TrySend(Buffers.Copy(bytes)), Is.True);

                    client.Process();

                    Assert.That(client.Session.State,
                        Is.EqualTo(NetworkSessionState.Closed));
                    Assert.That(client.TryConsumeRecoveryTransition(out var recovery),
                        Is.True);
                    Assert.That(recovery.Phase,
                        Is.EqualTo(NetworkRecoveryPhase.DisconnectRequired));
                    Assert.That(recovery.Reason,
                        Is.EqualTo(NetworkRecoveryReason.ProtocolIncompatible));
                }
            }
            finally { World<ClientAWorld>.Destroy(); }
        }

        [Test]
        public void ForeignVersionWithInvalidHeaderCrcRequestsKeyframe()
        {
            CreateReplicationWorld<ClientAWorld>(false);
            try
            {
                var schema = Schema<ClientAWorld>(false);
                MemoryNetworkTransport.CreatePair(new ConnectionId(95),
                    out var clientTransport, out var serverTransport);
                using (clientTransport)
                using (serverTransport)
                using (var client = new NetworkClient<ClientAWorld>(clientTransport,
                           schema, new ScopeId(1)))
                {
                    Assert.That(client.Session.Admit(schema.Fingerprint, 1, 1,
                        new ScopeId(1)), Is.EqualTo(NetworkAdmissionResult.Accepted));
                    var packetHeader = Packet(PacketKind.FullSnapshot, 1, 1);
                    packetHeader.ServerTick = 1;
                    packetHeader.SchemaFingerprint = schema.Fingerprint;
                    Assert.That(NetworkPacket.TryEncode(Buffers, packetHeader,
                        ReadOnlySpan<byte>.Empty, out var packet), Is.True);
                    var bytes = packet.Memory.ToArray();
                    packet.Dispose();
                    bytes[4] = 3;
                    bytes[5] = 0;
                    Assert.That(serverTransport.TrySend(Buffers.Copy(bytes)), Is.True);

                    client.Process();

                    Assert.That(client.Session.State,
                        Is.EqualTo(NetworkSessionState.Established));
                    Assert.That(client.TryConsumeRecoveryTransition(out var recovery),
                        Is.True);
                    Assert.That(recovery.Phase,
                        Is.EqualTo(NetworkRecoveryPhase.AwaitingKeyframe));
                    Assert.That(recovery.Reason,
                        Is.EqualTo(NetworkRecoveryReason.SnapshotRejected));
                }
            }
            finally { World<ClientAWorld>.Destroy(); }
        }

        [Test]
        public void TruncatedForeignVersionRequestsKeyframe()
        {
            CreateReplicationWorld<ClientAWorld>(false);
            try
            {
                var schema = Schema<ClientAWorld>(false);
                MemoryNetworkTransport.CreatePair(new ConnectionId(94),
                    out var clientTransport, out var serverTransport);
                using (clientTransport)
                using (serverTransport)
                using (var client = new NetworkClient<ClientAWorld>(clientTransport,
                           schema, new ScopeId(1)))
                {
                    Assert.That(client.Session.Admit(schema.Fingerprint, 1, 1,
                        new ScopeId(1)), Is.EqualTo(NetworkAdmissionResult.Accepted));
                    var packetHeader = Packet(PacketKind.FullSnapshot, 1, 1);
                    packetHeader.ServerTick = 1;
                    packetHeader.SchemaFingerprint = schema.Fingerprint;
                    Assert.That(NetworkPacket.TryEncode(Buffers, packetHeader,
                        ReadOnlySpan<byte>.Empty, out var packet), Is.True);
                    var bytes = packet.Memory.ToArray();
                    packet.Dispose();
                    bytes[4] = 3;
                    bytes[5] = 0;
                    Write32(bytes, 80, 0);
                    Write32(bytes, 80, Crc32(bytes));
                    Assert.That(serverTransport.TrySend(Buffers.Copy(
                        bytes.AsSpan(0, PacketHeader.Size - 1))), Is.True);

                    client.Process();

                    Assert.That(client.Session.State,
                        Is.EqualTo(NetworkSessionState.Established));
                    Assert.That(client.TryConsumeRecoveryTransition(out var recovery),
                        Is.True);
                    Assert.That(recovery.Phase,
                        Is.EqualTo(NetworkRecoveryPhase.AwaitingKeyframe));
                    Assert.That(recovery.Reason,
                        Is.EqualTo(NetworkRecoveryReason.SnapshotRejected));
                }
            }
            finally { World<ClientAWorld>.Destroy(); }
        }

        [Test]
        public void CurrentVersionMalformedSnapshotRequestsKeyframe()
        {
            CreateReplicationWorld<ClientAWorld>(false);
            try
            {
                var schema = Schema<ClientAWorld>(false);
                MemoryNetworkTransport.CreatePair(new ConnectionId(96),
                    out var clientTransport, out var serverTransport);
                using (clientTransport)
                using (serverTransport)
                using (var client = new NetworkClient<ClientAWorld>(clientTransport,
                           schema, new ScopeId(1)))
                {
                    Assert.That(client.Session.Admit(schema.Fingerprint, 1, 1,
                        new ScopeId(1)), Is.EqualTo(NetworkAdmissionResult.Accepted));
                    var packetHeader = Packet(PacketKind.FullSnapshot, 1, 1);
                    packetHeader.ServerTick = 1;
                    packetHeader.SchemaFingerprint = schema.Fingerprint;
                    Assert.That(NetworkPacket.TryEncode(Buffers, packetHeader,
                        new byte[] { 1 }, out var packet), Is.True);
                    Assert.That(serverTransport.TrySend(packet), Is.True);

                    client.Process();

                    Assert.That(client.Session.State,
                        Is.EqualTo(NetworkSessionState.Established));
                    Assert.That(client.TryConsumeRecoveryTransition(out var recovery),
                        Is.True);
                    Assert.That(recovery.Phase,
                        Is.EqualTo(NetworkRecoveryPhase.AwaitingKeyframe));
                    Assert.That(recovery.Reason,
                        Is.EqualTo(NetworkRecoveryReason.SnapshotRejected));
                }
            }
            finally { World<ClientAWorld>.Destroy(); }
        }

        [Test]
        public void SnapshotSourceIdCollisionAndMalformedPacketNeverMutateClientLocalEntity()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ConflictWorld>(false);
            try
            {
                var authoritySchema = Schema<AuthorityWorld>(true);
                var clientSchema = Schema<ConflictWorld>(false);
                var authority = World<AuthorityWorld>.NewEntity<TestEntity>();
                authority.Set(new TestComponent { Value = 5 });
                var local = World<ConflictWorld>.NewEntityByGID<TestEntity>(authority.GID);
                local.Set(new TestComponent { Value = 99 });
                var capture = new NetworkReplicator<AuthorityWorld>(authoritySchema, (scope, entity) => true, new ScopeId(3));
                Assert.That(capture.Capture(1, out var snapshot), Is.EqualTo(SnapshotCaptureResult.Success));
                var apply = new NetworkReplicator<ConflictWorld>(clientSchema, new ScopeId(3));
                Assert.That(apply.Stage(snapshot, out var staged), Is.EqualTo(SnapshotApplyResult.Success));
                Assert.That(apply.Apply(staged), Is.EqualTo(SnapshotApplyResult.Success));
                Assert.That(local.Read<TestComponent>().Value, Is.EqualTo(99));

                var replicaCount = 0;
                foreach (var entity in World<ConflictWorld>.Query().Entities())
                {
                    if (entity.EntityType != default(TestEntity).Id() || entity.GID == local.GID)
                        continue;
                    Assert.That(entity.Read<TestComponent>().Value, Is.EqualTo(5));
                    replicaCount++;
                }
                Assert.That(replicaCount, Is.EqualTo(1));

                var malformed = new byte[snapshot.ByteLength - 1];
                snapshot.Bytes.Span.Slice(0, malformed.Length).CopyTo(malformed);
                var bad = new NetworkSnapshot(1, snapshot.SchemaFingerprint,
                    snapshot.Scope, Lease(malformed), snapshot.EntityCount,
                    snapshot.RecordCount);
                Assert.That(apply.Stage(bad, out _), Is.Not.EqualTo(SnapshotApplyResult.Success));
                Assert.That(local.Read<TestComponent>().Value, Is.EqualTo(99));
            }
            finally { World<AuthorityWorld>.Destroy(); World<ConflictWorld>.Destroy(); }
        }

        [Test]
        public void ServerDispatchReturnsPolicyRejectedWithoutRejectedEventReceiver()
        {
            World<RejectWorld>.Create(WorldConfig.Default());
            World<RejectWorld>.Types().Event<TestCommand>().Event<NetworkCommandAcceptedEvent<TestCommand>>().Event<NetworkCommandRejectedEvent<TestCommand>>();
            World<RejectWorld>.Initialize();
            try
            {
                var clientFactory = NetworkCompilerSupport.Create<RejectWorld>(); clientFactory.Command<TestCommand>(new NetworkTypeId(10)); var clientSchema = clientFactory.Freeze();
                var rawFactory = NetworkCompilerSupport.Create<RejectWorld>(); rawFactory.Command<TestCommand>(new NetworkTypeId(10)); var rawSchema = rawFactory.Freeze();
                var policyFactory = NetworkCompilerSupport.Create<RejectWorld>(); policyFactory.Command<TestCommand, RejectPolicy>(new NetworkTypeId(10)); var policySchema = policyFactory.Freeze();
                var client = new NetworkSession<RejectWorld>(new ConnectionId(1), NetworkRole.Client, clientSchema); client.Admit(clientSchema.Fingerprint, 1, 1, default);
                client.CreateCommand(new TestCommand { Value = 7 }, 1, out var command);
                var raw = new NetworkSession<RejectWorld>(new ConnectionId(1), NetworkRole.Server, rawSchema); raw.Admit(rawSchema.Fingerprint, 1, 1, default);
                var rawCoordinator = new NetworkServerCoordinator<RejectWorld>(); rawCoordinator.Add(raw);
                Assert.That(rawCoordinator.Queue(command, 1), Is.EqualTo(NetworkCommandResult.SchemaMismatch));
                var server = new NetworkSession<RejectWorld>(new ConnectionId(1), NetworkRole.Server, policySchema); server.Admit(policySchema.Fingerprint, 1, 1, default);
                Assert.That(server.Validate(command, 1, 2, 8, out var entry), Is.EqualTo(NetworkCommandResult.Queued));
                Assert.That(server.Dispatch(command, entry), Is.EqualTo(NetworkCommandResult.PolicyRejected));
            }
            finally { World<RejectWorld>.Destroy(); }
        }

        [Test]
        public void ServerOwnsMonotonicTickAndGameplayPrecedesCapture()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            try
            {
                MemoryNetworkTransport.CreatePair(new ConnectionId(44), out var clientTransport, out var serverTransport);
                using (clientTransport) using (serverTransport)
                {
                    var serverObserver = new DiagnosticsCollector();
                    var clientObserver = new DiagnosticsCollector();
                    var server = new NetworkServer<AuthorityWorld>(Schema<AuthorityWorld>(true), (scope, entity) => true, observer: serverObserver);
                    server.AddConnection(serverTransport, 1, 9, new ScopeId(3), serverObserver);
                    var clientSchema = Schema<ClientAWorld>(false);
                    var client = new NetworkClient<ClientAWorld>(clientTransport, clientSchema, new ScopeId(3), clientObserver);
                    Assert.That(clientObserver.Sessions[0].NextSendPacketSequence, Is.EqualTo(1));
                    Assert.That(client.BeginHandshake(), Is.True);
                    Assert.That(clientObserver.Sessions[clientObserver.Sessions.Count - 1].NextSendPacketSequence, Is.EqualTo(2));
                    server.Receive();
                    EntityGID gid = default;
                    server.Tick(tick =>
                    {
                        Assert.That(tick, Is.EqualTo(1));
                        var entity = World<AuthorityWorld>.NewEntity<TestEntity>();
                        entity.Set(new TestComponent { Value = 42 }); gid = entity.GID;
                    });
                    client.Process();
                    Assert.That(server.ServerTick, Is.EqualTo(1));
                    Assert.That(gid.TryUnpack<ClientAWorld>(out var replica), Is.True);
                    Assert.That(replica.Read<TestComponent>().Value, Is.EqualTo(42));

                    var phases = new HashSet<NetworkPhase>();
                    foreach (var value in serverObserver.Events) phases.Add(value.Phase);
                    foreach (var value in clientObserver.Events) phases.Add(value.Phase);
                    Assert.That(phases, Is.EquivalentTo(new[] { NetworkPhase.Receive, NetworkPhase.Decode, NetworkPhase.CommandDispatch, NetworkPhase.SnapshotCapture, NetworkPhase.SnapshotApply, NetworkPhase.Send }));
                    Assert.That(serverObserver.Count(NetworkPhase.Send), Is.EqualTo(2));
                    Assert.That(clientObserver.Count(NetworkPhase.Send), Is.EqualTo(2));
                    var captured = serverObserver.Single(NetworkPhase.SnapshotCapture);
                    Assert.That(captured.Entities, Is.EqualTo(1)); Assert.That(captured.Records, Is.EqualTo(1));
                    Assert.That(captured.HistoryTicks, Is.EqualTo(1)); Assert.That(captured.HistoryBytes, Is.GreaterThan(0));
                    var applied = clientObserver.Single(NetworkPhase.SnapshotApply);
                    Assert.That(applied.Entities, Is.EqualTo(1)); Assert.That(applied.Records, Is.EqualTo(1));
                    var decodedSnapshot = clientObserver.Single(NetworkPhase.Decode, NetworkPacketKind.FullSnapshot);
                    var ack = clientObserver.Single(NetworkPhase.Send, NetworkPacketKind.Ack);
                    Assert.That(decodedSnapshot.Timestamp, Is.LessThanOrEqualTo(applied.Timestamp));
                    Assert.That(applied.Timestamp, Is.LessThanOrEqualTo(ack.Timestamp));
                    Assert.That(clientObserver.Count(NetworkPhase.SnapshotApply), Is.EqualTo(1));
                    Assert.That(clientObserver.Count(NetworkPhase.Send, NetworkPacketKind.Ack), Is.EqualTo(1));
                    Assert.That(serverObserver.Snapshots.Count, Is.EqualTo(1));
                    Assert.That(clientObserver.Snapshots.Count, Is.EqualTo(1));
                    Assert.That(serverObserver.Snapshots[0].PayloadHash, Is.EqualTo(clientObserver.Snapshots[0].PayloadHash));
                    Assert.That(clientObserver.Sessions[clientObserver.Sessions.Count - 1].AcknowledgedSnapshotTick, Is.EqualTo(1));
                    Assert.That(serverObserver.Sessions[serverObserver.Sessions.Count - 1].NextSendPacketSequence, Is.EqualTo(3));

                    var nonSnapshot = new PacketHeader
                    {
                        Kind = PacketKind.ResyncRequest,
                        Flags = PacketFlags.ReliableOrdered,
                        SessionEpoch = 9,
                        PacketSequence = 3,
                        ServerTick = 7,
                        SchemaFingerprint = clientSchema.Fingerprint
                    };
                    Assert.That(NetworkPacket.TryEncode(Buffers, nonSnapshot, ReadOnlySpan<byte>.Empty, out var nonSnapshotPacket), Is.True);
                    Assert.That(serverTransport.TrySend(nonSnapshotPacket), Is.True);
                    client.Process();
                    Assert.That(client.ServerTick, Is.EqualTo(7));
                    Assert.That(client.AcknowledgedSnapshotTick, Is.EqualTo(1));
                    nonSnapshot.PacketSequence = 4;
                    nonSnapshot.ServerTick = 3;
                    Assert.That(NetworkPacket.TryEncode(Buffers, nonSnapshot, ReadOnlySpan<byte>.Empty, out var olderTickPacket), Is.True);
                    Assert.That(serverTransport.TrySend(olderTickPacket), Is.True);
                    client.Process();
                    Assert.That(client.ServerTick, Is.EqualTo(7), "validated non-snapshot ticks must not regress the authoritative cursor");
                    Assert.That(client.SendCommand(new TestCommand { Value = 5 }, 8), Is.EqualTo(NetworkCommandResult.Queued));
                    var commandDiagnostics = clientObserver.Sessions[clientObserver.Sessions.Count - 1];
                    Assert.That(commandDiagnostics.ServerTick, Is.EqualTo(7));
                    Assert.That(commandDiagnostics.NextSendPacketSequence, Is.EqualTo(3));
                }
            }
            finally { World<AuthorityWorld>.Destroy(); World<ClientAWorld>.Destroy(); }
        }

        [Test]
        public void ServerDispatchTelemetryPreservesPolicyOutcomeAndActiveSessionGauges()
        {
            World<RejectWorld>.Create(WorldConfig.Default());
            World<RejectWorld>.Types().Event<TestCommand>().Event<NetworkCommandAcceptedEvent<TestCommand>>().Event<NetworkCommandRejectedEvent<TestCommand>>();
            World<RejectWorld>.Initialize();
            try
            {
                var clientFactory = NetworkCompilerSupport.Create<RejectWorld>(); clientFactory.Command<TestCommand>(new NetworkTypeId(10)); var clientSchema = clientFactory.Freeze();
                var serverFactory = NetworkCompilerSupport.Create<RejectWorld>(); serverFactory.Command<TestCommand, RejectPolicy>(new NetworkTypeId(10)); var serverSchema = serverFactory.Freeze();
                MemoryNetworkTransport.CreatePair(new ConnectionId(77), out var clientTransport, out var serverTransport);
                MemoryNetworkTransport.CreatePair(new ConnectionId(78), out var otherClientTransport, out var otherServerTransport);
                MemoryNetworkTransport.CreatePair(new ConnectionId(79), out var pendingClientTransport, out var pendingServerTransport);
                using (clientTransport) using (serverTransport) using (otherClientTransport) using (otherServerTransport) using (pendingClientTransport) using (pendingServerTransport)
                {
                    var observer = new TraceCollector();
                    var server = new NetworkServer<RejectWorld>(serverSchema, (scope, entity) => false, observer: observer);
                    var session = server.AddConnection(serverTransport, 1, 5, default, observer);
                    server.AddConnection(otherServerTransport, 2, 6, default, observer);
                    server.AddConnection(pendingServerTransport, 3, 7, default, observer);
                    var client = new NetworkClient<RejectWorld>(clientTransport, clientSchema);
                    var otherClient = new NetworkClient<RejectWorld>(otherClientTransport, clientSchema);
                    client.BeginHandshake(); otherClient.BeginHandshake(); server.Receive(); server.Tick(_ => { }); client.Process(); otherClient.Process();
                    observer.Events.Clear();
                    Assert.That(client.SendCommand(new TestCommand { Value = 7 }, 2), Is.EqualTo(NetworkCommandResult.Queued));
                    server.Receive(); server.Tick(_ => { });
                    var dispatch = observer.Single(NetworkPhase.CommandDispatch);
                    Assert.That(observer.Count(NetworkPhase.CommandDispatch), Is.EqualTo(1));
                    Assert.That(dispatch.Result, Is.EqualTo(NetworkResultCategory.Policy));
                    Assert.That(dispatch.Commands, Is.EqualTo(1)); Assert.That(dispatch.AcceptedCommands, Is.EqualTo(0)); Assert.That(dispatch.RejectedCommands, Is.EqualTo(1));
                    Assert.That(dispatch.ActiveConnections, Is.EqualTo(3)); Assert.That(dispatch.ActivePeers, Is.EqualTo(2));

                    var disconnect = Packet(PacketKind.Disconnect, 5, 3); disconnect.SchemaFingerprint = serverSchema.Fingerprint;
                    Assert.That(NetworkPacket.TryEncode(Buffers, disconnect, ReadOnlySpan<byte>.Empty, out var packet), Is.True);
                    Assert.That(clientTransport.TrySend(packet), Is.True); server.Receive();
                    Assert.That(session.State, Is.EqualTo(NetworkSessionState.Closed));
                    var decoded = observer.Single(NetworkPhase.Decode, NetworkPacketKind.Disconnect);
                    Assert.That(decoded.ActiveConnections, Is.EqualTo(2)); Assert.That(decoded.ActivePeers, Is.EqualTo(1));
                    MemoryNetworkTransport.CreatePair(new ConnectionId(1), out var reconnectClient,
                        out var reconnectServer);
                    using (reconnectClient)
                    using (reconnectServer)
                    {
                        Assert.DoesNotThrow(() => server.AddConnection(reconnectServer, 4, 8,
                            default));
                        Assert.That(server.RemoveConnection(new ConnectionId(1)), Is.True);
                    }
                }
            }
            finally { World<RejectWorld>.Destroy(); }
        }

        [Test]
        public void PacketValidationIsStateEpochAndStrictSequenceBound()
        {
            var schema = NetworkCompilerSupport.Create<TestWorld>().Freeze();
            var handshake = new NetworkSession<TestWorld>(new ConnectionId(1), NetworkRole.Server, schema);
            var hello = Packet(PacketKind.Hello, 0, 1); Assert.That(handshake.ValidatePacket(in hello), Is.EqualTo(PacketValidationResult.Success));
            Assert.That(handshake.ValidatePacket(in hello), Is.EqualTo(PacketValidationResult.Sequence));
            var client = new NetworkSession<TestWorld>(new ConnectionId(2), NetworkRole.Client, schema);
            var ready = Packet(PacketKind.Ready, 7, 1); Assert.That(client.ValidatePacket(in ready), Is.EqualTo(PacketValidationResult.Success));

            var classified = new NetworkSession<TestWorld>(new ConnectionId(3), NetworkRole.Server, schema);
            Assert.That(classified.Admit(schema.Fingerprint, 1, 7, default), Is.EqualTo(NetworkAdmissionResult.Accepted));
            var wrongRoleReplay = Packet(PacketKind.FullSnapshot, 6, 99);
            Assert.That(classified.ValidatePacket(in wrongRoleReplay), Is.EqualTo(PacketValidationResult.WrongRole));
            var first = Packet(PacketKind.Ack, 7, 1);
            Assert.That(classified.ValidatePacket(in first), Is.EqualTo(PacketValidationResult.Success), "wrong-role rejection must not consume the cursor");

            var kinds = new[] { PacketKind.Hello, PacketKind.Ready, PacketKind.CommandBatch, PacketKind.FullSnapshot, PacketKind.Ack, PacketKind.ResyncRequest, PacketKind.Disconnect };
            for (var i = 0; i < kinds.Length; i++)
            {
                AssertPacketDirection(schema, NetworkRole.Server, kinds[i], kinds[i] == PacketKind.CommandBatch || kinds[i] == PacketKind.Ack || kinds[i] == PacketKind.ResyncRequest || kinds[i] == PacketKind.Disconnect, (uint)(10 + i));
                AssertPacketDirection(schema, NetworkRole.Client, kinds[i], kinds[i] == PacketKind.FullSnapshot || kinds[i] == PacketKind.ResyncRequest || kinds[i] == PacketKind.Disconnect, (uint)(30 + i));
            }
        }

        [Test]
        public void SnapshotDiagnosticsPreserveEveryApplyResultCategory()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            try
            {
                var authorityEntity = World<AuthorityWorld>.NewEntity<TestEntity>();
                authorityEntity.Set(new TestComponent { Value = 1 });
                var authority = new NetworkReplicator<AuthorityWorld>(Schema<AuthorityWorld>(true), (scope, entity) => true, new ScopeId(1));
                Assert.That(authority.Capture(1, out var capture), Is.EqualTo(SnapshotCaptureResult.Success));
                var schema = Schema<ClientAWorld>(false);
                var client = new NetworkReplicator<ClientAWorld>(schema, new ScopeId(1));

                Assert.That(NetworkClient<ClientAWorld>.DiagnosticResult(client.Stage(null, out _)), Is.EqualTo(NetworkResultCategory.Limits));
                var wrongScope = new NetworkSnapshot(1, schema.Fingerprint,
                    new ScopeId(2), Lease(Array.Empty<byte>()), 0, 0);
                Assert.That(NetworkClient<ClientAWorld>.DiagnosticResult(client.Stage(wrongScope, out _)), Is.EqualTo(NetworkResultCategory.Schema));
                var malformed = new NetworkSnapshot(1, schema.Fingerprint,
                    new ScopeId(1), Lease(new byte[] { 0, 0, 0, 0, 1 }), 0, 0);
                Assert.That(NetworkClient<ClientAWorld>.DiagnosticResult(client.Stage(malformed, out _)), Is.EqualTo(NetworkResultCategory.Malformed));
                Assert.That(client.Stage(capture, out var staged), Is.EqualTo(SnapshotApplyResult.Success));
                Assert.That(client.Apply(staged), Is.EqualTo(SnapshotApplyResult.Success));
                foreach (var entity in World<ClientAWorld>.Query().Entities())
                    if (entity.EntityType == default(TestEntity).Id())
                        entity.Destroy();
                Assert.That(NetworkClient<ClientAWorld>.DiagnosticResult(client.Stage(capture, out _)), Is.EqualTo(NetworkResultCategory.World));
                Assert.That(NetworkClient<ClientAWorld>.DiagnosticResult(SnapshotApplyResult.Success), Is.EqualTo(NetworkResultCategory.Success));
            }
            finally { World<AuthorityWorld>.Destroy(); World<ClientAWorld>.Destroy(); }
        }

        [Test]
        public void CapturesAreScopeDisjointAndStagedSnapshotsAreOwnerBound()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            try
            {
                var authoritySchema = Schema<AuthorityWorld>(true);
                var missingSelector = new NetworkReplicator<AuthorityWorld>(authoritySchema);
                Assert.Throws<InvalidOperationException>(() => missingSelector.Capture(1, out _));
                var first = World<AuthorityWorld>.NewEntity<TestEntity>(); first.Set(new TestComponent { Value = 1 });
                var second = World<AuthorityWorld>.NewEntity<SecondEntity>(); second.Set(new TestComponent { Value = 2 });
                var capture = new NetworkReplicator<AuthorityWorld>(authoritySchema, scopeSelector: (scope, entity) => entity.Read<TestComponent>().Value == (int)scope.Value);
                Assert.That(capture.Capture(1, new ScopeId(1), out var one), Is.EqualTo(SnapshotCaptureResult.Success));
                Assert.That(capture.Capture(1, new ScopeId(2), out var two), Is.EqualTo(SnapshotCaptureResult.Success));
                Assert.That(one.EntityCount, Is.EqualTo(1));
                Assert.That(two.EntityCount, Is.EqualTo(1));
                Assert.That(one.PayloadHash, Is.Not.EqualTo(two.PayloadHash));
                var allKinds = new NetworkReplicator<AuthorityWorld>(authoritySchema, (scope, entity) => true);
                Assert.That(allKinds.Capture(2, new ScopeId(3), out var both), Is.EqualTo(SnapshotCaptureResult.Success));
                Assert.That(both.EntityCount, Is.EqualTo(2), "generated entity-kind invokers must capture both kinds exactly once");

                var clientSchema = Schema<ClientAWorld>(false);
                var owner = new NetworkReplicator<ClientAWorld>(clientSchema, new ScopeId(1));
                var other = new NetworkReplicator<ClientAWorld>(clientSchema, new ScopeId(1));
                Assert.That(owner.Stage(one, out var staged), Is.EqualTo(SnapshotApplyResult.Success));
                Assert.That(other.Apply(staged), Is.EqualTo(SnapshotApplyResult.SchemaMismatch));
            }
            finally { World<AuthorityWorld>.Destroy(); World<ClientAWorld>.Destroy(); }
        }

        [Test]
        public void StageRejectsDisabledNonDisableableRecordBeforeWorldMutation()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            try
            {
                var entity = World<AuthorityWorld>.NewEntity<TestEntity>();
                entity.Set(new TestComponent { Value = 4 }); entity.Set<TestTag>();
                var capture = new NetworkReplicator<AuthorityWorld>(Schema<AuthorityWorld>(true), (scope, value) => true, new ScopeId(5));
                Assert.That(capture.Capture(1, out var snapshot), Is.EqualTo(SnapshotCaptureResult.Success));
                var bytes = snapshot.Bytes.ToArray();
                Assert.That(bytes.Length, Is.GreaterThan(40));
                bytes[40] = 1; // second sorted record is TestTag; byte 40 is its disabled flag.
                var malformed = new NetworkSnapshot(snapshot.ServerTick,
                    snapshot.SchemaFingerprint, snapshot.Scope, Lease(bytes),
                    snapshot.EntityCount, snapshot.RecordCount);
                var apply = new NetworkReplicator<ClientAWorld>(Schema<ClientAWorld>(false), new ScopeId(5));
                Assert.That(apply.Stage(malformed, out _), Is.EqualTo(SnapshotApplyResult.Malformed));
                Assert.That(entity.GID.TryUnpack<ClientAWorld>(out _), Is.False);
            }
            finally { World<AuthorityWorld>.Destroy(); World<ClientAWorld>.Destroy(); }
        }

        [Test]
        public void DeclaredStaticEcsConfigVersionParticipatesInFingerprint()
        {
            var first = NetworkCompilerSupport.Create<TestWorld>();
            first.Component<VersionOneComponent>(new NetworkTypeId(55), NetworkCompilerSupport.ComponentVersion<VersionOneComponent>());
            var same = NetworkCompilerSupport.Create<TestWorld>();
            same.Component<VersionOneComponent>(new NetworkTypeId(55), NetworkCompilerSupport.ComponentVersion<VersionOneComponent>());
            var changed = NetworkCompilerSupport.Create<TestWorld>();
            changed.Component<VersionTwoComponent>(new NetworkTypeId(55), NetworkCompilerSupport.ComponentVersion<VersionTwoComponent>());
            var firstFingerprint = first.Freeze().Fingerprint;
            Assert.That(firstFingerprint, Is.EqualTo(same.Freeze().Fingerprint));
            Assert.That(changed.Freeze().Fingerprint, Is.Not.EqualTo(firstFingerprint));
        }

        [Test]
        public void UnconfiguredTypesUseWireVersionZero()
        {
            var schema = Schema<TestWorld>(false);
            Assert.That(schema.TryGet(new NetworkTypeId(2), out var component), Is.True);
            Assert.That(schema.TryGet(new NetworkTypeId(10), out var command), Is.True);
            Assert.That(schema.TryGet(new NetworkTypeId(5), out var owner), Is.True);
            Assert.That(component.Version, Is.Zero);
            Assert.That(command.Version, Is.Zero);
            Assert.That(owner.Version, Is.Zero);
            Assert.That(owner.RuntimeType, Is.EqualTo(typeof(NetworkOwnerComponent)));
        }

        [Test]
        public void DetailedDiagnosticsExposeMetadataOnlyWhileLegacyObserverRemainsValid()
        {
            INetworkObserver legacy = new TraceCollector();
            Assert.That(legacy, Is.Not.InstanceOf<INetworkDiagnosticsObserver>());
            AssertMetadataOnly(typeof(NetworkSessionDiagnostics));
            AssertMetadataOnly(typeof(NetworkSnapshotDiagnostics));
        }

        [Test]
        public void SessionsValidateAndDispatchCommandsInTargetPeerSequenceOrder()
        {
            World<TestWorld>.Create(WorldConfig.Default());
            World<TestWorld>.Types().Event<TestCommand>().Event<NetworkCommandAcceptedEvent<TestCommand>>().Event<NetworkCommandRejectedEvent<TestCommand>>();
            World<TestWorld>.Initialize();
            var receiver = World<TestWorld>.RegisterEventReceiver<NetworkCommandAcceptedEvent<TestCommand>>();
            try
            {
                var clientFactory = NetworkCompilerSupport.Create<TestWorld>();
                clientFactory.Command<TestCommand>(new NetworkTypeId(10));
                var clientSchema = clientFactory.Freeze();
                var serverFactory = NetworkCompilerSupport.Create<TestWorld>();
                serverFactory.Command<TestCommand, AllowPolicy>(new NetworkTypeId(10));
                var serverSchema = serverFactory.Freeze();
                Assert.That(clientSchema.Fingerprint, Is.EqualTo(serverSchema.Fingerprint));

                var clientA = new NetworkSession<TestWorld>(new ConnectionId(1), NetworkRole.Client, clientSchema);
                var clientB = new NetworkSession<TestWorld>(new ConnectionId(2), NetworkRole.Client, clientSchema);
                var serverA = new NetworkSession<TestWorld>(new ConnectionId(1), NetworkRole.Server, serverSchema);
                var serverB = new NetworkSession<TestWorld>(new ConnectionId(2), NetworkRole.Server, serverSchema);
                Assert.That(clientA.Admit(serverSchema.Fingerprint, 2, 5, new ScopeId(1)), Is.EqualTo(NetworkAdmissionResult.Accepted));
                Assert.That(serverA.Admit(clientSchema.Fingerprint, 2, 5, new ScopeId(1)), Is.EqualTo(NetworkAdmissionResult.Accepted));
                Assert.That(clientB.Admit(serverSchema.Fingerprint, 1, 6, new ScopeId(1)), Is.EqualTo(NetworkAdmissionResult.Accepted));
                Assert.That(serverB.Admit(clientSchema.Fingerprint, 1, 6, new ScopeId(1)), Is.EqualTo(NetworkAdmissionResult.Accepted));
                clientA.CreateCommand(new TestCommand { Value = 20 }, 10, out var a);
                clientB.CreateCommand(new TestCommand { Value = 10 }, 10, out var b);
                var coordinator = new NetworkServerCoordinator<TestWorld>();
                coordinator.Add(serverA); coordinator.Add(serverB);
                Assert.That(coordinator.Queue(a, 10), Is.EqualTo(NetworkCommandResult.Queued));
                Assert.That(coordinator.Queue(b, 10), Is.EqualTo(NetworkCommandResult.Queued));
                Assert.That(coordinator.Dispatch(10).Total, Is.EqualTo(2));
                var index = 0;
                foreach (var item in receiver)
                {
                    Assert.That(item.Value.Command.Value, Is.EqualTo(index == 0 ? 10 : 20));
                    index++;
                }
                Assert.That(index, Is.EqualTo(2));
            }
            finally
            {
                World<TestWorld>.DeleteEventReceiver(ref receiver);
                World<TestWorld>.Destroy();
            }
        }

        [Test]
        public void NdjsonOverflowWritesExplicitGapWithoutPayloadData()
        {
            using var stream = new MemoryStream();
            using (var log = new NetworkNdjsonLog(stream, 1))
            {
                var value = new NetworkTraceEvent(NetworkPhase.Decode, NetworkTraceKind.Point, NetworkResultCategory.Success,
                    NetworkRole.Client, 1, 2, 3, 4, 5, 6, 1, 0, 0, 0, 0, 0, 1, 1, 7);
                log.Observe(in value); log.Observe(in value); log.Flush();
            }
            var text = System.Text.Encoding.UTF8.GetString(stream.ToArray());
            StringAssert.Contains("\"phase\":\"decode\"", text);
            StringAssert.Contains("\"packet_kind\":\"none\"", text);
            StringAssert.Contains("\"history_ticks\":0", text);
            StringAssert.Contains("\"history_bytes\":0", text);
            StringAssert.Contains("\"accepted_commands\":0", text);
            StringAssert.Contains("\"rejected_commands\":0", text);
            StringAssert.Contains("\"client_server_tick_gap\":0", text);
            StringAssert.Contains("\"duration_ns\":0", text);
            StringAssert.Contains("\"schema_fingerprint\":", text);
            StringAssert.Contains("\"kind\":\"gap\"", text);
        }

        [Test]
        public void AdmissionPolicy_CommitsBeforeLifecycleNotification()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            try
            {
                MemoryNetworkTransport.CreatePair(new ConnectionId(71),
                    out var clientTransport, out var serverTransport);
                using (clientTransport)
                using (serverTransport)
                {
                    var order = new List<string>();
                    var policy = new RecordingAdmissionPolicy(order, true);
                    var observer = new OrderedPeerObserver(order);
                    var server = new NetworkServer<AuthorityWorld>(
                        Schema<AuthorityWorld>(true), static (_, _) => true,
                        peerObserver: observer, admissionPolicy: policy);
                    server.AddConnection(serverTransport, 3, 8, new ScopeId(1));
                    var client = new NetworkClient<ClientAWorld>(
                        clientTransport, Schema<ClientAWorld>(false), new ScopeId(1));

                    client.BeginHandshake();
                    server.Receive();
                    server.Tick(_ => { });
                    client.Process();

                    Assert.That(order, Is.EqualTo(new[] { "policy", "admitted" }));
                    Assert.That(policy.RollbackCount, Is.Zero);
                    Assert.That(client.Session.State, Is.EqualTo(NetworkSessionState.Established));
                }
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
                World<ClientAWorld>.Destroy();
            }
        }

        [Test]
        public void AdmissionPolicy_RejectionRollsBackWithoutLifecycleNotification()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            try
            {
                MemoryNetworkTransport.CreatePair(new ConnectionId(72),
                    out var clientTransport, out var serverTransport);
                using (clientTransport)
                using (serverTransport)
                {
                    var order = new List<string>();
                    var policy = new RecordingAdmissionPolicy(order, false);
                    var observer = new OrderedPeerObserver(order);
                    var server = new NetworkServer<AuthorityWorld>(
                        Schema<AuthorityWorld>(true), static (_, _) => true,
                        peerObserver: observer, admissionPolicy: policy);
                    server.AddConnection(serverTransport, 4, 9, new ScopeId(1));
                    var client = new NetworkClient<ClientAWorld>(
                        clientTransport, Schema<ClientAWorld>(false), new ScopeId(1));

                    client.BeginHandshake();
                    server.Receive();
                    client.Process();

                    Assert.That(order, Is.EqualTo(new[] { "policy", "rollback" }));
                    Assert.That(policy.RollbackCount, Is.EqualTo(1));
                    Assert.That(client.Session.State, Is.EqualTo(NetworkSessionState.Closed));
                }
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
                World<ClientAWorld>.Destroy();
            }
        }

        [Test]
        public void AdmissionObserverFailure_DisconnectsCommittedPeerAndRunsCleanup()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            try
            {
                MemoryNetworkTransport.CreatePair(new ConnectionId(73),
                    out var clientTransport, out var serverTransport);
                using (clientTransport)
                using (serverTransport)
                {
                    var order = new List<string>();
                    var policy = new RecordingAdmissionPolicy(order, true);
                    var observer = new ThrowingPeerObserver(order);
                    var server = new NetworkServer<AuthorityWorld>(
                        Schema<AuthorityWorld>(true), static (_, _) => true,
                        peerObserver: observer, admissionPolicy: policy);
                    server.AddConnection(serverTransport, 5, 10, new ScopeId(1));
                    var client = new NetworkClient<ClientAWorld>(
                        clientTransport, Schema<ClientAWorld>(false), new ScopeId(1));

                    client.BeginHandshake();
                    server.Receive();
                    client.Process();

                    Assert.That(order, Is.EqualTo(new[]
                    {
                        "policy",
                        "admitted",
                        "disconnected"
                    }));
                    Assert.That(client.Session.State, Is.EqualTo(NetworkSessionState.Closed));
                    Assert.That(server.RemoveConnection(new ConnectionId(73)), Is.False);
                }
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
                World<ClientAWorld>.Destroy();
            }
        }

        [Test]
        public void WarmCommandAndSnapshotCoreAllocatesNoManagedMemoryPerTick()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            var pool = new NetworkBufferPool(4L << 20);
            MemoryNetworkTransport.CreatePair(new ConnectionId(801),
                out var clientTransport, out var serverTransport);
            var server = new NetworkServer<AuthorityWorld>(Schema<AuthorityWorld>(true),
                static (_, _) => true, bufferPool: pool);
            var client = new NetworkClient<ClientAWorld>(clientTransport,
                Schema<ClientAWorld>(false), new ScopeId(1), bufferPool: pool);
            try
            {
                var authority = World<AuthorityWorld>.NewEntity<TestEntity>();
                authority.Set(new TestComponent { Value = 1 });
                server.AddConnection(serverTransport, 1, 1, new ScopeId(1));
                client.BeginHandshake();
                server.Receive();
                server.BeginTick();
                server.CompleteTick();
                client.Process();

                for (uint tick = 2; tick < 130; tick++)
                    RunCoreTick(client, server, tick);

                GC.Collect();
                GC.WaitForPendingFinalizers();
                GC.Collect();
                var before = GC.GetAllocatedBytesForCurrentThread();
                for (uint tick = 130; tick < 1_130; tick++)
                    RunCoreTick(client, server, tick);
                var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

                Assert.That(allocated, Is.Zero);
                Assert.That(client.CaptureMemoryDiagnostics().PendingCommands,
                    Is.LessThanOrEqualTo(4));
                Assert.That(server.CaptureMemoryDiagnostics().PendingCommands,
                    Is.Zero);
            }
            finally
            {
                client.Dispose();
                server.Dispose();
                clientTransport.Dispose();
                serverTransport.Dispose();
                Assert.That(pool.CaptureDiagnostics().OutstandingLeases, Is.Zero);
                pool.Dispose();
                World<AuthorityWorld>.Destroy();
                World<ClientAWorld>.Destroy();
            }
        }

        private static void RunCoreTick(NetworkClient<ClientAWorld> client,
            NetworkServer<AuthorityWorld> server, uint tick)
        {
            client.QueueCommand(new TestCommand { Value = (int)tick }, tick, out _);
            client.FlushCommands(tick);
            server.Receive();
            server.BeginTick();
            server.CompleteTick();
            client.Process();
        }

        private static void CreateReplicationWorld<TWorld>(bool server) where TWorld : struct, IWorldType
        {
            World<TWorld>.Create(WorldConfig.Default());
            var types = World<TWorld>.Types();
            types.RegisterAll(typeof(NetworkOwnerComponent).Assembly);
            types.EntityType<TestEntity>(); types.EntityType<SecondEntity>(); types.Tag<TestTag>(); types.Component<TestComponent>(); types.Event<TestCommand>();
            if (server) { types.Event<NetworkCommandAcceptedEvent<TestCommand>>(); types.Event<NetworkCommandRejectedEvent<TestCommand>>(); }
            World<TWorld>.Initialize();
        }

        private static NetworkSchema<TWorld> Schema<TWorld>(bool server) where TWorld : struct, IWorldType
        {
            var factory = NetworkCompilerSupport.Create<TWorld>();
            factory.Entity<TestEntity>(new NetworkTypeId(1));
            factory.Entity<SecondEntity>(new NetworkTypeId(4));
            factory.DisableableComponent<TestComponent>(new NetworkTypeId(2));
            factory.Component<NetworkOwnerComponent>(new NetworkTypeId(5));
            factory.Tag<TestTag>(new NetworkTypeId(3));
            if (server) factory.Command<TestCommand, AllowAnyPolicy<TWorld>>(new NetworkTypeId(10));
            else factory.Command<TestCommand>(new NetworkTypeId(10));
            return factory.Freeze();
        }

        private static PacketHeader Packet(PacketKind kind, uint epoch, uint sequence) => new PacketHeader
        {
            Kind = kind,
            Flags = kind == PacketKind.CommandBatch
                ? PacketFlags.UnreliableSequenced
                : PacketFlags.ReliableOrdered,
            SessionEpoch = epoch,
            PacketSequence = sequence
        };

        private static NetworkBufferLease Lease(ReadOnlySpan<byte> source)
        {
            return Buffers.Copy(source);
        }

        private static uint Read32(ReadOnlySpan<byte> source, int offset) =>
            (uint)(source[offset] | source[offset + 1] << 8 |
                   source[offset + 2] << 16 | source[offset + 3] << 24);

        private static void Write32(Span<byte> destination, int offset, uint value)
        {
            destination[offset] = (byte)value;
            destination[offset + 1] = (byte)(value >> 8);
            destination[offset + 2] = (byte)(value >> 16);
            destination[offset + 3] = (byte)(value >> 24);
        }

        private static uint Crc32(ReadOnlySpan<byte> source)
        {
            var crc = uint.MaxValue;
            for (var i = 0; i < source.Length; i++)
            {
                crc ^= source[i];
                for (var bit = 0; bit < 8; bit++)
                {
                    crc = (crc & 1) == 0
                        ? crc >> 1
                        : 0xedb88320U ^ crc >> 1;
                }
            }
            return ~crc;
        }

        private static void AssertMetadataOnly(Type type)
        {
            foreach (var property in type.GetProperties())
            {
                Assert.That(property.PropertyType, Is.Not.EqualTo(typeof(byte[])));
                Assert.That(property.PropertyType, Is.Not.EqualTo(typeof(ReadOnlyMemory<byte>)));
                Assert.That(property.PropertyType, Is.Not.EqualTo(typeof(EntityGID)));
            }
        }

        private static void AssertPacketDirection(NetworkSchema<TestWorld> schema, NetworkRole role, PacketKind kind, bool allowed, uint connection)
        {
            var session = new NetworkSession<TestWorld>(new ConnectionId(connection), role, schema);
            Assert.That(session.Admit(schema.Fingerprint, 1, 7, default), Is.EqualTo(NetworkAdmissionResult.Accepted));
            var wrongEpoch = Packet(kind, 6, 1); Assert.That(session.ValidatePacket(in wrongEpoch), Is.EqualTo(allowed ? PacketValidationResult.WrongEpoch : PacketValidationResult.WrongRole));
            var outOfOrder = Packet(kind, 7, 2);
            var outOfOrderResult = allowed && kind == PacketKind.CommandBatch
                ? PacketValidationResult.Success
                : allowed ? PacketValidationResult.Sequence : PacketValidationResult.WrongRole;
            Assert.That(session.ValidatePacket(in outOfOrder), Is.EqualTo(outOfOrderResult));
            var candidate = Packet(kind, 7, 1);
            var candidateResult = allowed && kind == PacketKind.CommandBatch
                ? PacketValidationResult.Sequence
                : allowed ? PacketValidationResult.Success : PacketValidationResult.WrongRole;
            Assert.That(session.ValidatePacket(in candidate), Is.EqualTo(candidateResult));
            if (allowed && kind != PacketKind.CommandBatch)
                Assert.That(session.ValidatePacket(in candidate), Is.EqualTo(PacketValidationResult.Sequence));
            else
            {
                var fallback = Packet(role == NetworkRole.Server ? PacketKind.Ack : PacketKind.FullSnapshot, 7, 1);
                Assert.That(session.ValidatePacket(in fallback), Is.EqualTo(PacketValidationResult.Success), $"{role} rejected {kind} without consuming sequence");
            }
            Assert.That(session.State, Is.EqualTo(NetworkSessionState.Established));
        }

        public struct TestWorld : IWorldType { }
        public struct AuthorityWorld : IWorldType { }
        public struct ClientAWorld : IWorldType { }
        public struct ClientBWorld : IWorldType { }
        public struct ConflictWorld : IWorldType { }
        public struct ReconnectWorld : IWorldType { }
        public struct MismatchWorld : IWorldType { }
        public struct RejectWorld : IWorldType { }
        public struct TestEntity : IEntityType, INetworkType { public byte Id() => 1; }
        private struct InputWorld : IWorldType { }
        internal struct TestInput : IEvent, INetworkCommand
        {
            public int Value;
            public void Write(ref BinaryPackWriter writer) => writer.WriteInt(Value);
            public void Read(ref BinaryPackReader reader, byte version) =>
                Value = reader.ReadInt();
        }
        private struct AllowInputPolicy : INetworkCommandPolicy<InputWorld, TestInput>
        {
            public bool Authorize(in NetworkCommandContext context,
                in TestInput command) => true;
        }
        public struct SecondEntity : IEntityType, INetworkType { public byte Id() => 2; }
        public struct TestTag : ITag, INetworkType { }
        public struct TestComponent : IComponent, IDisableable, INetworkType
        {
            public int Value;
            public void Write<TWorld>(ref BinaryPackWriter writer, World<TWorld>.Entity self) where TWorld : struct, IWorldType => writer.WriteInt(Value);
            public void Read<TWorld>(ref BinaryPackReader reader, World<TWorld>.Entity self, byte version, bool disabled) where TWorld : struct, IWorldType => Value = reader.ReadInt();
        }
        public struct TestCommand : IEvent, INetworkCommand
        {
            public int Value;
            public void Write(ref BinaryPackWriter writer) => writer.WriteInt(Value);
            public void Read(ref BinaryPackReader reader, byte version) => Value = reader.ReadInt();
        }
        public struct VersionOneComponent : IComponent, IComponentConfig<VersionOneComponent>, INetworkType
        {
            public ComponentTypeConfig<VersionOneComponent> Config() => new ComponentTypeConfig<VersionOneComponent>(version: 1);
            public void Write<TWorld>(ref BinaryPackWriter writer, World<TWorld>.Entity self) where TWorld : struct, IWorldType { }
            public void Read<TWorld>(ref BinaryPackReader reader, World<TWorld>.Entity self, byte version, bool disabled) where TWorld : struct, IWorldType { }
        }
        public struct VersionTwoComponent : IComponent, IComponentConfig<VersionTwoComponent>, INetworkType
        {
            public ComponentTypeConfig<VersionTwoComponent> Config() => new ComponentTypeConfig<VersionTwoComponent>(version: 2);
            public void Write<TWorld>(ref BinaryPackWriter writer, World<TWorld>.Entity self) where TWorld : struct, IWorldType { }
            public void Read<TWorld>(ref BinaryPackReader reader, World<TWorld>.Entity self, byte version, bool disabled) where TWorld : struct, IWorldType { }
        }
        public struct AllowPolicy : INetworkCommandPolicy<TestWorld, TestCommand>
        {
            public bool Authorize(in NetworkCommandContext context, in TestCommand command) => true;
        }
        public struct AllowAnyPolicy<TWorld> : INetworkCommandPolicy<TWorld, TestCommand> where TWorld : struct, IWorldType { public bool Authorize(in NetworkCommandContext context, in TestCommand command) => true; }
        public struct RejectPolicy : INetworkCommandPolicy<RejectWorld, TestCommand> { public bool Authorize(in NetworkCommandContext context, in TestCommand command) => false; }

        private class TraceCollector : INetworkObserver
        {
            internal readonly List<NetworkTraceEvent> Events = new List<NetworkTraceEvent>();
            public void Observe(in NetworkTraceEvent value) => Events.Add(value);
            internal int Count(NetworkPhase phase) { var count = 0; for (var i = 0; i < Events.Count; i++) if (Events[i].Phase == phase) count++; return count; }
            internal int Count(NetworkPhase phase, NetworkPacketKind packetKind) { var count = 0; for (var i = 0; i < Events.Count; i++) if (Events[i].Phase == phase && Events[i].PacketKind == packetKind) count++; return count; }
            internal NetworkTraceEvent Single(NetworkPhase phase) { for (var i = 0; i < Events.Count; i++) if (Events[i].Phase == phase) return Events[i]; throw new InvalidOperationException("Missing phase " + phase); }
            internal NetworkTraceEvent Single(NetworkPhase phase, NetworkPacketKind packetKind) { for (var i = 0; i < Events.Count; i++) if (Events[i].Phase == phase && Events[i].PacketKind == packetKind) return Events[i]; throw new InvalidOperationException("Missing phase " + phase + " / " + packetKind); }
        }

        private sealed class DiagnosticsCollector : TraceCollector, INetworkDiagnosticsObserver
        {
            internal readonly List<NetworkSessionDiagnostics> Sessions = new List<NetworkSessionDiagnostics>();
            internal readonly List<NetworkSnapshotDiagnostics> Snapshots = new List<NetworkSnapshotDiagnostics>();
            public void ObserveSession(in NetworkSessionDiagnostics value) => Sessions.Add(value);
            public void ObserveSnapshot(in NetworkSnapshotDiagnostics value) => Snapshots.Add(value);
        }

        private sealed class RecordingAdmissionPolicy : INetworkPeerAdmissionPolicy
        {
            private readonly List<string> _order;
            private readonly bool _accept;

            internal RecordingAdmissionPolicy(List<string> order, bool accept)
            {
                _order = order;
                _accept = accept;
            }

            internal int RollbackCount { get; private set; }

            public bool TryAdmit(
                in NetworkPeerData peer,
                out NetworkAdmissionRejection reason)
            {
                _order.Add("policy");
                reason = _accept
                    ? NetworkAdmissionRejection.None
                    : NetworkAdmissionRejection.Capacity;
                return _accept;
            }

            public void Rollback(in NetworkPeerData peer)
            {
                RollbackCount++;
                _order.Add("rollback");
            }
        }

        private sealed class OrderedPeerObserver : INetworkPeerObserver
        {
            private readonly List<string> _order;

            internal OrderedPeerObserver(List<string> order)
            {
                _order = order;
            }

            public void Admitted(in NetworkPeerData peer) => _order.Add("admitted");

            public void Disconnected(in NetworkPeerData peer) => _order.Add("disconnected");
        }

        private sealed class ThrowingPeerObserver : INetworkPeerObserver
        {
            private readonly List<string> _order;

            internal ThrowingPeerObserver(List<string> order)
            {
                _order = order;
            }

            public void Admitted(in NetworkPeerData peer)
            {
                _order.Add("admitted");
                throw new InvalidOperationException("admission observer failure");
            }

            public void Disconnected(in NetworkPeerData peer) => _order.Add("disconnected");
        }
    }
}
