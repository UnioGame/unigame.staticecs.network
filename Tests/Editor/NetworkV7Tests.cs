using System;
using System.Collections.Generic;
using System.IO;
using FFS.Libraries.StaticEcs;
using FFS.Libraries.StaticPack;
using NUnit.Framework;

namespace UniGame.StaticEcs.Network.Tests
{
    public sealed partial class NetworkV7Tests
    {
        private static readonly NetworkBufferPool Buffers = new NetworkBufferPool(64L << 20);

        [Test]
        public void TypeIdsAndPacketHeaderAreCanonicalV7AndRejectV6()
        {
            var id = NetworkCompilerSupport.TypeId("SourceGenerator.Tests", "Demo.Position");
            Assert.That(id.Value, Is.EqualTo(4089044646u));
            Assert.That(NetworkCompilerSupport.TypeId("game.shared", "Demo.Position").Value, Is.EqualTo(1933934308u));
            var header = new PacketHeader
            {
                Kind = PacketKind.SnapshotChunk,
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
            Assert.That(ProtocolLimits.Version, Is.EqualTo(7));
            Assert.That(decoded.SchemaFingerprint, Is.EqualTo(header.SchemaFingerprint));
            Assert.That(decoded.SimulationFingerprint,
                Is.EqualTo(header.SimulationFingerprint));
            Assert.That(decoded.ContentFingerprint, Is.EqualTo(header.ContentFingerprint));
            bytes[4] = 6;
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
            header.Kind = (PacketKind)4;
            header.PayloadLength = 0;
            Assert.That(header.TryWrite(bytes), Is.False);
        }

        [Test]
        public void ResyncPayloadRequiresOneNonZeroCorrelationId()
        {
            Span<byte> bytes = stackalloc byte[ResyncRequestPayload.Size];
            Assert.That(new ResyncRequestPayload(42).TryWrite(bytes), Is.True);
            Assert.That(ResyncRequestPayload.TryRead(bytes, out var decoded),
                Is.True);
            Assert.That(decoded.CorrelationId, Is.EqualTo(42));
            Assert.That(ResyncRequestPayload.TryRead(ReadOnlySpan<byte>.Empty,
                out _), Is.False);
            bytes.Clear();
            Assert.That(ResyncRequestPayload.TryRead(bytes, out _), Is.False);
        }

        [Test]
        public void SnapshotChunkHeaderRoundTrips()
        {
            var header = new SnapshotChunkHeader
            {
                PayloadKind = SnapshotPayloadKind.Delta,
                SnapshotTick = 9,
                BaselineTick = 8,
                TotalLength = 17,
                TotalHash = 0x0102030405060708UL,
                ChunkIndex = 1,
                ChunkCount = 2
            };
            var bytes = new byte[SnapshotChunkHeader.Size];
            Assert.That(header.TryWrite(bytes), Is.True);
            Assert.That(SnapshotChunkHeader.TryRead(bytes, out var decoded), Is.True);
            Assert.That(decoded.PayloadKind, Is.EqualTo(header.PayloadKind));
            Assert.That(decoded.SnapshotTick, Is.EqualTo(header.SnapshotTick));
            Assert.That(decoded.BaselineTick, Is.EqualTo(header.BaselineTick));
            Assert.That(decoded.TotalLength, Is.EqualTo(header.TotalLength));
            Assert.That(decoded.TotalHash, Is.EqualTo(header.TotalHash));
            Assert.That(decoded.ChunkIndex, Is.EqualTo(header.ChunkIndex));
            Assert.That(decoded.ChunkCount, Is.EqualTo(header.ChunkCount));

            header.PayloadKind = SnapshotPayloadKind.Keyframe;
            header.BaselineTick = 0;
            header.ResyncCorrelationId = 42;
            Assert.That(header.TryWrite(bytes), Is.True);
            Assert.That(SnapshotChunkHeader.TryRead(bytes, out decoded), Is.True);
            Assert.That(decoded.ResyncCorrelationId, Is.EqualTo(42));
        }

        [Test]
        public void SnapshotChunkHeaderRejectsInvalidValues()
        {
            var header = new SnapshotChunkHeader
            {
                PayloadKind = SnapshotPayloadKind.Delta,
                SnapshotTick = 9,
                BaselineTick = 8,
                TotalLength = 1,
                TotalHash = 1,
                ChunkCount = 1
            };
            var bytes = new byte[SnapshotChunkHeader.Size];
            Assert.That(header.TryWrite(bytes), Is.True);
            bytes[0] = 0;
            Assert.That(SnapshotChunkHeader.TryRead(bytes, out _), Is.False);

            header.PayloadKind = (SnapshotPayloadKind)3;
            Assert.That(header.TryWrite(bytes), Is.False);
            header.PayloadKind = SnapshotPayloadKind.Keyframe;
            header.BaselineTick = 1;
            Assert.That(header.TryWrite(bytes), Is.False);
            header.PayloadKind = SnapshotPayloadKind.Delta;
            header.BaselineTick = 0;
            Assert.That(header.TryWrite(bytes), Is.False);
            header.BaselineTick = header.SnapshotTick;
            Assert.That(header.TryWrite(bytes), Is.False);
            header.BaselineTick = 8;
            header.ResyncCorrelationId = 1;
            Assert.That(header.TryWrite(bytes), Is.False);
            header.ResyncCorrelationId = 0;
            header.TotalLength = 0;
            Assert.That(header.TryWrite(bytes), Is.False);
            header.TotalLength = 1;
            header.ChunkCount = 0;
            Assert.That(header.TryWrite(bytes), Is.False);
            header.ChunkCount = 1;
            header.ChunkIndex = 1;
            Assert.That(header.TryWrite(bytes), Is.False);
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
            Assert.That(NetworkSimulationConfig.MinimumCommandRedundancy, Is.EqualTo(1));
            Assert.That(NetworkSimulationConfig.DefaultCommandRedundancy, Is.EqualTo(3));
            Assert.That(NetworkSimulationConfig.MaximumCommandRedundancy, Is.EqualTo(32));
            Assert.That(first.CommandRedundancy,
                Is.EqualTo(NetworkSimulationConfig.DefaultCommandRedundancy));
            Assert.Throws<ArgumentOutOfRangeException>(() =>
                new NetworkSimulationConfigResource(20, commandRedundancy:
                    NetworkSimulationConfig.MinimumCommandRedundancy - 1));
            Assert.Throws<ArgumentOutOfRangeException>(() =>
                new NetworkSimulationConfigResource(20, commandRedundancy:
                    NetworkSimulationConfig.MaximumCommandRedundancy + 1));
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
                    var packetHeader = Packet(PacketKind.SnapshotChunk, 1, 1);
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
                    var packetHeader = Packet(PacketKind.SnapshotChunk, 1, 1);
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
        public void ForeignVersionWithInvalidHeaderCrcClosesSession()
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
                    var packetHeader = Packet(PacketKind.SnapshotChunk, 1, 1);
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
        public void TruncatedForeignVersionClosesSession()
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
                    var packetHeader = Packet(PacketKind.SnapshotChunk, 1, 1);
                    packetHeader.ServerTick = 1;
                    packetHeader.SchemaFingerprint = schema.Fingerprint;
                    Assert.That(NetworkPacket.TryEncode(Buffers, packetHeader,
                        new byte[] { 1 }, out var packet), Is.True);
                    var bytes = packet.Memory.ToArray();
                    packet.Dispose();
                    bytes[4] = 3;
                    bytes[5] = 0;
                    Write32(bytes, 80, 0);
                    Write32(bytes, 80, Crc32(bytes.AsSpan(0, PacketHeader.Size)));
                    Assert.That(serverTransport.TrySend(Buffers.Copy(
                        bytes.AsSpan(0, bytes.Length - 1))), Is.True);

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
        public void ForeignVersionWithCorruptPayloadHashClosesSession()
        {
            CreateReplicationWorld<ClientAWorld>(false);
            try
            {
                var schema = Schema<ClientAWorld>(false);
                MemoryNetworkTransport.CreatePair(new ConnectionId(93),
                    out var clientTransport, out var serverTransport);
                using (clientTransport)
                using (serverTransport)
                using (var client = new NetworkClient<ClientAWorld>(clientTransport,
                           schema, new ScopeId(1)))
                {
                    Assert.That(client.Session.Admit(schema.Fingerprint, 1, 1,
                        new ScopeId(1)), Is.EqualTo(NetworkAdmissionResult.Accepted));
                    var packetHeader = Packet(PacketKind.SnapshotChunk, 1, 1);
                    packetHeader.ServerTick = 1;
                    packetHeader.SchemaFingerprint = schema.Fingerprint;
                    Assert.That(NetworkPacket.TryEncode(Buffers, packetHeader,
                        new byte[] { 1 }, out var packet), Is.True);
                    var bytes = packet.Memory.ToArray();
                    packet.Dispose();
                    bytes[4] = 3;
                    bytes[5] = 0;
                    Write32(bytes, 80, 0);
                    Write32(bytes, 80, Crc32(bytes.AsSpan(0, PacketHeader.Size)));
                    bytes[PacketHeader.Size] ^= 1;
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



























        private static SnapshotChunkHeader DeltaHeader(NetworkSnapshot baseline,
            NetworkSnapshot target) => new SnapshotChunkHeader
        {
            PayloadKind = SnapshotPayloadKind.Delta,
            SnapshotTick = target.ServerTick,
            BaselineTick = baseline.ServerTick,
            TotalLength = checked((uint)target.ByteLength),
            TotalHash = target.PayloadHash,
            ChunkIndex = 0,
            ChunkCount = 1
        };

        private static SnapshotChunkHeader KeyframeHeader(
            NetworkSnapshot snapshot) => new SnapshotChunkHeader
        {
            PayloadKind = SnapshotPayloadKind.Keyframe,
            SnapshotTick = snapshot.ServerTick,
            BaselineTick = 0,
            TotalLength = checked((uint)snapshot.ByteLength),
            TotalHash = snapshot.PayloadHash,
            ChunkIndex = 0,
            ChunkCount = 1
        };

        private static void SendPeerPacket(INetworkTransport transport,
            SchemaFingerprint schema, PacketKind kind, uint epoch,
            uint sequence, uint acknowledgedTick)
        {
            var header = Packet(kind, epoch, sequence);
            header.SchemaFingerprint = schema;
            header.AcknowledgedSnapshotTick = acknowledgedTick;
            Assert.That(NetworkPacket.TryEncode(Buffers, header,
                ReadOnlySpan<byte>.Empty, out var packet), Is.True);
            Assert.That(transport.TrySend(packet), Is.True);
        }

        private static SnapshotChunkHeader ReceiveChunk(
            INetworkTransport transport)
        {
            Assert.That(transport.TryReceive(out var packet), Is.True);
            try
            {
                Assert.That(NetworkPacket.TryDecode(packet, out var header,
                    out var payload), Is.True);
                Assert.That(header.Kind, Is.EqualTo(PacketKind.SnapshotChunk));
                Assert.That(SnapshotChunkHeader.TryRead(payload.Span,
                    out var chunk), Is.True);
                Assert.That(chunk.SnapshotTick, Is.EqualTo(header.ServerTick));
                Assert.That(chunk.ChunkIndex, Is.Zero);
                Assert.That(chunk.ChunkCount, Is.EqualTo(1));
                return chunk;
            }
            finally
            {
                packet.Dispose();
            }
        }

        private static SnapshotChunkHeader InspectSnapshotChunk(
            NetworkBufferLease packet, out ReadOnlyMemory<byte> body)
        {
            Assert.That(NetworkPacket.TryDecode(packet, out var header,
                out var payload), Is.True);
            Assert.That(header.Kind, Is.EqualTo(PacketKind.SnapshotChunk));
            Assert.That(SnapshotChunkHeader.TryRead(payload.Span,
                out var chunk), Is.True);
            body = payload.Slice(SnapshotChunkHeader.Size);
            return chunk;
        }

        private static void SendSnapshotChunk(INetworkTransport transport,
            SchemaFingerprint schema, uint sequence,
            SnapshotChunkHeader chunk, ReadOnlySpan<byte> body)
        {
            var payload = new byte[checked(
                SnapshotChunkHeader.Size + body.Length)];
            Assert.That(chunk.TryWrite(payload), Is.True);
            body.CopyTo(payload.AsSpan(SnapshotChunkHeader.Size));
            var header = Packet(PacketKind.SnapshotChunk, 1, sequence);
            header.ServerTick = chunk.SnapshotTick;
            header.SchemaFingerprint = schema;
            Assert.That(NetworkPacket.TryEncode(Buffers, header,
                payload, out var packet), Is.True);
            Assert.That(transport.TrySend(packet), Is.True);
        }

        private static int ReadReplicaValue()
        {
            var found = false;
            var value = 0;
            foreach (var entity in World<ClientAWorld>.Query(
                         default(EntityIs<TestEntity>)).Entities())
            {
                Assert.That(found, Is.False);
                found = true;
                value = entity.Read<TestComponent>().Value;
            }
            Assert.That(found, Is.True);
            return value;
        }

        private static void AssertDeltaRejected(NetworkBufferPool pool,
            NetworkSnapshot baseline, ReadOnlySpan<byte> delta,
            in SnapshotChunkHeader header, SchemaFingerprint schema,
            ScopeId scope)
        {
            var outstanding = pool.CaptureDiagnostics().OutstandingLeases;
            Assert.That(SnapshotDeltaCodec.TryReconstruct(pool, baseline, delta,
                in header, schema, scope, out var canonical,
                out var entities, out var records), Is.False);
            Assert.That(canonical, Is.Null);
            Assert.That(entities, Is.Zero);
            Assert.That(records, Is.Zero);
            Assert.That(pool.CaptureDiagnostics().OutstandingLeases,
                Is.EqualTo(outstanding));
        }

        private static void Swap(byte[] bytes, int first, int second, int count)
        {
            for (var index = 0; index < count; index++)
            {
                var value = bytes[first + index];
                bytes[first + index] = bytes[second + index];
                bytes[second + index] = value;
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
            var outOfOrderResult = allowed &&
                                   (kind == PacketKind.CommandBatch || kind == PacketKind.SnapshotChunk)
                ? PacketValidationResult.Success
                : allowed ? PacketValidationResult.Sequence : PacketValidationResult.WrongRole;
            Assert.That(session.ValidatePacket(in outOfOrder), Is.EqualTo(outOfOrderResult));
            var candidate = Packet(kind, 7, 1);
            var candidateResult = allowed && kind == PacketKind.CommandBatch
                ? PacketValidationResult.Duplicate
                : allowed ? PacketValidationResult.Success : PacketValidationResult.WrongRole;
            Assert.That(session.ValidatePacket(in candidate), Is.EqualTo(candidateResult));
            if (allowed && kind == PacketKind.SnapshotChunk)
                Assert.That(session.ValidatePacket(in candidate),
                    Is.EqualTo(PacketValidationResult.Success));
            else if (allowed && kind != PacketKind.CommandBatch)
                Assert.That(session.ValidatePacket(in candidate), Is.EqualTo(
                    kind == PacketKind.TransactionCommand ||
                    kind == PacketKind.TransactionReceipt
                        ? PacketValidationResult.Duplicate
                        : PacketValidationResult.Sequence));
            else
            {
                var fallback = Packet(role == NetworkRole.Server ? PacketKind.Ack : PacketKind.SnapshotChunk, 7, 1);
                Assert.That(session.ValidatePacket(in fallback), Is.EqualTo(PacketValidationResult.Success), $"{role} rejected {kind} without consuming sequence");
            }
            Assert.That(session.State, Is.EqualTo(NetworkSessionState.Established));
        }

        private sealed class LimitedNetworkTransport : INetworkTransport
        {
            private readonly INetworkTransport _inner;

            internal LimitedNetworkTransport(INetworkTransport inner,
                int maxUnreliablePayloadBytes,
                int maxReliablePayloadBytes = 0)
            {
                _inner = inner;
                MaxUnreliablePayloadBytes = maxUnreliablePayloadBytes;
                MaxReliablePayloadBytes = maxReliablePayloadBytes > 0
                    ? maxReliablePayloadBytes
                    : inner.MaxReliablePayloadBytes;
            }

            public ConnectionId Connection => _inner.Connection;
            public int MaxReliablePayloadBytes { get; set; }
            public int MaxUnreliablePayloadBytes { get; set; }
            internal int SentPacketCount { get; private set; }
            internal int LargestSentPacketBytes { get; private set; }
            internal int FailOnSendNumber { get; set; }

            public bool TrySend(NetworkBufferLease packet)
            {
                SentPacketCount++;
                LargestSentPacketBytes = Math.Max(LargestSentPacketBytes,
                    packet?.Length ?? 0);
                if (FailOnSendNumber == SentPacketCount)
                {
                    packet?.Dispose();
                    return false;
                }
                return _inner.TrySend(packet);
            }

            internal void ResetSentPackets()
            {
                SentPacketCount = 0;
                LargestSentPacketBytes = 0;
            }

            public bool TryReceive(out NetworkBufferLease packet) =>
                _inner.TryReceive(out packet);

            public void Dispose() => _inner.Dispose();
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
