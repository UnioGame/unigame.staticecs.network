using System;
using System.IO;
using FFS.Libraries.StaticEcs;
using FFS.Libraries.StaticPack;
using NUnit.Framework;

namespace UniGame.StaticEcs.Network.Tests
{
    public sealed class NetworkV2Tests
    {
        [Test]
        public void TypeIdsAndPacketHeaderAreCanonicalV2()
        {
            var id = NetworkCompilerSupport.TypeId("SourceGenerator.Tests", "Demo.Position");
            Assert.That(id.Value, Is.EqualTo(4089044646u));
            Assert.That(NetworkCompilerSupport.TypeId("game.shared", "Demo.Position").Value, Is.EqualTo(1933934308u));
            var header = new PacketHeader
            {
                Kind = PacketKind.FullSnapshot,
                Compression = NetworkCompression.None,
                ServerTick = 42,
                TargetTick = PacketHeader.NoneTick,
                PayloadLength = 3,
                SchemaFingerprint = new SchemaFingerprint(1, 2),
                PayloadHash = 7
            };
            var bytes = new byte[PacketHeader.Size];
            Assert.That(header.TryWrite(bytes), Is.True);
            Assert.That(PacketHeader.TryRead(bytes, out var decoded), Is.True);
            Assert.That(PacketHeader.Version, Is.EqualTo(2));
            Assert.That(decoded.SchemaFingerprint, Is.EqualTo(header.SchemaFingerprint));
            bytes[10] = 1;
            Assert.That(PacketHeader.TryRead(bytes, out _), Is.False);
            header.Kind = PacketKind.Hello;
            Assert.That(NetworkPacket.TryEncode(header, new byte[] { 1, 2, 3 }, out var packet), Is.True);
            packet[packet.Length - 1] ^= 1;
            Assert.That(NetworkPacket.TryDecode(packet, header.SchemaFingerprint, out _, out _), Is.False);
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
            Assert.That(mock.ClientA.TrySend(new byte[] { 1 }), Is.True);
            Assert.That(mock.ClientB.TrySend(new byte[] { 2 }), Is.True);
            Assert.That(mock.ServerA.TryReceive(out var first), Is.True);
            Assert.That(mock.ServerB.TryReceive(out var second), Is.True);
            CollectionAssert.AreEqual(new byte[] { 1 }, first);
            CollectionAssert.AreEqual(new byte[] { 2 }, second);
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
            var capture = new NetworkSnapshot(7, default, new ScopeId(9), new byte[] { 1 }, 0, 0);
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
            var receiver = World<AuthorityWorld>.RegisterEventReceiver<NetworkCommandAccepted<TestCommand>>();
            try
            {
                var authoritySchema = Schema<AuthorityWorld>(true);
                var clientASchema = Schema<ClientAWorld>(false);
                var clientBSchema = Schema<ClientBWorld>(false);
                using var mock = new TwoClientNetworkMock();
                var server = new NetworkServer<AuthorityWorld>(authoritySchema, 4, 1024 * 1024);
                server.AddConnection(mock.ServerA, 2, 11, new ScopeId(7));
                server.AddConnection(mock.ServerB, 1, 12, new ScopeId(7));
                var clientA = new NetworkClient<ClientAWorld>(mock.ClientA, clientASchema, new ScopeId(7));
                var clientB = new NetworkClient<ClientBWorld>(mock.ClientB, clientBSchema, new ScopeId(7));
                var authority = World<AuthorityWorld>.NewEntity<TestEntity>();
                authority.Set<NetworkTag>();
                authority.Set(new TestComponent { Value = 1 });
                var gid = authority.GID;

                Assert.That(clientA.BeginHandshake(), Is.True);
                Assert.That(clientB.BeginHandshake(), Is.True);
                server.Tick(1);
                clientA.Process(); clientB.Process();
                Assert.That(clientA.Session.State, Is.EqualTo(NetworkSessionState.Established));
                Assert.That(clientB.Session.State, Is.EqualTo(NetworkSessionState.Established));
                Assert.That(gid.TryUnpack<ClientAWorld>(out var replicaA), Is.True);
                Assert.That(gid.TryUnpack<ClientBWorld>(out var replicaB), Is.True);
                Assert.That(replicaA.Read<TestComponent>().Value, Is.EqualTo(1));
                Assert.That(replicaB.Read<TestComponent>().Value, Is.EqualTo(1));

                authority.Set(new TestComponent { Value = 2 });
                Assert.That(clientA.SendCommand(new TestCommand { Value = 20 }, 2), Is.EqualTo(NetworkCommandResult.Queued));
                Assert.That(clientB.SendCommand(new TestCommand { Value = 10 }, 2), Is.EqualTo(NetworkCommandResult.Queued));
                server.Tick(2);
                clientA.Process(); clientB.Process();
                Assert.That(replicaA.Read<TestComponent>().Value, Is.EqualTo(2));
                Assert.That(replicaB.Read<TestComponent>().Value, Is.EqualTo(2));
                authority.Disable<TestComponent>();
                Assert.That(authority.HasDisabled<TestComponent>(), Is.True);
                server.Tick(3); clientA.Process(); clientB.Process();
                Assert.That(replicaA.HasDisabled<TestComponent>(), Is.True);
                Assert.That(replicaB.HasDisabled<TestComponent>(), Is.True);
                var index = 0;
                foreach (var item in receiver) { Assert.That(item.Value.Command.Value, Is.EqualTo(index++ == 0 ? 10 : 20)); }
                Assert.That(index, Is.EqualTo(2));
                Assert.That(clientA.AcknowledgedSnapshotTick, Is.EqualTo(3));
                Assert.That(clientA.AcknowledgedCommandSequence, Is.EqualTo(1));

                Assert.That(mock.ServerA.TrySend(new byte[] { 1, 2, 3 }), Is.True);
                clientA.Process();
                Assert.That(clientA.ResyncRequested, Is.True);

                authority.Destroy();
                server.Tick(4);
                clientA.Process(); clientB.Process();
                Assert.That(gid.TryUnpack<ClientAWorld>(out _), Is.False);
                Assert.That(gid.TryUnpack<ClientBWorld>(out _), Is.False);

                Assert.That(server.RemoveConnection(new ConnectionId(1)), Is.True);
                CreateReplicationWorld<ReconnectWorld>(false);
                MemoryNetworkTransport.CreatePair(new ConnectionId(1), out var reconnectTransport, out var reconnectServer);
                using (reconnectTransport) using (reconnectServer)
                {
                    server.AddConnection(reconnectServer, 2, 22, new ScopeId(7));
                    var reconnect = new NetworkClient<ReconnectWorld>(reconnectTransport, Schema<ReconnectWorld>(false), new ScopeId(7));
                    reconnect.BeginHandshake(); server.Tick(5); reconnect.Process();
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

        [Test]
        public void SchemaMismatchHandshakeRejectsAndRequestsResync()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var authoritySchema = Schema<AuthorityWorld>(true);
                var mismatchFactory = NetworkCompilerSupport.Create<MismatchWorld>();
                mismatchFactory.Entity<TestEntity>(new NetworkTypeId(1)); mismatchFactory.DisableableComponent<TestComponent>(new NetworkTypeId(2)); mismatchFactory.Tag<TestTag>(new NetworkTypeId(3)); mismatchFactory.Command<TestCommand>(new NetworkTypeId(10));
                var mismatchSchema = mismatchFactory.Freeze();
                MemoryNetworkTransport.CreatePair(new ConnectionId(99), out var clientTransport, out var serverTransport);
                using (clientTransport) using (serverTransport)
                {
                    var server = new NetworkServer<AuthorityWorld>(authoritySchema);
                    var serverSession = server.AddConnection(serverTransport, 9, 4, new ScopeId(1));
                    var client = new NetworkClient<MismatchWorld>(clientTransport, mismatchSchema, new ScopeId(1));
                    client.BeginHandshake(); server.Tick(1); client.Process();
                    Assert.That(serverSession.State, Is.EqualTo(NetworkSessionState.Rejected));
                    Assert.That(client.ResyncRequested, Is.True);
                }
            }
            finally { World<AuthorityWorld>.Destroy(); }
        }

        [Test]
        public void SnapshotConflictAndMalformedPacketNeverMutateClientLocalEntity()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ConflictWorld>(false);
            try
            {
                var authoritySchema = Schema<AuthorityWorld>(true);
                var clientSchema = Schema<ConflictWorld>(false);
                var authority = World<AuthorityWorld>.NewEntity<TestEntity>();
                authority.Set<NetworkTag>(); authority.Set(new TestComponent { Value = 5 });
                var local = World<ConflictWorld>.NewEntityByGID<TestEntity>(authority.GID);
                local.Set(new TestComponent { Value = 99 });
                var capture = new NetworkReplicator<AuthorityWorld>(authoritySchema, new ScopeId(3));
                Assert.That(capture.Capture(1, out var snapshot), Is.EqualTo(SnapshotCaptureResult.Success));
                var apply = new NetworkReplicator<ConflictWorld>(clientSchema, new ScopeId(3));
                Assert.That(apply.Stage(snapshot, out _), Is.EqualTo(SnapshotApplyResult.EntityConflict));
                Assert.That(local.Read<TestComponent>().Value, Is.EqualTo(99));

                var malformed = snapshot.Bytes.ToArray(); malformed[malformed.Length - 1] ^= 1;
                var bad = new NetworkSnapshot(1, snapshot.SchemaFingerprint, snapshot.Scope, malformed, snapshot.EntityCount, snapshot.RecordCount);
                Assert.That(apply.Stage(bad, out _), Is.Not.EqualTo(SnapshotApplyResult.Success));
                Assert.That(local.Read<TestComponent>().Value, Is.EqualTo(99));
            }
            finally { World<AuthorityWorld>.Destroy(); World<ConflictWorld>.Destroy(); }
        }

        [Test]
        public void ServerDispatchIsFailClosedWithoutPolicyAndPublishesRejectionWithPolicy()
        {
            World<RejectWorld>.Create(WorldConfig.Default());
            World<RejectWorld>.Types().Event<TestCommand>().Event<NetworkCommandAccepted<TestCommand>>().Event<NetworkCommandRejected<TestCommand>>();
            World<RejectWorld>.Initialize();
            var rejected = World<RejectWorld>.RegisterEventReceiver<NetworkCommandRejected<TestCommand>>();
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
                var coordinator = new NetworkServerCoordinator<RejectWorld>(); coordinator.Add(server);
                Assert.That(coordinator.Queue(command, 1), Is.EqualTo(NetworkCommandResult.Queued));
                Assert.That(coordinator.Dispatch(1), Is.EqualTo(1));
                var count = 0; foreach (var item in rejected) { Assert.That(item.Value.Command.Value, Is.EqualTo(7)); count++; }
                Assert.That(count, Is.EqualTo(1));
            }
            finally { World<RejectWorld>.DeleteEventReceiver(ref rejected); World<RejectWorld>.Destroy(); }
        }

        [Test]
        public void SessionsValidateAndDispatchCommandsInTargetPeerSequenceOrder()
        {
            World<TestWorld>.Create(WorldConfig.Default());
            World<TestWorld>.Types().Event<TestCommand>().Event<NetworkCommandAccepted<TestCommand>>().Event<NetworkCommandRejected<TestCommand>>();
            World<TestWorld>.Initialize();
            var receiver = World<TestWorld>.RegisterEventReceiver<NetworkCommandAccepted<TestCommand>>();
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
                Assert.That(coordinator.Dispatch(10), Is.EqualTo(2));
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
            StringAssert.Contains("\"client_server_tick_gap\":0", text);
            StringAssert.Contains("\"duration_ns\":0", text);
            StringAssert.Contains("\"schema_fingerprint\":", text);
            StringAssert.Contains("\"kind\":\"gap\"", text);
        }

        private static void CreateReplicationWorld<TWorld>(bool server) where TWorld : struct, IWorldType
        {
            World<TWorld>.Create(WorldConfig.Default());
            var types = World<TWorld>.Types();
            types.EntityType<TestEntity>(); types.Tag<NetworkTag>(); types.Component<TestComponent>(); types.Event<TestCommand>();
            if (server) { types.Event<NetworkCommandAccepted<TestCommand>>(); types.Event<NetworkCommandRejected<TestCommand>>(); }
            World<TWorld>.Initialize();
        }

        private static NetworkSchema<TWorld> Schema<TWorld>(bool server) where TWorld : struct, IWorldType
        {
            var factory = NetworkCompilerSupport.Create<TWorld>();
            factory.Entity<TestEntity>(new NetworkTypeId(1));
            factory.DisableableComponent<TestComponent>(new NetworkTypeId(2));
            if (server) factory.Command<TestCommand, AllowAnyPolicy<TWorld>>(new NetworkTypeId(10));
            else factory.Command<TestCommand>(new NetworkTypeId(10));
            return factory.Freeze();
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
        public struct AllowPolicy : INetworkCommandPolicy<TestWorld, TestCommand>
        {
            public bool Authorize(in NetworkCommandContext context, in TestCommand command) => true;
        }
        public struct AllowAnyPolicy<TWorld> : INetworkCommandPolicy<TWorld, TestCommand> where TWorld : struct, IWorldType { public bool Authorize(in NetworkCommandContext context, in TestCommand command) => true; }
        public struct RejectPolicy : INetworkCommandPolicy<RejectWorld, TestCommand> { public bool Authorize(in NetworkCommandContext context, in TestCommand command) => false; }
    }
}
