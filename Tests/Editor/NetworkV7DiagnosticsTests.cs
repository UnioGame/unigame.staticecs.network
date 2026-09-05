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
        public void ServerDiagnosticsCountersTrackAdmissionRejectionAndReadyFailure()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            try
            {
                var serverSchema = Schema<AuthorityWorld>(true);
                var clientSchema = Schema<ClientAWorld>(false);
                MemoryNetworkTransport.CreatePair(new ConnectionId(901),
                    out var rejectClientTransport, out var rejectServerTransport);
                using (rejectClientTransport)
                using (rejectServerTransport)
                {
                    var events = new TraceCollector();
                    using (var server = new NetworkServer<AuthorityWorld>(
                        serverSchema, static (_, _) => true, observer: events,
                        admissionPolicy: new RecordingAdmissionPolicy(
                            new List<string>(), false)))
                    using (var client = new NetworkClient<ClientAWorld>(
                        rejectClientTransport, clientSchema))
                    {
                        server.AddConnection(rejectServerTransport, 1, 1,
                            new ScopeId(1));
                        client.BeginHandshake();
                        server.Receive();
                        client.Process();

                        var rejection = events.Single(NetworkPhase.Decode,
                            NetworkPacketKind.Hello);
                        Assert.That(rejection.ActiveConnections, Is.EqualTo(1));
                        Assert.That(rejection.ActivePeers, Is.Zero);
                        var disconnect = events.Single(NetworkPhase.Send,
                            NetworkPacketKind.Disconnect);
                        Assert.That(disconnect.ActiveConnections, Is.EqualTo(1));
                        Assert.That(disconnect.ActivePeers, Is.Zero);
                        Assert.That(server.ConnectionCount, Is.Zero);
                        Assert.That(client.Session.State,
                            Is.EqualTo(NetworkSessionState.Closed));
                    }
                }

                MemoryNetworkTransport.CreatePair(new ConnectionId(902),
                    out var rejectedEpochClientTransport,
                    out var rejectedEpochServerTransport);
                using (rejectedEpochClientTransport)
                using (rejectedEpochServerTransport)
                {
                    var events = new TraceCollector();
                    using (var server = new NetworkServer<AuthorityWorld>(
                        serverSchema, static (_, _) => true, observer: events))
                    using (var client = new NetworkClient<ClientAWorld>(
                        rejectedEpochClientTransport, clientSchema))
                    {
                        server.AddConnection(rejectedEpochServerTransport, 2, 0,
                            new ScopeId(1));
                        client.BeginHandshake();
                        server.Receive();
                        client.Process();

                        var rejection = events.Single(NetworkPhase.Decode,
                            NetworkPacketKind.Hello);
                        Assert.That(rejection.ActiveConnections, Is.Zero);
                        Assert.That(rejection.ActivePeers, Is.Zero);
                        Assert.That(server.ConnectionCount, Is.Zero);
                        Assert.That(client.Session.State,
                            Is.EqualTo(NetworkSessionState.Closed));
                    }
                }

                MemoryNetworkTransport.CreatePair(new ConnectionId(906),
                    out var readyClientTransport, out var readyServerInner);
                using (readyClientTransport)
                using (var readyServerTransport = new LimitedNetworkTransport(
                    readyServerInner, readyServerInner.MaxUnreliablePayloadBytes))
                {
                    readyServerTransport.FailOnSendNumber = 1;
                    var events = new TraceCollector();
                    using (var server = new NetworkServer<AuthorityWorld>(
                        serverSchema, static (_, _) => true, observer: events))
                    using (var client = new NetworkClient<ClientAWorld>(
                        readyClientTransport, clientSchema))
                    {
                        server.AddConnection(readyServerTransport, 2, 2,
                            new ScopeId(1));
                        client.BeginHandshake();
                        server.Receive();
                        client.Process();

                        var ready = events.Single(NetworkPhase.Send,
                            NetworkPacketKind.Ready);
                        Assert.That(ready.Result,
                            Is.EqualTo(NetworkResultCategory.Transport));
                        Assert.That(ready.ActiveConnections, Is.EqualTo(1));
                        Assert.That(ready.ActivePeers, Is.EqualTo(1));
                        var disconnect = events.Single(NetworkPhase.Send,
                            NetworkPacketKind.Disconnect);
                        Assert.That(disconnect.ActiveConnections, Is.Zero);
                        Assert.That(disconnect.ActivePeers, Is.Zero);
                        Assert.That(server.ConnectionCount, Is.Zero);
                        Assert.That(client.Session.State,
                            Is.EqualTo(NetworkSessionState.Closed));
                    }
                }
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
                World<ClientAWorld>.Destroy();
            }
        }

        [Test]
        public void ServerDiagnosticsCountersRemainIdempotentThroughObserverFailureAndDisposal()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            try
            {
                var serverSchema = Schema<AuthorityWorld>(true);
                var clientSchema = Schema<ClientAWorld>(false);
                MemoryNetworkTransport.CreatePair(new ConnectionId(903),
                    out var clientTransport, out var serverTransport);
                using (clientTransport)
                using (serverTransport)
                {
                    var events = new TraceCollector();
                    var order = new List<string>();
                    var peerObserver = new ThrowingPeerObserver(order);
                    using (var server = new NetworkServer<AuthorityWorld>(
                        serverSchema, static (_, _) => true, observer: events,
                        peerObserver: peerObserver))
                    using (var client = new NetworkClient<ClientAWorld>(
                        clientTransport, clientSchema))
                    {
                        server.AddConnection(serverTransport, 3, 3,
                            new ScopeId(1));
                        client.BeginHandshake();
                        server.Receive();
                        client.Process();

                        Assert.That(order, Is.EqualTo(new[]
                        {
                            "admitted", "disconnected"
                        }));
                        var disconnect = events.Single(NetworkPhase.Send,
                            NetworkPacketKind.Disconnect);
                        Assert.That(disconnect.ActiveConnections, Is.EqualTo(1));
                        Assert.That(disconnect.ActivePeers, Is.EqualTo(1));
                        Assert.That(server.ConnectionCount, Is.Zero);
                        Assert.That(server.RemoveConnection(new ConnectionId(903)),
                            Is.False);
                        Assert.That(server.RemoveConnection(new ConnectionId(903)),
                            Is.False);

                        events.Events.Clear();
                        MemoryNetworkTransport.CreatePair(new ConnectionId(904),
                            out var retryClientTransport, out var retryServerTransport);
                        using (retryClientTransport)
                        using (retryServerTransport)
                        using (var retryClient = new NetworkClient<ClientAWorld>(
                            retryClientTransport, clientSchema))
                        {
                            server.AddConnection(retryServerTransport, 4, 4,
                                new ScopeId(1));
                            retryClient.BeginHandshake();
                            server.Receive();
                            retryClient.Process();

                            var ready = events.Single(NetworkPhase.Send,
                                NetworkPacketKind.Ready);
                            Assert.That(ready.ActiveConnections, Is.EqualTo(1));
                            Assert.That(ready.ActivePeers, Is.EqualTo(1));
                            Assert.That(server.ConnectionCount, Is.Zero);
                        }
                        Assert.That(order, Is.EqualTo(new[]
                        {
                            "admitted", "disconnected", "admitted", "disconnected"
                        }));
                    }
                }

                MemoryNetworkTransport.CreatePair(new ConnectionId(905),
                    out var disposeClientTransport, out var disposeServerTransport);
                using (disposeClientTransport)
                using (disposeServerTransport)
                {
                    var peerObserver = new TestPeerObserver();
                    using (var server = new NetworkServer<AuthorityWorld>(
                        serverSchema, static (_, _) => true,
                        peerObserver: peerObserver))
                    using (var client = new NetworkClient<ClientAWorld>(
                        disposeClientTransport, clientSchema))
                    {
                        server.AddConnection(disposeServerTransport, 5, 5,
                            new ScopeId(1));
                        client.BeginHandshake();
                        server.Receive();
                        client.Process();
                        Assert.That(server.ConnectionCount, Is.EqualTo(1));

                        server.Dispose();
                        server.Dispose();
                        Assert.That(server.ConnectionCount, Is.Zero);
                        Assert.That(peerObserver.DisconnectedPeers.Count,
                            Is.EqualTo(1));
                    }
                }
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
                World<ClientAWorld>.Destroy();
            }
        }

        [Test]
        public void ReentrantDisconnectRemovalLeavesOtherPeersAndDisposesOnce()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            try
            {
                var serverSchema = Schema<AuthorityWorld>(true);
                var clientSchema = Schema<ClientAWorld>(false);
                MemoryNetworkTransport.CreatePair(new ConnectionId(911),
                    out var clientATransport, out var serverATransport);
                MemoryNetworkTransport.CreatePair(new ConnectionId(912),
                    out var clientBTransport, out var serverBTransport);
                using (clientATransport)
                using (serverATransport)
                using (clientBTransport)
                using (serverBTransport)
                {
                    var peerObserver = new ReentrantRemovalPeerObserver(
                        new ConnectionId(911));
                    using (var server = new NetworkServer<AuthorityWorld>(
                        serverSchema, static (_, _) => true,
                        peerObserver: peerObserver))
                    using (var clientA = new NetworkClient<ClientAWorld>(
                        clientATransport, clientSchema))
                    using (var clientB = new NetworkClient<ClientAWorld>(
                        clientBTransport, clientSchema))
                    {
                        peerObserver.Server = server;
                        server.AddConnection(serverATransport, 1, 1,
                            new ScopeId(1));
                        server.AddConnection(serverBTransport, 2, 2,
                            new ScopeId(1));
                        clientA.BeginHandshake();
                        clientB.BeginHandshake();
                        server.Receive();
                        clientA.Process();
                        clientB.Process();

                        Assert.That(server.ConnectionCount, Is.EqualTo(2));
                        Assert.That(server.RemoveConnection(new ConnectionId(911)),
                            Is.True);
                        Assert.That(peerObserver.ReentrantRemovalResult,
                            Is.False);
                        Assert.That(server.ConnectionCount, Is.EqualTo(1));
                        Assert.That(server.RemoveConnection(new ConnectionId(911)),
                            Is.False);
                        Assert.That(peerObserver.DisconnectedPeers.Count,
                            Is.EqualTo(1));

                        server.Dispose();
                        server.Dispose();
                        Assert.That(server.ConnectionCount, Is.Zero);
                        Assert.That(peerObserver.DisconnectedPeers.Count,
                            Is.EqualTo(2));
                        Assert.That(peerObserver.DisconnectedPeers[0].PeerId,
                            Is.EqualTo(1));
                        Assert.That(peerObserver.DisconnectedPeers[1].PeerId,
                            Is.EqualTo(2));
                    }
                }
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
                World<ClientAWorld>.Destroy();
            }
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
            var wrongRoleReplay = Packet(PacketKind.SnapshotChunk, 6, 99);
            Assert.That(classified.ValidatePacket(in wrongRoleReplay), Is.EqualTo(PacketValidationResult.WrongRole));
            var first = Packet(PacketKind.Ack, 7, 1);
            Assert.That(classified.ValidatePacket(in first), Is.EqualTo(PacketValidationResult.Success), "wrong-role rejection must not consume the cursor");

            var kinds = new[] { PacketKind.Hello, PacketKind.Ready, PacketKind.CommandBatch, PacketKind.SnapshotChunk, PacketKind.Ack, PacketKind.ResyncRequest, PacketKind.Disconnect, PacketKind.TransactionCommand, PacketKind.TransactionReceipt };
            for (var i = 0; i < kinds.Length; i++)
            {
                AssertPacketDirection(schema, NetworkRole.Server, kinds[i], kinds[i] == PacketKind.CommandBatch || kinds[i] == PacketKind.Ack || kinds[i] == PacketKind.ResyncRequest || kinds[i] == PacketKind.Disconnect || kinds[i] == PacketKind.TransactionCommand, (uint)(10 + i));
                AssertPacketDirection(schema, NetworkRole.Client, kinds[i], kinds[i] == PacketKind.SnapshotChunk || kinds[i] == PacketKind.ResyncRequest || kinds[i] == PacketKind.Disconnect || kinds[i] == PacketKind.TransactionReceipt, (uint)(30 + i));
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
        public void NdjsonWritesServerTickNameAndDuration()
        {
            using var stream = new MemoryStream();
            using (var log = new NetworkNdjsonLog(stream, 1))
            {
                var value = new NetworkTraceEvent(NetworkPhase.ServerTick,
                    NetworkTraceKind.Point, NetworkResultCategory.Success,
                    NetworkRole.Server, 0, 0, 0, 19, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 7, durationNanoseconds: 1234);
                log.Observe(in value);
            }

            var text = System.Text.Encoding.UTF8.GetString(stream.ToArray());
            StringAssert.Contains("\"phase\":\"server_tick\"", text);
            StringAssert.Contains("\"duration_ns\":1234", text);
        }

        [Test]
        public void WarmCommandAndSnapshotCoreAllocatesNoManagedMemoryPerTick()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            var pool = new NetworkBufferPool(4L << 20);
            MemoryNetworkTransport.CreatePair(new ConnectionId(801),
                out var clientEndpoint, out var serverEndpoint);
            var reliableLimit = PacketHeader.Size + SnapshotChunkHeader.Size + 8;
            var clientTransport = new LimitedNetworkTransport(clientEndpoint,
                clientEndpoint.MaxUnreliablePayloadBytes, reliableLimit);
            var serverTransport = new LimitedNetworkTransport(serverEndpoint,
                serverEndpoint.MaxUnreliablePayloadBytes, reliableLimit);
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

                serverTransport.ResetSentPackets();
                for (uint tick = 2; tick < 130; tick++)
                    RunCoreTick(client, server, tick);
                Assert.That(serverTransport.SentPacketCount,
                    Is.GreaterThan(128));

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

        private sealed class ReentrantRemovalPeerObserver : INetworkPeerObserver
        {
            private readonly ConnectionId _connection;
            internal NetworkServer<AuthorityWorld> Server;
            internal readonly List<NetworkPeerData> DisconnectedPeers =
                new List<NetworkPeerData>();
            internal bool ReentrantRemovalResult { get; private set; }

            internal ReentrantRemovalPeerObserver(ConnectionId connection)
            {
                _connection = connection;
            }

            public void Admitted(in NetworkPeerData peer)
            {
            }

            public void Disconnected(in NetworkPeerData peer)
            {
                DisconnectedPeers.Add(peer);
                ReentrantRemovalResult = Server.RemoveConnection(_connection);
            }
        }

    }
}