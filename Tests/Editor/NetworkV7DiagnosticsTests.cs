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

        [Test]
        public void SnapshotPacketPreparationNestsEncodeAndTransportScopes()
        {
            Assert.That((int)NetworkDiagnosticPhase.Command, Is.Zero);
            Assert.That((int)NetworkDiagnosticPhase.OwnerLookup,
                Is.EqualTo(1));
            Assert.That((int)NetworkDiagnosticPhase.Snapshot, Is.EqualTo(2));
            Assert.That((int)NetworkDiagnosticPhase.PacketPreparation,
                Is.EqualTo(3));
            Assert.That((int)NetworkDiagnosticPhase.ReliableDrain,
                Is.EqualTo(4));
            Assert.That((int)NetworkDiagnosticPhase.NativeUpdate,
                Is.EqualTo(5));
            Assert.That((int)NetworkDiagnosticPhase.ReceiveCallback,
                Is.EqualTo(6));
            Assert.That((int)NetworkDiagnosticPhase.SnapshotChunkEncode,
                Is.EqualTo(7));
            Assert.That((int)NetworkDiagnosticPhase.TransportTrySend,
                Is.EqualTo(8));
            Assert.That((int)NetworkDiagnosticPhase.SnapshotDeltaEncode,
                Is.EqualTo(9));
            Assert.That((int)NetworkDiagnosticPhase.SnapshotDiagnostics,
                Is.EqualTo(10));
            Assert.That((int)NetworkDiagnosticPhase.SnapshotCapture,
                Is.EqualTo(11));
            Assert.That((int)NetworkDiagnosticPhase.Count, Is.EqualTo(12));

            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            MemoryNetworkTransport.CreatePair(new ConnectionId(921),
                out var clientTransport, out var serverTransport);
            var original = NetworkDiagnosticMarkers.Sink;
            var sink = new RecordingMarkerSink();
            try
            {
                NetworkDiagnosticMarkers.Sink = sink;
                using (clientTransport)
                using (serverTransport)
                using (var server = new NetworkServer<AuthorityWorld>(
                    Schema<AuthorityWorld>(true), static (_, _) => true))
                using (var client = new NetworkClient<ClientAWorld>(
                    clientTransport, Schema<ClientAWorld>(false),
                    new ScopeId(1)))
                {
                    var authority = World<AuthorityWorld>.NewEntity<TestEntity>();
                    authority.Set(new TestComponent { Value = 1 });
                    server.AddConnection(serverTransport, 1, 1,
                        new ScopeId(1));
                    client.BeginHandshake();
                    server.Receive();
                    server.BeginTick();
                    server.CompleteTick();
                    client.Process();

                    AssertRelevantSnapshotScopes(sink,
                        "begin:PacketPreparation",
                        "begin:SnapshotChunkEncode",
                        "end:SnapshotChunkEncode",
                        "begin:TransportTrySend",
                        "end:TransportTrySend",
                        "end:PacketPreparation");
                }
            }
            finally
            {
                NetworkDiagnosticMarkers.Sink = original;
                World<AuthorityWorld>.Destroy();
                World<ClientAWorld>.Destroy();
            }
        }

        [Test]
        public void FailedSnapshotTransportSendStillEmitsNestedTransportScope()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            MemoryNetworkTransport.CreatePair(new ConnectionId(922),
                out var clientTransport, out var serverInner);
            using (clientTransport)
            using (var serverTransport = new LimitedNetworkTransport(serverInner,
                serverInner.MaxUnreliablePayloadBytes))
            {
                var original = NetworkDiagnosticMarkers.Sink;
                var sink = new RecordingMarkerSink();
                var observer = new TraceCollector();
                try
                {
                    NetworkDiagnosticMarkers.Sink = sink;
                    using (var server = new NetworkServer<AuthorityWorld>(
                        Schema<AuthorityWorld>(true), static (_, _) => true,
                        observer: observer))
                    using (var client = new NetworkClient<ClientAWorld>(
                        clientTransport, Schema<ClientAWorld>(false),
                        new ScopeId(1)))
                    {
                        var authority = World<AuthorityWorld>.NewEntity<TestEntity>();
                        authority.Set(new TestComponent { Value = 1 });
                        server.AddConnection(serverTransport, 1, 1,
                            new ScopeId(1));
                        client.BeginHandshake();
                        server.Receive();
                        serverTransport.ResetSentPackets();
                        serverTransport.FailOnSendNumber = 1;
                        server.BeginTick();
                        server.CompleteTick();

                        var send = observer.Single(NetworkPhase.Send,
                            NetworkPacketKind.SnapshotChunk);
                        Assert.That(send.Result,
                            Is.EqualTo(NetworkResultCategory.Transport));
                        AssertRelevantSnapshotScopes(sink,
                            "begin:PacketPreparation",
                            "begin:SnapshotChunkEncode",
                            "end:SnapshotChunkEncode",
                            "begin:TransportTrySend",
                            "end:TransportTrySend",
                            "end:PacketPreparation");
                    }
                }
                finally
                {
                    NetworkDiagnosticMarkers.Sink = original;
                    World<AuthorityWorld>.Destroy();
                    World<ClientAWorld>.Destroy();
                }
            }
        }

        [Test]
        public void NonSnapshotTransportSendEmitsNoSnapshotDiagnosticScopes()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            MemoryNetworkTransport.CreatePair(new ConnectionId(923),
                out var clientTransport, out var serverInner);
            using (clientTransport)
            using (var serverTransport = new LimitedNetworkTransport(serverInner,
                serverInner.MaxUnreliablePayloadBytes))
            {
                var original = NetworkDiagnosticMarkers.Sink;
                var sink = new RecordingMarkerSink();
                try
                {
                    NetworkDiagnosticMarkers.Sink = sink;
                    using (var server = new NetworkServer<AuthorityWorld>(
                        Schema<AuthorityWorld>(true), static (_, _) => true))
                    using (var client = new NetworkClient<ClientAWorld>(
                        clientTransport, Schema<ClientAWorld>(false),
                        new ScopeId(1)))
                    {
                        server.AddConnection(serverTransport, 1, 1,
                            new ScopeId(1));
                        client.BeginHandshake();
                        server.Receive();

                        Assert.That(serverTransport.SentPacketCount,
                            Is.GreaterThan(0));
                        Assert.That(PhaseEventCount(sink,
                            NetworkDiagnosticPhase.SnapshotChunkEncode,
                            begin: true), Is.Zero);
                        Assert.That(PhaseEventCount(sink,
                            NetworkDiagnosticPhase.SnapshotChunkEncode,
                            begin: false), Is.Zero);
                        Assert.That(PhaseEventCount(sink,
                            NetworkDiagnosticPhase.TransportTrySend,
                            begin: true), Is.Zero);
                        Assert.That(PhaseEventCount(sink,
                            NetworkDiagnosticPhase.TransportTrySend,
                            begin: false), Is.Zero);
                    }
                }
                finally
                {
                    NetworkDiagnosticMarkers.Sink = original;
                    World<AuthorityWorld>.Destroy();
                    World<ClientAWorld>.Destroy();
                }
            }
        }

        [Test]
        public void ExactPreflightRejectionEmitsNoSnapshotPreparationScopes()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            MemoryNetworkTransport.CreatePair(new ConnectionId(924),
                out var clientTransport, out var serverEndpoint);
            var original = NetworkDiagnosticMarkers.Sink;
            var sink = new RecordingMarkerSink();
            try
            {
                using (clientTransport)
                using (var serverTransport = new PreflightNetworkTransport(
                           serverEndpoint))
                using (var server = new NetworkServer<AuthorityWorld>(
                    Schema<AuthorityWorld>(true), static (_, _) => true))
                {
                    var authority =
                        World<AuthorityWorld>.NewEntity<TestEntity>();
                    authority.Set(new TestComponent { Value = 1 });
                    server.AddConnection(serverTransport, 1, 1,
                        new ScopeId(1));
                    SendPeerPacket(clientTransport,
                        Schema<AuthorityWorld>(true).Fingerprint,
                        PacketKind.Hello, 0, 1, 0);
                    server.Receive();
                    Assert.That(clientTransport.TryReceive(out var ready),
                        Is.True);
                    ready.Dispose();

                    NetworkDiagnosticMarkers.Sink = sink;
                    serverTransport.ResetCounters();
                    serverTransport.ProbeResults.Enqueue(true);
                    serverTransport.ProbeResults.Enqueue(false);
                    server.BeginTick();
                    server.CompleteTick();

                    Assert.That(serverTransport.SentPacketCount, Is.Zero);
                    var phases = new[]
                    {
                        NetworkDiagnosticPhase.PacketPreparation,
                        NetworkDiagnosticPhase.SnapshotDeltaEncode,
                        NetworkDiagnosticPhase.SnapshotChunkEncode,
                        NetworkDiagnosticPhase.TransportTrySend,
                    };
                    Assert.That(CollectPhaseEvents(sink, phases), Is.Empty,
                        "an exact preflight rejection must emit no " +
                        "preparation, encode, or transport scopes");
                    for (var i = 0; i < phases.Length; i++)
                    {
                        Assert.That(PhaseEventCount(sink, phases[i],
                            begin: true), Is.Zero, "begin count for " +
                            phases[i]);
                        Assert.That(PhaseEventCount(sink, phases[i],
                            begin: false), Is.Zero, "end count for " +
                            phases[i]);
                    }
                    Assert.That(sink.Begins, Is.EqualTo(sink.Ends));
                }
            }
            finally
            {
                NetworkDiagnosticMarkers.Sink = original;
                World<AuthorityWorld>.Destroy();
            }
        }

        [Test]
        public void SnapshotDeltaEncodeScopeFollowsDeltaCacheDecision()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                var scope = new ScopeId(43);
                using var pool = new NetworkBufferPool(0);
                using var mock = new TwoClientNetworkMock();
                using var server = new NetworkServer<AuthorityWorld>(schema,
                    static (_, _) => true, bufferPool: pool);
                var original = NetworkDiagnosticMarkers.Sink;
                var sink = new RecordingMarkerSink();
                try
                {
                    server.AddConnection(mock.ServerA, 1, 11, scope);
                    server.AddConnection(mock.ServerB, 2, 22, scope);
                    SendPeerPacket(mock.ClientA, schema.Fingerprint,
                        PacketKind.Hello, 0, 1, 0);
                    SendPeerPacket(mock.ClientB, schema.Fingerprint,
                        PacketKind.Hello, 0, 1, 0);
                    server.Receive();
                    Assert.That(mock.ClientA.TryReceive(out var readyA),
                        Is.True);
                    readyA.Dispose();
                    Assert.That(mock.ClientB.TryReceive(out var readyB),
                        Is.True);
                    readyB.Dispose();

                    var first = World<AuthorityWorld>.NewEntity<TestEntity>();
                    first.Set(new TestComponent { Value = 1 });
                    var second = World<AuthorityWorld>.NewEntity<SecondEntity>();
                    second.Set(new TestComponent { Value = 10 });
                    server.Tick(_ => { });
                    ReceiveChunk(mock.ClientA);
                    ReceiveChunk(mock.ClientB);

                    SendPeerPacket(mock.ClientA, schema.Fingerprint,
                        PacketKind.Ack, 11, 2, 1);
                    SendPeerPacket(mock.ClientB, schema.Fingerprint,
                        PacketKind.Ack, 22, 2, 1);
                    server.Receive();

                    first.Set(new TestComponent { Value = 2 });
                    NetworkDiagnosticMarkers.Sink = sink;
                    server.Tick(_ => { });
                    NetworkDiagnosticMarkers.Sink = original;
                    ReceiveChunk(mock.ClientA);
                    ReceiveChunk(mock.ClientB);
                    Assert.That(PhaseEventCount(sink,
                        NetworkDiagnosticPhase.SnapshotDeltaEncode,
                        begin: true), Is.EqualTo(1),
                        "one cache-miss encode serves both peers");
                    Assert.That(PhaseEventCount(sink,
                        NetworkDiagnosticPhase.SnapshotDeltaEncode,
                        begin: false), Is.EqualTo(1));
                    Assert.That(PhaseEventCount(sink,
                        NetworkDiagnosticPhase.SnapshotDiagnostics,
                        begin: true), Is.EqualTo(2),
                        "one diagnostics report per peer");

                    SendPeerPacket(mock.ClientA, schema.Fingerprint,
                        PacketKind.Ack, 11, 3, 2);
                    server.Receive();

                    first.Set(new TestComponent { Value = 3 });
                    sink.Sequence.Clear();
                    NetworkDiagnosticMarkers.Sink = sink;
                    server.Tick(_ => { });
                    NetworkDiagnosticMarkers.Sink = original;
                    ReceiveChunk(mock.ClientA);
                    ReceiveChunk(mock.ClientB);
                    Assert.That(PhaseEventCount(sink,
                        NetworkDiagnosticPhase.SnapshotDeltaEncode,
                        begin: true), Is.EqualTo(2),
                        "different baselines encode separately");
                    Assert.That(PhaseEventCount(sink,
                        NetworkDiagnosticPhase.SnapshotDeltaEncode,
                        begin: false), Is.EqualTo(2));
                }
                finally
                {
                    NetworkDiagnosticMarkers.Sink = original;
                }

                server.Dispose();
                Assert.That(pool.CaptureDiagnostics().OutstandingLeases,
                    Is.Zero);
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
            }
        }

        [Test]
        public void SnapshotDeltaEncodeScopeEmitsOnceForCachedNegativeDecision()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                var scope = new ScopeId(44);
                using var pool = new NetworkBufferPool(0);
                using var mock = new TwoClientNetworkMock();
                using var server = new NetworkServer<AuthorityWorld>(schema,
                    static (_, _) => true, bufferPool: pool);
                var original = NetworkDiagnosticMarkers.Sink;
                var sink = new RecordingMarkerSink();
                try
                {
                    server.AddConnection(mock.ServerA, 1, 11, scope);
                    server.AddConnection(mock.ServerB, 2, 22, scope);
                    SendPeerPacket(mock.ClientA, schema.Fingerprint,
                        PacketKind.Hello, 0, 1, 0);
                    SendPeerPacket(mock.ClientB, schema.Fingerprint,
                        PacketKind.Hello, 0, 1, 0);
                    server.Receive();
                    Assert.That(mock.ClientA.TryReceive(out var readyA),
                        Is.True);
                    readyA.Dispose();
                    Assert.That(mock.ClientB.TryReceive(out var readyB),
                        Is.True);
                    readyB.Dispose();

                    // Tick 1 captures an empty scope, so the first entity add at
                    // tick 2 grows the delta past the canonical target length and
                    // the codec rejects the candidate.
                    server.Tick(_ => { });
                    Assert.That(ReceiveChunk(mock.ClientA).PayloadKind,
                        Is.EqualTo(SnapshotPayloadKind.Keyframe));
                    Assert.That(ReceiveChunk(mock.ClientB).PayloadKind,
                        Is.EqualTo(SnapshotPayloadKind.Keyframe));

                    SendPeerPacket(mock.ClientA, schema.Fingerprint,
                        PacketKind.Ack, 11, 2, 1);
                    SendPeerPacket(mock.ClientB, schema.Fingerprint,
                        PacketKind.Ack, 22, 2, 1);
                    server.Receive();

                    var entity = World<AuthorityWorld>.NewEntity<TestEntity>();
                    entity.Set(new TestComponent { Value = 1 });
                    NetworkDiagnosticMarkers.Sink = sink;
                    server.Tick(_ => { });
                    NetworkDiagnosticMarkers.Sink = original;
                    Assert.That(ReceiveChunk(mock.ClientA).PayloadKind,
                        Is.EqualTo(SnapshotPayloadKind.Keyframe));
                    Assert.That(ReceiveChunk(mock.ClientB).PayloadKind,
                        Is.EqualTo(SnapshotPayloadKind.Keyframe));
                    Assert.That(PhaseEventCount(sink,
                        NetworkDiagnosticPhase.SnapshotDeltaEncode,
                        begin: true), Is.EqualTo(1),
                        "a rejected candidate is cached, not re-encoded");
                    Assert.That(PhaseEventCount(sink,
                        NetworkDiagnosticPhase.SnapshotDeltaEncode,
                        begin: false), Is.EqualTo(1));
                }
                finally
                {
                    NetworkDiagnosticMarkers.Sink = original;
                }

                server.Dispose();
                Assert.That(pool.CaptureDiagnostics().OutstandingLeases,
                    Is.Zero);
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
            }
        }

        [Test]
        public void ExactRejectedCachedConsumerEmitsNoExtraEncodeScope()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                var scope = new ScopeId(45);
                using var pool = new NetworkBufferPool(0);
                using var mock = new TwoClientNetworkMock();
                using (var serverTransport =
                       new PreflightNetworkTransport(mock.ServerA))
                using (var server = new NetworkServer<AuthorityWorld>(schema,
                    static (_, _) => true, bufferPool: pool))
                {
                    var original = NetworkDiagnosticMarkers.Sink;
                    var sink = new RecordingMarkerSink();
                    try
                    {
                        // Peer B is served first, so peer A consumes the cached
                        // delta decision before its exact preflight rejects.
                        server.AddConnection(mock.ServerB, 2, 22, scope);
                        server.AddConnection(serverTransport, 1, 11, scope);
                        SendPeerPacket(mock.ClientA, schema.Fingerprint,
                            PacketKind.Hello, 0, 1, 0);
                        SendPeerPacket(mock.ClientB, schema.Fingerprint,
                            PacketKind.Hello, 0, 1, 0);
                        server.Receive();
                        Assert.That(mock.ClientA.TryReceive(out var readyA),
                            Is.True);
                        readyA.Dispose();
                        Assert.That(mock.ClientB.TryReceive(out var readyB),
                            Is.True);
                        readyB.Dispose();

                        var first =
                            World<AuthorityWorld>.NewEntity<TestEntity>();
                        first.Set(new TestComponent { Value = 1 });
                        var second =
                            World<AuthorityWorld>.NewEntity<SecondEntity>();
                        second.Set(new TestComponent { Value = 10 });
                        server.Tick(_ => { });
                        ReceiveChunk(mock.ClientA);
                        ReceiveChunk(mock.ClientB);

                        SendPeerPacket(mock.ClientA, schema.Fingerprint,
                            PacketKind.Ack, 11, 2, 1);
                        SendPeerPacket(mock.ClientB, schema.Fingerprint,
                            PacketKind.Ack, 22, 2, 1);
                        server.Receive();

                        first.Set(new TestComponent { Value = 2 });
                        serverTransport.ResetCounters();
                        serverTransport.ProbeResults.Enqueue(true);
                        serverTransport.ProbeResults.Enqueue(false);
                        NetworkDiagnosticMarkers.Sink = sink;
                        server.Tick(_ => { });
                        NetworkDiagnosticMarkers.Sink = original;

                        Assert.That(serverTransport.PreflightCalls,
                            Is.EqualTo(2));
                        Assert.That(serverTransport.SentPacketCount, Is.Zero,
                            "the exact preflight must reject the cached chunk");
                        Assert.That(mock.ClientA.TryReceive(out _), Is.False);
                        Assert.That(ReceiveChunk(mock.ClientB).PayloadKind,
                            Is.EqualTo(SnapshotPayloadKind.Delta));
                        Assert.That(PhaseEventCount(sink,
                            NetworkDiagnosticPhase.SnapshotDeltaEncode,
                            begin: true), Is.EqualTo(1),
                            "a cached consumer must not re-encode");
                        Assert.That(PhaseEventCount(sink,
                            NetworkDiagnosticPhase.SnapshotDeltaEncode,
                            begin: false), Is.EqualTo(1));
                    }
                    finally
                    {
                        NetworkDiagnosticMarkers.Sink = original;
                    }
                }

                Assert.That(pool.CaptureDiagnostics().OutstandingLeases,
                    Is.Zero);
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
            }
        }

        [Test]
        public void SnapshotDiagnosticsScopeNestsInsideSnapshotPerPeer()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            MemoryNetworkTransport.CreatePair(new ConnectionId(925),
                out var clientTransport, out var serverTransport);
            var original = NetworkDiagnosticMarkers.Sink;
            var sink = new RecordingMarkerSink();
            try
            {
                using (clientTransport)
                using (serverTransport)
                using (var server = new NetworkServer<AuthorityWorld>(
                    Schema<AuthorityWorld>(true), static (_, _) => true))
                using (var client = new NetworkClient<ClientAWorld>(
                    clientTransport, Schema<ClientAWorld>(false),
                    new ScopeId(1)))
                {
                    var authority =
                        World<AuthorityWorld>.NewEntity<TestEntity>();
                    authority.Set(new TestComponent { Value = 1 });
                    server.AddConnection(serverTransport, 1, 1,
                        new ScopeId(1));
                    client.BeginHandshake();
                    server.Receive();
                    NetworkDiagnosticMarkers.Sink = sink;
                    server.BeginTick();
                    server.CompleteTick();

                    Assert.That(sink.Begins, Is.EqualTo(sink.Ends));
                    var snapshotBegin = sink.Sequence.IndexOf(
                        "begin:Snapshot");
                    var diagnosticsBegin = sink.Sequence.IndexOf(
                        "begin:SnapshotDiagnostics");
                    var diagnosticsEnd = sink.Sequence.IndexOf(
                        "end:SnapshotDiagnostics");
                    var snapshotEnd = sink.Sequence.IndexOf("end:Snapshot");
                    Assert.That(snapshotBegin,
                        Is.GreaterThanOrEqualTo(0));
                    Assert.That(diagnosticsBegin,
                        Is.GreaterThan(snapshotBegin));
                    Assert.That(diagnosticsEnd,
                        Is.GreaterThan(diagnosticsBegin));
                    Assert.That(snapshotEnd, Is.GreaterThan(diagnosticsEnd));
                    Assert.That(PhaseEventCount(sink,
                        NetworkDiagnosticPhase.SnapshotDiagnostics,
                        begin: true), Is.EqualTo(1));
                    Assert.That(PhaseEventCount(sink,
                        NetworkDiagnosticPhase.SnapshotDiagnostics,
                        begin: false), Is.EqualTo(1));
                }
            }
            finally
            {
                NetworkDiagnosticMarkers.Sink = original;
                World<AuthorityWorld>.Destroy();
                World<ClientAWorld>.Destroy();
            }
        }

        [Test]
        public void DiagnosticScopeEndsWhenMeasuredBodyThrows()
        {
            var original = NetworkDiagnosticMarkers.Sink;
            var sink = new RecordingMarkerSink();
            try
            {
                NetworkDiagnosticMarkers.Sink = sink;
                Assert.Throws<InvalidOperationException>(() =>
                {
                    using (NetworkDiagnosticMarkers.Measure(
                               NetworkDiagnosticPhase.SnapshotDeltaEncode))
                        throw new InvalidOperationException("measured body");
                });
                Assert.That(sink.Sequence, Is.EqualTo(new[]
                {
                    "begin:SnapshotDeltaEncode", "end:SnapshotDeltaEncode"
                }));
                Assert.That(sink.Begins, Is.EqualTo(sink.Ends));
            }
            finally
            {
                NetworkDiagnosticMarkers.Sink = original;
            }
        }

        private static void AssertRelevantSnapshotScopes(
            RecordingMarkerSink sink, params string[] expected)
        {
            var phases = new[]
            {
                NetworkDiagnosticPhase.PacketPreparation,
                NetworkDiagnosticPhase.SnapshotChunkEncode,
                NetworkDiagnosticPhase.TransportTrySend,
            };
            Assert.That(CollectPhaseEvents(sink, phases),
                Is.EqualTo(expected));
            for (var i = 0; i < phases.Length; i++)
            {
                Assert.That(PhaseEventCount(sink, phases[i], begin: true),
                    Is.EqualTo(1), "begin count for " + phases[i]);
                Assert.That(PhaseEventCount(sink, phases[i], begin: false),
                    Is.EqualTo(1), "end count for " + phases[i]);
            }
            Assert.That(sink.Begins, Is.EqualTo(sink.Ends),
                "diagnostic scopes must remain balanced");
        }

        private static List<string> CollectPhaseEvents(
            RecordingMarkerSink sink, NetworkDiagnosticPhase[] phases)
        {
            var filtered = new List<string>();
            for (var i = 0; i < sink.Sequence.Count; i++)
            {
                var entry = sink.Sequence[i];
                for (var p = 0; p < phases.Length; p++)
                {
                    var name = phases[p].ToString();
                    if (entry == "begin:" + name || entry == "end:" + name)
                    {
                        filtered.Add(entry);
                        break;
                    }
                }
            }
            return filtered;
        }

        private static int PhaseEventCount(RecordingMarkerSink sink,
            NetworkDiagnosticPhase phase, bool begin)
        {
            var expected = (begin ? "begin:" : "end:") + phase;
            var count = 0;
            for (var i = 0; i < sink.Sequence.Count; i++)
                if (sink.Sequence[i] == expected)
                    count++;
            return count;
        }

        [Test]
        public void DiagnosticScopeBindsOneImmutableSinkAcrossReplacement()
        {
            var original = NetworkDiagnosticMarkers.Sink;
            var first = new RecordingMarkerSink();
            var second = new RecordingMarkerSink();
            try
            {
                NetworkDiagnosticMarkers.Sink = first;
                using (NetworkDiagnosticMarkers.Measure(
                           NetworkDiagnosticPhase.Command))
                {
                    NetworkDiagnosticMarkers.Sink = second;
                }

                Assert.That(first.Begins, Is.EqualTo(1));
                Assert.That(first.Ends, Is.EqualTo(1));
                Assert.That(second.Begins, Is.Zero);
                Assert.That(second.Ends, Is.Zero);
                Assert.That(first.Sequence, Is.EqualTo(new[]
                {
                    "begin:Command", "end:Command"
                }));
                Assert.That(second.Sequence, Is.Empty);
                Assert.That(NetworkDiagnosticMarkers.Sink,
                    Is.SameAs(second));
            }
            finally
            {
                NetworkDiagnosticMarkers.Sink = original;
            }
        }

        [Test]
        public void DiagnosticScopeSkipsEndWhenNoSinkWasBoundAtBegin()
        {
            var original = NetworkDiagnosticMarkers.Sink;
            var installed = new RecordingMarkerSink();
            try
            {
                NetworkDiagnosticMarkers.Sink = null;
                using (NetworkDiagnosticMarkers.Measure(
                           NetworkDiagnosticPhase.OwnerLookup))
                {
                    NetworkDiagnosticMarkers.Sink = installed;
                }

                Assert.That(installed.Begins, Is.Zero);
                Assert.That(installed.Ends, Is.Zero);
                Assert.That(installed.Sequence, Is.Empty);
            }
            finally
            {
                NetworkDiagnosticMarkers.Sink = original;
            }
        }

        private sealed class RecordingMarkerSink : INetworkDiagnosticMarkerSink
        {
            internal int Begins { get; private set; }
            internal int Ends { get; private set; }
            internal readonly List<string> Sequence = new List<string>();

            public void Begin(NetworkDiagnosticPhase phase)
            {
                Begins++;
                Sequence.Add("begin:" + phase);
            }

            public void End(NetworkDiagnosticPhase phase)
            {
                Ends++;
                Sequence.Add("end:" + phase);
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