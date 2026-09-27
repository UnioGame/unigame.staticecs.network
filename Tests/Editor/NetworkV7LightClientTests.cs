using System;
using System.Diagnostics;
using FFS.Libraries.StaticEcs;
using NUnit.Framework;

namespace UniGame.StaticEcs.Network.Tests
{
    // NCORE-26b: end-to-end coverage for NetworkClient's canonicalOnlyApply ("light client") mode
    // through the real handshake/snapshot/ACK pipeline, reusing NetworkV7Tests' shared fixtures
    // (CreateReplicationWorld, Schema<TWorld>, Packet, TraceCollector, AuthorityWorld, TestEntity,
    // TestComponent). A light client's whole point is that it is indistinguishable from a full
    // client on the wire while never touching an ECS world, so every test here asserts both halves:
    // wire-observable behaviour (ACKs, tick/baseline progression, resync-on-corruption) together
    // with World<LightClientWorld>.Status staying NotCreated throughout.
    public sealed partial class NetworkV7Tests
    {
        public struct LightClientWorld : IWorldType { }

        [Test]
        public void LightClientNeverCreatesEcsWorldAndTracksBaselineAcrossKeyframeAndDelta()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                Assert.That(World<LightClientWorld>.Status, Is.EqualTo(WorldStatus.NotCreated));

                var authoritySchema = Schema<AuthorityWorld>(true);
                var lightSchema = Schema<LightClientWorld>(false);
                var authority = World<AuthorityWorld>.NewEntity<TestEntity>();
                authority.Set(new TestComponent { Value = 1 });

                MemoryNetworkTransport.CreatePair(new ConnectionId(201),
                    out var clientTransport, out var serverTransport);
                using (clientTransport)
                using (serverTransport)
                {
                    var server = new NetworkServer<AuthorityWorld>(authoritySchema,
                        (scope, entity) => true);
                    server.AddConnection(serverTransport, 1, 1, new ScopeId(1));
                    var observer = new TraceCollector();
                    using var client = new NetworkClient<LightClientWorld>(clientTransport,
                        lightSchema, new ScopeId(1), observer,
                        canonicalOnlyApply: true);

                    Assert.That(client.BeginHandshake(), Is.True);
                    server.Receive();
                    server.Tick(_ => { });
                    client.Process();

                    // First tick: keyframe. A light client never creates the world it is generic
                    // over — this is the direct proof it skipped NetworkReplicator.Stage/Apply.
                    Assert.That(World<LightClientWorld>.Status, Is.EqualTo(WorldStatus.NotCreated));
                    Assert.That(client.AcknowledgedSnapshotTick, Is.EqualTo(server.ServerTick));
                    Assert.That(client.History.Count, Is.EqualTo(1));
                    Assert.That(client.History.TryGet(server.ServerTick, out _), Is.True);
                    Assert.That(observer.Count(NetworkPhase.Send, NetworkPacketKind.Ack),
                        Is.EqualTo(1));

                    authority.Set(new TestComponent { Value = 2 });
                    server.Receive();
                    server.Tick(_ => { });
                    client.Process();

                    // Second tick: delta against the keyframe baseline History already retained.
                    Assert.That(World<LightClientWorld>.Status, Is.EqualTo(WorldStatus.NotCreated));
                    Assert.That(client.AcknowledgedSnapshotTick, Is.EqualTo(server.ServerTick));
                    Assert.That(client.History.Count, Is.EqualTo(2));
                    Assert.That(observer.Count(NetworkPhase.Send, NetworkPacketKind.Ack),
                        Is.EqualTo(2));
                    Assert.That(observer.Count(NetworkPhase.SnapshotApply),
                        Is.EqualTo(2));
                    // No malformed/schema/protocol trace category ever fired on this connection.
                    for (var i = 0; i < observer.Events.Count; i++)
                        Assert.That(observer.Events[i].Result,
                            Is.Not.EqualTo(NetworkResultCategory.Malformed));
                }
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
                if (World<LightClientWorld>.Status == WorldStatus.Initialized)
                    World<LightClientWorld>.Destroy();
            }
        }

        [Test]
        public void LightClientMalformedSnapshotRequestsKeyframeJustLikeFullClientWithoutAnyEcsWorld()
        {
            Assert.That(World<LightClientWorld>.Status, Is.EqualTo(WorldStatus.NotCreated));
            var schema = Schema<LightClientWorld>(false);
            MemoryNetworkTransport.CreatePair(new ConnectionId(202),
                out var clientTransport, out var serverTransport);
            using (clientTransport)
            using (serverTransport)
            using (var client = new NetworkClient<LightClientWorld>(clientTransport,
                       schema, new ScopeId(1), canonicalOnlyApply: true))
            {
                Assert.That(client.Session.Admit(schema.Fingerprint, 1, 1,
                    new ScopeId(1)), Is.EqualTo(NetworkAdmissionResult.Accepted));
                var packetHeader = Packet(PacketKind.SnapshotChunk, 1, 1);
                packetHeader.ServerTick = 1;
                packetHeader.SchemaFingerprint = schema.Fingerprint;
                // One byte is too short to even be a SnapshotChunkHeader (Size > 1): the exact
                // same malformed-payload shape CurrentVersionMalformedSnapshotRequestsKeyframe
                // exercises for a full client.
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
                Assert.That(client.AcknowledgedSnapshotTick, Is.Zero);
                Assert.That(client.History.Count, Is.Zero);
            }
            Assert.That(World<LightClientWorld>.Status, Is.EqualTo(WorldStatus.NotCreated));
        }

        [Test]
        public void FullAndLightClientsProduceIdenticalAckAndBaselineProgressionForTheSameServerTicks()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            try
            {
                Assert.That(World<LightClientWorld>.Status, Is.EqualTo(WorldStatus.NotCreated));

                var authoritySchema = Schema<AuthorityWorld>(true);
                var fullSchema = Schema<ClientAWorld>(false);
                var lightSchema = Schema<LightClientWorld>(false);
                var authority = World<AuthorityWorld>.NewEntity<TestEntity>();
                authority.Set(new TestComponent { Value = 0 });

                MemoryNetworkTransport.CreatePair(new ConnectionId(203),
                    out var fullClientTransport, out var fullServerTransport);
                MemoryNetworkTransport.CreatePair(new ConnectionId(204),
                    out var lightClientTransport, out var lightServerTransport);
                using (fullClientTransport) using (fullServerTransport)
                using (lightClientTransport) using (lightServerTransport)
                {
                    var server = new NetworkServer<AuthorityWorld>(authoritySchema,
                        (scope, entity) => true);
                    server.AddConnection(fullServerTransport, 1, 1, new ScopeId(1));
                    server.AddConnection(lightServerTransport, 2, 1, new ScopeId(1));
                    var fullObserver = new TraceCollector();
                    var lightObserver = new TraceCollector();
                    using var fullClient = new NetworkClient<ClientAWorld>(fullClientTransport,
                        fullSchema, new ScopeId(1), fullObserver);
                    using var lightClient = new NetworkClient<LightClientWorld>(lightClientTransport,
                        lightSchema, new ScopeId(1), lightObserver, canonicalOnlyApply: true);

                    Assert.That(fullClient.BeginHandshake(), Is.True);
                    Assert.That(lightClient.BeginHandshake(), Is.True);

                    for (var tick = 0; tick < 3; tick++)
                    {
                        authority.Set(new TestComponent { Value = tick });
                        server.Receive();
                        server.Tick(_ => { });
                        fullClient.Process();
                        lightClient.Process();

                        Assert.That(lightClient.AcknowledgedSnapshotTick,
                            Is.EqualTo(fullClient.AcknowledgedSnapshotTick),
                            "tick " + tick);
                        Assert.That(lightClient.History.Count,
                            Is.EqualTo(fullClient.History.Count), "tick " + tick);
                        Assert.That(
                            lightObserver.Count(NetworkPhase.Send, NetworkPacketKind.Ack),
                            Is.EqualTo(
                                fullObserver.Count(NetworkPhase.Send, NetworkPacketKind.Ack)),
                            "tick " + tick);
                    }

                    // The light peer never instantiated its generic world even after three
                    // established, acknowledged ticks; the full peer's world (created up front by
                    // CreateReplicationWorld) is unaffected by that difference.
                    Assert.That(World<LightClientWorld>.Status, Is.EqualTo(WorldStatus.NotCreated));
                    Assert.That(World<ClientAWorld>.Status, Is.EqualTo(WorldStatus.Initialized));
                }
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
                World<ClientAWorld>.Destroy();
                if (World<LightClientWorld>.Status == WorldStatus.Initialized)
                    World<LightClientWorld>.Destroy();
            }
        }

        [Test]
        public void ApplicationAckRetriesRejectedClientAcknowledgementAndReleasesNextSnapshot()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var authoritySchema = Schema<AuthorityWorld>(true);
                var lightSchema = Schema<LightClientWorld>(false);
                var authority = World<AuthorityWorld>.NewEntity<TestEntity>();
                authority.Set(new TestComponent { Value = 1 });
                MemoryNetworkTransport.CreatePair(new ConnectionId(205),
                    out var clientEndpoint, out var serverEndpoint);
                using (var transport = new RejectAcknowledgementTransport(clientEndpoint))
                using (serverEndpoint)
                using (var pool = new NetworkBufferPool(1024 * 1024))
                using (var server = new NetworkServer<AuthorityWorld>(authoritySchema,
                           static (_, _) => true,
                           snapshotAdmissionMode: NetworkSnapshotAdmissionMode.ApplicationAck))
                using (var client = new NetworkClient<LightClientWorld>(transport,
                           lightSchema, new ScopeId(1), bufferPool: pool,
                           canonicalOnlyApply: true))
                {
                    server.AddConnection(serverEndpoint, 1, 1, new ScopeId(1));
                    Assert.That(client.BeginHandshake(), Is.True);
                    server.Receive();
                    transport.RejectNextAcknowledgements = 2;
                    server.Tick(_ => { });
                    client.Process();
                    Assert.That(client.AcknowledgedSnapshotTick, Is.EqualTo(1));
                    Assert.That(transport.RejectedAcknowledgements, Is.EqualTo(1));

                    server.Receive();
                    server.Tick(_ => { });
                    Assert.That(server.CaptureSnapshotAdmissionDiagnostics()
                        .SuppressedSnapshots, Is.GreaterThan(0));
                    client.Process();
                    Assert.That(transport.RejectedAcknowledgements, Is.EqualTo(2));
                    client.Process();
                    Assert.That(transport.AcceptedAcknowledgements, Is.EqualTo(1));

                    authority.Set(new TestComponent { Value = 2 });
                    server.Receive();
                    server.Tick(_ => { });
                    client.Process();
                    Assert.That(client.AcknowledgedSnapshotTick,
                        Is.EqualTo(server.ServerTick));
                    Assert.That(transport.AcceptedAcknowledgements, Is.EqualTo(2));
                }
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
            }
        }

        [Test]
        public void PersistentAcknowledgementSendFailureClosesSessionAfterFiveSeconds()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var authoritySchema = Schema<AuthorityWorld>(true);
                var lightSchema = Schema<LightClientWorld>(false);
                var authority = World<AuthorityWorld>.NewEntity<TestEntity>();
                authority.Set(new TestComponent { Value = 1 });
                MemoryNetworkTransport.CreatePair(new ConnectionId(206),
                    out var clientEndpoint, out var serverEndpoint);
                using (var transport = new RejectAcknowledgementTransport(clientEndpoint))
                using (serverEndpoint)
                using (var server = new NetworkServer<AuthorityWorld>(authoritySchema,
                           static (_, _) => true,
                           snapshotAdmissionMode: NetworkSnapshotAdmissionMode.ApplicationAck))
                using (var client = new NetworkClient<LightClientWorld>(transport,
                           lightSchema, new ScopeId(1), canonicalOnlyApply: true))
                {
                    server.AddConnection(serverEndpoint, 1, 1, new ScopeId(1));
                    Assert.That(client.BeginHandshake(), Is.True);
                    server.Receive();
                    transport.RejectNextAcknowledgements = int.MaxValue;
                    server.Tick(_ => { });
                    client.Process();
                    client.Process(Stopwatch.GetTimestamp() + 6L * Stopwatch.Frequency);
                    Assert.That(client.Session.State, Is.EqualTo(NetworkSessionState.Closed));
                    Assert.That(client.TryConsumeRecoveryTransition(out var recovery), Is.True);
                    Assert.That(recovery.Phase,
                        Is.EqualTo(NetworkRecoveryPhase.DisconnectRequired));
                    Assert.That(recovery.Reason,
                        Is.EqualTo(NetworkRecoveryReason.AcknowledgementSendFailed));
                }
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
            }
        }

        private sealed class RejectAcknowledgementTransport : INetworkTransport
        {
            private readonly INetworkTransport _inner;

            internal RejectAcknowledgementTransport(INetworkTransport inner) =>
                _inner = inner;

            internal int RejectNextAcknowledgements { get; set; }
            internal int RejectedAcknowledgements { get; private set; }
            internal int AcceptedAcknowledgements { get; private set; }
            public ConnectionId Connection => _inner.Connection;
            public int MaxReliablePayloadBytes => _inner.MaxReliablePayloadBytes;
            public int MaxUnreliablePayloadBytes => _inner.MaxUnreliablePayloadBytes;

            public bool TrySend(NetworkBufferLease packet)
            {
                if (!NetworkPacket.TryDecode(packet, out var header, out _) ||
                    header.Kind != PacketKind.Ack)
                    return _inner.TrySend(packet);
                if (RejectNextAcknowledgements > 0)
                {
                    RejectNextAcknowledgements--;
                    RejectedAcknowledgements++;
                    packet.Dispose();
                    return false;
                }
                AcceptedAcknowledgements++;
                return _inner.TrySend(packet);
            }

            public bool TryReceive(out NetworkBufferLease packet) =>
                _inner.TryReceive(out packet);

            public void Dispose() => _inner.Dispose();
        }
    }
}
