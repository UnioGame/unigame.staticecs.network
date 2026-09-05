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

        [TestCase(NetworkRecoveryReason.PredictionHistoryUnavailable,
            NetworkResyncReason.PredictionHistoryUnavailable,
            NetworkResyncSource.ClientPrediction)]
        [TestCase(NetworkRecoveryReason.SnapshotApplyFailed,
            NetworkResyncReason.SnapshotApplyFailed,
            NetworkResyncSource.None)]
        [TestCase(NetworkRecoveryReason.ProtocolIncompatible,
            NetworkResyncReason.ProtocolIncompatible,
            NetworkResyncSource.None)]
        public void ClientFullResyncTracesRecoveryReason(
            NetworkRecoveryReason recoveryReason,
            NetworkResyncReason expectedTraceReason,
            NetworkResyncSource expectedTraceSource)
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            try
            {
                MemoryNetworkTransport.CreatePair(new ConnectionId(96),
                    out var clientTransport, out var serverTransport);
                using (clientTransport)
                using (serverTransport)
                {
                    var observer = new TraceCollector();
                    var server = new NetworkServer<AuthorityWorld>(
                        Schema<AuthorityWorld>(true), static (_, _) => false);
                    server.AddConnection(serverTransport, 7, 15, new ScopeId(1));
                    var client = new NetworkClient<ClientAWorld>(clientTransport,
                        Schema<ClientAWorld>(false), new ScopeId(1), observer);
                    Assert.That(client.BeginHandshake(), Is.True);
                    server.Receive();
                    server.Tick(_ => { });
                    client.Process();
                    server.Receive();

                    observer.Events.Clear();
                    client.RequestFullResync(recoveryReason);

                    Assert.That(observer.Single(NetworkPhase.Send,
                        NetworkPacketKind.ResyncRequest).ResyncReason,
                        Is.EqualTo(expectedTraceReason));
                    Assert.That(observer.Single(NetworkPhase.Send,
                        NetworkPacketKind.ResyncRequest).ResyncSource,
                        Is.EqualTo(expectedTraceSource));
                }
            }
            finally
            {
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

                    var disconnect = Packet(PacketKind.Disconnect, 9, 2);
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
                    var packetHeader = Packet(PacketKind.SnapshotChunk, 1, 1);
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
        public void SnapshotDeltaCodec_ReconstructsNoOpAndCanonicalChanges()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            var pool = new NetworkBufferPool(4L << 20);
            var schema = Schema<AuthorityWorld>(true);
            var scope = new ScopeId(17);
            var replicator = new NetworkReplicator<AuthorityWorld>(schema,
                static (_, _) => true, scope, bufferPool: pool);
            NetworkSnapshot baseline = null;
            NetworkSnapshot unchanged = null;
            NetworkSnapshot target = null;
            NetworkSnapshot reconstructed = null;
            NetworkBufferLease delta = null;
            NetworkBufferLease canonical = null;
            try
            {
                var patched = World<AuthorityWorld>.NewEntity<TestEntity>();
                patched.Set(new TestComponent { Value = 1 });
                patched.Set<TestTag>();
                var removed = World<AuthorityWorld>.NewEntity<SecondEntity>();
                removed.Set(new TestComponent { Value = 2 });
                var metadataOnly = World<AuthorityWorld>.NewEntity<TestEntity>();

                Assert.That(replicator.Capture(1, out baseline),
                    Is.EqualTo(SnapshotCaptureResult.Success));
                var borrowedBytes = baseline.Bytes.ToArray();
                Assert.That(replicator.Capture(2, out unchanged),
                    Is.EqualTo(SnapshotCaptureResult.Success));
                Assert.That(SnapshotDeltaCodec.TryEncode(pool, baseline,
                    unchanged, out delta), Is.True);
                Assert.That(delta.Length, Is.EqualTo(12));
                Assert.That(Read32(delta.Span, 8), Is.Zero);
                var header = DeltaHeader(baseline, unchanged);
                Assert.That(SnapshotDeltaCodec.TryReconstruct(pool, baseline,
                    delta.Span, in header, schema.Fingerprint, scope,
                    out canonical, out var entities, out var records), Is.True);
                reconstructed = replicator.CreateSnapshot(header.SnapshotTick,
                    schema.Fingerprint, scope, canonical, entities, records);
                canonical = null;
                Assert.That(reconstructed.Bytes.Span.SequenceEqual(
                    unchanged.Bytes.Span), Is.True);
                var pooledDescriptor = reconstructed;
                reconstructed.Dispose();
                reconstructed = null;
                delta.Dispose();
                delta = null;
                unchanged.Dispose();
                unchanged = null;

                patched.Set(new TestComponent { Value = 3 });
                patched.Delete<TestTag>();
                patched.Set(new NetworkOwnerComponent { PeerId = 9 });
                metadataOnly.Disable();
                removed.Destroy();
                var added = World<AuthorityWorld>.NewEntity<SecondEntity>();
                added.Set(new TestComponent { Value = 4 });
                Assert.That(replicator.Capture(3, out target),
                    Is.EqualTo(SnapshotCaptureResult.Success));

                Assert.That(SnapshotDeltaCodec.TryEncode(pool, baseline,
                    target, out delta), Is.True);
                Assert.That(Read32(delta.Span, 8), Is.GreaterThan(0));
                header = DeltaHeader(baseline, target);
                Assert.That(header.TotalLength, Is.EqualTo(target.ByteLength));
                Assert.That(header.TotalHash, Is.EqualTo(target.PayloadHash));
                Assert.That(SnapshotDeltaCodec.TryReconstruct(pool, baseline,
                    delta.Span, in header, schema.Fingerprint, scope,
                    out canonical, out entities, out records), Is.True);
                reconstructed = replicator.CreateSnapshot(header.SnapshotTick,
                    schema.Fingerprint, scope, canonical, entities, records);
                canonical = null;
                Assert.That(reconstructed, Is.SameAs(pooledDescriptor));
                Assert.That(reconstructed.EntityCount,
                    Is.EqualTo(target.EntityCount));
                Assert.That(reconstructed.RecordCount,
                    Is.EqualTo(target.RecordCount));
                Assert.That(reconstructed.Bytes.Span.SequenceEqual(
                    target.Bytes.Span), Is.True);
                Assert.That(baseline.Bytes.Span.SequenceEqual(borrowedBytes),
                    Is.True, "borrowed baseline must remain unchanged");
            }
            finally
            {
                canonical?.Dispose();
                reconstructed?.Dispose();
                delta?.Dispose();
                target?.Dispose();
                unchanged?.Dispose();
                baseline?.Dispose();
                replicator.Dispose();
                Assert.That(pool.CaptureDiagnostics().OutstandingLeases,
                    Is.Zero);
                pool.Dispose();
                World<AuthorityWorld>.Destroy();
            }
        }

        [Test]
        public void SnapshotDeltaCodec_RejectsMalformedAndInvalidOperations()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            var pool = new NetworkBufferPool(4L << 20);
            var schema = Schema<AuthorityWorld>(true);
            var scope = new ScopeId(19);
            var replicator = new NetworkReplicator<AuthorityWorld>(schema,
                static (_, _) => true, scope, bufferPool: pool);
            var snapshots = new List<NetworkSnapshot>();
            var leases = new List<NetworkBufferLease>();
            try
            {
                var first = World<AuthorityWorld>.NewEntity<TestEntity>();
                first.Set(new TestComponent { Value = 1 });
                first.Set<TestTag>();
                var second = World<AuthorityWorld>.NewEntity<SecondEntity>();
                second.Set(new TestComponent { Value = 2 });
                Assert.That(replicator.Capture(1, out var baseline),
                    Is.EqualTo(SnapshotCaptureResult.Success));
                snapshots.Add(baseline);
                first.Set(new TestComponent { Value = 7 });
                first.Delete<TestTag>();
                first.Set(new NetworkOwnerComponent { PeerId = 5 });
                Assert.That(replicator.Capture(2, out var target),
                    Is.EqualTo(SnapshotCaptureResult.Success));
                snapshots.Add(target);
                Assert.That(SnapshotDeltaCodec.TryEncode(pool, baseline,
                    target, out var patchDelta), Is.True);
                leases.Add(patchDelta);
                var header = DeltaHeader(baseline, target);

                AssertDeltaRejected(pool, baseline,
                    patchDelta.Span.Slice(0, patchDelta.Length - 1), in header,
                    schema.Fingerprint, scope);
                var unknownOperation = patchDelta.Span.ToArray();
                unknownOperation[12] = 0;
                AssertDeltaRejected(pool, baseline, unknownOperation, in header,
                    schema.Fingerprint, scope);
                var wrongCount = patchDelta.Span.ToArray();
                Write32(wrongCount, 0, Read32(wrongCount, 0) + 1);
                AssertDeltaRejected(pool, baseline, wrongCount, in header,
                    schema.Fingerprint, scope);
                var wrongLength = header;
                wrongLength.TotalLength++;
                AssertDeltaRejected(pool, baseline, patchDelta.Span,
                    in wrongLength, schema.Fingerprint, scope);
                var wrongHash = header;
                wrongHash.TotalHash ^= 1;
                AssertDeltaRejected(pool, baseline, patchDelta.Span,
                    in wrongHash, schema.Fingerprint, scope);

                Assert.That(replicator.Capture(10, out var removeBaseline),
                    Is.EqualTo(SnapshotCaptureResult.Success));
                snapshots.Add(removeBaseline);
                first.Destroy();
                second.Destroy();
                Assert.That(replicator.Capture(10, out var emptyBaseline),
                    Is.EqualTo(SnapshotCaptureResult.Success));
                snapshots.Add(emptyBaseline);
                Assert.That(replicator.Capture(11, out var emptyTarget),
                    Is.EqualTo(SnapshotCaptureResult.Success));
                snapshots.Add(emptyTarget);
                Assert.That(SnapshotDeltaCodec.TryEncode(pool, removeBaseline,
                    emptyTarget, out var removeDelta), Is.True);
                leases.Add(removeDelta);
                Assert.That(removeDelta.Length, Is.EqualTo(30));
                Assert.That(Read32(removeDelta.Span, 8), Is.EqualTo(2));
                var removeHeader = DeltaHeader(removeBaseline, emptyTarget);
                var reordered = removeDelta.Span.ToArray();
                Swap(reordered, 12, 21, 9);
                AssertDeltaRejected(pool, removeBaseline, reordered,
                    in removeHeader, schema.Fingerprint, scope);
                var duplicate = removeDelta.Span.ToArray();
                Array.Copy(duplicate, 12, duplicate, 21, 9);
                AssertDeltaRejected(pool, removeBaseline, duplicate,
                    in removeHeader, schema.Fingerprint, scope);
                AssertDeltaRejected(pool, emptyBaseline, removeDelta.Span,
                    in removeHeader, schema.Fingerprint, scope);

                var replacement = World<AuthorityWorld>.NewEntity<TestEntity>();
                Assert.That(replicator.Capture(20,
                    out var missingRecordBaseline),
                    Is.EqualTo(SnapshotCaptureResult.Success));
                snapshots.Add(missingRecordBaseline);
                replacement.Set(new TestComponent { Value = 1 });
                Assert.That(replicator.Capture(20, out var replaceBaseline),
                    Is.EqualTo(SnapshotCaptureResult.Success));
                snapshots.Add(replaceBaseline);
                replacement.Set(new TestComponent { Value = 2 });
                Assert.That(replicator.Capture(21, out var replaceTarget),
                    Is.EqualTo(SnapshotCaptureResult.Success));
                snapshots.Add(replaceTarget);
                Assert.That(SnapshotDeltaCodec.TryEncode(pool, replaceBaseline,
                    replaceTarget, out var replaceDelta), Is.True);
                leases.Add(replaceDelta);
                var replaceHeader = DeltaHeader(replaceBaseline, replaceTarget);
                AssertDeltaRejected(pool, missingRecordBaseline,
                    replaceDelta.Span, in replaceHeader, schema.Fingerprint,
                    scope);
                Assert.That(pool.CaptureDiagnostics().OutstandingLeases,
                    Is.EqualTo(snapshots.Count + leases.Count));
            }
            finally
            {
                for (var i = 0; i < leases.Count; i++)
                    leases[i].Dispose();
                for (var i = 0; i < snapshots.Count; i++)
                    snapshots[i].Dispose();
                replicator.Dispose();
                Assert.That(pool.CaptureDiagnostics().OutstandingLeases,
                    Is.Zero);
                pool.Dispose();
                World<AuthorityWorld>.Destroy();
            }
        }

        [Test]
        public void RepeatedLocalResyncKeepsCorrelationUntilKeyframeAck()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            var pool = new NetworkBufferPool(4L << 20);
            NetworkSimulator simulator = null;
            NetworkServer<AuthorityWorld> server = null;
            NetworkClient<ClientAWorld> client = null;
            var observer = new TraceCollector();
            try
            {
                var immediate = NetworkSimulationPresets.Create(
                    NetworkSimulationPreset.Immediate);
                simulator = new NetworkSimulator(new ConnectionId(840),
                    in immediate);
                server = new NetworkServer<AuthorityWorld>(
                    Schema<AuthorityWorld>(true), static (_, _) => true,
                    bufferPool: pool);
                client = new NetworkClient<ClientAWorld>(simulator.Client,
                    Schema<ClientAWorld>(false), new ScopeId(1), observer,
                    bufferPool: pool);
                var entity = World<AuthorityWorld>.NewEntity<TestEntity>();
                entity.Set(new TestComponent { Value = 1 });
                server.AddConnection(simulator.Server, 1, 1, new ScopeId(1));

                Assert.That(client.BeginHandshake(), Is.True);
                simulator.Advance(0);
                server.Receive();
                server.Tick(_ => { });
                simulator.Advance(0);
                client.Process();
                simulator.Advance(0);
                server.Receive();
                Assert.That(client.Session.State,
                    Is.EqualTo(NetworkSessionState.Established));

                observer.Events.Clear();
                client.RequestFullResync(
                    NetworkRecoveryReason.PredictionHistoryUnavailable);
                simulator.Advance(0);
                server.Receive();
                client.RequestFullResync(NetworkRecoveryReason.SnapshotRejected);

                uint firstCorrelation = 0;
                uint secondCorrelation = 0;
                var requestCount = 0;
                for (var index = 0; index < observer.Events.Count; index++)
                {
                    var value = observer.Events[index];
                    if (value.Phase != NetworkPhase.Send ||
                        value.PacketKind != NetworkPacketKind.ResyncRequest)
                        continue;
                    if (requestCount++ == 0)
                        firstCorrelation = value.ResyncCorrelationId;
                    else
                        secondCorrelation = value.ResyncCorrelationId;
                }
                Assert.That(requestCount, Is.EqualTo(2));
                Assert.That(firstCorrelation, Is.Not.Zero);
                Assert.That(secondCorrelation, Is.EqualTo(firstCorrelation));

                entity.Set(new TestComponent { Value = 2 });
                server.Tick(_ => { });
                var keyframeTick = server.ServerTick;
                simulator.Advance(0);
                client.Process();
                simulator.Advance(0);
                server.Receive();

                Assert.That(client.AcknowledgedSnapshotTick,
                    Is.EqualTo(keyframeTick));
                Assert.That(client.Session.State,
                    Is.EqualTo(NetworkSessionState.Established));
                Assert.That(server.ConnectionCount, Is.EqualTo(1));
                Assert.That(observer.Single(NetworkPhase.Send,
                        NetworkPacketKind.Ack).ResyncCorrelationId,
                    Is.EqualTo(firstCorrelation));
                var stats = simulator.CaptureStats();
                Assert.That(stats.ClientToServer.QueuedPackets, Is.Zero);
                Assert.That(stats.ServerToClient.QueuedPackets, Is.Zero);
                Assert.That(stats.ReplayErrors, Is.Zero);
            }
            finally
            {
                client?.Dispose();
                server?.Dispose();
                simulator?.Dispose();
                Assert.That(pool.CaptureDiagnostics().OutstandingLeases,
                    Is.Zero);
                pool.Dispose();
                if (World<AuthorityWorld>.Status == WorldStatus.Initialized)
                    World<AuthorityWorld>.Destroy();
                if (World<ClientAWorld>.Status == WorldStatus.Initialized)
                    World<ClientAWorld>.Destroy();
            }
        }

        [Test]
        public void SnapshotChunksRespectBoundariesReorderRecoveryAndOwnership()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            var pool = new NetworkBufferPool(64L << 20);
            NetworkReplicator<AuthorityWorld> probe = null;
            NetworkSnapshot sample = null;
            NetworkBufferLease first = null;
            NetworkBufferLease second = null;
            NetworkBufferLease late = null;
            LimitedNetworkTransport clientTransport = null;
            LimitedNetworkTransport serverTransport = null;
            NetworkServer<AuthorityWorld> server = null;
            NetworkClient<ClientAWorld> client = null;
            var observer = new TraceCollector();
            try
            {
                var authoritySchema = Schema<AuthorityWorld>(true);
                var clientSchema = Schema<ClientAWorld>(false);
                var scope = new ScopeId(41);
                Assert.That(clientSchema.Fingerprint,
                    Is.EqualTo(authoritySchema.Fingerprint));
                MemoryNetworkTransport.CreatePair(new ConnectionId(841),
                    out var clientEndpoint, out var serverEndpoint);
                clientTransport = new LimitedNetworkTransport(clientEndpoint,
                    clientEndpoint.MaxUnreliablePayloadBytes);
                serverTransport = new LimitedNetworkTransport(serverEndpoint,
                    serverEndpoint.MaxUnreliablePayloadBytes);
                server = new NetworkServer<AuthorityWorld>(authoritySchema,
                    static (_, _) => true, bufferPool: pool);
                client = new NetworkClient<ClientAWorld>(clientTransport,
                    clientSchema, scope, observer, bufferPool: pool);
                probe = new NetworkReplicator<AuthorityWorld>(authoritySchema,
                    static (_, _) => true, scope, bufferPool: pool);
                var entity = World<AuthorityWorld>.NewEntity<TestEntity>();
                entity.Set(new TestComponent { Value = 1 });
                Assert.That(probe.Capture(1, out sample),
                    Is.EqualTo(SnapshotCaptureResult.Success));
                var exactPacketBytes = checked(PacketHeader.Size +
                    SnapshotChunkHeader.Size + sample.ByteLength);
                Assert.That(sample.ByteLength, Is.GreaterThan(1));

                server.AddConnection(serverTransport, 1, 1, scope);
                Assert.That(client.BeginHandshake(), Is.True);
                server.Receive();
                client.Process();
                clientTransport.MaxReliablePayloadBytes = exactPacketBytes;
                serverTransport.MaxReliablePayloadBytes = exactPacketBytes;
                serverTransport.ResetSentPackets();
                server.Tick(_ => { });
                Assert.That(serverTransport.SentPacketCount, Is.EqualTo(1));
                Assert.That(serverTransport.LargestSentPacketBytes,
                    Is.EqualTo(exactPacketBytes));
                client.Process();
                server.Receive();
                Assert.That(client.AcknowledgedSnapshotTick, Is.EqualTo(1));
                Assert.That(ReadReplicaValue(), Is.EqualTo(1));

                entity.Set(new TestComponent { Value = 2 });
                observer.Events.Clear();
                client.RequestFullResync(
                    NetworkRecoveryReason.PredictionHistoryUnavailable);
                var correlationId = observer.Single(NetworkPhase.Send,
                    NetworkPacketKind.ResyncRequest).ResyncCorrelationId;
                Assert.That(correlationId, Is.Not.Zero);
                server.Receive();
                clientTransport.MaxReliablePayloadBytes = exactPacketBytes - 1;
                serverTransport.MaxReliablePayloadBytes = exactPacketBytes - 1;
                serverTransport.ResetSentPackets();
                server.Tick(_ => { });
                Assert.That(serverTransport.SentPacketCount, Is.EqualTo(2));
                Assert.That(serverTransport.LargestSentPacketBytes,
                    Is.LessThanOrEqualTo(exactPacketBytes - 1));
                Assert.That(clientTransport.TryReceive(out first), Is.True);
                Assert.That(clientTransport.TryReceive(out second), Is.True);
                var firstChunk = InspectSnapshotChunk(first, out _);
                var secondChunk = InspectSnapshotChunk(second, out _);
                Assert.That(firstChunk.ChunkIndex, Is.Zero);
                Assert.That(secondChunk.ChunkIndex, Is.EqualTo(1));
                Assert.That(firstChunk.ResyncCorrelationId,
                    Is.EqualTo(correlationId));
                Assert.That(secondChunk.ResyncCorrelationId,
                    Is.EqualTo(correlationId));
                Assert.That(serverTransport.TrySend(second.Retain()), Is.True);
                Assert.That(serverTransport.TrySend(second), Is.True);
                second = null;
                Assert.That(serverTransport.TrySend(first), Is.True);
                first = null;
                client.Process();
                server.Receive();
                Assert.That(client.AcknowledgedSnapshotTick, Is.EqualTo(2));
                Assert.That(ReadReplicaValue(), Is.EqualTo(2));
                Assert.That(observer.Single(NetworkPhase.SnapshotApply)
                    .ResyncCorrelationId, Is.EqualTo(correlationId));
                Assert.That(observer.Single(NetworkPhase.Send,
                    NetworkPacketKind.Ack).ResyncCorrelationId,
                    Is.EqualTo(correlationId));

                entity.Set(new TestComponent { Value = 3 });
                client.RequestFullResync(NetworkRecoveryReason.SnapshotRejected);
                server.Receive();
                serverTransport.ResetSentPackets();
                serverTransport.FailOnSendNumber = 2;
                server.Tick(_ => { });
                Assert.That(serverTransport.SentPacketCount, Is.EqualTo(2));
                Assert.That(clientTransport.TryReceive(out first), Is.True);
                Assert.That(InspectSnapshotChunk(first, out _).ChunkIndex,
                    Is.Zero);
                late = first.Retain();
                serverTransport.FailOnSendNumber = 0;
                Assert.That(serverTransport.TrySend(first), Is.True);
                first = null;
                client.Process();
                Assert.That(client.AcknowledgedSnapshotTick, Is.EqualTo(2));

                entity.Set(new TestComponent { Value = 4 });
                serverTransport.ResetSentPackets();
                server.Tick(_ => { });
                Assert.That(serverTransport.SentPacketCount, Is.EqualTo(2));
                Assert.That(clientTransport.TryReceive(out first), Is.True);
                Assert.That(clientTransport.TryReceive(out second), Is.True);
                Assert.That(serverTransport.TrySend(second.Retain()), Is.True);
                Assert.That(serverTransport.TrySend(second), Is.True);
                second = null;
                Assert.That(serverTransport.TrySend(first), Is.True);
                first = null;
                Assert.That(serverTransport.TrySend(late), Is.True);
                late = null;
                client.Process();
                server.Receive();
                Assert.That(client.AcknowledgedSnapshotTick, Is.EqualTo(4));
                Assert.That(ReadReplicaValue(), Is.EqualTo(4));

                entity.Set(new TestComponent { Value = 5 });
                client.RequestFullResync(NetworkRecoveryReason.SnapshotRejected);
                server.Receive();
                server.Tick(_ => { });
                Assert.That(clientTransport.TryReceive(out first), Is.True);
                Assert.That(clientTransport.TryReceive(out second), Is.True);
                var conflictChunk = InspectSnapshotChunk(first,
                    out var conflictBody);
                var conflict = conflictBody.ToArray();
                conflict[conflict.Length - 1] ^= 1;
                Assert.That(serverTransport.TrySend(first.Retain()), Is.True);
                SendSnapshotChunk(serverTransport, clientSchema.Fingerprint,
                    conflictChunk.ChunkIndex + 1, conflictChunk, conflict);
                client.Process();
                Assert.That(client.AcknowledgedSnapshotTick, Is.EqualTo(4));
                Assert.That(ReadReplicaValue(), Is.EqualTo(4));
                Assert.That(client.TryConsumeRecoveryTransition(
                    out var recovery), Is.True);
                Assert.That(recovery.Phase,
                    Is.EqualTo(NetworkRecoveryPhase.AwaitingKeyframe));
                Assert.That(serverTransport.TrySend(first), Is.True);
                first = null;
                Assert.That(serverTransport.TrySend(second), Is.True);
                second = null;
                client.Process();
                Assert.That(client.AcknowledgedSnapshotTick, Is.EqualTo(4));
                Assert.That(ReadReplicaValue(), Is.EqualTo(4));
                Assert.That(client.TryConsumeRecoveryTransition(out _),
                    Is.False);

                server.Receive();
                entity.Set(new TestComponent { Value = 6 });
                server.Tick(_ => { });
                Assert.That(clientTransport.TryReceive(out first), Is.True);
                Assert.That(clientTransport.TryReceive(out second), Is.True);
                Assert.That(serverTransport.TrySend(first.Retain()), Is.True);
                client.Process();
                Assert.That(client.AcknowledgedSnapshotTick, Is.EqualTo(4));
                observer.Events.Clear();
                client.Process(long.MaxValue);
                Assert.That(client.AcknowledgedSnapshotTick, Is.EqualTo(4));
                Assert.That(ReadReplicaValue(), Is.EqualTo(4));
                Assert.That(observer.Single(NetworkPhase.Send,
                    NetworkPacketKind.ResyncRequest).ResyncSource,
                    Is.EqualTo(NetworkResyncSource.ClientSnapshotAssemblyTimeout));
                Assert.That(client.TryConsumeRecoveryTransition(out recovery),
                    Is.True);
                Assert.That(recovery.Phase,
                    Is.EqualTo(NetworkRecoveryPhase.AwaitingKeyframe));
                Assert.That(serverTransport.TrySend(first), Is.True);
                first = null;
                Assert.That(serverTransport.TrySend(second), Is.True);
                second = null;
                client.Process();
                Assert.That(client.AcknowledgedSnapshotTick, Is.EqualTo(4));
                Assert.That(ReadReplicaValue(), Is.EqualTo(4));
                Assert.That(client.TryConsumeRecoveryTransition(out _),
                    Is.False);
            }
            finally
            {
                late?.Dispose();
                second?.Dispose();
                first?.Dispose();
                sample?.Dispose();
                probe?.Dispose();
                client?.Dispose();
                server?.Dispose();
                clientTransport?.Dispose();
                serverTransport?.Dispose();
                Assert.That(pool.CaptureDiagnostics().OutstandingLeases,
                    Is.Zero);
                pool.Dispose();
                if (World<AuthorityWorld>.Status == WorldStatus.Initialized)
                    World<AuthorityWorld>.Destroy();
                if (World<ClientAWorld>.Status == WorldStatus.Initialized)
                    World<ClientAWorld>.Destroy();
            }
        }

        [Test]
        public void ServerAckValidationKeepsCursorAndRecoveryBoundary()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                MemoryNetworkTransport.CreatePair(new ConnectionId(811),
                    out var clientTransport, out var serverTransport);
                using (clientTransport)
                using (serverTransport)
                using (var server = new NetworkServer<AuthorityWorld>(schema,
                           static (_, _) => true, historyTicks: 4))
                {
                    server.AddConnection(serverTransport, 1, 1,
                        new ScopeId(1));
                    SendPeerPacket(clientTransport, schema.Fingerprint,
                        PacketKind.Hello, 0, 1, 0);
                    server.Receive();
                    Assert.That(clientTransport.TryReceive(out var ready), Is.True);
                    ready.Dispose();
                    var entity = World<AuthorityWorld>.NewEntity<TestEntity>();
                    entity.Set(new TestComponent { Value = 1 });

                    for (var tick = 1; tick <= 5; tick++)
                    {
                        server.Tick(_ => { });
                        Assert.That(ReceiveChunk(clientTransport).PayloadKind,
                            Is.EqualTo(SnapshotPayloadKind.Keyframe));
                    }

                    SendPeerPacket(clientTransport, schema.Fingerprint,
                        PacketKind.Ack, 1, 2, 1);
                    server.Receive();
                    server.Tick(_ => { });
                    Assert.That(ReceiveChunk(clientTransport).PayloadKind,
                        Is.EqualTo(SnapshotPayloadKind.Keyframe),
                        "evicted ACK must not advance the baseline cursor");

                    SendPeerPacket(clientTransport, schema.Fingerprint,
                        PacketKind.Ack, 1, 3, 6);
                    server.Receive();
                    server.Tick(_ => { });
                    var delta = ReceiveChunk(clientTransport);
                    Assert.That(delta.PayloadKind,
                        Is.EqualTo(SnapshotPayloadKind.Delta));
                    Assert.That(delta.BaselineTick, Is.EqualTo(6));

                    SendPeerPacket(clientTransport, schema.Fingerprint,
                        PacketKind.Ack, 1, 4, 6);
                    server.Receive();
                    server.Tick(_ => { });
                    delta = ReceiveChunk(clientTransport);
                    Assert.That(delta.PayloadKind,
                        Is.EqualTo(SnapshotPayloadKind.Delta));
                    Assert.That(delta.BaselineTick, Is.EqualTo(6));

                    SendPeerPacket(clientTransport, schema.Fingerprint,
                        PacketKind.Ack, 1, 5, 99);
                    server.Receive();
                    server.Tick(_ => { });
                    Assert.That(ReceiveChunk(clientTransport).PayloadKind,
                        Is.EqualTo(SnapshotPayloadKind.Keyframe));

                    SendPeerPacket(clientTransport, schema.Fingerprint,
                        PacketKind.Ack, 1, 6, 9);
                    server.Receive();
                    server.Tick(_ => { });
                    Assert.That(ReceiveChunk(clientTransport).PayloadKind,
                        Is.EqualTo(SnapshotPayloadKind.Delta));

                    SendPeerPacket(clientTransport, schema.Fingerprint,
                        PacketKind.Ack, 1, 7, 8);
                    server.Receive();
                    Assert.That(server.TryGetConnection(0, out var connection),
                        Is.True);
                    Assert.That(connection.Ticks.AcknowledgedSnapshotTick,
                        Is.EqualTo(9));
                    server.Tick(_ => { });
                    delta = ReceiveChunk(clientTransport);
                    Assert.That(delta.PayloadKind,
                        Is.EqualTo(SnapshotPayloadKind.Delta));
                    Assert.That(delta.BaselineTick, Is.EqualTo(9));

                    SendPeerPacket(clientTransport, schema.Fingerprint,
                        PacketKind.Ack, 1, 8, 0);
                    server.Receive();
                    Assert.That(server.TryGetConnection(0, out connection),
                        Is.True);
                    Assert.That(connection.Ticks.AcknowledgedSnapshotTick,
                        Is.EqualTo(9));
                    server.Tick(_ => { });
                    delta = ReceiveChunk(clientTransport);
                    Assert.That(delta.PayloadKind,
                        Is.EqualTo(SnapshotPayloadKind.Delta));
                    Assert.That(delta.BaselineTick, Is.EqualTo(9));
                }
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
            }
        }

        [Test]
        public void ServerAckAtRecoveryBoundaryClearsBackpressuredKeyframe()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                MemoryNetworkTransport.CreatePair(new ConnectionId(813),
                    out var clientTransport, out var serverEndpoint);
                using (clientTransport)
                using (var serverTransport = new LimitedNetworkTransport(
                           serverEndpoint,
                           serverEndpoint.MaxUnreliablePayloadBytes))
                using (var server = new NetworkServer<AuthorityWorld>(schema,
                           static (_, _) => true))
                {
                    server.AddConnection(serverTransport, 1, 1,
                        new ScopeId(1));
                    SendPeerPacket(clientTransport, schema.Fingerprint,
                        PacketKind.Hello, 0, 1, 0);
                    server.Receive();
                    Assert.That(clientTransport.TryReceive(out var ready), Is.True);
                    ready.Dispose();

                    var entity = World<AuthorityWorld>.NewEntity<TestEntity>();
                    entity.Set(new TestComponent { Value = 1 });
                    server.Tick(_ => { });
                    Assert.That(ReceiveChunk(clientTransport).PayloadKind,
                        Is.EqualTo(SnapshotPayloadKind.Keyframe));
                    SendPeerPacket(clientTransport, schema.Fingerprint,
                        PacketKind.Ack, 1, 2, 1);
                    server.Receive();

                    Span<byte> payload = stackalloc byte[ResyncRequestPayload.Size];
                    Assert.That(new ResyncRequestPayload(17).TryWrite(payload),
                        Is.True);
                    var request = Packet(PacketKind.ResyncRequest, 1, 3);
                    request.SchemaFingerprint = schema.Fingerprint;
                    Assert.That(NetworkPacket.TryEncode(Buffers, request, payload,
                        out var requestPacket), Is.True);
                    Assert.That(clientTransport.TrySend(requestPacket), Is.True);
                    server.Receive();

                    entity.Set(new TestComponent { Value = 2 });
                    serverTransport.MaxReliablePayloadBytes = PacketHeader.Size +
                        SnapshotChunkHeader.Size + 1;
                    serverTransport.ResetSentPackets();
                    serverTransport.FailOnSendNumber = 2;
                    server.Tick(_ => { });
                    Assert.That(serverTransport.SentPacketCount, Is.EqualTo(2));
                    Assert.That(clientTransport.TryReceive(out var partial), Is.True);
                    partial.Dispose();

                    serverTransport.FailOnSendNumber = 0;
                    serverTransport.MaxReliablePayloadBytes =
                        serverEndpoint.MaxReliablePayloadBytes;
                    SendPeerPacket(clientTransport, schema.Fingerprint,
                        PacketKind.Ack, 1, 4, 2);
                    server.Receive();

                    server.Tick(_ => { });
                    var delta = ReceiveChunk(clientTransport);
                    Assert.That(delta.PayloadKind,
                        Is.EqualTo(SnapshotPayloadKind.Delta));
                    Assert.That(delta.BaselineTick, Is.EqualTo(2));
                }
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
            }
        }

        [Test]
        public void ClientRequiresBaselineAndOnlyKeyframeClearsRecovery()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            var schema = Schema<AuthorityWorld>(true);
            NetworkReplicator<AuthorityWorld> capture = null;
            try
            {
            var clientSchema = Schema<ClientAWorld>(false);
            var scope = new ScopeId(23);
            Assert.That(clientSchema.Fingerprint, Is.EqualTo(schema.Fingerprint));
            capture = new NetworkReplicator<AuthorityWorld>(schema,
                static (_, _) => true, scope);
            MemoryNetworkTransport.CreatePair(new ConnectionId(812),
                out var clientTransport, out var serverTransport);
            using (clientTransport)
            using (serverTransport)
            using (var client = new NetworkClient<ClientAWorld>(clientTransport,
                       clientSchema, scope))
            {
                NetworkSnapshot baseline = null;
                NetworkSnapshot target = null;
                NetworkSnapshot next = null;
                NetworkSnapshot rejected = null;
                NetworkBufferLease delta = null;
                try
                {
                    Assert.That(client.Session.Admit(clientSchema.Fingerprint,
                        1, 1, scope), Is.EqualTo(NetworkAdmissionResult.Accepted));
                    var entity = World<AuthorityWorld>.NewEntity<TestEntity>();
                    entity.Set(new TestComponent { Value = 1 });
                    var unchanged = World<AuthorityWorld>
                        .NewEntity<SecondEntity>();
                    unchanged.Set(new TestComponent { Value = 99 });
                    Assert.That(capture.Capture(1, out baseline),
                        Is.EqualTo(SnapshotCaptureResult.Success));
                    entity.Set(new TestComponent { Value = 2 });
                    Assert.That(capture.Capture(2, out target),
                        Is.EqualTo(SnapshotCaptureResult.Success));
                    Assert.That(SnapshotDeltaCodec.TryEncode(Buffers, baseline,
                        target, out delta), Is.True);
                    Assert.That(delta.Length,
                        Is.LessThan(target.ByteLength));
                    SendSnapshotChunk(serverTransport, clientSchema.Fingerprint,
                        1, DeltaHeader(baseline, target), delta.Span);
                    client.Process();
                    Assert.That(client.AcknowledgedSnapshotTick, Is.Zero);
                    Assert.That(client.History.Count, Is.Zero);
                    Assert.That(client.TryConsumeRecoveryTransition(
                        out var recovery), Is.True);
                    Assert.That(recovery.Phase,
                        Is.EqualTo(NetworkRecoveryPhase.AwaitingKeyframe));
                    delta.Dispose();
                    delta = null;

                    var keyframe = KeyframeHeader(target);
                    Assert.That(serverTransport.TryReceive(out var request),
                        Is.True);
                    try
                    {
                        Assert.That(NetworkPacket.TryDecode(request,
                            out var requestHeader, out var requestBytes), Is.True);
                        Assert.That(requestHeader.Kind,
                            Is.EqualTo(PacketKind.ResyncRequest));
                        Assert.That(ResyncRequestPayload.TryRead(
                            requestBytes.Span, out var requestPayload), Is.True);
                        keyframe.ResyncCorrelationId =
                            requestPayload.CorrelationId;
                    }
                    finally
                    {
                        request.Dispose();
                    }
                    SendSnapshotChunk(serverTransport, clientSchema.Fingerprint,
                        1, keyframe, target.Bytes.Span);
                    client.Process();
                    Assert.That(client.AcknowledgedSnapshotTick, Is.EqualTo(2));
                    Assert.That(client.TryConsumeRecoveryTransition(out recovery),
                        Is.True);
                    Assert.That(recovery.Phase,
                        Is.EqualTo(NetworkRecoveryPhase.None));
                    Assert.That(ReadReplicaValue(), Is.EqualTo(2));

                    entity.Set(new TestComponent { Value = 3 });
                    Assert.That(capture.Capture(3, out next),
                        Is.EqualTo(SnapshotCaptureResult.Success));
                    Assert.That(SnapshotDeltaCodec.TryEncode(Buffers, target,
                        next, out delta), Is.True);
                    Assert.That(delta.Length,
                        Is.LessThan(next.ByteLength));
                    SendSnapshotChunk(serverTransport, clientSchema.Fingerprint,
                        1, DeltaHeader(target, next), delta.Span);
                    client.Process();
                    Assert.That(client.AcknowledgedSnapshotTick, Is.EqualTo(3));
                    Assert.That(ReadReplicaValue(), Is.EqualTo(3));
                    Assert.That(client.TryConsumeRecoveryTransition(out _),
                        Is.False, "delta apply must not clear recovery");
                    delta.Dispose();
                    delta = null;

                    entity.Set(new TestComponent { Value = 4 });
                    Assert.That(capture.Capture(4, out rejected),
                        Is.EqualTo(SnapshotCaptureResult.Success));
                    Assert.That(SnapshotDeltaCodec.TryEncode(Buffers, next,
                        rejected, out delta), Is.True);
                    Assert.That(delta.Length,
                        Is.LessThan(rejected.ByteLength));
                    var corrupt = delta.Span.ToArray();
                    corrupt[corrupt.Length - 1] ^= 1;
                    SendSnapshotChunk(serverTransport, clientSchema.Fingerprint,
                        1, DeltaHeader(next, rejected), corrupt);
                    client.Process();
                    Assert.That(client.AcknowledgedSnapshotTick, Is.EqualTo(3));
                    Assert.That(ReadReplicaValue(), Is.EqualTo(3),
                        "rejected delta must not partially mutate ECS");
                    Assert.That(client.History.Count, Is.EqualTo(2));
                    Assert.That(client.History.Bytes,
                        Is.LessThanOrEqualTo(client.History.MaxBytes));
                    Assert.That(client.TryConsumeRecoveryTransition(out recovery),
                        Is.True);
                    Assert.That(recovery.Phase,
                        Is.EqualTo(NetworkRecoveryPhase.AwaitingKeyframe));
                }
                finally
                {
                    delta?.Dispose();
                    rejected?.Dispose();
                    next?.Dispose();
                    target?.Dispose();
                    baseline?.Dispose();
                }
            }
            }
            finally
            {
                capture?.Dispose();
                if (World<AuthorityWorld>.Status == WorldStatus.Initialized)
                    World<AuthorityWorld>.Destroy();
                if (World<ClientAWorld>.Status == WorldStatus.Initialized)
                    World<ClientAWorld>.Destroy();
            }
        }


        [Test]
        public void ServerDeltaCacheReusesSameBaselineAndIsolatesDifferentBaselines()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                var scope = new ScopeId(42);
                using var pool = new NetworkBufferPool(0);
                using (var mock = new TwoClientNetworkMock())
                using (var server = new NetworkServer<AuthorityWorld>(schema,
                           static (_, _) => true, bufferPool: pool))
                {
                    server.AddConnection(mock.ServerA, 1, 11, scope);
                    server.AddConnection(mock.ServerB, 2, 22, scope);
                    SendPeerPacket(mock.ClientA, schema.Fingerprint,
                        PacketKind.Hello, 0, 1, 0);
                    SendPeerPacket(mock.ClientB, schema.Fingerprint,
                        PacketKind.Hello, 0, 1, 0);
                    server.Receive();
                    Assert.That(mock.ClientA.TryReceive(out var readyA), Is.True);
                    readyA.Dispose();
                    Assert.That(mock.ClientB.TryReceive(out var readyB), Is.True);
                    readyB.Dispose();

                    var first = World<AuthorityWorld>.NewEntity<TestEntity>();
                    first.Set(new TestComponent { Value = 1 });
                    var second = World<AuthorityWorld>.NewEntity<SecondEntity>();
                    second.Set(new TestComponent { Value = 10 });
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

                    first.Set(new TestComponent { Value = 2 });
                    var beforeShared = pool.CaptureDiagnostics().PoolMisses;
                    server.Tick(_ => { });
                    var sharedMisses = pool.CaptureDiagnostics().PoolMisses -
                        beforeShared;
                    var sharedA = ReceiveSnapshotChunk(mock.ClientA,
                        out var sharedPacketA, out var sharedBodyA);
                    var sharedB = ReceiveSnapshotChunk(mock.ClientB,
                        out var sharedPacketB, out var sharedBodyB);
                    Assert.That(sharedA.PayloadKind,
                        Is.EqualTo(SnapshotPayloadKind.Delta));
                    Assert.That(sharedB.PayloadKind,
                        Is.EqualTo(SnapshotPayloadKind.Delta));
                    Assert.That(sharedA.BaselineTick, Is.EqualTo(1));
                    Assert.That(sharedB.BaselineTick, Is.EqualTo(1));
                    CollectionAssert.AreEqual(sharedBodyA, sharedBodyB);
                    Assert.That(sharedPacketA.SessionEpoch, Is.EqualTo(11));
                    Assert.That(sharedPacketB.SessionEpoch, Is.EqualTo(22));
                    Assert.That(sharedPacketA.SessionEpoch,
                        Is.Not.EqualTo(sharedPacketB.SessionEpoch));
                    Assert.That(sharedMisses, Is.GreaterThan(0));

                    Assert.That(server.TryGetCapture(scope, 1,
                        out var baselineOne), Is.True);
                    Assert.That(server.TryGetCapture(scope, 2,
                        out var targetTwo), Is.True);
                    AssertReconstructedSnapshot(pool, baselineOne,
                        sharedBodyA, in sharedA, schema.Fingerprint, scope,
                        targetTwo);

                    SendPeerPacket(mock.ClientA, schema.Fingerprint,
                        PacketKind.Ack, 11, 3, 2);
                    server.Receive();
                    first.Set(new TestComponent { Value = 3 });
                    var beforeDifferent = pool.CaptureDiagnostics().PoolMisses;
                    server.Tick(_ => { });
                    var differentMisses = pool.CaptureDiagnostics().PoolMisses -
                        beforeDifferent;
                    var differentA = ReceiveSnapshotChunk(mock.ClientA,
                        out var differentPacketA, out var differentBodyA);
                    var differentB = ReceiveSnapshotChunk(mock.ClientB,
                        out var differentPacketB, out var differentBodyB);
                    Assert.That(differentA.PayloadKind,
                        Is.EqualTo(SnapshotPayloadKind.Delta));
                    Assert.That(differentB.PayloadKind,
                        Is.EqualTo(SnapshotPayloadKind.Delta));
                    Assert.That(differentA.BaselineTick, Is.EqualTo(2));
                    Assert.That(differentB.BaselineTick, Is.EqualTo(1));
                    Assert.That(differentPacketA.SessionEpoch,
                        Is.EqualTo(11));
                    Assert.That(differentPacketB.SessionEpoch,
                        Is.EqualTo(22));
                    Assert.That(differentMisses,
                        Is.EqualTo(sharedMisses + 1));
                    Assert.That(server.TryGetCapture(scope, 3,
                        out var targetThree), Is.True);
                    AssertReconstructedSnapshot(pool,
                        baselineOne, differentBodyB, in differentB,
                        schema.Fingerprint, scope, targetThree);
                    Assert.That(server.TryGetCapture(scope, 2,
                        out var baselineTwo), Is.True);
                    AssertReconstructedSnapshot(pool,
                        baselineTwo, differentBodyA, in differentA,
                        schema.Fingerprint, scope, targetThree);
                    Assert.That(differentBodyA.Length,
                        Is.GreaterThan(0));
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
        public void ServerDeltaCacheReleasesLeaseWhenSendThrowsAndRetrySucceeds()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                var scope = new ScopeId(43);
                using var pool = new NetworkBufferPool(0);
                MemoryNetworkTransport.CreatePair(new ConnectionId(43),
                    out var clientTransport, out var serverEndpoint);
                using (clientTransport)
                using (var throwingTransport = new ThrowingSendTransport(
                           serverEndpoint))
                using (var server = new NetworkServer<AuthorityWorld>(schema,
                           static (_, _) => true, bufferPool: pool))
                {
                    server.AddConnection(throwingTransport, 1, 31, scope);
                    SendPeerPacket(clientTransport, schema.Fingerprint,
                        PacketKind.Hello, 0, 1, 0);
                    server.Receive();
                    Assert.That(clientTransport.TryReceive(out var ready), Is.True);
                    ready.Dispose();

                    var entity = World<AuthorityWorld>.NewEntity<TestEntity>();
                    entity.Set(new TestComponent { Value = 1 });
                    var unchanged = World<AuthorityWorld>.NewEntity<SecondEntity>();
                    unchanged.Set(new TestComponent { Value = 10 });
                    server.Tick(_ => { });
                    Assert.That(ReceiveChunk(clientTransport).PayloadKind,
                        Is.EqualTo(SnapshotPayloadKind.Keyframe));

                    SendPeerPacket(clientTransport, schema.Fingerprint,
                        PacketKind.Ack, 31, 2, 1);
                    server.Receive();
                    entity.Set(new TestComponent { Value = 2 });

                    throwingTransport.ThrowOnSend = true;
                    Assert.Throws<InvalidOperationException>(() =>
                        server.Tick(_ => { }));
                    Assert.That(server.ServerTick, Is.EqualTo(1));
                    Assert.That(pool.CaptureDiagnostics().OutstandingLeases,
                        Is.EqualTo(2),
                        "cached delta lease must be released when send throws");

                    throwingTransport.ThrowOnSend = false;
                    Assert.DoesNotThrow(() => server.Tick(_ => { }));
                    Assert.That(server.ServerTick, Is.EqualTo(2));
                    Assert.That(ReceiveChunk(clientTransport).PayloadKind,
                        Is.EqualTo(SnapshotPayloadKind.Delta));
                }

                Assert.That(pool.CaptureDiagnostics().OutstandingLeases,
                    Is.Zero);
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
            }
        }

        private sealed class ThrowingSendTransport : INetworkTransport
        {
            private readonly INetworkTransport _inner;

            internal ThrowingSendTransport(INetworkTransport inner)
            {
                _inner = inner;
            }

            internal bool ThrowOnSend { get; set; }

            public ConnectionId Connection => _inner.Connection;
            public int MaxReliablePayloadBytes =>
                _inner.MaxReliablePayloadBytes;
            public int MaxUnreliablePayloadBytes =>
                _inner.MaxUnreliablePayloadBytes;

            public bool TrySend(NetworkBufferLease packet)
            {
                if (ThrowOnSend)
                {
                    packet?.Dispose();
                    throw new InvalidOperationException(
                        "test transport send failure");
                }

                return _inner.TrySend(packet);
            }

            public bool TryReceive(out NetworkBufferLease packet) =>
                _inner.TryReceive(out packet);

            public void Dispose() => _inner.Dispose();
        }

        private static SnapshotChunkHeader ReceiveSnapshotChunk(
            INetworkTransport transport, out PacketHeader packetHeader,
            out byte[] body)
        {
            Assert.That(transport.TryReceive(out var packet), Is.True);
            try
            {
                Assert.That(NetworkPacket.TryDecode(packet,
                    out packetHeader, out var payload), Is.True);
                Assert.That(packetHeader.Kind,
                    Is.EqualTo(PacketKind.SnapshotChunk));
                Assert.That(SnapshotChunkHeader.TryRead(payload.Span,
                    out var chunk), Is.True);
                body = payload.Slice(SnapshotChunkHeader.Size).ToArray();
                return chunk;
            }
            finally
            {
                packet.Dispose();
            }
        }

        private static void AssertReconstructedSnapshot(NetworkBufferPool pool,
            NetworkSnapshot baseline, byte[] body,
            in SnapshotChunkHeader header, SchemaFingerprint schema,
            ScopeId scope, NetworkSnapshot target)
        {
            NetworkBufferLease canonical = null;
            try
            {
                Assert.That(SnapshotDeltaCodec.TryReconstruct(pool, baseline,
                    body, in header, schema, scope, out canonical,
                    out _, out _), Is.True);
                Assert.That(canonical.Span.SequenceEqual(target.Bytes.Span),
                    Is.True);
            }
            finally
            {
                canonical?.Dispose();
            }
        }

    }
}