using System;
using System.Buffers;
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
                for (var i = 0; i < 8; i++)
                {
                    var ballast = World<AuthorityWorld>.NewEntity<TestEntity>();
                    ballast.Set(new TestComponent { Value = 100 + i });
                }
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
                var expectedDelta = new byte[12];
                Write32(expectedDelta, 0,
                    checked((uint)unchanged.EntityCount));
                Write32(expectedDelta, 4,
                    checked((uint)unchanged.RecordCount));
                Assert.That(delta.Span.SequenceEqual(expectedDelta), Is.True);
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
                var ballast = World<AuthorityWorld>.NewEntity<TestEntity>();
                ballast.Set(new TestComponent { Value = 50 });
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
        public void SnapshotDeltaCodec_RejectsEqualityOverflowAndMalformedCandidates()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            var content = new NetworkBufferPool(4L << 20);
            var probe = new NetworkBufferPool(1L << 20);
            var schema = Schema<AuthorityWorld>(true);
            var scope = new ScopeId(29);
            var replicator = new NetworkReplicator<AuthorityWorld>(schema,
                static (_, _) => true, scope, bufferPool: content);
            NetworkSnapshot emptyBaseline = null;
            NetworkSnapshot oneBaseline = null;
            NetworkSnapshot equalTarget = null;
            NetworkSnapshot overflowTarget = null;
            NetworkSnapshot malformed = null;
            NetworkBufferPool exceptionProbe = null;
            NetworkBufferLease delta = null;
            try
            {
                Assert.That(replicator.Capture(1, out emptyBaseline),
                    Is.EqualTo(SnapshotCaptureResult.Success));
                Assert.That(emptyBaseline.EntityCount, Is.Zero);
                World<AuthorityWorld>.NewEntity<TestEntity>();
                Assert.That(replicator.Capture(2, out oneBaseline),
                    Is.EqualTo(SnapshotCaptureResult.Success));
                for (var i = 0; i < 7; i++)
                    World<AuthorityWorld>.NewEntity<SecondEntity>();
                Assert.That(replicator.Capture(3, out equalTarget),
                    Is.EqualTo(SnapshotCaptureResult.Success));
                Assert.That(equalTarget.EntityCount, Is.EqualTo(8));
                Assert.That(equalTarget.ByteLength, Is.EqualTo(4 + 8 * 15));
                Assert.That(equalTarget.ByteLength, Is.EqualTo(12 + 7 * 16));

                var beforeEqual = probe.CaptureDiagnostics();
                Assert.That(SnapshotDeltaCodec.TryEncode(probe, oneBaseline,
                    equalTarget, out delta), Is.False);
                Assert.That(delta, Is.Null);
                AssertRejectedCandidate(probe, in beforeEqual, 1);

                for (var i = 0; i < 8; i++)
                    World<AuthorityWorld>.NewEntity<SecondEntity>();
                Assert.That(replicator.Capture(4, out overflowTarget),
                    Is.EqualTo(SnapshotCaptureResult.Success));
                Assert.That(overflowTarget.EntityCount, Is.EqualTo(16));
                Assert.That(overflowTarget.ByteLength,
                    Is.EqualTo(4 + 16 * 15));
                Assert.That(12 + 16 * 16,
                    Is.GreaterThan(overflowTarget.ByteLength));

                var beforeOverflow = probe.CaptureDiagnostics();
                Assert.That(SnapshotDeltaCodec.TryEncode(probe, emptyBaseline,
                    overflowTarget, out delta), Is.False);
                Assert.That(delta, Is.Null);
                AssertRejectedCandidate(probe, in beforeOverflow, 0);

                var malformedBytes = oneBaseline.Bytes.ToArray();
                malformed = new NetworkSnapshot(3,
                    oneBaseline.SchemaFingerprint, oneBaseline.Scope,
                    content.Copy(malformedBytes), oneBaseline.EntityCount,
                    oneBaseline.RecordCount + 1);
                var beforeMalformed = probe.CaptureDiagnostics();
                Assert.That(SnapshotDeltaCodec.TryEncode(probe, oneBaseline,
                    malformed, out delta), Is.False);
                Assert.That(delta, Is.Null);
                AssertRejectedCandidate(probe, in beforeMalformed, 0);

                exceptionProbe = new NetworkBufferPool(1L << 20);
                var beforeException = exceptionProbe.CaptureDiagnostics();
                try
                {
                    SnapshotDeltaCodec.AfterCandidateRentForTests =
                        static () => throw new InvalidOperationException();
                    Assert.That(SnapshotDeltaCodec.TryEncode(exceptionProbe,
                        oneBaseline, equalTarget, out delta), Is.False);
                    Assert.That(delta, Is.Null);
                }
                finally
                {
                    SnapshotDeltaCodec.AfterCandidateRentForTests = null;
                }
                AssertRejectedCandidate(exceptionProbe, in beforeException, 1);
            }
            finally
            {
                delta?.Dispose();
                malformed?.Dispose();
                overflowTarget?.Dispose();
                equalTarget?.Dispose();
                oneBaseline?.Dispose();
                emptyBaseline?.Dispose();
                replicator.Dispose();
                Assert.That(content.CaptureDiagnostics().OutstandingLeases,
                    Is.Zero);
                Assert.That(probe.CaptureDiagnostics().OutstandingLeases,
                    Is.Zero);
                exceptionProbe?.Dispose();
                content.Dispose();
                probe.Dispose();
                World<AuthorityWorld>.Destroy();
            }
        }

        [Test]
        public void SnapshotDeltaCodec_ReusesCandidateCapacityAcrossRepeatedEncodes()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            var content = new NetworkBufferPool(4L << 20);
            var probe = new NetworkBufferPool(1L << 20);
            var schema = Schema<AuthorityWorld>(true);
            var scope = new ScopeId(31);
            var replicator = new NetworkReplicator<AuthorityWorld>(schema,
                static (_, _) => true, scope, bufferPool: content);
            NetworkSnapshot baseline = null;
            NetworkSnapshot target = null;
            NetworkBufferLease delta = null;
            try
            {
                var first = default(World<AuthorityWorld>.Entity);
                for (var i = 0; i < 10; i++)
                {
                    var entity = World<AuthorityWorld>.NewEntity<TestEntity>();
                    entity.Set(new TestComponent { Value = i });
                    if (i == 0)
                        first = entity;
                }
                Assert.That(replicator.Capture(1, out baseline),
                    Is.EqualTo(SnapshotCaptureResult.Success));
                first.Set(new TestComponent { Value = 100 });
                Assert.That(replicator.Capture(2, out target),
                    Is.EqualTo(SnapshotCaptureResult.Success));
                Assert.That(target.ByteLength, Is.GreaterThan(256));

                var before = probe.CaptureDiagnostics();
                for (var i = 0; i < 3; i++)
                {
                    Assert.That(SnapshotDeltaCodec.TryEncode(probe, baseline,
                        target, out delta), Is.True);
                    Assert.That(delta.Length, Is.LessThan(target.ByteLength));
                    delta.Dispose();
                    delta = null;
                }
                var after = probe.CaptureDiagnostics();
                Assert.That(after.OutstandingLeases, Is.Zero);
                Assert.That(after.OutstandingBytes, Is.Zero);
                Assert.That(after.PoolMisses - before.PoolMisses,
                    Is.EqualTo(1));
                Assert.That(after.RetainedBytes,
                    Is.GreaterThanOrEqualTo(target.ByteLength));
                Assert.That(after.RetainedBytes,
                    Is.LessThan(target.ByteLength * 2));
                Assert.That(after.RetainedBytes,
                    Is.EqualTo(after.RetainedHighWaterBytes));
                Assert.That(after.RetainedBytes,
                    Is.EqualTo(after.OutstandingHighWaterBytes));
            }
            finally
            {
                delta?.Dispose();
                target?.Dispose();
                baseline?.Dispose();
                replicator.Dispose();
                Assert.That(content.CaptureDiagnostics().OutstandingLeases,
                    Is.Zero);
                Assert.That(probe.CaptureDiagnostics().OutstandingLeases,
                    Is.Zero);
                content.Dispose();
                probe.Dispose();
                World<AuthorityWorld>.Destroy();
            }
        }

        private static void AssertRejectedCandidate(NetworkBufferPool pool,
            in NetworkBufferPoolDiagnostics before, long expectedMisses)
        {
            var after = pool.CaptureDiagnostics();
            Assert.That(after.OutstandingLeases,
                Is.EqualTo(before.OutstandingLeases));
            Assert.That(after.OutstandingBytes,
                Is.EqualTo(before.OutstandingBytes));
            Assert.That(after.PoolMisses - before.PoolMisses,
                Is.EqualTo(expectedMisses));
            Assert.That(after.RetainedBytes, Is.GreaterThan(0));
            Assert.That(after.RetainedBytes,
                Is.EqualTo(after.OutstandingHighWaterBytes));
        }

        [Test]
        public void InFlightUncorrelatedKeyframeDoesNotCompleteCorrelatedRecovery()
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
                var clientSchema = Schema<ClientAWorld>(false);
                client = new NetworkClient<ClientAWorld>(simulator.Client,
                    clientSchema, new ScopeId(1), observer,
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
                Assert.That(client.Session.State,
                    Is.EqualTo(NetworkSessionState.Established));

                entity.Set(new TestComponent { Value = 2 });
                observer.Events.Clear();
                server.Tick(_ => { });
                var inFlightKeyframeTick = server.ServerTick;
                simulator.Advance(0);
                client.RequestFullResync(
                    NetworkRecoveryReason.PredictionHistoryUnavailable);
                var correlationId = observer.Single(NetworkPhase.Send,
                    NetworkPacketKind.ResyncRequest).ResyncCorrelationId;
                Assert.That(correlationId, Is.Not.Zero);
                client.Process();

                Assert.That(client.Session.State,
                    Is.EqualTo(NetworkSessionState.Established));
                Assert.That(client.AcknowledgedSnapshotTick,
                    Is.EqualTo(inFlightKeyframeTick));
                Assert.That(ReadReplicaValue(), Is.EqualTo(2));
                Assert.That(client.TryConsumeRecoveryTransition(
                    out var recovery), Is.True);
                Assert.That(recovery.Phase,
                    Is.EqualTo(NetworkRecoveryPhase.AwaitingKeyframe));
                Assert.That(observer.Single(NetworkPhase.Send,
                        NetworkPacketKind.Ack).ResyncCorrelationId,
                    Is.Zero);

                observer.Events.Clear();
                client.RequestFullResync(NetworkRecoveryReason.SnapshotRejected);
                Assert.That(observer.Single(NetworkPhase.Send,
                        NetworkPacketKind.ResyncRequest).ResyncCorrelationId,
                    Is.EqualTo(correlationId));
                simulator.Advance(0);
                server.Receive();

                entity.Set(new TestComponent { Value = 3 });
                server.Tick(_ => { });
                var keyframeTick = server.ServerTick;
                simulator.Advance(0);
                client.Process();

                Assert.That(client.AcknowledgedSnapshotTick,
                    Is.EqualTo(keyframeTick));
                Assert.That(ReadReplicaValue(), Is.EqualTo(3));
                Assert.That(client.Session.State,
                    Is.EqualTo(NetworkSessionState.Established));
                Assert.That(client.TryConsumeRecoveryTransition(out recovery),
                    Is.True);
                Assert.That(recovery.Phase,
                    Is.EqualTo(NetworkRecoveryPhase.None));
                Assert.That(observer.Single(NetworkPhase.Send,
                        NetworkPacketKind.Ack).ResyncCorrelationId,
                    Is.EqualTo(correlationId));
                simulator.Advance(0);
                server.Receive();

                observer.Events.Clear();
                client.RequestFullResync(NetworkRecoveryReason.SnapshotRejected);
                var conflictingCorrelation = observer.Single(NetworkPhase.Send,
                    NetworkPacketKind.ResyncRequest).ResyncCorrelationId;
                simulator.Advance(0);
                server.Receive();
                entity.Set(new TestComponent { Value = 4 });
                server.Tick(_ => { });
                simulator.Advance(0);
                var conflictChunk = ReceiveSnapshotChunk(simulator.Client,
                    out var conflictPacket, out var conflictBody);
                Assert.That(conflictChunk.ResyncCorrelationId,
                    Is.EqualTo(conflictingCorrelation));
                conflictChunk.ResyncCorrelationId = conflictingCorrelation + 1;
                SendSnapshotChunk(simulator.Server, clientSchema.Fingerprint,
                    conflictPacket.PacketSequence, conflictChunk, conflictBody);
                simulator.Advance(0);
                client.Process();

                Assert.That(client.Session.State,
                    Is.EqualTo(NetworkSessionState.Closed));
                Assert.That(client.TryConsumeRecoveryTransition(out recovery),
                    Is.True);
                Assert.That(recovery.Phase,
                    Is.EqualTo(NetworkRecoveryPhase.DisconnectRequired));
                Assert.That(recovery.Reason,
                    Is.EqualTo(NetworkRecoveryReason.ProtocolIncompatible));
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
        public void RejectedFirstDeltaChunkKeepsRecoveryDisabled()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                var scope = new ScopeId(44);
                using var pool = new NetworkBufferPool(0);
                MemoryNetworkTransport.CreatePair(new ConnectionId(814),
                    out var clientTransport, out var serverEndpoint);
                using (clientTransport)
                using (var serverTransport = new LimitedNetworkTransport(
                           serverEndpoint,
                           serverEndpoint.MaxUnreliablePayloadBytes))
                using (var server = new NetworkServer<AuthorityWorld>(schema,
                           static (_, _) => true, bufferPool: pool))
                {
                    server.AddConnection(serverTransport, 1, 1, scope);
                    SendPeerPacket(clientTransport, schema.Fingerprint,
                        PacketKind.Hello, 0, 1, 0);
                    server.Receive();
                    Assert.That(clientTransport.TryReceive(out var ready),
                        Is.True);
                    ready.Dispose();

                    var entity = World<AuthorityWorld>.NewEntity<TestEntity>();
                    entity.Set(new TestComponent { Value = 1 });
                    var unchanged =
                        World<AuthorityWorld>.NewEntity<SecondEntity>();
                    unchanged.Set(new TestComponent { Value = 10 });
                    server.Tick(_ => { });
                    var keyframe = ReceiveChunk(clientTransport);
                    Assert.That(keyframe.PayloadKind,
                        Is.EqualTo(SnapshotPayloadKind.Keyframe));
                    Assert.That(keyframe.SnapshotTick, Is.EqualTo(1));

                    SendPeerPacket(clientTransport, schema.Fingerprint,
                        PacketKind.Ack, 1, 2, 1);
                    server.Receive();

                    entity.Set(new TestComponent { Value = 2 });
                    serverTransport.ResetSentPackets();
                    serverTransport.FailOnSendNumber = 1;
                    server.Tick(_ => { });
                    Assert.That(serverTransport.SentPacketCount,
                        Is.EqualTo(1));

                    serverTransport.FailOnSendNumber = 0;
                    server.Tick(_ => { });
                    var delta = ReceiveChunk(clientTransport);
                    Assert.That(delta.PayloadKind,
                        Is.EqualTo(SnapshotPayloadKind.Delta));
                    Assert.That(delta.BaselineTick, Is.EqualTo(1));
                    Assert.That(delta.SnapshotTick, Is.EqualTo(3));
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
        public void RejectedInitialKeyframeIsRetriedAsKeyframe()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                using var pool = new NetworkBufferPool(0);
                MemoryNetworkTransport.CreatePair(new ConnectionId(815),
                    out var clientTransport, out var serverEndpoint);
                using (clientTransport)
                using (var serverTransport = new LimitedNetworkTransport(
                           serverEndpoint,
                           serverEndpoint.MaxUnreliablePayloadBytes))
                using (var server = new NetworkServer<AuthorityWorld>(schema,
                           static (_, _) => true, bufferPool: pool))
                {
                    server.AddConnection(serverTransport, 1, 1,
                        new ScopeId(45));
                    SendPeerPacket(clientTransport, schema.Fingerprint,
                        PacketKind.Hello, 0, 1, 0);
                    server.Receive();
                    Assert.That(clientTransport.TryReceive(out var ready),
                        Is.True);
                    ready.Dispose();

                    var entity = World<AuthorityWorld>.NewEntity<TestEntity>();
                    entity.Set(new TestComponent { Value = 1 });
                    serverTransport.ResetSentPackets();
                    serverTransport.FailOnSendNumber = 1;
                    server.Tick(_ => { });
                    Assert.That(serverTransport.SentPacketCount,
                        Is.EqualTo(1));

                    serverTransport.FailOnSendNumber = 0;
                    server.Tick(_ => { });
                    var retry = ReceiveChunk(clientTransport);
                    Assert.That(retry.PayloadKind,
                        Is.EqualTo(SnapshotPayloadKind.Keyframe));
                    Assert.That(retry.SnapshotTick, Is.EqualTo(2));
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
        public void RejectedLaterChunkOfMultiChunkDeltaKeepsRecovery()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                var scope = new ScopeId(46);
                using var pool = new NetworkBufferPool(0);
                MemoryNetworkTransport.CreatePair(new ConnectionId(816),
                    out var clientTransport, out var serverEndpoint);
                using (clientTransport)
                using (var serverTransport = new LimitedNetworkTransport(
                           serverEndpoint,
                           serverEndpoint.MaxUnreliablePayloadBytes))
                using (var server = new NetworkServer<AuthorityWorld>(schema,
                           static (_, _) => true, bufferPool: pool))
                {
                    server.AddConnection(serverTransport, 1, 1, scope);
                    SendPeerPacket(clientTransport, schema.Fingerprint,
                        PacketKind.Hello, 0, 1, 0);
                    server.Receive();
                    Assert.That(clientTransport.TryReceive(out var ready),
                        Is.True);
                    ready.Dispose();

                    var entity = World<AuthorityWorld>.NewEntity<TestEntity>();
                    entity.Set(new TestComponent { Value = 1 });
                    var unchanged =
                        World<AuthorityWorld>.NewEntity<SecondEntity>();
                    unchanged.Set(new TestComponent { Value = 10 });
                    server.Tick(_ => { });
                    Assert.That(ReceiveChunk(clientTransport).PayloadKind,
                        Is.EqualTo(SnapshotPayloadKind.Keyframe));

                    SendPeerPacket(clientTransport, schema.Fingerprint,
                        PacketKind.Ack, 1, 2, 1);
                    server.Receive();

                    entity.Set(new TestComponent { Value = 2 });
                    serverTransport.MaxReliablePayloadBytes = PacketHeader.Size +
                        SnapshotChunkHeader.Size + 1;
                    serverTransport.ResetSentPackets();
                    serverTransport.FailOnSendNumber = 2;
                    server.Tick(_ => { });
                    Assert.That(serverTransport.SentPacketCount,
                        Is.EqualTo(2));
                    var accepted = ReceiveSnapshotChunk(clientTransport,
                        out _, out _);
                    Assert.That(accepted.PayloadKind,
                        Is.EqualTo(SnapshotPayloadKind.Delta));
                    Assert.That(accepted.ChunkIndex, Is.Zero);
                    Assert.That(accepted.ChunkCount, Is.GreaterThan(1));

                    serverTransport.FailOnSendNumber = 0;
                    serverTransport.MaxReliablePayloadBytes =
                        serverEndpoint.MaxReliablePayloadBytes;
                    server.Tick(_ => { });
                    var recovery = ReceiveSnapshotChunk(clientTransport,
                        out _, out _);
                    Assert.That(recovery.PayloadKind,
                        Is.EqualTo(SnapshotPayloadKind.Keyframe));
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
        public void PreflightRejectionSkipsSnapshotPreparationAndKeepsRecoveryDisabled()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                using var pool = new NetworkBufferPool(0);
                MemoryNetworkTransport.CreatePair(new ConnectionId(817),
                    out var clientTransport, out var serverEndpoint);
                using (clientTransport)
                using (var serverTransport = new PreflightNetworkTransport(
                           serverEndpoint))
                using (var server = new NetworkServer<AuthorityWorld>(schema,
                           static (_, _) => true, bufferPool: pool))
                {
                    server.AddConnection(serverTransport, 1, 1,
                        new ScopeId(47));
                    SendPeerPacket(clientTransport, schema.Fingerprint,
                        PacketKind.Hello, 0, 1, 0);
                    server.Receive();
                    Assert.That(clientTransport.TryReceive(out var ready),
                        Is.True);
                    ready.Dispose();

                    var entity = World<AuthorityWorld>.NewEntity<TestEntity>();
                    entity.Set(new TestComponent { Value = 1 });
                    var unchanged =
                        World<AuthorityWorld>.NewEntity<SecondEntity>();
                    unchanged.Set(new TestComponent { Value = 10 });
                    server.Tick(_ => { });
                    Assert.That(ReceiveChunk(clientTransport).PayloadKind,
                        Is.EqualTo(SnapshotPayloadKind.Keyframe));

                    SendPeerPacket(clientTransport, schema.Fingerprint,
                        PacketKind.Ack, 1, 2, 1);
                    server.Receive();

                    entity.Set(new TestComponent { Value = 2 });
                    serverTransport.CanAccept = false;
                    serverTransport.ResetCounters();
                    server.Tick(_ => { });

                    Assert.That(serverTransport.PreflightCalls,
                        Is.EqualTo(1));
                    Assert.That(serverTransport.LastRequestedBytes,
                        Is.EqualTo(PacketHeader.Size +
                            SnapshotChunkHeader.Size + 1));
                    Assert.That(serverTransport.SentPacketCount, Is.Zero);
                    Assert.That(clientTransport.TryReceive(out _), Is.False);

                    serverTransport.CanAccept = true;
                    server.Tick(_ => { });
                    var delta = ReceiveChunk(clientTransport);
                    Assert.That(delta.PayloadKind,
                        Is.EqualTo(SnapshotPayloadKind.Delta),
                        "preflight rejection must not force recovery");
                    Assert.That(delta.BaselineTick, Is.EqualTo(1));
                    Assert.That(delta.SnapshotTick, Is.EqualTo(3));
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
        public void MaxReliableLimitIsCheckedBeforePreflight()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                using var pool = new NetworkBufferPool(0);
                MemoryNetworkTransport.CreatePair(new ConnectionId(819),
                    out var clientTransport, out var serverEndpoint);
                using (clientTransport)
                using (var serverTransport = new PreflightNetworkTransport(
                           serverEndpoint))
                using (var server = new NetworkServer<AuthorityWorld>(schema,
                           static (_, _) => true, bufferPool: pool))
                {
                    server.AddConnection(serverTransport, 1, 1,
                        new ScopeId(49));
                    SendPeerPacket(clientTransport, schema.Fingerprint,
                        PacketKind.Hello, 0, 1, 0);
                    server.Receive();
                    Assert.That(clientTransport.TryReceive(out var ready),
                        Is.True);
                    ready.Dispose();

                    var entity = World<AuthorityWorld>.NewEntity<TestEntity>();
                    entity.Set(new TestComponent { Value = 1 });
                    serverTransport.MaxReliablePayloadBytes =
                        PacketHeader.Size + SnapshotChunkHeader.Size;
                    serverTransport.ResetCounters();
                    server.Tick(_ => { });
                    Assert.That(serverTransport.PreflightCalls, Is.Zero,
                        "the hard reliable limit must be checked first");
                    Assert.That(serverTransport.SentPacketCount, Is.Zero);
                    Assert.That(clientTransport.TryReceive(out _), Is.False);

                    serverTransport.MaxReliablePayloadBytes =
                        serverEndpoint.MaxReliablePayloadBytes;
                    server.Tick(_ => { });
                    Assert.That(ReceiveChunk(clientTransport).PayloadKind,
                        Is.EqualTo(SnapshotPayloadKind.Keyframe));
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
        public void PreflightAcceptanceDoesNotGuaranteeTrySendAdmission()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                using var pool = new NetworkBufferPool(0);
                MemoryNetworkTransport.CreatePair(new ConnectionId(820),
                    out var clientTransport, out var serverEndpoint);
                using (clientTransport)
                using (var serverTransport = new PreflightNetworkTransport(
                           serverEndpoint))
                using (var server = new NetworkServer<AuthorityWorld>(schema,
                           static (_, _) => true, bufferPool: pool))
                {
                    server.AddConnection(serverTransport, 1, 1,
                        new ScopeId(50));
                    SendPeerPacket(clientTransport, schema.Fingerprint,
                        PacketKind.Hello, 0, 1, 0);
                    server.Receive();
                    Assert.That(clientTransport.TryReceive(out var ready),
                        Is.True);
                    ready.Dispose();

                    var entity = World<AuthorityWorld>.NewEntity<TestEntity>();
                    entity.Set(new TestComponent { Value = 1 });
                    var unchanged =
                        World<AuthorityWorld>.NewEntity<SecondEntity>();
                    unchanged.Set(new TestComponent { Value = 10 });
                    server.Tick(_ => { });
                    Assert.That(ReceiveChunk(clientTransport).PayloadKind,
                        Is.EqualTo(SnapshotPayloadKind.Keyframe));

                    SendPeerPacket(clientTransport, schema.Fingerprint,
                        PacketKind.Ack, 1, 2, 1);
                    server.Receive();

                    entity.Set(new TestComponent { Value = 2 });
                    serverTransport.ResetSentPackets();
                    serverTransport.FailOnSendNumber = 1;
                    server.Tick(_ => { });
                    Assert.That(serverTransport.PreflightCalls,
                        Is.GreaterThanOrEqualTo(1));
                    Assert.That(serverTransport.SentPacketCount,
                        Is.EqualTo(1));
                    Assert.That(clientTransport.TryReceive(out _), Is.False,
                        "a raced TrySend rejection must consume its lease");

                    serverTransport.FailOnSendNumber = 0;
                    server.Tick(_ => { });
                    var delta = ReceiveChunk(clientTransport);
                    Assert.That(delta.PayloadKind,
                        Is.EqualTo(SnapshotPayloadKind.Delta),
                        "a raced first delta chunk must not force recovery");
                    Assert.That(delta.BaselineTick, Is.EqualTo(1));
                    Assert.That(delta.SnapshotTick, Is.EqualTo(3));
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
        public void ExactPreflightRejectsKeyframeBeforeEncodeAndTrySend()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                var scope = new ScopeId(51);
                using var pool = new NetworkBufferPool(0);
                MemoryNetworkTransport.CreatePair(new ConnectionId(821),
                    out var clientTransport, out var serverEndpoint);
                using (clientTransport)
                using (var serverTransport = new PreflightNetworkTransport(
                           serverEndpoint))
                using (var server = new NetworkServer<AuthorityWorld>(schema,
                           static (_, _) => true, bufferPool: pool))
                {
                    server.AddConnection(serverTransport, 1, 1, scope);
                    SendPeerPacket(clientTransport, schema.Fingerprint,
                        PacketKind.Hello, 0, 1, 0);
                    server.Receive();
                    Assert.That(clientTransport.TryReceive(out var ready),
                        Is.True);
                    ready.Dispose();

                    var entity = World<AuthorityWorld>.NewEntity<TestEntity>();
                    entity.Set(new TestComponent { Value = 1 });

                    serverTransport.ResetCounters();
                    serverTransport.ProbeResults.Enqueue(true);
                    serverTransport.ProbeResults.Enqueue(false);

                    var before = pool.CaptureDiagnostics();
                    server.Tick(_ => { });
                    var after = pool.CaptureDiagnostics();

                    Assert.That(serverTransport.PreflightCalls, Is.EqualTo(2));
                    Assert.That(serverTransport.ProbeBytes[0],
                        Is.EqualTo(PacketHeader.Size +
                            SnapshotChunkHeader.Size + 1),
                        "the first probe is the minimum-size preflight");
                    Assert.That(server.TryGetCapture(scope, 1, out var capture),
                        Is.True);
                    Assert.That(serverTransport.ProbeBytes[1],
                        Is.EqualTo(PacketHeader.Size +
                            SnapshotChunkHeader.Size + capture.ByteLength),
                        "the exact probe includes the encoded chunk body");
                    Assert.That(serverTransport.SentPacketCount, Is.Zero,
                        "an exact preflight rejection must not call TrySend");
                    Assert.That(clientTransport.TryReceive(out _), Is.False);
                    Assert.That(after.OutstandingLeases - before.OutstandingLeases,
                        Is.EqualTo(1),
                        "only the snapshot capture lease may remain");
                    Assert.That(after.PoolMisses - before.PoolMisses,
                        Is.EqualTo(1),
                        "no packet buffer may be rented for a rejected chunk");

                    serverTransport.ResetCounters();
                    server.Tick(_ => { });
                    Assert.That(ReceiveChunk(clientTransport).PayloadKind,
                        Is.EqualTo(SnapshotPayloadKind.Keyframe),
                        "keyframe rejection must retain recovery");
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
        public void ExactPreflightRejectsFirstDeltaChunkBeforeEncodeAndTrySend()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                var scope = new ScopeId(52);
                using var pool = new NetworkBufferPool(0);
                MemoryNetworkTransport.CreatePair(new ConnectionId(822),
                    out var clientTransport, out var serverEndpoint);
                using (clientTransport)
                using (var serverTransport = new PreflightNetworkTransport(
                           serverEndpoint))
                using (var server = new NetworkServer<AuthorityWorld>(schema,
                           static (_, _) => true, bufferPool: pool))
                {
                    server.AddConnection(serverTransport, 1, 1, scope);
                    SendPeerPacket(clientTransport, schema.Fingerprint,
                        PacketKind.Hello, 0, 1, 0);
                    server.Receive();
                    Assert.That(clientTransport.TryReceive(out var ready),
                        Is.True);
                    ready.Dispose();

                    var entity = World<AuthorityWorld>.NewEntity<TestEntity>();
                    entity.Set(new TestComponent { Value = 1 });
                    var unchanged =
                        World<AuthorityWorld>.NewEntity<SecondEntity>();
                    unchanged.Set(new TestComponent { Value = 10 });
                    server.Tick(_ => { });
                    Assert.That(ReceiveChunk(clientTransport).PayloadKind,
                        Is.EqualTo(SnapshotPayloadKind.Keyframe));

                    SendPeerPacket(clientTransport, schema.Fingerprint,
                        PacketKind.Ack, 1, 2, 1);
                    server.Receive();

                    entity.Set(new TestComponent { Value = 2 });
                    serverTransport.ResetCounters();
                    serverTransport.ProbeResults.Enqueue(true);
                    serverTransport.ProbeResults.Enqueue(false);

                    var before = pool.CaptureDiagnostics();
                    server.Tick(_ => { });
                    var after = pool.CaptureDiagnostics();

                    Assert.That(serverTransport.PreflightCalls, Is.EqualTo(2));
                    Assert.That(serverTransport.ProbeBytes[0],
                        Is.EqualTo(PacketHeader.Size +
                            SnapshotChunkHeader.Size + 1));
                    Assert.That(serverTransport.ProbeBytes[1],
                        Is.GreaterThan(serverTransport.ProbeBytes[0]),
                        "a delta chunk body exceeds the minimum probe body");
                    Assert.That(serverTransport.SentPacketCount, Is.Zero,
                        "a rejected first delta chunk must not call TrySend");
                    Assert.That(clientTransport.TryReceive(out _), Is.False);
                    Assert.That(after.PoolMisses - before.PoolMisses,
                        Is.EqualTo(2),
                        "only the snapshot capture and delta encode may rent");

                    serverTransport.ResetCounters();
                    server.Tick(_ => { });
                    var delta = ReceiveChunk(clientTransport);
                    Assert.That(delta.PayloadKind,
                        Is.EqualTo(SnapshotPayloadKind.Delta),
                        "a rejected first delta chunk must not force recovery");
                    Assert.That(delta.BaselineTick, Is.EqualTo(1));
                    Assert.That(delta.SnapshotTick, Is.EqualTo(3));
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
        public void ExactPreflightRejectsLaterChunkAfterAcceptedChunkKeepsRecovery()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                var scope = new ScopeId(53);
                using var pool = new NetworkBufferPool(0);
                MemoryNetworkTransport.CreatePair(new ConnectionId(823),
                    out var clientTransport, out var serverEndpoint);
                using (clientTransport)
                using (var serverTransport = new PreflightNetworkTransport(
                           serverEndpoint))
                using (var server = new NetworkServer<AuthorityWorld>(schema,
                           static (_, _) => true, bufferPool: pool))
                {
                    server.AddConnection(serverTransport, 1, 1, scope);
                    SendPeerPacket(clientTransport, schema.Fingerprint,
                        PacketKind.Hello, 0, 1, 0);
                    server.Receive();
                    Assert.That(clientTransport.TryReceive(out var ready),
                        Is.True);
                    ready.Dispose();

                    var entity = World<AuthorityWorld>.NewEntity<TestEntity>();
                    entity.Set(new TestComponent { Value = 1 });
                    var unchanged =
                        World<AuthorityWorld>.NewEntity<SecondEntity>();
                    unchanged.Set(new TestComponent { Value = 10 });
                    server.Tick(_ => { });
                    Assert.That(ReceiveChunk(clientTransport).PayloadKind,
                        Is.EqualTo(SnapshotPayloadKind.Keyframe));

                    SendPeerPacket(clientTransport, schema.Fingerprint,
                        PacketKind.Ack, 1, 2, 1);
                    server.Receive();

                    entity.Set(new TestComponent { Value = 2 });
                    serverTransport.MaxReliablePayloadBytes = PacketHeader.Size +
                        SnapshotChunkHeader.Size + 1;
                    serverTransport.ResetCounters();
                    serverTransport.ProbeResults.Enqueue(true);
                    serverTransport.ProbeResults.Enqueue(true);
                    serverTransport.ProbeResults.Enqueue(false);
                    server.Tick(_ => { });

                    Assert.That(serverTransport.PreflightCalls, Is.EqualTo(3),
                        "minimum plus two chunk probes");
                    Assert.That(serverTransport.ProbeBytes[0],
                        Is.EqualTo(PacketHeader.Size +
                            SnapshotChunkHeader.Size + 1));
                    Assert.That(serverTransport.SentPacketCount,
                        Is.EqualTo(1),
                        "only the first accepted chunk may be sent");
                    var accepted = ReceiveSnapshotChunk(clientTransport,
                        out _, out _);
                    Assert.That(accepted.PayloadKind,
                        Is.EqualTo(SnapshotPayloadKind.Delta));
                    Assert.That(accepted.ChunkIndex, Is.Zero);
                    Assert.That(accepted.ChunkCount, Is.GreaterThan(1));

                    serverTransport.MaxReliablePayloadBytes =
                        serverEndpoint.MaxReliablePayloadBytes;
                    serverTransport.ResetCounters();
                    server.Tick(_ => { });
                    Assert.That(ReceiveChunk(clientTransport).PayloadKind,
                        Is.EqualTo(SnapshotPayloadKind.Keyframe),
                        "a later rejected chunk must force keyframe recovery");
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
        public void ExactPreflightProbesMultiChunkKeyframeSizesIncludingShortLastChunk()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                var scope = new ScopeId(54);
                using var pool = new NetworkBufferPool(0);
                MemoryNetworkTransport.CreatePair(new ConnectionId(824),
                    out var clientTransport, out var serverEndpoint);
                using (clientTransport)
                using (var serverTransport = new PreflightNetworkTransport(
                           serverEndpoint))
                using (var server = new NetworkServer<AuthorityWorld>(schema,
                           static (_, _) => true, bufferPool: pool))
                {
                    server.AddConnection(serverTransport, 1, 1, scope);
                    SendPeerPacket(clientTransport, schema.Fingerprint,
                        PacketKind.Hello, 0, 1, 0);
                    server.Receive();
                    Assert.That(clientTransport.TryReceive(out var ready),
                        Is.True);
                    ready.Dispose();

                    for (var i = 0; i < 5; i++)
                    {
                        var entity =
                            World<AuthorityWorld>.NewEntity<TestEntity>();
                        entity.Set(new TestComponent { Value = i });
                    }

                    server.Tick(_ => { });
                    ReceiveSnapshotChunk(clientTransport, out _, out _);
                    Assert.That(server.TryGetCapture(scope, 1, out var first),
                        Is.True);
                    var bodyLength = first.ByteLength;
                    Assert.That(bodyLength, Is.GreaterThan(2));

                    var maxBody = bodyLength / 2 + 1;
                    serverTransport.MaxReliablePayloadBytes = PacketHeader.Size +
                        SnapshotChunkHeader.Size + maxBody;
                    serverTransport.ResetCounters();
                    server.Tick(_ => { });
                    Assert.That(server.TryGetCapture(scope, 2, out var second),
                        Is.True);
                    Assert.That(second.ByteLength, Is.EqualTo(bodyLength));

                    var expectedChunks =
                        (int)((bodyLength + (long)maxBody - 1L) / maxBody);
                    Assert.That(expectedChunks, Is.EqualTo(2));
                    Assert.That(serverTransport.ProbeBytes.Count,
                        Is.EqualTo(1 + expectedChunks));
                    Assert.That(serverTransport.ProbeBytes[0],
                        Is.EqualTo(PacketHeader.Size +
                            SnapshotChunkHeader.Size + 1),
                        "the first probe is the minimum-size preflight");
                    var offset = 0;
                    for (var i = 0; i < expectedChunks; i++)
                    {
                        var chunkBody = Math.Min(maxBody, bodyLength - offset);
                        Assert.That(serverTransport.ProbeBytes[1 + i],
                            Is.EqualTo(PacketHeader.Size +
                                SnapshotChunkHeader.Size + chunkBody));
                        offset += chunkBody;
                    }
                    Assert.That(offset, Is.EqualTo(bodyLength));
                    Assert.That(bodyLength - (expectedChunks - 1) * maxBody,
                        Is.LessThan(maxBody),
                        "the last chunk is shorter than a full chunk body");

                    var chunkOffset = 0;
                    for (uint i = 0; i < expectedChunks; i++)
                    {
                        var chunk = ReceiveSnapshotChunk(clientTransport,
                            out _, out var chunkBody);
                        Assert.That(chunk.PayloadKind,
                            Is.EqualTo(SnapshotPayloadKind.Keyframe));
                        Assert.That(chunk.ChunkIndex, Is.EqualTo(i));
                        Assert.That(chunk.ChunkCount,
                            Is.EqualTo((uint)expectedChunks));
                        var expected = Math.Min(maxBody,
                            bodyLength - chunkOffset);
                        Assert.That(chunkBody.Length, Is.EqualTo(expected));
                        chunkOffset += expected;
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
        public void TransportWithoutPreflightSendsSnapshotUnchanged()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                var scope = new ScopeId(55);
                using var pool = new NetworkBufferPool(0);
                MemoryNetworkTransport.CreatePair(new ConnectionId(825),
                    out var clientTransport, out var serverEndpoint);
                using (clientTransport)
                using (serverEndpoint)
                using (var server = new NetworkServer<AuthorityWorld>(schema,
                           static (_, _) => true, bufferPool: pool))
                {
                    server.AddConnection(serverEndpoint, 1, 1, scope);
                    SendPeerPacket(clientTransport, schema.Fingerprint,
                        PacketKind.Hello, 0, 1, 0);
                    server.Receive();
                    Assert.That(clientTransport.TryReceive(out var ready),
                        Is.True);
                    ready.Dispose();

                    var entity = World<AuthorityWorld>.NewEntity<TestEntity>();
                    entity.Set(new TestComponent { Value = 1 });
                    server.Tick(_ => { });
                    Assert.That(ReceiveChunk(clientTransport).PayloadKind,
                        Is.EqualTo(SnapshotPayloadKind.Keyframe));
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

        private sealed class PreflightNetworkTransport : INetworkTransport,
            INetworkReliableSendPreflight
        {
            private readonly INetworkTransport _inner;

            internal PreflightNetworkTransport(INetworkTransport inner)
            {
                _inner = inner;
                MaxReliablePayloadBytes = inner.MaxReliablePayloadBytes;
                MaxUnreliablePayloadBytes = inner.MaxUnreliablePayloadBytes;
            }

            internal bool CanAccept { get; set; } = true;
            internal int PreflightCalls { get; private set; }
            internal int LastRequestedBytes { get; private set; }
            internal int SentPacketCount { get; private set; }
            internal int FailOnSendNumber { get; set; }
            internal readonly Queue<bool> ProbeResults = new Queue<bool>();
            internal readonly List<int> ProbeBytes = new List<int>();

            public ConnectionId Connection => _inner.Connection;
            public int MaxReliablePayloadBytes { get; set; }
            public int MaxUnreliablePayloadBytes { get; set; }

            public bool CanAcceptReliablePacket(int packetBytes)
            {
                PreflightCalls++;
                LastRequestedBytes = packetBytes;
                ProbeBytes.Add(packetBytes);
                if (ProbeResults.Count > 0)
                    return ProbeResults.Dequeue();
                return CanAccept;
            }

            public bool TrySend(NetworkBufferLease packet)
            {
                SentPacketCount++;
                if (FailOnSendNumber == SentPacketCount)
                {
                    packet?.Dispose();
                    return false;
                }

                return _inner.TrySend(packet);
            }

            internal void ResetSentPackets() => SentPacketCount = 0;

            internal void ResetCounters()
            {
                SentPacketCount = 0;
                PreflightCalls = 0;
                ProbeResults.Clear();
                ProbeBytes.Clear();
            }

            public bool TryReceive(out NetworkBufferLease packet) =>
                _inner.TryReceive(out packet);

            public void Dispose() => _inner.Dispose();
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

        [Test]
        public void SnapshotLayoutIndex_MatchesParserOracleAcrossOperations()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            var pool = new NetworkBufferPool(4L << 20);
            var schema = Schema<AuthorityWorld>(true);
            var scope = new ScopeId(61);
            var replicator = new NetworkReplicator<AuthorityWorld>(schema,
                static (_, _) => true, scope, bufferPool: pool);
            NetworkSnapshot baseline = null;
            NetworkSnapshot unchanged = null;
            NetworkSnapshot target = null;
            try
            {
                for (var i = 0; i < 16; i++)
                {
                    var ballast = World<AuthorityWorld>.NewEntity<TestEntity>();
                    ballast.Set(new TestComponent { Value = 50 + i });
                }
                var patched = World<AuthorityWorld>.NewEntity<TestEntity>();
                patched.Set(new TestComponent { Value = 2 });
                patched.Set<TestTag>();
                var removed = World<AuthorityWorld>.NewEntity<SecondEntity>();
                removed.Set(new TestComponent { Value = 3 });

                Assert.That(replicator.Capture(1, out baseline),
                    Is.EqualTo(SnapshotCaptureResult.Success));
                Assert.That(replicator.Capture(2, out unchanged),
                    Is.EqualTo(SnapshotCaptureResult.Success));
                Assert.That(unchanged.Bytes.Span.SequenceEqual(
                    baseline.Bytes.Span), Is.True);
                AssertIndexedDeltaMatchesParser(pool, baseline, unchanged);

                patched.Set(new TestComponent { Value = 4 });
                patched.Delete<TestTag>();
                removed.Destroy();
                var added = World<AuthorityWorld>.NewEntity<SecondEntity>();
                added.Set(new TestComponent { Value = 5 });
                Assert.That(replicator.Capture(3, out target),
                    Is.EqualTo(SnapshotCaptureResult.Success));
                AssertIndexedDeltaMatchesParser(pool, baseline, target);
                AssertIndexedDeltaMatchesParser(pool, unchanged, target);
            }
            finally
            {
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
        public void SnapshotLayoutIndex_NeverBuildsForPublicSnapshots()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            var pool = new NetworkBufferPool(4L << 20);
            var schema = Schema<AuthorityWorld>(true);
            var replicator = new NetworkReplicator<AuthorityWorld>(schema,
                static (_, _) => true, new ScopeId(62), bufferPool: pool);
            var recording = new RecordingLayoutPool();
            var previous = SnapshotLayoutMemory.Pool;
            NetworkSnapshot capturedBaseline = null;
            NetworkSnapshot capturedTarget = null;
            NetworkSnapshot baseline = null;
            NetworkSnapshot target = null;
            NetworkBufferLease baselineLease = null;
            NetworkBufferLease targetLease = null;
            try
            {
                SnapshotLayoutMemory.Pool = recording;
                for (var i = 0; i < 16; i++)
                {
                    var ballast = World<AuthorityWorld>.NewEntity<TestEntity>();
                    ballast.Set(new TestComponent { Value = 50 + i });
                }
                var entity = World<AuthorityWorld>.NewEntity<TestEntity>();
                entity.Set(new TestComponent { Value = 1 });
                Assert.That(replicator.Capture(1, out capturedBaseline),
                    Is.EqualTo(SnapshotCaptureResult.Success));
                entity.Set(new TestComponent { Value = 2 });
                Assert.That(replicator.Capture(2, out capturedTarget),
                    Is.EqualTo(SnapshotCaptureResult.Success));

                baselineLease = pool.Copy(capturedBaseline.Bytes.Span);
                baseline = new NetworkSnapshot(capturedBaseline.ServerTick,
                    capturedBaseline.SchemaFingerprint, capturedBaseline.Scope,
                    baselineLease, capturedBaseline.EntityCount,
                    capturedBaseline.RecordCount);
                baselineLease = null;
                targetLease = pool.Copy(capturedTarget.Bytes.Span);
                target = new NetworkSnapshot(capturedTarget.ServerTick,
                    capturedTarget.SchemaFingerprint, capturedTarget.Scope,
                    targetLease, capturedTarget.EntityCount,
                    capturedTarget.RecordCount);
                targetLease = null;

                Assert.That(SnapshotDeltaCodec.TryEncode(pool, baseline,
                    target, out var delta), Is.True);
                delta.Dispose();
                Assert.That(recording.Rented.Count, Is.Zero,
                    "public descriptors must keep the parser/hash path");
            }
            finally
            {
                SnapshotLayoutMemory.Pool = previous;
                baselineLease?.Dispose();
                targetLease?.Dispose();
                target?.Dispose();
                baseline?.Dispose();
                capturedTarget?.Dispose();
                capturedBaseline?.Dispose();
                replicator.Dispose();
                Assert.That(pool.CaptureDiagnostics().OutstandingLeases,
                    Is.Zero);
                pool.Dispose();
                World<AuthorityWorld>.Destroy();
            }
        }

        [Test]
        public void SnapshotLayoutIndex_ReturnsEveryRentalExactlyOnce()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            var pool = new NetworkBufferPool(4L << 20);
            var schema = Schema<AuthorityWorld>(true);
            var replicator = new NetworkReplicator<AuthorityWorld>(schema,
                static (_, _) => true, new ScopeId(63), bufferPool: pool);
            var recording = new RecordingLayoutPool();
            var previous = SnapshotLayoutMemory.Pool;
            NetworkSnapshot baseline = null;
            NetworkSnapshot target = null;
            NetworkBufferLease delta = null;
            try
            {
                SnapshotLayoutMemory.Pool = recording;
                for (var i = 0; i < 16; i++)
                {
                    var ballast = World<AuthorityWorld>.NewEntity<TestEntity>();
                    ballast.Set(new TestComponent { Value = 50 + i });
                }
                var entity = World<AuthorityWorld>.NewEntity<TestEntity>();
                entity.Set(new TestComponent { Value = 1 });
                Assert.That(replicator.Capture(1, out baseline),
                    Is.EqualTo(SnapshotCaptureResult.Success));
                entity.Set(new TestComponent { Value = 2 });
                Assert.That(replicator.Capture(2, out target),
                    Is.EqualTo(SnapshotCaptureResult.Success));

                Assert.That(SnapshotDeltaCodec.TryEncode(pool, baseline,
                    target, out delta), Is.True);
                delta.Dispose();
                delta = null;
                Assert.That(recording.Rented.Count, Is.EqualTo(4),
                    "baseline and target each rent entity and record arrays");
                Assert.That(recording.Returned.Count, Is.Zero,
                    "a live layout keeps its rentals until disposal");

                baseline.Dispose();
                Assert.That(recording.Returned.Count, Is.EqualTo(2));
                target.Dispose();
                Assert.That(recording.Returned.Count, Is.EqualTo(4));

                baseline.Dispose();
                target.Dispose();
                Assert.That(recording.Returned.Count, Is.EqualTo(4));

                baseline = null;
                target = null;
                entity.Set(new TestComponent { Value = 3 });
                Assert.That(replicator.Capture(3, out baseline),
                    Is.EqualTo(SnapshotCaptureResult.Success));
                entity.Set(new TestComponent { Value = 4 });
                Assert.That(replicator.Capture(4, out target),
                    Is.EqualTo(SnapshotCaptureResult.Success));
                Assert.That(SnapshotDeltaCodec.TryEncode(pool, baseline,
                    target, out delta), Is.True);
                delta.Dispose();
                delta = null;
                Assert.That(recording.Rented.Count, Is.EqualTo(8),
                    "a reused descriptor must rent a fresh layout");
                Assert.That(recording.Returned.Count, Is.EqualTo(4));
            }
            finally
            {
                SnapshotLayoutMemory.Pool = previous;
                delta?.Dispose();
                baseline?.Dispose();
                target?.Dispose();
                AssertReturnedExactlyOnce(recording);
                replicator.Dispose();
                Assert.That(pool.CaptureDiagnostics().OutstandingLeases,
                    Is.Zero);
                pool.Dispose();
                World<AuthorityWorld>.Destroy();
            }
        }

        [Test]
        public void SnapshotLayoutIndex_ReturnsRentalsWhenValidationFails()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            var content = new NetworkBufferPool(4L << 20);
            var schema = Schema<AuthorityWorld>(true);
            var replicator = new NetworkReplicator<AuthorityWorld>(schema,
                static (_, _) => true, new ScopeId(64), bufferPool: content);
            var recording = new RecordingLayoutPool();
            var previous = SnapshotLayoutMemory.Pool;
            NetworkSnapshot good = null;
            NetworkSnapshot malformed = null;
            NetworkBufferLease malformedLease = null;
            try
            {
                SnapshotLayoutMemory.Pool = recording;
                var entity = World<AuthorityWorld>.NewEntity<TestEntity>();
                entity.Set(new TestComponent { Value = 1 });
                Assert.That(replicator.Capture(1, out good),
                    Is.EqualTo(SnapshotCaptureResult.Success));

                var bytes = good.Bytes.ToArray();
                malformedLease = content.Copy(bytes);
                malformed = replicator.CreateSnapshot(good.ServerTick + 1,
                    good.SchemaFingerprint, good.Scope, malformedLease,
                    good.EntityCount, good.RecordCount + 1);
                malformedLease = null;

                Assert.That(malformed.TryGetLayout(out _), Is.False);
                Assert.That(recording.Rented.Count, Is.EqualTo(2),
                    "the build rents before validation completes");
                Assert.That(recording.Returned.Count, Is.EqualTo(2),
                    "a failed build returns both rentals immediately");
                AssertReturnedExactlyOnce(recording);

                Assert.That(SnapshotDeltaCodec.TryEncode(content, good,
                    malformed, out var delta), Is.False);
                Assert.That(delta, Is.Null);
            }
            finally
            {
                SnapshotLayoutMemory.Pool = previous;
                malformedLease?.Dispose();
                malformed?.Dispose();
                good?.Dispose();
                AssertReturnedExactlyOnce(recording);
                replicator.Dispose();
                Assert.That(content.CaptureDiagnostics().OutstandingLeases,
                    Is.Zero);
                content.Dispose();
                World<AuthorityWorld>.Destroy();
            }
        }

        [Test]
        public void SnapshotLayoutIndex_SteadyStateEncodeAllocatesNoManagedMemory()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            var pool = new NetworkBufferPool(4L << 20);
            var schema = Schema<AuthorityWorld>(true);
            var replicator = new NetworkReplicator<AuthorityWorld>(schema,
                static (_, _) => true, new ScopeId(65), bufferPool: pool);
            NetworkSnapshot baseline = null;
            NetworkSnapshot target = null;
            NetworkBufferLease warm = null;
            NetworkBufferLease delta = null;
            try
            {
                var first = World<AuthorityWorld>.NewEntity<TestEntity>();
                first.Set(new TestComponent { Value = 1 });
                for (var i = 0; i < 8; i++)
                {
                    var entity = World<AuthorityWorld>.NewEntity<TestEntity>();
                    entity.Set(new TestComponent { Value = 100 + i });
                }
                Assert.That(replicator.Capture(1, out baseline),
                    Is.EqualTo(SnapshotCaptureResult.Success));
                first.Set(new TestComponent { Value = 2 });
                Assert.That(replicator.Capture(2, out target),
                    Is.EqualTo(SnapshotCaptureResult.Success));

                Assert.That(SnapshotDeltaCodec.TryEncode(pool, baseline,
                    target, out warm), Is.True);
                warm.Dispose();
                warm = null;

                GC.Collect();
                GC.WaitForPendingFinalizers();
                GC.Collect();
                var before = GC.GetAllocatedBytesForCurrentThread();
                var encoded = true;
                for (var i = 0; i < 32; i++)
                {
                    if (!SnapshotDeltaCodec.TryEncode(pool, baseline, target,
                            out delta))
                        encoded = false;
                    delta?.Dispose();
                    delta = null;
                }
                var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
                Assert.That(encoded, Is.True);
                Assert.That(allocated, Is.Zero,
                    "steady-state indexed encode must not allocate managed memory");
            }
            finally
            {
                warm?.Dispose();
                delta?.Dispose();
                target?.Dispose();
                baseline?.Dispose();
                replicator.Dispose();
                Assert.That(pool.CaptureDiagnostics().OutstandingLeases,
                    Is.Zero);
                pool.Dispose();
                World<AuthorityWorld>.Destroy();
            }
        }

        [Test]
        public void SnapshotLayoutIndex_ReturnsEntityRentalWhenRecordRentThrows()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            var pool = new NetworkBufferPool(4L << 20);
            var schema = Schema<AuthorityWorld>(true);
            var replicator = new NetworkReplicator<AuthorityWorld>(schema,
                static (_, _) => true, new ScopeId(66), bufferPool: pool);
            var throwing = new ThrowingRecordRentPool();
            var previous = SnapshotLayoutMemory.Pool;
            NetworkSnapshot baseline = null;
            NetworkSnapshot target = null;
            NetworkBufferLease delta = null;
            try
            {
                SnapshotLayoutMemory.Pool = throwing;
                for (var i = 0; i < 16; i++)
                {
                    var ballast = World<AuthorityWorld>.NewEntity<TestEntity>();
                    ballast.Set(new TestComponent { Value = 50 + i });
                }
                var entity = World<AuthorityWorld>.NewEntity<TestEntity>();
                entity.Set(new TestComponent { Value = 1 });
                Assert.That(replicator.Capture(1, out baseline),
                    Is.EqualTo(SnapshotCaptureResult.Success));
                entity.Set(new TestComponent { Value = 2 });
                Assert.That(replicator.Capture(2, out target),
                    Is.EqualTo(SnapshotCaptureResult.Success));

                Assert.That(baseline.TryGetLayout(out _), Is.False);
                Assert.That(baseline.HasLayout, Is.False);
                Assert.That(target.HasLayout, Is.False);
                Assert.That(throwing.RentedEntities.Count, Is.EqualTo(1));
                Assert.That(throwing.ReturnedEntities.Count, Is.EqualTo(1));
                Assert.That(ReferenceEquals(throwing.RentedEntities[0],
                    throwing.ReturnedEntities[0]), Is.True);

                // TryEncode must still fall back to the parser without leaking
                _ = SnapshotDeltaCodec.TryEncode(pool, baseline, target,
                    out delta);
                Assert.That(baseline.HasLayout, Is.False);
                Assert.That(throwing.RentedEntities.Count,
                    Is.EqualTo(throwing.ReturnedEntities.Count));
            }
            finally
            {
                SnapshotLayoutMemory.Pool = previous;
                delta?.Dispose();
                target?.Dispose();
                baseline?.Dispose();
                replicator.Dispose();
                Assert.That(pool.CaptureDiagnostics().OutstandingLeases,
                    Is.Zero);
                pool.Dispose();
                World<AuthorityWorld>.Destroy();
            }
        }

        [Test]
        public void SnapshotLayoutIndex_DoesNotPublishBaselineForUnindexedTarget()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            var pool = new NetworkBufferPool(4L << 20);
            var schema = Schema<AuthorityWorld>(true);
            var replicator = new NetworkReplicator<AuthorityWorld>(schema,
                static (_, _) => true, new ScopeId(67), bufferPool: pool);
            var recording = new RecordingLayoutPool();
            var previous = SnapshotLayoutMemory.Pool;
            NetworkSnapshot captured = null;
            NetworkSnapshot baseline = null;
            NetworkSnapshot publicTarget = null;
            NetworkBufferLease targetLease = null;
            NetworkBufferLease delta = null;
            try
            {
                SnapshotLayoutMemory.Pool = recording;
                for (var i = 0; i < 16; i++)
                {
                    var ballast = World<AuthorityWorld>.NewEntity<TestEntity>();
                    ballast.Set(new TestComponent { Value = 50 + i });
                }
                var entity = World<AuthorityWorld>.NewEntity<TestEntity>();
                entity.Set(new TestComponent { Value = 1 });
                Assert.That(replicator.Capture(1, out captured),
                    Is.EqualTo(SnapshotCaptureResult.Success));
                entity.Set(new TestComponent { Value = 2 });
                Assert.That(replicator.Capture(2, out baseline),
                    Is.EqualTo(SnapshotCaptureResult.Success));
                targetLease = pool.Copy(captured.Bytes.Span);
                publicTarget = new NetworkSnapshot(baseline.ServerTick + 1,
                    baseline.SchemaFingerprint, baseline.Scope, targetLease,
                    baseline.EntityCount, baseline.RecordCount);
                targetLease = null;

                _ = SnapshotDeltaCodec.TryEncode(pool, baseline, publicTarget,
                    out delta);
                Assert.That(baseline.HasLayout, Is.False,
                    "a baseline-only layout must not be published");
                Assert.That(recording.Rented.Count,
                    Is.EqualTo(recording.Returned.Count));
            }
            finally
            {
                SnapshotLayoutMemory.Pool = previous;
                delta?.Dispose();
                targetLease?.Dispose();
                publicTarget?.Dispose();
                baseline?.Dispose();
                captured?.Dispose();
                replicator.Dispose();
                Assert.That(pool.CaptureDiagnostics().OutstandingLeases,
                    Is.Zero);
                pool.Dispose();
                World<AuthorityWorld>.Destroy();
            }
        }

        [Test]
        public void SnapshotLayoutIndex_FallsBackBeforeRentingOverMemoryBound()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            var content = new NetworkBufferPool(4L << 20);
            var schema = Schema<AuthorityWorld>(true);
            var replicator = new NetworkReplicator<AuthorityWorld>(schema,
                static (_, _) => true, new ScopeId(68), bufferPool: content);
            var recording = new RecordingLayoutPool();
            var previous = SnapshotLayoutMemory.Pool;
            NetworkSnapshot good = null;
            NetworkSnapshot huge = null;
            NetworkBufferLease hugeLease = null;
            try
            {
                SnapshotLayoutMemory.Pool = recording;
                var entity = World<AuthorityWorld>.NewEntity<TestEntity>();
                entity.Set(new TestComponent { Value = 1 });
                Assert.That(replicator.Capture(1, out good),
                    Is.EqualTo(SnapshotCaptureResult.Success));

                var bytes = good.Bytes.ToArray();
                hugeLease = content.Copy(bytes);
                huge = replicator.CreateSnapshot(good.ServerTick + 1,
                    good.SchemaFingerprint, good.Scope, hugeLease,
                    ProtocolLimits.MaxEntities,
                    ProtocolLimits.MaxEntities *
                    ProtocolLimits.MaxRecordsPerEntity);
                hugeLease = null;

                Assert.That(huge.TryGetLayout(out _), Is.False);
                Assert.That(huge.HasLayout, Is.False);
                Assert.That(recording.Rented.Count, Is.Zero,
                    "the memory bound must be checked before any rental");
            }
            finally
            {
                SnapshotLayoutMemory.Pool = previous;
                hugeLease?.Dispose();
                huge?.Dispose();
                good?.Dispose();
                replicator.Dispose();
                Assert.That(content.CaptureDiagnostics().OutstandingLeases,
                    Is.Zero);
                content.Dispose();
                World<AuthorityWorld>.Destroy();
            }
        }

        private static void AssertIndexedDeltaMatchesParser(
            NetworkBufferPool pool, NetworkSnapshot baseline,
            NetworkSnapshot target)
        {
            NetworkBufferLease indexed = null;
            NetworkBufferLease parsed = null;
            NetworkSnapshot baselineClone = null;
            NetworkSnapshot targetClone = null;
            try
            {
                Assert.That(SnapshotDeltaCodec.TryEncode(pool, baseline,
                    target, out indexed), Is.True);
                baselineClone = new NetworkSnapshot(baseline.ServerTick,
                    baseline.SchemaFingerprint, baseline.Scope,
                    pool.Copy(baseline.Bytes.Span), baseline.EntityCount,
                    baseline.RecordCount);
                targetClone = new NetworkSnapshot(target.ServerTick,
                    target.SchemaFingerprint, target.Scope,
                    pool.Copy(target.Bytes.Span), target.EntityCount,
                    target.RecordCount);
                Assert.That(SnapshotDeltaCodec.TryEncode(pool, baselineClone,
                    targetClone, out parsed), Is.True);
                Assert.That(indexed.Span.SequenceEqual(parsed.Span), Is.True,
                    "indexed encode must be byte-identical to the parser oracle");
            }
            finally
            {
                parsed?.Dispose();
                targetClone?.Dispose();
                baselineClone?.Dispose();
                indexed?.Dispose();
            }
        }

        private static void AssertReturnedExactlyOnce(
            RecordingLayoutPool recording)
        {
            Assert.That(recording.Returned.Count,
                Is.EqualTo(recording.Rented.Count));
            // Compare as a multiset: the array pool is free to hand the same
            // array back out for a later rental, but each rental must return
            // exactly one matching array.
            for (var i = 0; i < recording.Rented.Count; i++)
            {
                var rented = 0;
                var returned = 0;
                for (var j = 0; j < recording.Rented.Count; j++)
                    if (ReferenceEquals(recording.Rented[i],
                            recording.Rented[j]))
                        rented++;
                for (var j = 0; j < recording.Returned.Count; j++)
                    if (ReferenceEquals(recording.Rented[i],
                            recording.Returned[j]))
                        returned++;
                Assert.That(returned, Is.EqualTo(rented),
                    "every rental must be returned exactly once");
            }
        }

        private sealed class RecordingLayoutPool : ISnapshotLayoutPool
        {
            internal readonly List<object> Rented = new List<object>();
            internal readonly List<object> Returned = new List<object>();

            public SnapshotEntityLayout[] RentEntities(int length)
            {
                var array = ArrayPool<SnapshotEntityLayout>.Shared.Rent(length);
                Rented.Add(array);
                return array;
            }

            public void ReturnEntities(SnapshotEntityLayout[] array)
            {
                Returned.Add(array);
                ArrayPool<SnapshotEntityLayout>.Shared.Return(array);
            }

            public SnapshotRecordLayout[] RentRecords(int length)
            {
                var array = ArrayPool<SnapshotRecordLayout>.Shared.Rent(length);
                Rented.Add(array);
                return array;
            }

            public void ReturnRecords(SnapshotRecordLayout[] array)
            {
                Returned.Add(array);
                ArrayPool<SnapshotRecordLayout>.Shared.Return(array);
            }
        }

        private sealed class ThrowingRecordRentPool : ISnapshotLayoutPool
        {
            internal readonly List<object> RentedEntities =
                new List<object>();
            internal readonly List<object> ReturnedEntities =
                new List<object>();

            public SnapshotEntityLayout[] RentEntities(int length)
            {
                var array = ArrayPool<SnapshotEntityLayout>.Shared.Rent(length);
                RentedEntities.Add(array);
                return array;
            }

            public void ReturnEntities(SnapshotEntityLayout[] array)
            {
                ReturnedEntities.Add(array);
                ArrayPool<SnapshotEntityLayout>.Shared.Return(array);
            }

            public SnapshotRecordLayout[] RentRecords(int length) =>
                throw new InvalidOperationException("test record rent failure");

            public void ReturnRecords(SnapshotRecordLayout[] array) =>
                ArrayPool<SnapshotRecordLayout>.Shared.Return(array);
        }

    }
}
