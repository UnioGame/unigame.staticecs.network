using System.Linq;
using FFS.Libraries.StaticEcs;
using NUnit.Framework;

namespace UniGame.StaticEcs.Network.Tests
{
    public sealed partial class NetworkV7Tests
    {
        [TestCase(0L, 0, 0L)]
        [TestCase(15L, 15, 15L)]
        [TestCase(16L, 16, 17L)]
        [TestCase(31L, 23, 31L)]
        [TestCase(1000L, 63, 1023L)]
        public void ValueHistogramBucketUpperBoundCoversValue(long value,
            int expectedBucket, long expectedUpperBound)
        {
            var bucket = NetworkValueHistogram.BucketOf(value);
            Assert.That(bucket, Is.EqualTo(expectedBucket));
            Assert.That(NetworkValueHistogram.UpperBound(bucket),
                Is.EqualTo(expectedUpperBound));
            Assert.That(NetworkValueHistogram.UpperBound(bucket), Is.GreaterThanOrEqualTo(value));
            Assert.That(NetworkValueHistogram.UpperBound(NetworkValueHistogram.BucketCount - 1),
                Is.EqualTo(int.MaxValue));
        }

        [TestCase(false)]
        [TestCase(true)]
        public void ReplicationDiagnosticsSplitKeyframeAndDeltaWithReplicaChurn(bool smallDatagram)
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                using var pool = new NetworkBufferPool(0);
                MemoryNetworkTransport.CreatePair(new ConnectionId(901),
                    out var clientTransport, out var serverTransport);
                using (clientTransport)
                using (serverTransport)
                using (var server = new NetworkServer<AuthorityWorld>(schema,
                           static (_, _) => true, bufferPool: pool))
                {
                    // A datagram that holds the headers and eight body bytes is smaller than any delta.
                    INetworkTransport peerTransport = smallDatagram
                        ? new LimitedNetworkTransport(serverTransport,
                            PacketHeader.Size + SnapshotChunkHeader.Size + 8)
                        : serverTransport;
                    server.AddConnection(peerTransport, 1, 1, new ScopeId(5));
                    SendPeerPacket(clientTransport, schema.Fingerprint,
                        PacketKind.Hello, 0, 1, 0);
                    server.Receive();
                    Assert.That(clientTransport.TryReceive(out var ready), Is.True);
                    ready.Dispose();

                    var patched = World<AuthorityWorld>.NewEntity<TestEntity>();
                    patched.Set(new TestComponent { Value = 1 });
                    var removed = World<AuthorityWorld>.NewEntity<SecondEntity>();
                    removed.Set(new TestComponent { Value = 2 });
                    for (var i = 0; i < 6; i++)
                        World<AuthorityWorld>.NewEntity<TestEntity>()
                            .Set(new TestComponent { Value = 100 + i });
                    server.Tick(_ => { });
                    Assert.That(ReceiveChunk(clientTransport).PayloadKind,
                        Is.EqualTo(SnapshotPayloadKind.Keyframe));
                    SendPeerPacket(clientTransport, schema.Fingerprint,
                        PacketKind.Ack, 1, 2, 1);
                    server.Receive();

                    patched.Set(new TestComponent { Value = 3 });
                    removed.Destroy();
                    World<AuthorityWorld>.NewEntity<TestEntity>()
                        .Set(new TestComponent { Value = 4 });
                    server.Tick(_ => { });
                    Assert.That(ReceiveChunk(clientTransport).PayloadKind,
                        Is.EqualTo(SnapshotPayloadKind.Delta));

                    var diagnostics = server.CaptureReplicationDiagnostics();
                    Assert.That(diagnostics.KeyframeSnapshots, Is.EqualTo(1));
                    Assert.That(diagnostics.KeyframeEntities, Is.EqualTo(8));
                    Assert.That(diagnostics.KeyframeBytes, Is.GreaterThan(0));
                    Assert.That(diagnostics.DeltaSnapshots, Is.EqualTo(1));
                    Assert.That(diagnostics.DeltaTargetEntities, Is.EqualTo(8));
                    Assert.That(diagnostics.DeltaEntityAdds, Is.EqualTo(1));
                    Assert.That(diagnostics.DeltaEntityRemoves, Is.EqualTo(1));
                    Assert.That(diagnostics.DeltaEntityPatches, Is.EqualTo(1));
                    Assert.That(diagnostics.DeltaBytes,
                        Is.LessThan(diagnostics.KeyframeBytes));
                    Assert.That(diagnostics.DeltaSnapshotBytes.Count, Is.EqualTo(1),
                        "A keyframe is not a delta sample.");
                    Assert.That(diagnostics.DeltaSnapshotBytes.Sum,
                        Is.EqualTo(diagnostics.DeltaBytes));
                    Assert.That(diagnostics.DeltaSnapshotBytes.Max,
                        Is.EqualTo(diagnostics.DeltaBytes));
                    Assert.That(diagnostics.DeltaSnapshotsOverDatagram,
                        Is.EqualTo(smallDatagram ? 1 : 0));
                    Assert.That(diagnostics.DeltaBaselineLagTicks.Count, Is.EqualTo(1));
                    Assert.That(diagnostics.DeltaBaselineLagTicks.Sum, Is.EqualTo(1),
                        "The delta of tick 2 is encoded against the acknowledged tick 1.");
                    Assert.That(diagnostics.ScopeCaptureEntities.Count, Is.EqualTo(2));
                    Assert.That(diagnostics.ScopeCaptureEntities.Sum, Is.EqualTo(16));
                    Assert.That(diagnostics.ScopeCaptureEntities.Max, Is.EqualTo(8));
                    Assert.That(diagnostics.ScopeCaptureBytes.Count, Is.EqualTo(2));
                    Assert.That(diagnostics.CaptureFailures(
                        SnapshotCaptureResult.LimitExceeded), Is.Zero);
                    Assert.That(diagnostics.Census, Is.Null);
                }
                Assert.That(pool.CaptureDiagnostics().OutstandingLeases, Is.Zero);
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
            }
        }

        [Test]
        public void ReplicationCensusCountsChangedUnchangedAndObservedEntities()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                using var pool = new NetworkBufferPool(0);
                MemoryNetworkTransport.CreatePair(new ConnectionId(902),
                    out var clientTransport, out var serverTransport);
                using (clientTransport)
                using (serverTransport)
                using (var server = new NetworkServer<AuthorityWorld>(schema,
                           static (_, entity) => entity.EntityType == default(TestEntity).Id(),
                           bufferPool: pool))
                {
                    server.EnableReplicationCensus(1);
                    server.AddConnection(serverTransport, 1, 1, new ScopeId(6));
                    SendPeerPacket(clientTransport, schema.Fingerprint,
                        PacketKind.Hello, 0, 1, 0);
                    server.Receive();
                    Assert.That(clientTransport.TryReceive(out var ready), Is.True);
                    ready.Dispose();

                    var moving = World<AuthorityWorld>.NewEntity<TestEntity>();
                    moving.Set(new TestComponent { Value = 1 });
                    World<AuthorityWorld>.NewEntity<TestEntity>()
                        .Set(new TestComponent { Value = 2 });
                    World<AuthorityWorld>.NewEntity<SecondEntity>()
                        .Set(new TestComponent { Value = 3 });
                    server.Tick(_ => { });
                    ReceiveChunk(clientTransport);
                    SendPeerPacket(clientTransport, schema.Fingerprint,
                        PacketKind.Ack, 1, 2, 1);
                    server.Receive();

                    moving.Set(new TestComponent { Value = 5 });
                    server.Tick(_ => { });

                    var census = server.CaptureReplicationDiagnostics().Census;
                    Assert.That(census, Is.Not.Null);
                    Assert.That(census.IntervalTicks, Is.EqualTo(1));
                    Assert.That(census.Ticks, Is.EqualTo(2));
                    Assert.That(census.Failures, Is.Zero);
                    Assert.That(census.ScopeComparisonsUnavailable, Is.EqualTo(1));
                    Assert.That(census.ScopeComparisons, Is.EqualTo(1));
                    Assert.That(census.PeersWithoutCapture, Is.Zero);

                    var tested = census.EntityKinds.Single(kind => kind.Name == nameof(TestEntity));
                    Assert.That(tested.WorldEntities, Is.EqualTo(4));
                    Assert.That(tested.ObservedEntities, Is.EqualTo(4));
                    Assert.That(tested.ComparedEntities, Is.EqualTo(2));
                    Assert.That(tested.ChangedEntities, Is.EqualTo(1));
                    Assert.That(tested.AddedEntities, Is.Zero);
                    Assert.That(tested.RemovedEntities, Is.Zero);

                    var unobserved = census.EntityKinds.Single(kind => kind.Name == nameof(SecondEntity));
                    Assert.That(unobserved.WorldEntities, Is.EqualTo(2));
                    Assert.That(unobserved.ObservedEntities, Is.Zero);

                    var component = census.RecordTypes.Single(record => record.Name == nameof(TestComponent));
                    Assert.That(component.ComparedRecords, Is.EqualTo(2));
                    Assert.That(component.UnchangedRecords, Is.EqualTo(1));
                }
                Assert.That(pool.CaptureDiagnostics().OutstandingLeases, Is.Zero);
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
            }
        }
    }
}
