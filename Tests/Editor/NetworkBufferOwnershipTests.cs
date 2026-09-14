namespace UniGame.StaticEcs.Network.Tests
{
    using NUnit.Framework;

    /// <summary>Verifies explicit ownership release for pooled network buffers.</summary>
    public sealed class NetworkBufferOwnershipTests
    {
        [Test]
        public void FailedSendConsumesLease()
        {
            using var pool = new NetworkBufferPool(1024);
            MemoryNetworkTransport.CreatePair(new ConnectionId(1), out var client,
                out var server);
            client.Dispose();

            Assert.That(client.TrySend(pool.Copy(new byte[] { 1, 2, 3 })), Is.False);
            AssertReleased(pool);
            server.Dispose();
        }

        [Test]
        public void DisposedSimulatorOwnerConsumesSendLeaseAndReceiveReturnsFalse()
        {
            using var pool = new NetworkBufferPool(1024);
            var config = NetworkSimulationPresets.Create(NetworkSimulationPreset.Immediate);
            using var simulator = new NetworkSimulator(new ConnectionId(2), in config);
            var endpoint = simulator.Client;
            var completePacketLimit = PacketHeader.Size + ProtocolLimits.MaxWirePayloadBytes;
            Assert.That(endpoint.MaxReliablePayloadBytes, Is.EqualTo(completePacketLimit));
            Assert.That(endpoint.MaxUnreliablePayloadBytes, Is.EqualTo(completePacketLimit));

            simulator.Dispose();

            Assert.That(endpoint.TrySend(pool.Copy(new byte[] { 1 })), Is.False);
            AssertReleased(pool);
            Assert.That(endpoint.TryReceive(out var packet), Is.False);
            Assert.That(packet, Is.Null);
        }

        [Test]
        public void HistoryEvictionAndClearReleaseSnapshots()
        {
            using var pool = new NetworkBufferPool(1024);
            var history = new NetworkHistory<NetworkSnapshot>(1, 1024,
                snapshot => snapshot.ByteLength, snapshot => snapshot.Dispose());

            history.Store(1, Snapshot(pool, 1));
            history.Store(2, Snapshot(pool, 2));

            Assert.That(pool.CaptureDiagnostics().OutstandingLeases, Is.EqualTo(1));
            history.Clear();
            AssertReleased(pool);
        }

        [Test]
        public void PooledSnapshotDescriptorDisposeIsIdempotentAndReusable()
        {
            using var pool = new NetworkBufferPool(1024);
            var snapshots = new NetworkSnapshotPool(2);
            var descriptor = snapshots.Rent(1, default, new ScopeId(7),
                pool.Copy(new byte[] { 1, 2, 3, 4 }), 1, 1);

            descriptor.Dispose();
            AssertReleased(pool);
            descriptor.Dispose();
            AssertReleased(pool);

            var reused = snapshots.Rent(2, default, new ScopeId(7),
                pool.Copy(new byte[] { 5, 6, 7, 8 }), 1, 1);
            Assert.That(reused, Is.SameAs(descriptor));
            reused.Dispose();
            AssertReleased(pool);
        }

        [Test]
        public void SimulatorLossDuplicationAndResetReleaseEveryLease()
        {
            using var pool = new NetworkBufferPool(4096);
            var config = NetworkSimulationPresets.Create(NetworkSimulationPreset.Local);
            config.LossProbability = 1f;
            using var simulator = new NetworkSimulator(new ConnectionId(1), in config);

            Assert.That(simulator.Client.TrySend(pool.Copy(new byte[] { 1 })), Is.True);
            AssertReleased(pool);

            config.LossProbability = 0f;
            config.DuplicateProbability = 1f;
            config.LatencyMilliseconds = 100;
            simulator.ApplyConfig(in config);
            Assert.That(simulator.Client.TrySend(pool.Copy(new byte[] { 2 })), Is.True);
            Assert.That(pool.CaptureDiagnostics().OutstandingLeases, Is.EqualTo(2));

            simulator.Reset();
            AssertReleased(pool);
        }

        [Test]
        public void PoolDisposalAllowsOutstandingLeaseToReturn()
        {
            var pool = new NetworkBufferPool(1024);
            var lease = pool.Rent(8);

            pool.Dispose();
            lease.Dispose();

            AssertReleased(pool);
        }

        [Test]
        public void ActiveBucketReusesAfterLargerInactiveBucketConsumesRetainedBudget()
        {
            const int inactiveCapacity = 4096;
            using var pool = new NetworkBufferPool(inactiveCapacity);

            RentAndRelease(pool, inactiveCapacity);
            RentAndRelease(pool, 64);
            var afterWarmup = pool.CaptureDiagnostics().PoolMisses;
            for (var i = 0; i < 8; i++)
                RentAndRelease(pool, 64);

            var diagnostics = pool.CaptureDiagnostics();
            Assert.That(diagnostics.PoolMisses, Is.EqualTo(afterWarmup),
                "warm active bucket must reuse instead of reallocating every rent");
            Assert.That(diagnostics.OutstandingLeases, Is.Zero);
            Assert.That(diagnostics.RetainedBytes,
                Is.LessThanOrEqualTo(inactiveCapacity));
        }

        [Test]
        public void MultiBucketWorkingSetFitsBudgetWithoutMissGrowthOrPingPong()
        {
            var sizes = new[] { 256, 512, 1024 };
            using var pool = new NetworkBufferPool(4096);

            RentAndRelease(pool, 4096);

            for (var i = 0; i < sizes.Length; i++)
                RentAndRelease(pool, sizes[i]);
            var warm = pool.CaptureDiagnostics();
            Assert.That(warm.RetainedBytes, Is.EqualTo(256 + 512 + 1024));
            Assert.That(warm.RetainedBytes, Is.LessThanOrEqualTo(4096));

            for (var cycle = 0; cycle < 4; cycle++)
                for (var i = 0; i < sizes.Length; i++)
                    RentAndRelease(pool, sizes[i]);

            var diagnostics = pool.CaptureDiagnostics();
            Assert.That(diagnostics.PoolMisses, Is.EqualTo(warm.PoolMisses));
            Assert.That(diagnostics.RetainedBytes,
                Is.EqualTo(warm.RetainedBytes));
            Assert.That(diagnostics.OutstandingLeases, Is.Zero);
        }

        [Test]
        public void AdoptedNonBucketBufferDisposesWithoutThrowingAndIsNotRetained()
        {
            using var pool = new NetworkBufferPool(1024);
            var lease = pool.Adopt(new byte[300], 300);
            var retained = lease.Retain();
            Assert.That(lease.Length, Is.EqualTo(300));

            lease.Dispose();
            Assert.That(pool.CaptureDiagnostics().OutstandingLeases,
                Is.EqualTo(1));

            Assert.DoesNotThrow(() => retained.Dispose());

            var diagnostics = pool.CaptureDiagnostics();
            Assert.That(diagnostics.RetainedBytes, Is.Zero);
            Assert.That(diagnostics.OutstandingLeases, Is.Zero);
            Assert.That(diagnostics.OutstandingBytes, Is.Zero);

            RentAndRelease(pool, 256);
            Assert.That(pool.CaptureDiagnostics().RetainedBytes,
                Is.EqualTo(256));
        }

        [Test]
        public void NoSingleSufficientVictimFallsBackToBoundedDiscard()
        {
            using var pool = new NetworkBufferPool(512);
            var first = pool.Rent(256);
            var second = pool.Rent(256);
            first.Dispose();
            second.Dispose();
            var before = pool.CaptureDiagnostics();
            Assert.That(before.RetainedBytes, Is.EqualTo(512));

            RentAndRelease(pool, 512);

            var after = pool.CaptureDiagnostics();
            Assert.That(after.RetainedBytes, Is.EqualTo(512),
                "no single victim frees enough, so the incoming buffer is discarded");
            Assert.That(after.PoolMisses, Is.EqualTo(before.PoolMisses + 1));
            RentAndRelease(pool, 256);
            Assert.That(pool.CaptureDiagnostics().PoolMisses,
                Is.EqualTo(after.PoolMisses));
        }

        [Test]
        public void ZeroRetainedBudgetDiscardsIncomingWithoutReplacement()
        {
            using var pool = new NetworkBufferPool(0);

            RentAndRelease(pool, 256);
            RentAndRelease(pool, 256);

            var diagnostics = pool.CaptureDiagnostics();
            Assert.That(diagnostics.RetainedBytes, Is.Zero);
            Assert.That(diagnostics.OutstandingLeases, Is.Zero);
            Assert.That(diagnostics.PoolMisses, Is.EqualTo(2));
        }

        [Test]
        public void OversizedIncomingBufferIsDiscardedBeforeCachedBuffers()
        {
            using var pool = new NetworkBufferPool(512);
            RentAndRelease(pool, 512);
            var before = pool.CaptureDiagnostics();

            RentAndRelease(pool, 1024);

            var after = pool.CaptureDiagnostics();
            Assert.That(after.RetainedBytes, Is.EqualTo(512),
                "oversized buffer must not evict smaller cached buffers");
            Assert.That(after.PoolMisses, Is.EqualTo(before.PoolMisses + 1));
        }

        [Test]
        public void ExactRemainingBudgetRetainsWithoutReplacement()
        {
            using var pool = new NetworkBufferPool(512);
            var first = pool.Rent(256);
            var second = pool.Rent(256);
            first.Dispose();
            var before = pool.CaptureDiagnostics();
            Assert.That(before.RetainedBytes, Is.EqualTo(256));

            second.Dispose();

            var after = pool.CaptureDiagnostics();
            Assert.That(after.RetainedBytes, Is.EqualTo(512));
            Assert.That(after.PoolMisses, Is.EqualTo(before.PoolMisses));
        }

        [Test]
        public void ReplacementDeferredUntilLastOwnerReferenceIsReleased()
        {
            using var pool = new NetworkBufferPool(4096);
            RentAndRelease(pool, 4096);

            var lease = pool.Rent(64);
            var retained = lease.Retain();
            lease.Dispose();

            Assert.That(pool.CaptureDiagnostics().RetainedBytes,
                Is.EqualTo(4096),
                "shared owner must not return its buffer before the last reference closes");

            retained.Dispose();

            var diagnostics = pool.CaptureDiagnostics();
            Assert.That(diagnostics.RetainedBytes, Is.EqualTo(256));
            Assert.That(diagnostics.OutstandingLeases, Is.Zero);
        }

        private static NetworkSnapshot Snapshot(NetworkBufferPool pool, uint tick) =>
            new NetworkSnapshot(tick, default, default,
                pool.Copy(new[] { (byte)tick }), 0, 0);

        private static void RentAndRelease(NetworkBufferPool pool, int length)
        {
            var lease = pool.Rent(length);
            Assert.That(lease.Length, Is.EqualTo(length));
            lease.Dispose();
        }

        private static void AssertReleased(NetworkBufferPool pool)
        {
            var diagnostics = pool.CaptureDiagnostics();
            Assert.That(diagnostics.OutstandingLeases, Is.Zero);
            Assert.That(diagnostics.OutstandingBytes, Is.Zero);
        }
    }
}
