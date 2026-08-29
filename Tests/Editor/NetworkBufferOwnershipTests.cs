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

        private static NetworkSnapshot Snapshot(NetworkBufferPool pool, uint tick) =>
            new NetworkSnapshot(tick, default, default,
                pool.Copy(new[] { (byte)tick }), 0, 0);

        private static void AssertReleased(NetworkBufferPool pool)
        {
            var diagnostics = pool.CaptureDiagnostics();
            Assert.That(diagnostics.OutstandingLeases, Is.Zero);
            Assert.That(diagnostics.OutstandingBytes, Is.Zero);
        }
    }
}
