using System.Collections.Generic;
using NUnit.Framework;

namespace UniGame.StaticEcs.Network.Tests
{
    internal sealed class NetworkSimulatorTests
    {
        private static readonly NetworkBufferPool Buffers = new NetworkBufferPool(1 << 20);

        [Test]
        public void SameSeedProducesSameDecisionsAndDelivery()
        {
            var config = NetworkSimulationPresets.Create(NetworkSimulationPreset.Unstable);
            config.Seed = 42;
            using var first = new NetworkSimulator(new ConnectionId(1), in config);
            using var second = new NetworkSimulator(new ConnectionId(1), in config);

            for (var index = 0; index < 64; index++)
            {
                Assert.That(first.Client.TrySend(Packet(index)),
                    Is.EqualTo(second.Client.TrySend(Packet(index))));
            }
            first.Advance(1000);
            second.Advance(1000);

            CollectionAssert.AreEqual(ReadAll(first.Server), ReadAll(second.Server));
            CollectionAssert.AreEqual(first.CaptureDecisions(), second.CaptureDecisions());
        }

        [Test]
        public void PacketDoesNotArriveBeforeScheduledTime()
        {
            var config = NetworkSimulationPresets.Create(NetworkSimulationPreset.Immediate);
            config.LatencyMilliseconds = 50;
            using var simulator = new NetworkSimulator(new ConnectionId(2), in config);

            Assert.That(simulator.Client.TrySend(Packet(1)), Is.True);
            simulator.Advance(49);
            Assert.That(simulator.Server.TryReceive(out _), Is.False);
            simulator.Advance(1);
            Assert.That(simulator.Server.TryReceive(out var packet), Is.True);
            Assert.That(packet.Span[0], Is.EqualTo(1));
            packet.Dispose();
        }

        [Test]
        public void BandwidthTokensAccumulateForWholePacket()
        {
            var config = NetworkSimulationPresets.Create(NetworkSimulationPreset.Immediate);
            config.BandwidthBytesPerSecond = 10;
            using var simulator = new NetworkSimulator(new ConnectionId(3), in config);

            Assert.That(simulator.Client.TrySend(Bytes(15)), Is.True);
            simulator.Advance(1000);
            Assert.That(simulator.Server.TryReceive(out _), Is.False);
            simulator.Advance(500);
            Assert.That(simulator.Server.TryReceive(out var packet), Is.True);
            Assert.That(packet.Length, Is.EqualTo(15));
            packet.Dispose();
        }

        [Test]
        public void BoundsDropNewestAndNeverGrowPastLimits()
        {
            var config = NetworkSimulationPresets.Create(NetworkSimulationPreset.Immediate);
            config.LatencyMilliseconds = 100;
            config.MaxQueuedPackets = 2;
            config.MaxQueuedBytes = 4;
            using var simulator = new NetworkSimulator(new ConnectionId(4), in config);

            Assert.That(simulator.Client.TrySend(Bytes(2)), Is.True);
            Assert.That(simulator.Client.TrySend(Bytes(2)), Is.True);
            Assert.That(simulator.Client.TrySend(Bytes(1)), Is.False);

            var stats = simulator.CaptureStats().ClientToServer;
            Assert.That(stats.QueuedPackets, Is.EqualTo(2));
            Assert.That(stats.QueuedBytes, Is.EqualTo(4));
            Assert.That(stats.OverflowPackets, Is.EqualTo(1));
        }

        [Test]
        public void PauseRetainsQueueAndResumeDelivers()
        {
            var config = NetworkSimulationPresets.Create(NetworkSimulationPreset.Immediate);
            using var simulator = new NetworkSimulator(new ConnectionId(5), in config);
            simulator.SetPaused(true);
            simulator.Client.TrySend(Packet(5));
            simulator.Advance(100);
            Assert.That(simulator.Server.TryReceive(out _), Is.False);
            Assert.That(simulator.CaptureStats().ClientToServer.QueuedPackets, Is.EqualTo(1));

            simulator.SetPaused(false);
            simulator.Advance(0);
            Assert.That(simulator.Server.TryReceive(out var resumed), Is.True);
            resumed.Dispose();
        }

        [Test]
        public void DisconnectClearsAndReconnectStartsNewGeneration()
        {
            var config = NetworkSimulationPresets.Create(NetworkSimulationPreset.Immediate);
            using var simulator = new NetworkSimulator(new ConnectionId(6), in config);
            simulator.Client.TrySend(Packet(6));
            var generation = simulator.CaptureStats().ConnectionGeneration;

            simulator.Disconnect();
            Assert.That(simulator.CaptureStats().ClientToServer.QueuedPackets, Is.Zero);
            Assert.That(simulator.Client.TrySend(Packet(7)), Is.False);
            simulator.Connect();
            Assert.That(simulator.CaptureStats().ConnectionGeneration, Is.GreaterThan(generation));
            Assert.That(simulator.Client.TrySend(Packet(8)), Is.True);
        }

        [Test]
        public void RecordedDecisionsReplayWithRelativeTiming()
        {
            var config = NetworkSimulationPresets.Create(NetworkSimulationPreset.Local);
            config.Seed = 73;
            using var simulator = new NetworkSimulator(new ConnectionId(7), in config);
            simulator.StartRecording();
            simulator.Client.TrySend(Packet(7));
            var recording = simulator.StopRecording();

            simulator.Reset();
            simulator.Advance(1000);
            simulator.StartReplay(recording);
            Assert.That(simulator.Client.TrySend(Packet(7)), Is.True);
            simulator.Advance(14);
            Assert.That(simulator.Server.TryReceive(out _), Is.False);
            simulator.Advance(20);
            Assert.That(simulator.Server.TryReceive(out var replayed), Is.True);
            replayed.Dispose();
            Assert.That(simulator.CaptureStats().ReplayErrors, Is.Zero);
        }

        [Test]
        public void ReplayMismatchFailsClosed()
        {
            var config = NetworkSimulationPresets.Create(NetworkSimulationPreset.Immediate);
            using var simulator = new NetworkSimulator(new ConnectionId(8), in config);
            var decisions = new List<NetworkSimulationDecision>
            {
                new NetworkSimulationDecision
                {
                    Direction = NetworkSimulationDirection.ServerToClient,
                    Ordinal = 1,
                    Bytes = 1,
                    Kind = NetworkSimulationDecisionKind.Scheduled
                }
            };

            simulator.StartReplay(decisions);
            Assert.That(simulator.Client.TrySend(Packet(1)), Is.False);
            Assert.That(simulator.CaptureStats().ReplayErrors, Is.EqualTo(1));
            simulator.Advance(0);
            Assert.That(simulator.Server.TryReceive(out _), Is.False);
        }

        [Test]
        public void DirectionsUseIndependentQueues()
        {
            var config = NetworkSimulationPresets.Create(NetworkSimulationPreset.Immediate);
            config.LatencyMilliseconds = 10;
            using var simulator = new NetworkSimulator(new ConnectionId(9), in config);
            simulator.Client.TrySend(Packet(1));
            simulator.Server.TrySend(Packet(2));
            var stats = simulator.CaptureStats();
            Assert.That(stats.ClientToServer.QueuedPackets, Is.EqualTo(1));
            Assert.That(stats.ServerToClient.QueuedPackets, Is.EqualTo(1));
            simulator.Advance(10);
            Assert.That(simulator.Server.TryReceive(out var serverPacket), Is.True);
            Assert.That(simulator.Client.TryReceive(out var clientPacket), Is.True);
            Assert.That(serverPacket.Span[0], Is.EqualTo(1));
            Assert.That(clientPacket.Span[0], Is.EqualTo(2));
            serverPacket.Dispose();
            clientPacket.Dispose();
        }

        [Test]
        public void DisposedEndpointRejectsSendAndReceiveWithoutDisposingLink()
        {
            var config = NetworkSimulationPresets.Create(NetworkSimulationPreset.Immediate);
            using var simulator = new NetworkSimulator(new ConnectionId(10), in config);
            simulator.Client.Dispose();

            Assert.That(simulator.Client.TrySend(Packet(1)), Is.False);
            Assert.That(simulator.Client.TryReceive(out _), Is.False);
            Assert.That(simulator.Server.TrySend(Packet(2)), Is.True);
            simulator.Advance(0);
            Assert.That(simulator.CaptureStats().ServerToClient.DeliveredPackets, Is.EqualTo(1));
        }

        private static NetworkBufferLease Packet(int value)
        {
            return Buffers.Copy(new[] { (byte)value });
        }

        private static NetworkBufferLease Bytes(int length) => Buffers.Rent(length);

        private static List<byte> ReadAll(INetworkTransport transport)
        {
            var result = new List<byte>();
            while (transport.TryReceive(out var packet))
            {
                result.Add(packet.Span[0]);
                packet.Dispose();
            }
            return result;
        }
    }
}
