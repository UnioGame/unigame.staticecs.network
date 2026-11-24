using System;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using NUnit.Framework;

namespace UniGame.StaticEcs.Network.Tests
{
    /// <summary>Real loopback datagrams through the relay, and the decision it takes for each of them.</summary>
    public sealed class NetworkLinkRelayTests
    {
        private const int ReceiveTimeoutMilliseconds = 5000;
        private const int SilenceMilliseconds = 250;

        private Socket _server;
        private Socket _client;
        private NetworkLinkRelay _relay;

        [SetUp]
        public void SetUp()
        {
            _server = CreateLoopbackSocket();
            _client = CreateLoopbackSocket();
        }

        [TearDown]
        public void TearDown()
        {
            _relay?.Dispose();
            _client.Dispose();
            _server.Dispose();
        }

        [Test]
        public void CleanLinkForwardsBothDirectionsUnchanged()
        {
            _relay = new NetworkLinkRelay();
            var link = OpenLink(default);

            SendFromClient(link, 1, 2, 3, 4);
            var reply = ReceiveOnServer(out var received);
            Assert.That(received, Is.EqualTo(new byte[] { 1, 2, 3, 4 }));

            _server.SendTo(new byte[] { 9, 8, 7 }, reply);
            Assert.That(ReceiveOnClient(), Is.EqualTo(new byte[] { 9, 8, 7 }));

            var diagnostics = _relay.CaptureDiagnostics();
            Assert.That(diagnostics.Links, Is.EqualTo(1));
            Assert.That(diagnostics.ClientToServer.ForwardedPackets, Is.EqualTo(1));
            Assert.That(diagnostics.ClientToServer.ForwardedBytes, Is.EqualTo(4));
            Assert.That(diagnostics.ServerToClient.ForwardedPackets, Is.EqualTo(1));
            Assert.That(diagnostics.ServerToClient.ForwardedBytes, Is.EqualTo(3));
            Assert.That(diagnostics.ClientToServer.DelayedPackets, Is.Zero);
            Assert.That(_relay.LastFailure, Is.Null);
        }

        [Test]
        public void LatencyHoldsEachDirectionForItsOwnDelay()
        {
            _relay = new NetworkLinkRelay();
            var link = OpenLink(new NetworkLinkImpairment { LatencyMs = 120f });

            var started = Stopwatch.GetTimestamp();
            SendFromClient(link, 5);
            var reply = ReceiveOnServer(out _);
            Assert.That(ElapsedMilliseconds(started), Is.GreaterThanOrEqualTo(110d));

            started = Stopwatch.GetTimestamp();
            _server.SendTo(new byte[] { 6 }, reply);
            ReceiveOnClient();
            Assert.That(ElapsedMilliseconds(started), Is.GreaterThanOrEqualTo(110d));

            var diagnostics = _relay.CaptureDiagnostics();
            Assert.That(diagnostics.ClientToServer.DelayedPackets, Is.EqualTo(1));
            Assert.That(diagnostics.ServerToClient.DelayedPackets, Is.EqualTo(1));
            Assert.That(diagnostics.ClientToServer.DelayMicrosecondsMaximum,
                Is.GreaterThanOrEqualTo(110_000));
            Assert.That(diagnostics.QueuedDatagramsHighWater, Is.EqualTo(1));
            Assert.That(diagnostics.QueuedDatagrams, Is.Zero);
        }

        [Test]
        public void FullLossDropsEveryDatagramAndCountsIt()
        {
            _relay = new NetworkLinkRelay();
            var link = OpenLink(new NetworkLinkImpairment { LossPercent = 100f });

            SendFromClient(link, 1);
            SendFromClient(link, 2);

            AssertServerHearsNothing();
            Assert.That(_relay.CaptureDiagnostics().ClientToServer.LossDrops, Is.EqualTo(2));
        }

        [Test]
        public void ImpairmentChangesApplyToAnOpenLink()
        {
            _relay = new NetworkLinkRelay();
            var link = OpenLink(new NetworkLinkImpairment { LossPercent = 100f });
            SendFromClient(link, 1);
            AssertServerHearsNothing();

            link.Impairment = default;
            SendFromClient(link, 2);

            ReceiveOnServer(out var received);
            Assert.That(received, Is.EqualTo(new byte[] { 2 }));
            Assert.That(link.Impairment.IsClean, Is.True);
        }

        [Test]
        public void BlackoutDropsBothFreshAndDelayedDatagramsUntilItEnds()
        {
            _relay = new NetworkLinkRelay();
            var link = OpenLink(new NetworkLinkImpairment { LatencyMs = 100f });
            SendFromClient(link, 1);

            // The first datagram is still waiting, so the blackout has to catch it when it comes due.
            link.Blackout(60_000f);
            SendFromClient(link, 2);
            AssertServerHearsNothing();
            Assert.That(_relay.CaptureDiagnostics().ClientToServer.BlackoutDrops, Is.EqualTo(2));

            link.Blackout(0f);
            SendFromClient(link, 3);
            ReceiveOnServer(out var received);
            Assert.That(received, Is.EqualTo(new byte[] { 3 }));
        }

        [Test]
        public void FullDelayQueueDropsTheNewestDatagram()
        {
            _relay = new NetworkLinkRelay(new NetworkLinkRelaySettings { MaximumQueuedDatagrams = 2 });
            var link = OpenLink(new NetworkLinkImpairment { LatencyMs = 1000f });

            for (byte index = 0; index < 5; index++)
                SendFromClient(link, index);

            ReceiveOnServer(out var first);
            ReceiveOnServer(out var second);
            Assert.That(first, Is.EqualTo(new byte[] { 0 }));
            Assert.That(second, Is.EqualTo(new byte[] { 1 }));
            AssertServerHearsNothing();
            var diagnostics = _relay.CaptureDiagnostics();
            Assert.That(diagnostics.ClientToServer.QueueLimitDrops, Is.EqualTo(3));
            Assert.That(diagnostics.QueuedDatagramsHighWater, Is.EqualTo(2));
        }

        [Test]
        public void DatagramAboveTheLimitIsDroppedAndTheLimitItselfPasses()
        {
            _relay = new NetworkLinkRelay(new NetworkLinkRelaySettings { MaximumDatagramBytes = 32 });
            var link = OpenLink(default);

            _client.SendTo(new byte[33], LinkEndPoint(link));
            _client.SendTo(new byte[32], LinkEndPoint(link));

            ReceiveOnServer(out var received);
            Assert.That(received.Length, Is.EqualTo(32));
            Assert.That(_relay.CaptureDiagnostics().ClientToServer.OversizedDrops, Is.EqualTo(1));
        }

        [Test]
        public void LinksKeepTheirClientsApart()
        {
            _relay = new NetworkLinkRelay();
            var firstLink = OpenLink(default);
            var secondLink = OpenLink(default);
            using var secondClient = CreateLoopbackSocket();

            SendFromClient(firstLink, 1);
            var firstReply = ReceiveOnServer(out _);
            secondClient.SendTo(new byte[] { 2 }, LinkEndPoint(secondLink));
            var secondReply = ReceiveOnServer(out _);
            Assert.That(secondReply, Is.Not.EqualTo(firstReply));

            _server.SendTo(new byte[] { 20 }, secondReply);
            _server.SendTo(new byte[] { 10 }, firstReply);

            Assert.That(ReceiveOnClient(), Is.EqualTo(new byte[] { 10 }));
            Assert.That(Receive(secondClient, out _), Is.EqualTo(new byte[] { 20 }));
        }

        [Test]
        public void LinkServesOnlyItsFirstSender()
        {
            _relay = new NetworkLinkRelay();
            var link = OpenLink(default);
            using var stranger = CreateLoopbackSocket();

            SendFromClient(link, 1);
            ReceiveOnServer(out _);
            stranger.SendTo(new byte[] { 2 }, LinkEndPoint(link));

            AssertServerHearsNothing();
        }

        [Test]
        public void ClosedLinkStopsForwardingAndTheRelayKeepsWorking()
        {
            _relay = new NetworkLinkRelay();
            var closed = OpenLink(default);
            SendFromClient(closed, 1);
            ReceiveOnServer(out _);

            _relay.Close(closed);
            Assert.That(closed.Closed, Is.True);
            Assert.That(_relay.CaptureDiagnostics().Links, Is.Zero);

            using var nextClient = CreateLoopbackSocket();
            var next = OpenLink(default);
            nextClient.SendTo(new byte[] { 7 }, LinkEndPoint(next));
            ReceiveOnServer(out var received);
            Assert.That(received, Is.EqualTo(new byte[] { 7 }));
        }

        [Test]
        public void DisposedRelayRefusesNewLinksAndClosesOpenOnes()
        {
            _relay = new NetworkLinkRelay();
            var link = OpenLink(new NetworkLinkImpairment { LatencyMs = 2000f });
            SendFromClient(link, 1);

            _relay.Dispose();
            _relay.Dispose();

            Assert.That(link.Closed, Is.True);
            Assert.Throws<ObjectDisposedException>(() => OpenLink(default));
            AssertServerHearsNothing();
        }

        [Test]
        public void OpenRejectsValuesOutsideTheirRange()
        {
            _relay = new NetworkLinkRelay();
            Assert.Throws<ArgumentOutOfRangeException>(() =>
                OpenLink(new NetworkLinkImpairment { LossPercent = 100.5f }));
            Assert.Throws<ArgumentOutOfRangeException>(() =>
                OpenLink(new NetworkLinkImpairment { LatencyMs = float.NaN }));
            Assert.Throws<ArgumentOutOfRangeException>(() =>
                OpenLink(new NetworkLinkImpairment { JitterMs = -1f }));
            Assert.Throws<ArgumentOutOfRangeException>(() =>
                _relay.Open(NetworkLinkRelay.LoopbackAddress, 0, default));

            var link = OpenLink(default);
            Assert.Throws<ArgumentOutOfRangeException>(() =>
                link.Impairment = new NetworkLinkImpairment
                {
                    LatencyMs = NetworkLinkImpairment.MaximumLatencyMs + 1f,
                });
            Assert.Throws<ArgumentOutOfRangeException>(() => link.Blackout(float.NaN));
            Assert.Throws<ArgumentOutOfRangeException>(() =>
                link.Blackout(NetworkLinkRelayLink.MaximumBlackoutMs + 1f));
            // Converted to a timestamp, this length overflows and would end in the past.
            Assert.Throws<ArgumentOutOfRangeException>(() => link.Blackout(1e15f));
            Assert.DoesNotThrow(() => link.Blackout(NetworkLinkRelayLink.MaximumBlackoutMs));
            Assert.That(link.BlackoutUntilTimestamp, Is.GreaterThan(System.Diagnostics.Stopwatch.GetTimestamp()));
        }

        [Test]
        public void ScheduleForwardsACleanDatagramAtOnce()
        {
            var forwarded = NetworkLinkRelay.TrySchedule(default, new System.Random(1), 1000, out var due);

            Assert.That(forwarded, Is.True);
            Assert.That(due, Is.EqualTo(1000));
        }

        [Test]
        public void ScheduleKeepsJitterInsideItsSpreadAndNeverBeforeNow()
        {
            var random = new System.Random(3);
            var narrow = new NetworkLinkImpairment { LatencyMs = 100f, JitterMs = 20f };
            var wide = new NetworkLinkImpairment { LatencyMs = 10f, JitterMs = 50f };
            var minimum = long.MaxValue;
            var maximum = long.MinValue;
            for (var index = 0; index < 2000; index++)
            {
                Assert.That(NetworkLinkRelay.TrySchedule(in narrow, random, 0, out var due), Is.True);
                minimum = Math.Min(minimum, due);
                maximum = Math.Max(maximum, due);
                Assert.That(NetworkLinkRelay.TrySchedule(in wide, random, 0, out var early), Is.True);
                Assert.That(early, Is.GreaterThanOrEqualTo(0));
                Assert.That(early, Is.LessThanOrEqualTo(Stopwatch.Frequency * 60 / 1000));
            }

            Assert.That(minimum, Is.GreaterThanOrEqualTo(Stopwatch.Frequency * 80 / 1000));
            Assert.That(maximum, Is.LessThanOrEqualTo(Stopwatch.Frequency * 120 / 1000));
            // The spread is actually used, not collapsed onto the latency.
            Assert.That(maximum - minimum, Is.GreaterThan(Stopwatch.Frequency * 30 / 1000));
        }

        [Test]
        public void ScheduleDropsAboutTheConfiguredShare()
        {
            var random = new System.Random(11);
            var impairment = new NetworkLinkImpairment { LossPercent = 10f };
            var dropped = 0;
            for (var index = 0; index < 20000; index++)
                if (!NetworkLinkRelay.TrySchedule(in impairment, random, 0, out _))
                    dropped++;

            Assert.That(dropped, Is.InRange(1800, 2200));
        }

        private NetworkLinkRelayLink OpenLink(NetworkLinkImpairment impairment) =>
            _relay.Open(NetworkLinkRelay.LoopbackAddress,
                ((IPEndPoint)_server.LocalEndPoint).Port, in impairment);

        private static IPEndPoint LinkEndPoint(NetworkLinkRelayLink link) =>
            new IPEndPoint(IPAddress.Loopback, link.LocalPort);

        private void SendFromClient(NetworkLinkRelayLink link, params byte[] payload) =>
            _client.SendTo(payload, LinkEndPoint(link));

        private EndPoint ReceiveOnServer(out byte[] payload)
        {
            payload = Receive(_server, out var sender);
            return sender;
        }

        private byte[] ReceiveOnClient() => Receive(_client, out _);

        private void AssertServerHearsNothing()
        {
            Assert.That(_server.Poll(SilenceMilliseconds * 1000, SelectMode.SelectRead), Is.False,
                "The server received a datagram the relay had to drop.");
        }

        private static byte[] Receive(Socket socket, out EndPoint sender)
        {
            sender = new IPEndPoint(IPAddress.Loopback, 0);
            Assert.That(socket.Poll(ReceiveTimeoutMilliseconds * 1000, SelectMode.SelectRead),
                Is.True, "No datagram arrived in time.");
            var buffer = new byte[4096];
            var length = socket.ReceiveFrom(buffer, ref sender);
            var payload = new byte[length];
            Buffer.BlockCopy(buffer, 0, payload, 0, length);
            return payload;
        }

        private static Socket CreateLoopbackSocket()
        {
            var socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
            socket.Bind(new IPEndPoint(IPAddress.Loopback, 0));
            return socket;
        }

        private static double ElapsedMilliseconds(long started) =>
            (Stopwatch.GetTimestamp() - started) * 1000d / Stopwatch.Frequency;
    }
}
