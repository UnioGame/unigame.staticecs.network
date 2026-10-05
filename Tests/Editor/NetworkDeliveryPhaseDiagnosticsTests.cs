using System.Diagnostics;
using NUnit.Framework;

namespace UniGame.StaticEcs.Network.Tests
{
    public sealed class NetworkDeliveryPhaseDiagnosticsTests
    {
        [Test]
        public void RecorderPairsSequenceStagesAndApplicationAckByTick()
        {
            var recorder = new NetworkDeliveryPhaseRecorder();
            var peer = recorder.RegisterPeer(41, 7);
            var baseline = recorder.Capture();
            var start = Stopwatch.GetTimestamp();

            Assert.That(recorder.BeginSnapshotChunk(peer, 100, 3, true, start),
                Is.True);
            recorder.RecordManagedFifoEnqueue(peer, 100, 3,
                start + Stopwatch.Frequency / 1000);
            recorder.RecordNativeTicketSubmission(peer, 100, 3,
                start + Stopwatch.Frequency * 2 / 1000);
            Assert.That(recorder.Capture().GetPeer(peer).OutstandingNativeTickets,
                Is.EqualTo(1));
            recorder.RecordDeliveryCallback(peer, 100, 3,
                start + Stopwatch.Frequency * 5 / 1000);
            Assert.That(recorder.Capture().GetPeer(peer).OutstandingNativeTickets,
                Is.Zero);
            recorder.RecordSnapshotBatchAccepted(peer, 101, 100,
                start + Stopwatch.Frequency * 7 / 1000);
            recorder.RecordApplicationAck(peer, 100,
                start + Stopwatch.Frequency * 10 / 1000);

            var delta = recorder.Capture().DeltaFrom(baseline);
            Assert.That(delta.Available, Is.True);
            Assert.That(delta.CapDrops, Is.Zero);
            Assert.That(delta.Truncated, Is.False);
            Assert.That(delta.PeerCount, Is.EqualTo(1));
            var value = delta.GetPeer(0);
            Assert.That(value.PeerId, Is.EqualTo(41));
            Assert.That(value.SessionEpoch, Is.EqualTo(7));
            Assert.That(value.NativeTicketSubmissions, Is.EqualTo(1));
            Assert.That(value.DeliveryCallbacks, Is.EqualTo(1));
            Assert.That(value.SnapshotBatchesAccepted, Is.EqualTo(1));
            Assert.That(value.ApplicationAcks, Is.EqualTo(1));
            Assert.That(value.ManagedFifoResidence.Samples, Is.EqualTo(1));
            Assert.That(value.NativeTicketLifetime.Samples, Is.EqualTo(1));
            Assert.That(value.CallbackToNextAccept.Samples, Is.EqualTo(1));
            Assert.That(value.ApplicationAckWait.Samples, Is.EqualTo(1));
            Assert.That(value.OutstandingSnapshotIdentities, Is.Zero);
            Assert.That(value.OutstandingNativeTickets, Is.Zero);
        }

        [Test]
        public void RecorderRejectsStaleAndDuplicateNativeDeliveryEvents()
        {
            var recorder = new NetworkDeliveryPhaseRecorder();
            var peer = recorder.RegisterPeer(41, 7);
            var baseline = recorder.Capture();
            var start = Stopwatch.GetTimestamp();
            var submission = start + Stopwatch.Frequency / 1000;

            Assert.That(recorder.BeginSnapshotChunk(peer, 100, 3, true, start),
                Is.True);
            recorder.RecordNativeTicketSubmission(peer, 100, 3, submission);
            recorder.RecordNativeDeliveryEvent(peer, 100, 3, submission - 1);
            recorder.RecordNativeDeliveryEvent(peer, 100, 3,
                submission + Stopwatch.Frequency / 1000);
            recorder.RecordNativeDeliveryEvent(peer, 100, 3,
                submission + Stopwatch.Frequency * 2 / 1000);
            recorder.RecordDeliveryCallback(peer, 100, 3,
                submission + Stopwatch.Frequency * 3 / 1000);

            var delta = recorder.Capture().DeltaFrom(baseline);
            var value = delta.GetPeer(0);
            Assert.That(delta.CapDrops, Is.EqualTo(2));
            Assert.That(delta.Truncated, Is.True);
            Assert.That(value.NativeDeliveryEvents, Is.EqualTo(3));
            Assert.That(value.SubmissionToNativeDeliveryEvent.Samples,
                Is.EqualTo(1));
            Assert.That(value.NativeDeliveryEventToOwnerDispatch.Samples,
                Is.EqualTo(1));
            Assert.That(value.OutstandingSnapshotIdentities, Is.Zero);
            Assert.That(value.OutstandingNativeTickets, Is.Zero);
        }

        [Test]
        public void RecorderFailsClosedForDuplicateNativeSubmissionAndOwnerCompletion()
        {
            var recorder = new NetworkDeliveryPhaseRecorder();
            var peer = recorder.RegisterPeer(41, 7);
            var baseline = recorder.Capture();
            var start = Stopwatch.GetTimestamp();
            var submission = start + Stopwatch.Frequency / 1000;

            Assert.That(recorder.BeginSnapshotChunk(peer, 100, 3, true, start),
                Is.True);
            recorder.RecordNativeTicketSubmission(peer, 100, 3, submission);
            recorder.RecordNativeTicketSubmission(peer, 100, 3, submission);
            recorder.RecordNativeDeliveryEvent(peer, 100, 3,
                submission + Stopwatch.Frequency / 1000);
            recorder.RecordDeliveryCallback(peer, 100, 3,
                submission + Stopwatch.Frequency * 2 / 1000);
            recorder.RecordDeliveryCallback(peer, 100, 3,
                submission + Stopwatch.Frequency * 3 / 1000);

            var delta = recorder.Capture().DeltaFrom(baseline);
            var value = delta.GetPeer(0);
            Assert.That(delta.CapDrops, Is.EqualTo(2));
            Assert.That(delta.Truncated, Is.True);
            Assert.That(value.SubmissionToNativeDeliveryEvent.Samples,
                Is.EqualTo(1));
            Assert.That(value.NativeDeliveryEventToOwnerDispatch.Samples,
                Is.EqualTo(1));
            Assert.That(value.NativeTicketLifetime.Samples, Is.EqualTo(1));
            Assert.That(value.OutstandingNativeTickets, Is.Zero);
        }

        [Test]
        public void RecorderInvalidatesAtOutstandingIdentityCap()
        {
            var recorder = new NetworkDeliveryPhaseRecorder();
            var peer = recorder.RegisterPeer(9, 2);
            for (uint sequence = 1;
                 sequence <= NetworkDeliveryPhaseDiagnostics
                     .MaximumOutstandingSnapshotIdentitiesPerPeer;
                 sequence++)
                Assert.That(recorder.BeginSnapshotChunk(peer, 10, sequence,
                    sequence == 1, Stopwatch.GetTimestamp()), Is.True);

            Assert.That(recorder.BeginSnapshotChunk(peer, 11, 33, true,
                Stopwatch.GetTimestamp()), Is.False);
            var value = recorder.Capture();
            Assert.That(value.CapDrops, Is.EqualTo(1));
            Assert.That(value.Truncated, Is.True);
            Assert.That(value.GetPeer(0).OutstandingSnapshotIdentities,
                Is.EqualTo(NetworkDeliveryPhaseDiagnostics
                .MaximumOutstandingSnapshotIdentitiesPerPeer));
        }

        [Test]
        public void RecorderInvalidatesWhenPeerCapIsExceeded()
        {
            var recorder = new NetworkDeliveryPhaseRecorder();
            for (uint peerId = 1;
                 peerId <= NetworkDeliveryPhaseDiagnostics.MaximumPeers;
                 peerId++)
                Assert.That(recorder.RegisterPeer(peerId, 1), Is.GreaterThanOrEqualTo(0));

            Assert.That(recorder.RegisterPeer(
                NetworkDeliveryPhaseDiagnostics.MaximumPeers + 1, 1), Is.EqualTo(-1));
            Assert.That(recorder.CapDrops, Is.EqualTo(1));
            Assert.That(recorder.Truncated, Is.True);
        }

        [Test]
        public void RecorderReusesSlotsAfterPreSampleDisconnectChurn()
        {
            var recorder = new NetworkDeliveryPhaseRecorder();
            for (uint epoch = 1;
                 epoch <= NetworkDeliveryPhaseDiagnostics.MaximumPeers + 32;
                 epoch++)
            {
                var slot = recorder.RegisterPeer(1, epoch);
                Assert.That(slot, Is.GreaterThanOrEqualTo(0));
                Assert.That(recorder.ReleasePeer(slot, 1, epoch), Is.True);
            }

            for (uint peerId = 1; peerId <= 1200; peerId++)
                Assert.That(recorder.RegisterPeer(peerId, 1),
                    Is.GreaterThanOrEqualTo(0));

            Assert.That(recorder.PeerCount, Is.EqualTo(1200));
            Assert.That(recorder.CapDrops, Is.Zero);
            Assert.That(recorder.Truncated, Is.False);
            Assert.That(recorder.Capture().PeerCount, Is.EqualTo(1200));
        }

        [Test]
        public void RecorderRetainsDisconnectedSampleIdentityForDeltaJoin()
        {
            var recorder = new NetworkDeliveryPhaseRecorder();
            var oldSlot = recorder.RegisterPeer(41, 7);
            recorder.RecordSnapshotBatchOfferAttempt(oldSlot);
            var baseline = recorder.Capture();

            Assert.That(recorder.ReleasePeer(oldSlot, 41, 7), Is.True);
            Assert.That(recorder.PeerCount, Is.Zero);
            var newSlot = recorder.RegisterPeer(41, 8);
            Assert.That(newSlot, Is.GreaterThanOrEqualTo(0));
            recorder.RecordSnapshotBatchOfferAttempt(newSlot);

            var end = recorder.Capture();
            Assert.That(end.PeerCount, Is.EqualTo(2));
            var delta = end.DeltaFrom(baseline);
            Assert.That(delta.Truncated, Is.False);
            Assert.That(delta.PeerCount, Is.EqualTo(2));
            Assert.That(delta.GetPeer(0).SnapshotBatchOfferAttempts,
                Is.Zero);
            Assert.That(delta.GetPeer(1).SnapshotBatchOfferAttempts,
                Is.EqualTo(1));
        }

        [Test]
        public void RecorderInvalidatesWhenSampleSessionExportCapIsExceeded()
        {
            var recorder = new NetworkDeliveryPhaseRecorder();
            var initial = recorder.RegisterPeer(1, 1);
            recorder.Capture();
            Assert.That(recorder.ReleasePeer(initial, 1, 1), Is.True);
            for (uint peerId = 2;
                 peerId <= NetworkDeliveryPhaseDiagnostics.MaximumPeers;
                 peerId++)
            {
                var slot = recorder.RegisterPeer(peerId, 1);
                Assert.That(slot, Is.GreaterThanOrEqualTo(0));
                Assert.That(recorder.ReleasePeer(slot, peerId, 1), Is.True);
            }

            Assert.That(recorder.RegisterPeer(
                NetworkDeliveryPhaseDiagnostics.MaximumPeers + 1, 1), Is.EqualTo(-1));
            var summary = recorder.Capture();
            Assert.That(recorder.PeerCount, Is.Zero);
            Assert.That(summary.PeerCount,
                Is.EqualTo(NetworkDeliveryPhaseDiagnostics.MaximumPeers));
            Assert.That(summary.CapDrops, Is.EqualTo(1));
            Assert.That(summary.Truncated, Is.True);
        }
    }
}
