using System.Diagnostics;
using NUnit.Framework;

namespace UniGame.StaticEcs.Network.Tests
{
    public sealed class NetworkPingProbeTrackerTests
    {
        private static readonly long Second = Stopwatch.Frequency;

        [Test]
        public void CountsAnsweredTimedOutAndJitterInLastMinute()
        {
            var tracker = new NetworkPingProbeTracker();
            tracker.RecordSent(Second);
            Assert.That(tracker.TryRecordPong(Second, Second + Second / 10,
                out _), Is.True);
            tracker.RecordSent(2 * Second);
            Assert.That(tracker.TryRecordPong(2 * Second,
                2 * Second + 3 * Second / 10, out _), Is.True);
            tracker.RecordSent(3 * Second);

            var values = tracker.Capture(6 * Second);
            Assert.That(values.Answered, Is.EqualTo(2));
            Assert.That(values.TimedOut, Is.EqualTo(1));
            Assert.That(values.JitterSeconds, Is.EqualTo(0.2d).Within(0.001d));
            Assert.That(values.UnansweredPercent, Is.EqualTo(100d / 3d).Within(0.001d));
            Assert.That(tracker.Capture(64 * Second).HasLossSample, Is.False);
        }

        [Test]
        public void IgnoresLateAndDuplicatePongsAndResets()
        {
            var tracker = new NetworkPingProbeTracker();
            tracker.RecordSent(Second);
            Assert.That(tracker.TryRecordPong(Second, 4 * Second, out _), Is.False);
            Assert.That(tracker.Capture(4 * Second).TimedOut, Is.EqualTo(1));
            tracker.RecordSent(5 * Second);
            Assert.That(tracker.TryRecordPong(5 * Second, 6 * Second, out _), Is.True);
            Assert.That(tracker.TryRecordPong(5 * Second, 6 * Second, out _), Is.False);
            tracker.Reset();
            Assert.That(tracker.Capture(7 * Second).HasLossSample, Is.False);
        }
    }
}
