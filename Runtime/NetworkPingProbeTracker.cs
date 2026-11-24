using System;
using System.Diagnostics;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Summarizes answered and timed-out clock probes in the last minute.</summary>
    public readonly struct NetworkPingDiagnostics
    {
        public NetworkPingDiagnostics(int answered, int timedOut, double jitterSeconds)
        {
            Answered = answered;
            TimedOut = timedOut;
            JitterSeconds = jitterSeconds;
        }

        /// <summary>Gets probes answered within three seconds and still in the 60-second window.</summary>
        public int Answered { get; }
        /// <summary>Gets probes that were unanswered after three seconds and are still in the window.</summary>
        public int TimedOut { get; }
        /// <summary>Gets mean absolute difference between consecutive answered RTT samples, in seconds.</summary>
        public double JitterSeconds { get; }
        public bool HasJitter => Answered > 1;
        public bool HasLossSample => Answered + TimedOut > 0;
        public double UnansweredPercent => HasLossSample
            ? 100d * TimedOut / (Answered + TimedOut)
            : 0d;
    }

    /// <summary>Tracks the existing reliable ping/pong probes without changing the wire format.</summary>
    public sealed class NetworkPingProbeTracker
    {
        private const int Capacity = 64;
        private readonly Probe[] _probes = new Probe[Capacity];
        private int _next;

        public void RecordSent(long sentTimestamp)
        {
            Expire(sentTimestamp);
            _probes[_next] = new Probe { Sent = sentTimestamp, Status = 1 };
            _next = (_next + 1) % Capacity;
        }

        public bool TryRecordPong(long sentTimestamp, long receivedTimestamp, out double seconds)
        {
            Expire(receivedTimestamp);
            seconds = 0d;
            for (var i = 0; i < Capacity; i++)
            {
                ref var probe = ref _probes[i];
                if (probe.Status != 1 || probe.Sent != sentTimestamp)
                    continue;
                seconds = (receivedTimestamp - sentTimestamp) / (double)Stopwatch.Frequency;
                if (seconds < 0d) return false;
                probe.Seconds = seconds;
                probe.Status = 2;
                return true;
            }
            return false;
        }

        public NetworkPingDiagnostics Capture(long nowTimestamp)
        {
            Expire(nowTimestamp);
            var answered = 0;
            var timedOut = 0;
            var jitterTotal = 0d;
            var previous = 0d;
            var hasPrevious = false;
            for (var offset = 0; offset < Capacity; offset++)
            {
                var probe = _probes[(_next + offset) % Capacity];
                if (probe.Status == 0 ||
                    (nowTimestamp - probe.Sent) / (double)Stopwatch.Frequency > 60d)
                    continue;
                if (probe.Status == 3)
                {
                    timedOut++;
                    continue;
                }
                if (probe.Status != 2) continue;
                answered++;
                if (hasPrevious) jitterTotal += Math.Abs(probe.Seconds - previous);
                previous = probe.Seconds;
                hasPrevious = true;
            }
            return new NetworkPingDiagnostics(answered, timedOut,
                answered > 1 ? jitterTotal / (answered - 1) : 0d);
        }

        public void Reset()
        {
            Array.Clear(_probes, 0, _probes.Length);
            _next = 0;
        }

        private void Expire(long nowTimestamp)
        {
            for (var i = 0; i < Capacity; i++)
                if (_probes[i].Status == 1 &&
                    (nowTimestamp - _probes[i].Sent) / (double)Stopwatch.Frequency >= 3d)
                    _probes[i].Status = 3;
        }

        private struct Probe
        {
            public long Sent;
            public double Seconds;
            public byte Status;
        }
    }
}
