using System;
using System.Diagnostics;
using FFS.Libraries.StaticEcs;

namespace UniGame.StaticEcs.Network
{
    public sealed partial class Session<TWorld> where TWorld : struct, IWorldType
    {
        private readonly ISessionObserver _observer;
        private ulong _eventId;
        private ulong _statsSteps, _statsReceivedPackets, _statsSentPackets;
        private ulong _statsReceivedBytes, _statsSentBytes, _statsDecodedBytes;
        private ulong _statsCommandsQueued, _statsCommandsAccepted, _statsCommandsRejected;
        private ulong _statsSnapshotsCaptured, _statsSnapshotsApplied, _statsResyncs;
        private ulong _statsSendRetries, _statsFaults, _statsObserverErrors;
        private long _decodeStarted;
        private Channel _decodeChannel;
        private bool _decodeOpen;
        private uint _controlSendAttempts, _reliableSendAttempts, _snapshotSendAttempts;

        /// <summary>Gets cumulative session diagnostics without allocating.</summary>
        public SessionStats Stats => new(_statsSteps, _statsReceivedPackets, _statsSentPackets,
            _statsReceivedBytes, _statsSentBytes, _statsDecodedBytes, _statsCommandsQueued,
            _statsCommandsAccepted, _statsCommandsRejected, _statsSnapshotsCaptured,
            _statsSnapshotsApplied, _statsResyncs, _statsSendRetries, _statsFaults, _statsObserverErrors);

        /// <summary>Looks up the canonical snapshot fingerprint retained for one tick.</summary>
        public HistoryLookup TryGetFingerprint(uint tick, out TickFingerprint fingerprint)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(Session<TWorld>));
            fingerprint = default;
            if (_history == null) return HistoryLookup.Missing;
            var lookup = _history.TryGet(tick, out var record);
            if (lookup != HistoryLookup.Found) return lookup;
            var lease = _config.Role == SessionRole.Server ? record.Generated : record.Received;
            var hash = _config.Role == SessionRole.Server ? record.GeneratedHash : record.ReceivedHash;
            fingerprint = new TickFingerprint(tick, hash, lease.IsValid ? lease.Length : 0);
            return HistoryLookup.Found;
        }

        private long ObserveBegin(SessionEventKind kind, uint tick = PacketHeader.NoneTick,
            PacketKind packet = (PacketKind)0, Channel channel = default, uint sequence = 0,
            ulong eventStep = ulong.MaxValue)
        {
            if (_observer == null) return 0;
            var timestamp = Stopwatch.GetTimestamp();
            Deliver(timestamp, 0, tick, sequence, 0, 0, 0, 0, 0, 0, kind,
                SessionEventPhase.Begin, packet, channel, false, false, eventStep);
            return timestamp;
        }

        private void ObserveEnd(SessionEventKind kind, long started, bool success,
            uint tick = PacketHeader.NoneTick, PacketKind packet = (PacketKind)0, Channel channel = default,
            uint sequence = 0, int wireBytes = 0, int decodedBytes = 0, int count = 0,
            ushort code = 0, ushort reason = 0, ulong hash = 0, bool retry = false,
            ulong eventStep = ulong.MaxValue, long ended = 0)
        {
            if (_observer == null) return;
            var timestamp = ended == 0 ? Stopwatch.GetTimestamp() : ended;
            Deliver(timestamp, started == 0 ? 0 : timestamp - started, tick, sequence, wireBytes,
                decodedBytes, count, code, reason, hash, kind, SessionEventPhase.End, packet, channel,
                success, retry, eventStep);
        }

        private void ObservePoint(SessionEventKind kind, bool success = true, uint tick = PacketHeader.NoneTick,
            PacketKind packet = (PacketKind)0, ushort code = 0, ushort reason = 0, ulong hash = 0)
        {
            if (_observer == null) return;
            Deliver(Stopwatch.GetTimestamp(), 0, tick, 0, 0, 0, 0, code, reason, hash,
                kind, SessionEventPhase.Point, packet, default, success, false, ulong.MaxValue);
        }

        private void Deliver(long timestamp, long elapsed, uint tick, uint sequence, int wireBytes,
            int decodedBytes, int count, ushort code, ushort reason, ulong hash, SessionEventKind kind,
            SessionEventPhase phase, PacketKind packet, Channel channel, bool success, bool retry,
            ulong eventStep)
        {
            var id = ++_eventId;
            var step = eventStep == ulong.MaxValue ? (_hasStep ? _lastStep : ulong.MaxValue) : eventStep;
            var value = new SessionEvent(id, step, timestamp, elapsed, tick,
                sequence, wireBytes, decodedBytes, count, code, reason, hash, _config.Role, kind,
                phase, _state, _error, packet, channel, success, retry);
            try { _observer.Observe(in value); }
            catch { _statsObserverErrors++; }
        }

        private void RecordDecoded(in PacketHeader header)
        {
            _statsDecodedBytes += header.DecodedPayloadLength;
            ObserveEnd(SessionEventKind.Decode, _decodeStarted, true, tick: header.ServerTick,
                packet: header.Kind, channel: _decodeChannel, sequence: header.PacketSequence,
                decodedBytes: checked((int)header.DecodedPayloadLength), hash: header.PayloadHash);
            _decodeOpen = false;
        }

        private void BeginDecodeObservation(Channel channel)
        {
            _decodeChannel = channel;
            _decodeOpen = true;
            _decodeStarted = ObserveBegin(SessionEventKind.Decode, channel: channel);
        }

        private void EndDecodeFailureIfOpen()
        {
            if (!_decodeOpen) return;
            ObserveEnd(SessionEventKind.Decode, _decodeStarted, false, channel: _decodeChannel);
            _decodeOpen = false;
        }

        private long ObservationTimestamp() => _observer == null ? 0 : Stopwatch.GetTimestamp();

        private bool ObservedSend(Channel channel, ref PacketLease packet, PacketKind kind,
            uint sequence, bool retry)
        {
            var wireBytes = packet.Length;
            var started = ObserveBegin(SessionEventKind.Send, packet: kind, channel: channel, sequence: sequence);
            var success = false;
            try
            {
                if (retry) { _statsSendRetries++; }
                success = _transport.TrySend(channel, ref packet);
                if (success)
                {
                    _statsSentPackets++;
                    _statsSentBytes += (ulong)wireBytes;
                }
                return success;
            }
            finally
            {
                ObserveEnd(SessionEventKind.Send, started, success, packet: kind, channel: channel,
                    sequence: sequence, wireBytes: wireBytes, code: success ? (ushort)1 : (ushort)0,
                    retry: retry);
            }
        }
    }
}
