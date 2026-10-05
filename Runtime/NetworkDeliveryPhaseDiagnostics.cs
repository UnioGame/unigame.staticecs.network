using System;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Bounded per-session server and transport delivery-stage counters for one sample.</summary>
    public readonly struct NetworkDeliveryPhasePeerDiagnostics
    {
        internal NetworkDeliveryPhasePeerDiagnostics(uint peerId, uint sessionEpoch,
            long snapshotBatchOfferAttempts, long pendingReceiptDeferrals,
            long pendingReliableDeferrals, long snapshotChunkOffers,
            long managedFifoEnqueues, long nativeTicketSubmissions,
            long deliveryCallbacks, long nativeDeliveryEvents,
            long snapshotBatchesAccepted,
            long applicationAcks, long transportSendFailures,
            int outstandingSnapshotIdentities, int outstandingNativeTickets,
            NetworkTimingHistogram managedFifoResidence,
            NetworkTimingHistogram nativeTicketLifetime,
            NetworkTimingHistogram submissionToNativeDeliveryEvent,
            NetworkTimingHistogram nativeDeliveryEventToOwnerDispatch,
            NetworkTimingHistogram callbackToNextAccept,
            NetworkTimingHistogram applicationAckWait)
        {
            PeerId = peerId;
            SessionEpoch = sessionEpoch;
            SnapshotBatchOfferAttempts = snapshotBatchOfferAttempts;
            PendingReceiptDeferrals = pendingReceiptDeferrals;
            PendingReliableDeferrals = pendingReliableDeferrals;
            SnapshotChunkOffers = snapshotChunkOffers;
            ManagedFifoEnqueues = managedFifoEnqueues;
            NativeTicketSubmissions = nativeTicketSubmissions;
            DeliveryCallbacks = deliveryCallbacks;
            NativeDeliveryEvents = nativeDeliveryEvents;
            SnapshotBatchesAccepted = snapshotBatchesAccepted;
            ApplicationAcks = applicationAcks;
            TransportSendFailures = transportSendFailures;
            OutstandingSnapshotIdentities = outstandingSnapshotIdentities;
            OutstandingNativeTickets = outstandingNativeTickets;
            ManagedFifoResidence = managedFifoResidence;
            NativeTicketLifetime = nativeTicketLifetime;
            SubmissionToNativeDeliveryEvent = submissionToNativeDeliveryEvent;
            NativeDeliveryEventToOwnerDispatch = nativeDeliveryEventToOwnerDispatch;
            CallbackToNextAccept = callbackToNextAccept;
            ApplicationAckWait = applicationAckWait;
        }

        public uint PeerId { get; }
        public uint SessionEpoch { get; }
        public long SnapshotBatchOfferAttempts { get; }
        public long PendingReceiptDeferrals { get; }
        public long PendingReliableDeferrals { get; }
        public long SnapshotChunkOffers { get; }
        public long ManagedFifoEnqueues { get; }
        public long NativeTicketSubmissions { get; }
        public long DeliveryCallbacks { get; }
        public long NativeDeliveryEvents { get; }
        public long SnapshotBatchesAccepted { get; }
        public long ApplicationAcks { get; }
        public long TransportSendFailures { get; }
        public int OutstandingSnapshotIdentities { get; }
        public int OutstandingNativeTickets { get; }
        public NetworkTimingHistogram ManagedFifoResidence { get; }
        public NetworkTimingHistogram NativeTicketLifetime { get; }
        public NetworkTimingHistogram SubmissionToNativeDeliveryEvent { get; }
        public NetworkTimingHistogram NativeDeliveryEventToOwnerDispatch { get; }
        public NetworkTimingHistogram CallbackToNextAccept { get; }
        public NetworkTimingHistogram ApplicationAckWait { get; }
    }

    /// <summary>Immutable, bounded snapshot of per-session delivery-stage telemetry.</summary>
    public sealed class NetworkDeliveryPhaseDiagnostics
    {
        public const int MaximumPeers = 2048;
        public const int MaximumOutstandingSnapshotIdentitiesPerPeer = 32;
        public static NetworkDeliveryPhaseDiagnostics Unavailable { get; } =
            new NetworkDeliveryPhaseDiagnostics(false, 0, false, null);

        private readonly NetworkDeliveryPhasePeerDiagnostics[] _peers;

        internal NetworkDeliveryPhaseDiagnostics(bool available, long capDrops,
            bool truncated, NetworkDeliveryPhasePeerDiagnostics[] peers)
        {
            Available = available;
            CapDrops = capDrops;
            Truncated = truncated;
            _peers = peers ?? Array.Empty<NetworkDeliveryPhasePeerDiagnostics>();
        }

        public bool Available { get; }
        public long CapDrops { get; }
        public bool Truncated { get; }
        public int PeerCount => _peers.Length;

        public NetworkDeliveryPhasePeerDiagnostics GetPeer(int index) =>
            _peers[index];

        /// <summary>Returns sample deltas while retaining any cap or truncation evidence.</summary>
        public NetworkDeliveryPhaseDiagnostics DeltaFrom(
            NetworkDeliveryPhaseDiagnostics baseline)
        {
            if (!Available || baseline == null || !baseline.Available)
                return new NetworkDeliveryPhaseDiagnostics(false, 0, false, null);

            var deltas = new NetworkDeliveryPhasePeerDiagnostics[_peers.Length];
            var truncated = Truncated || baseline.Truncated;
            for (var baselineIndex = 0;
                 baselineIndex < baseline._peers.Length; baselineIndex++)
            {
                var previous = baseline._peers[baselineIndex];
                var found = false;
                for (var currentIndex = 0; currentIndex < _peers.Length;
                     currentIndex++)
                {
                    if (_peers[currentIndex].PeerId == previous.PeerId &&
                        _peers[currentIndex].SessionEpoch == previous.SessionEpoch)
                    {
                        found = true;
                        break;
                    }
                }
                if (!found)
                    truncated = true;
            }
            for (var index = 0; index < _peers.Length; index++)
            {
                var current = _peers[index];
                var previous = default(NetworkDeliveryPhasePeerDiagnostics);
                for (var baselineIndex = 0; baselineIndex < baseline._peers.Length;
                     baselineIndex++)
                {
                    var candidate = baseline._peers[baselineIndex];
                    if (candidate.PeerId == current.PeerId &&
                        candidate.SessionEpoch == current.SessionEpoch)
                    {
                        previous = candidate;
                        break;
                    }
                }

                deltas[index] = new NetworkDeliveryPhasePeerDiagnostics(
                    current.PeerId, current.SessionEpoch,
                    CounterDelta(current.SnapshotBatchOfferAttempts,
                        previous.SnapshotBatchOfferAttempts, ref truncated),
                    CounterDelta(current.PendingReceiptDeferrals,
                        previous.PendingReceiptDeferrals, ref truncated),
                    CounterDelta(current.PendingReliableDeferrals,
                        previous.PendingReliableDeferrals, ref truncated),
                    CounterDelta(current.SnapshotChunkOffers,
                        previous.SnapshotChunkOffers, ref truncated),
                    CounterDelta(current.ManagedFifoEnqueues,
                        previous.ManagedFifoEnqueues, ref truncated),
                    CounterDelta(current.NativeTicketSubmissions,
                        previous.NativeTicketSubmissions, ref truncated),
                    CounterDelta(current.DeliveryCallbacks,
                        previous.DeliveryCallbacks, ref truncated),
                    CounterDelta(current.NativeDeliveryEvents,
                        previous.NativeDeliveryEvents, ref truncated),
                    CounterDelta(current.SnapshotBatchesAccepted,
                        previous.SnapshotBatchesAccepted, ref truncated),
                    CounterDelta(current.ApplicationAcks,
                        previous.ApplicationAcks, ref truncated),
                    CounterDelta(current.TransportSendFailures,
                        previous.TransportSendFailures, ref truncated),
                    current.OutstandingSnapshotIdentities,
                    current.OutstandingNativeTickets,
                    HistogramDelta(current.ManagedFifoResidence,
                        previous.ManagedFifoResidence, ref truncated),
                    HistogramDelta(current.NativeTicketLifetime,
                        previous.NativeTicketLifetime, ref truncated),
                    HistogramDelta(current.SubmissionToNativeDeliveryEvent,
                        previous.SubmissionToNativeDeliveryEvent, ref truncated),
                    HistogramDelta(current.NativeDeliveryEventToOwnerDispatch,
                        previous.NativeDeliveryEventToOwnerDispatch, ref truncated),
                    HistogramDelta(current.CallbackToNextAccept,
                        previous.CallbackToNextAccept, ref truncated),
                    HistogramDelta(current.ApplicationAckWait,
                        previous.ApplicationAckWait, ref truncated));
            }

            return new NetworkDeliveryPhaseDiagnostics(true,
                CounterDelta(CapDrops, baseline.CapDrops, ref truncated),
                truncated, deltas);
        }

        private static long CounterDelta(long current, long previous,
            ref bool truncated)
        {
            if (current == long.MaxValue || previous == long.MaxValue)
            {
                truncated = true;
                return 0;
            }
            if (current >= previous)
                return current - previous;
            truncated = true;
            return 0;
        }

        private static NetworkTimingHistogram HistogramDelta(
            NetworkTimingHistogram current, NetworkTimingHistogram previous,
            ref bool truncated)
        {
            var samples = CounterDelta(current.Samples, previous.Samples,
                ref truncated);
            var sum = CounterDelta(current.SumMicroseconds,
                previous.SumMicroseconds, ref truncated);
            return new NetworkTimingHistogram(samples, sum,
                samples == 0 ? 0 : current.MaxMicroseconds,
                CounterDelta(current.UpTo25Milliseconds,
                    previous.UpTo25Milliseconds, ref truncated),
                CounterDelta(current.UpTo50Milliseconds,
                    previous.UpTo50Milliseconds, ref truncated),
                CounterDelta(current.UpTo100Milliseconds,
                    previous.UpTo100Milliseconds, ref truncated),
                CounterDelta(current.UpTo200Milliseconds,
                    previous.UpTo200Milliseconds, ref truncated),
                CounterDelta(current.UpTo400Milliseconds,
                    previous.UpTo400Milliseconds, ref truncated),
                CounterDelta(current.UpTo800Milliseconds,
                    previous.UpTo800Milliseconds, ref truncated),
                CounterDelta(current.Overflow, previous.Overflow,
                    ref truncated));
        }
    }

    /// <summary>Collects opt-in delivery phases in fixed per-peer and per-identity bounds.</summary>
    public sealed class NetworkDeliveryPhaseRecorder
    {
        private readonly PeerState[] _peers = CreatePeerStates();
        private readonly bool[] _retained = new bool[
            NetworkDeliveryPhaseDiagnostics.MaximumPeers];
        private readonly bool[] _live = new bool[
            NetworkDeliveryPhaseDiagnostics.MaximumPeers];
        private int _retainedPeerCount;
        private int _livePeerCount;
        private bool _sampleCaptureStarted;
        private long _capDrops;
        private bool _truncated;

        public int PeerCount => _livePeerCount;
        public long CapDrops => _capDrops;
        public bool Truncated => _truncated;

        /// <summary>Registers one live peer session, returning -1 when capacity or identity is ambiguous.</summary>
        public int RegisterPeer(uint peerId, uint sessionEpoch)
        {
            for (var index = 0; index < _peers.Length; index++)
            {
                if (!_retained[index] || _peers[index].PeerId != peerId ||
                    _peers[index].SessionEpoch != sessionEpoch)
                    continue;
                if (_live[index])
                    return index;
                Drop();
                return -1;
            }

            if (_retainedPeerCount >= _peers.Length)
            {
                Drop();
                return -1;
            }

            for (var index = 0; index < _peers.Length; index++)
            {
                if (_retained[index])
                    continue;
                _peers[index].Reset(peerId, sessionEpoch);
                _retained[index] = true;
                _live[index] = true;
                _retainedPeerCount++;
                _livePeerCount++;
                return index;
            }

            Drop();
            return -1;
        }

        /// <summary>Releases a live slot, retaining its evidence after sample capture begins.</summary>
        public bool ReleasePeer(int peerIndex, uint peerId, uint sessionEpoch)
        {
            if ((uint)peerIndex >= (uint)_peers.Length || !_live[peerIndex])
                return false;
            var peer = _peers[peerIndex];
            if (peer.PeerId != peerId || peer.SessionEpoch != sessionEpoch)
            {
                Drop();
                return false;
            }

            _live[peerIndex] = false;
            _livePeerCount--;
            if (!_sampleCaptureStarted)
            {
                _retained[peerIndex] = false;
                _retainedPeerCount--;
                peer.Reset(0, 0);
            }
            return true;
        }

        public void RecordSnapshotBatchOfferAttempt(int peerIndex) =>
            Increment(peerIndex, Counter.SnapshotBatchOfferAttempts);

        public void RecordPendingReceiptDeferral(int peerIndex) =>
            Increment(peerIndex, Counter.PendingReceiptDeferrals);

        public void RecordPendingReliableDeferral(int peerIndex) =>
            Increment(peerIndex, Counter.PendingReliableDeferrals);

        /// <summary>Starts bounded tracking for one offered server-tick/packet-sequence identity.</summary>
        /// <returns>False when the peer slot is invalid or the 32-identity cap is full.</returns>
        public bool BeginSnapshotChunk(int peerIndex, uint serverTick,
            uint packetSequence, bool firstChunk, long offeredTimestamp)
        {
            if (!TryPeer(peerIndex, out var peer))
                return false;

            Increment(peerIndex, Counter.SnapshotChunkOffers);
            var existing = FindIdentity(peer, serverTick, packetSequence);
            if (existing >= 0)
                return true;
            var free = FindFreeIdentity(peer);
            if (free < 0)
            {
                Drop();
                return false;
            }

            peer.Identities[free] = new SnapshotIdentity
            {
                Active = true,
                ServerTick = serverTick,
                PacketSequence = packetSequence,
                FirstChunk = firstChunk,
                OfferedTimestamp = offeredTimestamp,
            };
            return true;
        }

        public void RecordSnapshotChunkSendFailure(int peerIndex,
            uint serverTick, uint packetSequence)
        {
            if (!TryPeer(peerIndex, out var peer))
                return;
            Increment(peerIndex, Counter.TransportSendFailures);
            ClearIdentity(peer, serverTick, packetSequence);
        }

        public void ClearSnapshotIdentity(int peerIndex, uint serverTick,
            uint packetSequence)
        {
            if (TryPeer(peerIndex, out var peer))
                ClearIdentity(peer, serverTick, packetSequence);
        }

        public void RecordManagedFifoEnqueue(int peerIndex, uint serverTick,
            uint packetSequence, long timestamp)
        {
            if (!TryPeer(peerIndex, out var peer))
                return;
            Increment(peerIndex, Counter.ManagedFifoEnqueues);
            var identity = FindIdentity(peer, serverTick, packetSequence);
            if (identity >= 0)
                peer.Identities[identity].ManagedFifoTimestamp = timestamp;
        }

        public void RecordNativeTicketSubmission(int peerIndex,
            uint serverTick, uint packetSequence, long timestamp)
        {
            if (!TryPeer(peerIndex, out var peer))
                return;
            Increment(peerIndex, Counter.NativeTicketSubmissions);
            var identityIndex = FindIdentity(peer, serverTick, packetSequence);
            if (identityIndex < 0 || timestamp <= 0)
            {
                Drop();
                return;
            }
            ref var identity = ref peer.Identities[identityIndex];
            if (identity.NativeTicketTimestamp != 0 ||
                (identity.NativeDeliveryEventTimestamp > 0 &&
                 timestamp > identity.NativeDeliveryEventTimestamp))
            {
                Drop();
                return;
            }
            if (identity.ManagedFifoTimestamp > 0 &&
                timestamp >= identity.ManagedFifoTimestamp)
                peer.ManagedFifoResidence.AddStopwatchTicks(
                    timestamp - identity.ManagedFifoTimestamp);
            identity.NativeTicketTimestamp = timestamp;
            if (identity.DeliveryCallbackTimestamp >= timestamp &&
                identity.DeliveryCallbackTimestamp > 0)
                peer.NativeTicketLifetime.AddStopwatchTicks(
                    identity.DeliveryCallbackTimestamp - timestamp);
            if (identity.NativeDeliveryEventTimestamp >= timestamp &&
                identity.NativeDeliveryEventTimestamp > 0)
                peer.SubmissionToNativeDeliveryEvent.AddStopwatchTicks(
                    identity.NativeDeliveryEventTimestamp - timestamp);
        }

        /// <summary>Records the producer-side native delivery-event callback entry.</summary>
        public void RecordNativeDeliveryEvent(int peerIndex, uint serverTick,
            uint packetSequence, long timestamp)
        {
            if (!TryPeer(peerIndex, out var peer))
                return;
            Increment(peerIndex, Counter.NativeDeliveryEvents);
            var identityIndex = FindIdentity(peer, serverTick, packetSequence);
            if (identityIndex < 0 || timestamp <= 0)
            {
                Drop();
                return;
            }
            ref var identity = ref peer.Identities[identityIndex];
            if (identity.NativeDeliveryEventTimestamp != 0 ||
                (identity.NativeTicketTimestamp > 0 &&
                 timestamp < identity.NativeTicketTimestamp))
            {
                Drop();
                return;
            }
            identity.NativeDeliveryEventTimestamp = timestamp;
            if (identity.NativeTicketTimestamp > 0)
                peer.SubmissionToNativeDeliveryEvent.AddStopwatchTicks(
                    timestamp - identity.NativeTicketTimestamp);
        }

        public void RecordDeliveryCallback(int peerIndex, uint serverTick,
            uint packetSequence, long timestamp)
        {
            if (!TryPeer(peerIndex, out var peer))
                return;
            Increment(peerIndex, Counter.DeliveryCallbacks);
            var identityIndex = FindIdentity(peer, serverTick, packetSequence);
            if (identityIndex < 0 || timestamp <= 0)
            {
                Drop();
                return;
            }
            ref var identity = ref peer.Identities[identityIndex];
            if (identity.DeliveryCallbackTimestamp != 0 ||
                (identity.NativeTicketTimestamp > 0 &&
                 timestamp < identity.NativeTicketTimestamp) ||
                (identity.NativeDeliveryEventTimestamp > 0 &&
                 timestamp < identity.NativeDeliveryEventTimestamp))
            {
                Drop();
                return;
            }
            if (identity.NativeTicketTimestamp > 0)
                peer.NativeTicketLifetime.AddStopwatchTicks(
                    timestamp - identity.NativeTicketTimestamp);
            if (identity.NativeDeliveryEventTimestamp > 0)
                peer.NativeDeliveryEventToOwnerDispatch.AddStopwatchTicks(
                    timestamp - identity.NativeDeliveryEventTimestamp);
            identity.DeliveryCallbackTimestamp = timestamp;
            if (identity.ApplicationAcked)
                identity = default;

            if (timestamp > peer.LastDeliveryCallbackTimestamp)
            {
                peer.LastDeliveryCallbackTimestamp = timestamp;
                peer.LastDeliveryCallbackTick = serverTick;
            }
        }

        public void RecordSnapshotBatchAccepted(int peerIndex,
            uint serverTick, uint previousSnapshotTick, long timestamp)
        {
            if (!TryPeer(peerIndex, out var peer))
                return;
            Increment(peerIndex, Counter.SnapshotBatchesAccepted);
            if (previousSnapshotTick != 0 &&
                peer.LastDeliveryCallbackTick == previousSnapshotTick &&
                peer.LastDeliveryCallbackTimestamp >
                    peer.ConsumedDeliveryCallbackTimestamp &&
                timestamp >= peer.LastDeliveryCallbackTimestamp)
            {
                peer.CallbackToNextAccept.AddStopwatchTicks(
                    timestamp - peer.LastDeliveryCallbackTimestamp);
                peer.ConsumedDeliveryCallbackTimestamp =
                    peer.LastDeliveryCallbackTimestamp;
            }
            peer.LastAcceptedSnapshotTick = serverTick;
        }

        public void RecordApplicationAck(int peerIndex, uint serverTick,
            long timestamp)
        {
            if (!TryPeer(peerIndex, out var peer))
                return;
            Increment(peerIndex, Counter.ApplicationAcks);
            for (var index = 0; index < peer.Identities.Length; index++)
            {
                ref var identity = ref peer.Identities[index];
                if (!identity.Active || identity.ServerTick != serverTick)
                    continue;
                if (identity.FirstChunk && identity.OfferedTimestamp > 0 &&
                    timestamp >= identity.OfferedTimestamp)
                    peer.ApplicationAckWait.AddStopwatchTicks(
                        timestamp - identity.OfferedTimestamp);
                identity.ApplicationAcked = true;
                if (identity.DeliveryCallbackTimestamp > 0)
                    identity = default;
            }
        }

        /// <summary>Starts sample retention and copies bounded counters into an immutable summary.</summary>
        public NetworkDeliveryPhaseDiagnostics Capture()
        {
            _sampleCaptureStarted = true;
            var peers = new NetworkDeliveryPhasePeerDiagnostics[
                _retainedPeerCount];
            var peer = 0;
            for (var index = 0; index < _peers.Length; index++)
                if (_retained[index])
                    peers[peer++] = _peers[index].Capture();
            return new NetworkDeliveryPhaseDiagnostics(true, _capDrops,
                _truncated, peers);
        }

        private bool TryPeer(int index, out PeerState peer)
        {
            if ((uint)index < (uint)_peers.Length && _live[index])
            {
                peer = _peers[index];
                return true;
            }
            peer = null;
            return false;
        }

        private void Increment(int index, Counter counter)
        {
            if (!TryPeer(index, out var peer))
                return;
            switch (counter)
            {
                case Counter.SnapshotBatchOfferAttempts:
                    peer.SnapshotBatchOfferAttempts = SaturatingIncrement(
                        peer.SnapshotBatchOfferAttempts);
                    break;
                case Counter.PendingReceiptDeferrals:
                    peer.PendingReceiptDeferrals = SaturatingIncrement(
                        peer.PendingReceiptDeferrals);
                    break;
                case Counter.PendingReliableDeferrals:
                    peer.PendingReliableDeferrals = SaturatingIncrement(
                        peer.PendingReliableDeferrals);
                    break;
                case Counter.SnapshotChunkOffers:
                    peer.SnapshotChunkOffers = SaturatingIncrement(
                        peer.SnapshotChunkOffers);
                    break;
                case Counter.ManagedFifoEnqueues:
                    peer.ManagedFifoEnqueues = SaturatingIncrement(
                        peer.ManagedFifoEnqueues);
                    break;
                case Counter.NativeTicketSubmissions:
                    peer.NativeTicketSubmissions = SaturatingIncrement(
                        peer.NativeTicketSubmissions);
                    break;
                case Counter.DeliveryCallbacks:
                    peer.DeliveryCallbacks = SaturatingIncrement(
                        peer.DeliveryCallbacks);
                    break;
                case Counter.NativeDeliveryEvents:
                    peer.NativeDeliveryEvents = SaturatingIncrement(
                        peer.NativeDeliveryEvents);
                    break;
                case Counter.SnapshotBatchesAccepted:
                    peer.SnapshotBatchesAccepted = SaturatingIncrement(
                        peer.SnapshotBatchesAccepted);
                    break;
                case Counter.ApplicationAcks:
                    peer.ApplicationAcks = SaturatingIncrement(
                        peer.ApplicationAcks);
                    break;
                case Counter.TransportSendFailures:
                    peer.TransportSendFailures = SaturatingIncrement(
                        peer.TransportSendFailures);
                    break;
            }
        }

        private static int FindIdentity(PeerState peer, uint serverTick,
            uint packetSequence)
        {
            for (var index = 0; index < peer.Identities.Length; index++)
            {
                var identity = peer.Identities[index];
                if (identity.Active && identity.ServerTick == serverTick &&
                    identity.PacketSequence == packetSequence)
                    return index;
            }
            return -1;
        }

        private static int FindFreeIdentity(PeerState peer)
        {
            for (var index = 0; index < peer.Identities.Length; index++)
                if (!peer.Identities[index].Active)
                    return index;
            return -1;
        }

        private static void ClearIdentity(PeerState peer, uint serverTick,
            uint packetSequence)
        {
            var index = FindIdentity(peer, serverTick, packetSequence);
            if (index >= 0)
                peer.Identities[index] = default;
        }

        private void Drop()
        {
            _capDrops = SaturatingIncrement(_capDrops);
            _truncated = true;
        }

        private static long SaturatingIncrement(long value) =>
            value == long.MaxValue ? value : value + 1;

        private static PeerState[] CreatePeerStates()
        {
            var peers = new PeerState[
                NetworkDeliveryPhaseDiagnostics.MaximumPeers];
            for (var index = 0; index < peers.Length; index++)
                peers[index] = new PeerState();
            return peers;
        }

        private enum Counter
        {
            SnapshotBatchOfferAttempts,
            PendingReceiptDeferrals,
            PendingReliableDeferrals,
            SnapshotChunkOffers,
            ManagedFifoEnqueues,
            NativeTicketSubmissions,
            DeliveryCallbacks,
            SnapshotBatchesAccepted,
            ApplicationAcks,
            TransportSendFailures,
            NativeDeliveryEvents,
        }

        private struct SnapshotIdentity
        {
            internal bool Active;
            internal bool FirstChunk;
            internal bool ApplicationAcked;
            internal uint ServerTick;
            internal uint PacketSequence;
            internal long OfferedTimestamp;
            internal long ManagedFifoTimestamp;
            internal long NativeTicketTimestamp;
            internal long DeliveryCallbackTimestamp;
            internal long NativeDeliveryEventTimestamp;
        }

        private sealed class PeerState
        {
            internal PeerState()
            {
                Identities = new SnapshotIdentity[
                    NetworkDeliveryPhaseDiagnostics.MaximumOutstandingSnapshotIdentitiesPerPeer];
            }

            internal uint PeerId;
            internal uint SessionEpoch;
            internal readonly SnapshotIdentity[] Identities;
            internal long SnapshotBatchOfferAttempts;
            internal long PendingReceiptDeferrals;
            internal long PendingReliableDeferrals;
            internal long SnapshotChunkOffers;
            internal long ManagedFifoEnqueues;
            internal long NativeTicketSubmissions;
            internal long DeliveryCallbacks;
            internal long NativeDeliveryEvents;
            internal long SnapshotBatchesAccepted;
            internal long ApplicationAcks;
            internal long TransportSendFailures;
            internal long LastDeliveryCallbackTimestamp;
            internal long ConsumedDeliveryCallbackTimestamp;
            internal uint LastDeliveryCallbackTick;
            internal uint LastAcceptedSnapshotTick;
            internal NetworkTimingHistogramAccumulator ManagedFifoResidence;
            internal NetworkTimingHistogramAccumulator NativeTicketLifetime;
            internal NetworkTimingHistogramAccumulator SubmissionToNativeDeliveryEvent;
            internal NetworkTimingHistogramAccumulator NativeDeliveryEventToOwnerDispatch;
            internal NetworkTimingHistogramAccumulator CallbackToNextAccept;
            internal NetworkTimingHistogramAccumulator ApplicationAckWait;

            internal void Reset(uint peerId, uint sessionEpoch)
            {
                PeerId = peerId;
                SessionEpoch = sessionEpoch;
                SnapshotBatchOfferAttempts = 0;
                PendingReceiptDeferrals = 0;
                PendingReliableDeferrals = 0;
                SnapshotChunkOffers = 0;
                ManagedFifoEnqueues = 0;
                NativeTicketSubmissions = 0;
                DeliveryCallbacks = 0;
                NativeDeliveryEvents = 0;
                SnapshotBatchesAccepted = 0;
                ApplicationAcks = 0;
                TransportSendFailures = 0;
                LastDeliveryCallbackTimestamp = 0;
                ConsumedDeliveryCallbackTimestamp = 0;
                LastDeliveryCallbackTick = 0;
                LastAcceptedSnapshotTick = 0;
                ManagedFifoResidence = default;
                NativeTicketLifetime = default;
                SubmissionToNativeDeliveryEvent = default;
                NativeDeliveryEventToOwnerDispatch = default;
                CallbackToNextAccept = default;
                ApplicationAckWait = default;
                Array.Clear(Identities, 0, Identities.Length);
            }

            internal NetworkDeliveryPhasePeerDiagnostics Capture()
            {
                var outstanding = 0;
                var outstandingNativeTickets = 0;
                for (var index = 0; index < Identities.Length; index++)
                {
                    var identity = Identities[index];
                    if (identity.Active)
                        outstanding++;
                    if (identity.Active && identity.NativeTicketTimestamp > 0 &&
                        identity.DeliveryCallbackTimestamp == 0)
                        outstandingNativeTickets++;
                }
                return new NetworkDeliveryPhasePeerDiagnostics(PeerId,
                    SessionEpoch, SnapshotBatchOfferAttempts,
                    PendingReceiptDeferrals, PendingReliableDeferrals,
                    SnapshotChunkOffers, ManagedFifoEnqueues,
                    NativeTicketSubmissions, DeliveryCallbacks,
                    NativeDeliveryEvents,
                    SnapshotBatchesAccepted, ApplicationAcks,
                    TransportSendFailures, outstanding,
                    outstandingNativeTickets,
                    ManagedFifoResidence.Capture(),
                    NativeTicketLifetime.Capture(),
                    SubmissionToNativeDeliveryEvent.Capture(),
                    NativeDeliveryEventToOwnerDispatch.Capture(),
                    CallbackToNextAccept.Capture(),
                    ApplicationAckWait.Capture());
            }
        }
    }

    /// <summary>Binds a server-assigned peer identity to transport delivery callbacks.</summary>
    public interface INetworkDeliveryPhaseTransport
    {
        /// <summary>Binds a recorder and peer slot, or clears the binding when recorder is null.</summary>
        void ConfigureDeliveryPhaseDiagnostics(
            NetworkDeliveryPhaseRecorder recorder, int peerIndex);
    }
}
