using System;
using System.Collections.Generic;
using System.IO;
using System.Diagnostics;
using FFS.Libraries.StaticEcs;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Selects when the server may offer the next snapshot batch to a peer.</summary>
    public enum NetworkSnapshotAdmissionMode : byte
    {
        /// <summary>Uses transport-reported pending reliable work.</summary>
        Current,

        /// <summary>Waits for the exact application ACK of each fully accepted batch.</summary>
        ApplicationAck,

        /// <summary>
        /// Admits a new snapshot while fewer than <c>maxSnapshotsInFlight</c> snapshot ticks sent to
        /// the peer remain unacknowledged and the transport has no reliable packet still waiting to
        /// be submitted. Unlike <see cref="Current"/>, it does not wait for the transport's delivery
        /// callback of the previous snapshot, so the snapshot cadence no longer depends on the
        /// round trip or on when the transport owner polls its events.
        /// </summary>
        Windowed,
    }

    /// <summary>Bounds shared by every host that configures <see cref="NetworkSnapshotAdmissionMode.Windowed"/>.</summary>
    public static class NetworkSnapshotAdmissionLimits
    {
        /// <summary>Default unacknowledged snapshot ticks per peer: two cover a 100 ms round trip at 20 Hz.</summary>
        public const int DefaultMaxSnapshotsInFlight = 2;
        /// <summary>Upper bound for the in-flight limit; each tick in flight holds reliable window slots on the transport.</summary>
        public const int MaxSnapshotsInFlight = 8;
    }

    /// <summary>Copies bounded cumulative counters for the snapshot admission policy.</summary>
    public readonly struct NetworkSnapshotAdmissionDiagnostics
    {
        public NetworkSnapshotAdmissionDiagnostics(
            NetworkSnapshotAdmissionMode mode, long suppressedSnapshots,
            long acknowledgementGaps, long recoveryAttempts,
            long recoveryBatchesAccepted, long partialBatchFailures,
            long pendingReceiptDeferrals = 0,
            long pendingReliableDeferrals = 0,
            long preflightRejections = 0,
            long chunkSendFailures = 0,
            long snapshotBatchesAccepted = 0,
            long snapshotOfferAttempts = 0,
            long captureFailures = 0,
            long preparationFailures = 0,
            NetworkTimingHistogram applicationAck = default,
            NetworkTimingHistogram callbackToSnapshotAdmission = default,
            long snapshotWindowDeferrals = 0,
            long catchUpTicksWithoutOffer = 0)
        {
            Mode = mode;
            SnapshotWindowDeferrals = snapshotWindowDeferrals;
            CatchUpTicksWithoutOffer = catchUpTicksWithoutOffer;
            SuppressedSnapshots = suppressedSnapshots;
            AcknowledgementGaps = acknowledgementGaps;
            RecoveryAttempts = recoveryAttempts;
            RecoveryBatchesAccepted = recoveryBatchesAccepted;
            PartialBatchFailures = partialBatchFailures;
            PendingReceiptDeferrals = pendingReceiptDeferrals;
            PendingReliableDeferrals = pendingReliableDeferrals;
            PreflightRejections = preflightRejections;
            ChunkSendFailures = chunkSendFailures;
            SnapshotBatchesAccepted = snapshotBatchesAccepted;
            SnapshotOfferAttempts = snapshotOfferAttempts;
            CaptureFailures = captureFailures;
            PreparationFailures = preparationFailures;
            ApplicationAck = applicationAck;
            CallbackToSnapshotAdmission = callbackToSnapshotAdmission;
        }

        public NetworkSnapshotAdmissionMode Mode { get; }
        public long SuppressedSnapshots { get; }
        public long AcknowledgementGaps { get; }
        public long RecoveryAttempts { get; }
        public long RecoveryBatchesAccepted { get; }
        public long PartialBatchFailures { get; }
        public long PendingReceiptDeferrals { get; }
        public long PendingReliableDeferrals { get; }
        public long PreflightRejections { get; }
        public long ChunkSendFailures { get; }
        public long SnapshotBatchesAccepted { get; }
        public long SnapshotOfferAttempts { get; }
        public long CaptureFailures { get; }
        public long PreparationFailures { get; }
        /// <summary>Snapshot submission to exact application ACK duration distribution.</summary>
        public NetworkTimingHistogram ApplicationAck { get; }
        /// <summary>Matching snapshot callback to the next fully accepted batch, in the owner clock.</summary>
        public NetworkTimingHistogram CallbackToSnapshotAdmission { get; }
        /// <summary>Snapshots deferred because the peer already had the maximum unacknowledged snapshot ticks in flight (<see cref="NetworkSnapshotAdmissionMode.Windowed"/> only).</summary>
        public long SnapshotWindowDeferrals { get; }
        /// <summary>Ticks completed with <c>offerSnapshots: false</c>: intermediate catch-up ticks the runner chose not to replicate.</summary>
        public long CatchUpTicksWithoutOffer { get; }
    }

    /// <summary>Bounded cumulative server scope and capture counters.</summary>
    public readonly struct NetworkScopeDiagnostics
    {
        public NetworkScopeDiagnostics(int occupiedScopes, long historyBytes,
            long historyLimitEvictions, long captureCount, long captureStopwatchTicks,
            long scopeChanges, long keyframeBatches,
            long scopeRefreshStopwatchTicks = 0,
            long cellCacheSweepCount = 0,
            long cellCacheSweepStopwatchTicks = 0,
            long cellCaptureCount = 0,
            long cellCaptureStopwatchTicks = 0,
            long cellMergeStopwatchTicks = 0,
            long historyStoreStopwatchTicks = 0,
            long deltaEncodeStopwatchTicks = 0,
            long transportTrySendStopwatchTicks = 0,
            long deltaCacheHits = 0, long deltaCacheMisses = 0,
            long scopeAssignmentStopwatchTicks = 0,
            long scopeCleanupStopwatchTicks = 0,
            int cellCaptureVariantCount = 0,
            long cellCaptureVariantBytes = 0,
            long cellCaptureVariantHits = 0,
            long cellCaptureVariantMisses = 0,
            long cellCaptureVariantEvictions = 0,
            long passiveNpcEncodedRecordBytes = 0,
            long encodedSnapshotBytes = 0,
            long snapshotFramingBytes = 0,
            long passiveNpcByteAttributionFailures = 0,
            long staleScopeAcknowledgements = 0,
            long lostBaselineAcknowledgements = 0,
            long resyncRequests = 0)
        {
            StaleScopeAcknowledgements = staleScopeAcknowledgements;
            LostBaselineAcknowledgements = lostBaselineAcknowledgements;
            ResyncRequests = resyncRequests;
            OccupiedScopes = occupiedScopes;
            HistoryBytes = historyBytes;
            HistoryLimitEvictions = historyLimitEvictions;
            CaptureCount = captureCount;
            CaptureStopwatchTicks = captureStopwatchTicks;
            ScopeChanges = scopeChanges;
            KeyframeBatches = keyframeBatches;
            ScopeRefreshStopwatchTicks = scopeRefreshStopwatchTicks;
            CellCacheSweepCount = cellCacheSweepCount;
            CellCacheSweepStopwatchTicks = cellCacheSweepStopwatchTicks;
            CellCaptureCount = cellCaptureCount;
            CellCaptureStopwatchTicks = cellCaptureStopwatchTicks;
            CellMergeStopwatchTicks = cellMergeStopwatchTicks;
            HistoryStoreStopwatchTicks = historyStoreStopwatchTicks;
            DeltaEncodeStopwatchTicks = deltaEncodeStopwatchTicks;
            TransportTrySendStopwatchTicks = transportTrySendStopwatchTicks;
            DeltaCacheHits = deltaCacheHits;
            DeltaCacheMisses = deltaCacheMisses;
            ScopeAssignmentStopwatchTicks = scopeAssignmentStopwatchTicks;
            ScopeCleanupStopwatchTicks = scopeCleanupStopwatchTicks;
            CellCaptureVariantCount = cellCaptureVariantCount;
            CellCaptureVariantBytes = cellCaptureVariantBytes;
            CellCaptureVariantHits = cellCaptureVariantHits;
            CellCaptureVariantMisses = cellCaptureVariantMisses;
            CellCaptureVariantEvictions = cellCaptureVariantEvictions;
            PassiveNpcEncodedRecordBytes = passiveNpcEncodedRecordBytes;
            EncodedSnapshotBytes = encodedSnapshotBytes;
            SnapshotFramingBytes = snapshotFramingBytes;
            PassiveNpcByteAttributionFailures = passiveNpcByteAttributionFailures;
        }

        public int OccupiedScopes { get; }
        public long HistoryBytes { get; }
        public long HistoryLimitEvictions { get; }
        public long CaptureCount { get; }
        public long CaptureStopwatchTicks { get; }
        public long ScopeChanges { get; }
        public long KeyframeBatches { get; }
        /// <summary>Acknowledgements of old-scope snapshots already on the wire when a scope changed.</summary>
        public long StaleScopeAcknowledgements { get; }
        /// <summary>Acknowledgements of a tick the history no longer holds, each forcing a keyframe.</summary>
        public long LostBaselineAcknowledgements { get; }
        /// <summary>Resync requests received from clients.</summary>
        public long ResyncRequests { get; }
        public long ScopeRefreshStopwatchTicks { get; }
        public long CellCacheSweepCount { get; }
        public long CellCacheSweepStopwatchTicks { get; }
        public long CellCaptureCount { get; }
        public long CellCaptureStopwatchTicks { get; }
        public long CellMergeStopwatchTicks { get; }
        public long HistoryStoreStopwatchTicks { get; }
        public long DeltaEncodeStopwatchTicks { get; }
        public long TransportTrySendStopwatchTicks { get; }
        public long DeltaCacheHits { get; }
        public long DeltaCacheMisses { get; }
        public long ScopeAssignmentStopwatchTicks { get; }
        public long ScopeCleanupStopwatchTicks { get; }
        public int CellCaptureVariantCount { get; }
        public long CellCaptureVariantBytes { get; }
        public long CellCaptureVariantHits { get; }
        public long CellCaptureVariantMisses { get; }
        public long CellCaptureVariantEvictions { get; }
        public long PassiveNpcEncodedRecordBytes { get; }
        public long EncodedSnapshotBytes { get; }
        public long SnapshotFramingBytes { get; }
        public long PassiveNpcByteAttributionFailures { get; }
    }

    /// <summary>Non-overlapping Stopwatch durations of the most recent authority tick.</summary>
    public readonly struct NetworkServerTickStageDiagnostics
    {
        public NetworkServerTickStageDiagnostics(long dispatch, long transactions,
            long scope, long snapshotPeers, long cleanup)
        {
            Dispatch = dispatch;
            Transactions = transactions;
            Scope = scope;
            SnapshotPeers = snapshotPeers;
            Cleanup = cleanup;
        }

        public long Dispatch { get; }
        public long Transactions { get; }
        public long Scope { get; }
        public long SnapshotPeers { get; }
        public long Cleanup { get; }
        public long Total => Dispatch + Transactions + Scope + SnapshotPeers + Cleanup;
    }

    /// <summary>Runs framed receive, decode, command dispatch, capture, and send for isolated server connections.</summary>
    public sealed class NetworkServer<TWorld> : IDisposable
        where TWorld : struct, IWorldType
    {
        private readonly uint _recoveryRetryTicks;
        private readonly int _unreliableDeltaChunks;
        private readonly uint _unreliableDeltaLostTicks;
        private long _deltaSnapshotsUnreliable;
        private long _unreliableDeltaWindowExpirations;
        private long _unreliableAcknowledgements;
        private readonly List<NetworkBufferLease> _uncachedChunkPayloads =
            new List<NetworkBufferLease>();
        private readonly uint _ownerStateIntervalTicks;
        /// <summary>Upper bound for <c>maxSnapshotsInFlight</c>; see <see cref="NetworkSnapshotAdmissionLimits"/>.</summary>
        public const int MaxSnapshotsInFlightLimit =
            NetworkSnapshotAdmissionLimits.MaxSnapshotsInFlight;
        private readonly NetworkSchema<TWorld> _schema;
        private readonly NetworkServerCoordinator<TWorld> _coordinator;
        private readonly NetworkReplicator<TWorld> _replicator;
        private readonly List<Peer> _peers = new List<Peer>();
        private readonly Dictionary<uint, ScopeId> _peerScopes = new Dictionary<uint, ScopeId>();
        private readonly bool _captureBudgetEnabled;
        private readonly INetworkObserver _observer;
        private readonly INetworkPeerObserver _peerObserver;
        private readonly INetworkPeerAdmissionPolicy _admissionPolicy;
        private readonly INetworkScopeProvider<TWorld> _scopeProvider;
        private readonly ulong _simulationFingerprint;
        private readonly ulong _contentFingerprint;
        private readonly uint _compatibilityTag;
        private readonly NetworkBufferPool _bufferPool;
        private readonly bool _ownsBufferPool;
        private readonly Dictionary<ScopeId, NetworkSnapshot> _captures =
            new Dictionary<ScopeId, NetworkSnapshot>();
        private readonly List<ScopeId> _departedScopes = new List<ScopeId>();
        private readonly Dictionary<SnapshotDeltaKey, SnapshotDeltaEntry> _snapshotDeltas =
            new Dictionary<SnapshotDeltaKey, SnapshotDeltaEntry>();
        private readonly Dictionary<ChunkPayloadKey, ChunkPayloadEntry> _chunkPayloads =
            new Dictionary<ChunkPayloadKey, ChunkPayloadEntry>();
        private uint _activeTick;
        private int _activeConnectionCount;
        private int _activePeerCount;
        private bool _disposed;
        private readonly NetworkSnapshotAdmissionMode _snapshotAdmissionMode;
        private readonly int _maxSnapshotsInFlight;
        private NetworkDeliveryPhaseRecorder _deliveryPhaseDiagnostics;
        private long _suppressedSnapshots;
        private long _snapshotWindowDeferrals;
        private long _catchUpTicksWithoutOffer;
        private long _acknowledgementGaps;
        private long _recoveryAttempts;
        private long _recoveryBatchesAccepted;
        private long _partialBatchFailures;
        private long _pendingReceiptDeferrals;
        private long _pendingReliableDeferrals;
        private long _preflightRejections;
        private long _chunkSendFailures;
        private long _snapshotBatchesAccepted;
        private long _snapshotOfferAttempts;
        private long _captureFailures;
        private long _preparationFailures;
        private long _scopeCaptureCount;
        private long _scopeCaptureStopwatchTicks;
        private long _scopeRefreshStopwatchTicks;
        private long _scopeAssignmentStopwatchTicks;
        private long _scopeCleanupStopwatchTicks;
        private long _historyStoreStopwatchTicks;
        private long _deltaEncodeStopwatchTicks;
        private long _deltaCacheHits;
        private long _deltaCacheMisses;
        private long _transportTrySendStopwatchTicks;
        private long _scopeChanges;
        private long _keyframeBatches;
        private long _staleScopeAcknowledgements;
        private long _lostBaselineAcknowledgements;
        private long _resyncRequests;
        private long _passiveNpcEncodedRecordBytes;
        private long _encodedSnapshotBytes;
        private long _snapshotFramingBytes;
        private long _passiveNpcByteAttributionFailures;
        private readonly long[] _captureFailuresByReason =
            new long[(int)SnapshotCaptureResult.HookFailed + 1];
        private readonly NetworkValueHistogramAccumulator _scopeCaptureEntities =
            new NetworkValueHistogramAccumulator();
        private readonly NetworkValueHistogramAccumulator _scopeCaptureBytes =
            new NetworkValueHistogramAccumulator();
        private long _keyframeSnapshots;
        private long _keyframeBytes;
        private long _keyframeEntities;
        private long _deltaSnapshots;
        private long _deltaBytes;
        private long _deltaTargetEntities;
        private long _deltaEntityAdds;
        private long _deltaEntityRemoves;
        private long _deltaEntityPatches;
        private readonly NetworkValueHistogramAccumulator _deltaSnapshotBytes =
            new NetworkValueHistogramAccumulator();
        private long _deltaSnapshotsOverDatagram;
        private readonly NetworkValueHistogramAccumulator _deltaBaselineLagTicks =
            new NetworkValueHistogramAccumulator();
        private NetworkReplicationCensus<TWorld> _replicationCensus;
        private NetworkTimingHistogramAccumulator _applicationAck;
        private NetworkTimingHistogramAccumulator _callbackToSnapshotAdmission;
        private readonly Func<long> _monotonicTimestamp;
        private NetworkServerTickStageDiagnostics _lastTickStages;
        private long _tickDispatchTicks;

        /// <summary>Gets the latest authoritative tick completed by this server.</summary>
        public uint ServerTick { get; private set; }

        public int ConnectionCount => _peers.Count;

        /// <summary>Returns durations from the most recently completed authority tick.</summary>
        public NetworkServerTickStageDiagnostics CaptureLastTickStageDiagnostics() =>
            _lastTickStages;

        /// <summary>Copies cumulative counters for the configured admission mode.</summary>
        public NetworkSnapshotAdmissionDiagnostics CaptureSnapshotAdmissionDiagnostics() =>
            new NetworkSnapshotAdmissionDiagnostics(_snapshotAdmissionMode,
                _suppressedSnapshots, _acknowledgementGaps, _recoveryAttempts,
                _recoveryBatchesAccepted, _partialBatchFailures,
                _pendingReceiptDeferrals, _pendingReliableDeferrals,
                _preflightRejections, _chunkSendFailures,
                _snapshotBatchesAccepted, _snapshotOfferAttempts,
                _captureFailures, _preparationFailures,
                _applicationAck.Capture(), _callbackToSnapshotAdmission.Capture(),
                _snapshotWindowDeferrals, _catchUpTicksWithoutOffer);

        public NetworkBufferPoolDiagnostics CaptureBufferDiagnostics() =>
            _bufferPool.CaptureDiagnostics();

        /// <summary>Copies scope occupancy, retained history and cumulative capture work.</summary>
        public NetworkScopeDiagnostics CaptureScopeDiagnostics() =>
            new NetworkScopeDiagnostics(_coordinator.HistoryScopeCount,
                _coordinator.HistoryBytes, _coordinator.HistoryLimitEvictions,
                _scopeCaptureCount, _scopeCaptureStopwatchTicks, _scopeChanges,
                _keyframeBatches, _scopeRefreshStopwatchTicks,
                _replicator.CellCacheSweepCount,
                _replicator.CellCacheSweepStopwatchTicks,
                _replicator.CellCaptureCount,
                _replicator.CellCaptureStopwatchTicks,
                _replicator.CellMergeStopwatchTicks,
                _historyStoreStopwatchTicks, _deltaEncodeStopwatchTicks,
                _transportTrySendStopwatchTicks, _deltaCacheHits,
                _deltaCacheMisses, _scopeAssignmentStopwatchTicks,
                _scopeCleanupStopwatchTicks,
                _replicator.CellCaptureVariantCount,
                _replicator.CellCaptureVariantBytes,
                _replicator.CellCaptureVariantHits,
                _replicator.CellCaptureVariantMisses,
                _replicator.CellCaptureVariantEvictions,
                _passiveNpcEncodedRecordBytes, _encodedSnapshotBytes,
                _snapshotFramingBytes, _passiveNpcByteAttributionFailures,
                _staleScopeAcknowledgements, _lostBaselineAcknowledgements, _resyncRequests);

        /// <summary>Copies cumulative replication counters; allocates, so read it at sample boundaries.</summary>
        public NetworkReplicationDiagnostics CaptureReplicationDiagnostics() =>
            new NetworkReplicationDiagnostics((long[])_captureFailuresByReason.Clone(),
                _scopeCaptureEntities.Capture(), _scopeCaptureBytes.Capture(),
                _keyframeSnapshots, _keyframeBytes, _keyframeEntities,
                _deltaSnapshots, _deltaBytes, _deltaTargetEntities,
                _deltaEntityAdds, _deltaEntityRemoves, _deltaEntityPatches,
                _replicationCensus?.Capture(), _deltaSnapshotBytes.Capture(),
                _deltaSnapshotsOverDatagram, _replicator.CaptureBudgetRefreshedEntities,
                _replicator.CaptureBudgetDeferredEntities,
                _replicator.CaptureBudgetOverflowCaptures, _ownedEntityRefreshPackets,
                _ownedEntityRefreshBytes, _replicator.CaptureBudgetBytes,
                _replicator.CaptureBudgetMaximumWaitTicks,
                _replicator.CaptureBudgetRefreshesObserversPersonally,
                new NetworkCaptureBudgetStageTicks(
                    _replicator.CaptureBudgetMergeStopwatchTicks,
                    _replicator.CaptureBudgetCompareStopwatchTicks,
                    _replicator.CaptureBudgetMeasureStopwatchTicks,
                    _replicator.CaptureBudgetWeightStopwatchTicks,
                    _replicator.CaptureBudgetSelectStopwatchTicks,
                    _replicator.CaptureBudgetWriteStopwatchTicks,
                    _ownedEntityRefreshPatchStopwatchTicks,
                    _ownedEntityRefreshEncodeStopwatchTicks,
                    _ownedEntityRefreshTransportStopwatchTicks),
                _ownedEntityRefreshTrailers, _deltaBaselineLagTicks.Capture(),
                _deltaSnapshotsUnreliable, _unreliableDeltaWindowExpirations,
                _unreliableAcknowledgements);

        /// <summary>
        /// Enables the sampled change and observation census before load admission. Every
        /// <paramref name="intervalTicks"/>-th offered tick compares each captured scope with its
        /// previous-tick capture and counts world entities per kind; it never changes what is sent.
        /// </summary>
        public void EnableReplicationCensus(int intervalTicks)
        {
            if (_replicationCensus != null)
                throw new InvalidOperationException("Replication census is already enabled.");
            _replicationCensus = new NetworkReplicationCensus<TWorld>(_schema, intervalTicks);
        }

        /// <summary>Captures current bounded endpoint memory and queue ownership.</summary>
        public NetworkMemoryDiagnostics CaptureMemoryDiagnostics() => new NetworkMemoryDiagnostics
        {
            Buffers = _bufferPool.CaptureDiagnostics(),
            HistoryBytes = _coordinator.HistoryBytes,
            HistoryScopes = _coordinator.HistoryScopeCount,
            HistoryLimitEvictions = _coordinator.HistoryLimitEvictions,
            PendingCommands = _coordinator.PendingCommandCount,
            PendingCommandBytes = _coordinator.PendingCommandBytes,
            PendingCommandsHighWater = _coordinator.PendingCommandsHighWater,
            PendingCommandBytesHighWater = _coordinator.PendingCommandBytesHighWater,
        };

        /// <param name="scopeProvider">
        /// Opt-in, off by default (NCORE-15). See <see cref="INetworkScopeProvider{TWorld}"/> for the
        /// exact contract. When supplied, this server reassigns each established peer's scope every
        /// tick via <see cref="INetworkScopeProvider{TWorld}.TryUpdateScope"/> and forces a keyframe
        /// on any change, and <see cref="NetworkReplicator{TWorld}.Capture"/> collects each scope's
        /// entities from the provider's index instead of the whole world. Leaving this null keeps
        /// every peer on its admission-time scope forever, matching today's behavior exactly.
        /// </param>
        /// <param name="updatePolicy">
        /// Opt-in, off by default (NCORE-16). See <see cref="INetworkUpdatePolicy{TWorld}"/> for the
        /// exact contract. When supplied, every scope's capture consults it per entity and reuses
        /// previously captured record bytes for a "hold" answer instead of paying for a fresh write.
        /// Leaving this null keeps capture behavior and cost byte-for-byte identical to before this
        /// parameter existed.
        /// </param>
        /// <param name="settings">Behaviour settings; null uses the defaults of <see cref="NetworkServerSettings"/>.</param>
        /// <param name="monotonicTimestamp">Diagnostic clock in Stopwatch ticks; null uses Stopwatch.GetTimestamp.</param>
        /// <param name="maxSnapshotsInFlight">
        /// Unacknowledged snapshot ticks one peer may hold before
        /// <see cref="NetworkSnapshotAdmissionMode.Windowed"/> defers the next one; ignored by the
        /// other modes. At 20 Hz each unit covers 50 ms of round trip without losing cadence.
        /// </param>
        public NetworkServer(NetworkSchema<TWorld> schema, NetworkScopeSelector<TWorld> scopeSelector, int historyTicks = 64, long historyBytes = 32 * 1024 * 1024, INetworkObserver observer = null, INetworkPeerObserver peerObserver = null, INetworkPeerAdmissionPolicy admissionPolicy = null, ulong simulationFingerprint = 0, ulong contentFingerprint = 0, NetworkBufferPool bufferPool = null, INetworkScopeProvider<TWorld> scopeProvider = null, INetworkUpdatePolicy<TWorld> updatePolicy = null, NetworkSnapshotAdmissionMode snapshotAdmissionMode = NetworkSnapshotAdmissionMode.Current, Func<long> monotonicTimestamp = null, int maxSnapshotsInFlight = NetworkSnapshotAdmissionLimits.DefaultMaxSnapshotsInFlight, INetworkScopeCaptureBudgetPolicy<TWorld> captureBudgetPolicy = null, NetworkServerSettings settings = null)
        {
            settings ??= new NetworkServerSettings();
            settings.Validate();
            _recoveryRetryTicks = (uint)settings.RecoveryRetryTicks;
            _unreliableDeltaChunks = settings.UnreliableDeltaChunks;
            _unreliableDeltaLostTicks = (uint)settings.UnreliableDeltaLostTicks;
            // Stop-and-wait on the exact tick cannot outlive a delta that is never resent.
            if (_unreliableDeltaChunks > 0 &&
                snapshotAdmissionMode == NetworkSnapshotAdmissionMode.ApplicationAck)
                throw new ArgumentException(
                    "Unreliable deltas cannot be combined with application-ack admission.",
                    nameof(settings));
            _ownerStateIntervalTicks = (uint)settings.OwnerStateIntervalTicks;
            _monotonicTimestamp = monotonicTimestamp ?? Stopwatch.GetTimestamp;
            _schema = schema ?? throw new ArgumentNullException(nameof(schema));
            if (scopeSelector == null) throw new ArgumentNullException(nameof(scopeSelector));
            if (snapshotAdmissionMode != NetworkSnapshotAdmissionMode.Current &&
                snapshotAdmissionMode != NetworkSnapshotAdmissionMode.ApplicationAck &&
                snapshotAdmissionMode != NetworkSnapshotAdmissionMode.Windowed)
                throw new ArgumentOutOfRangeException(nameof(snapshotAdmissionMode));
            if (maxSnapshotsInFlight < 1 || maxSnapshotsInFlight > MaxSnapshotsInFlightLimit)
                throw new ArgumentOutOfRangeException(nameof(maxSnapshotsInFlight));
            _maxSnapshotsInFlight = maxSnapshotsInFlight;
            _bufferPool = bufferPool ??
                new NetworkBufferPool(NetworkBufferPool.DefaultServerRetainedBytes);
            _ownsBufferPool = bufferPool == null;
            _coordinator = new NetworkServerCoordinator<TWorld>(historyTicks, historyBytes,
                settings);
            _replicator = new NetworkReplicator<TWorld>(schema, scopeSelector,
                bufferPool: _bufferPool, scopeProvider: scopeProvider, updatePolicy: updatePolicy,
                captureBudgetPolicy: captureBudgetPolicy,
                cellCacheStaleTicks: settings.CellCacheStaleTicks);
            _captureBudgetEnabled = captureBudgetPolicy != null;
            _observer = observer;
            _peerObserver = peerObserver;
            _admissionPolicy = admissionPolicy;
            _scopeProvider = scopeProvider;
            _simulationFingerprint = simulationFingerprint;
            _contentFingerprint = contentFingerprint;
            _compatibilityTag = PacketHeader.CompatibilityTagOf(_schema.Fingerprint,
                simulationFingerprint, contentFingerprint);
            _snapshotAdmissionMode = snapshotAdmissionMode;
        }

        public void Dispose()
        {
            if (_disposed)
                return;
            _disposed = true;
            ClearSnapshotDeltas();
            ClearChunkPayloads();
            while (_peers.Count > 0)
                CleanupPeer(_peers[_peers.Count - 1]);
            _peers.Clear();
            _coordinator.Clear();
            _replicator.Dispose();
            if (_ownsBufferPool)
                _bufferPool.Dispose();
        }

        /// <summary>Enables bounded per-peer delivery-stage diagnostics before load admission.</summary>
        public void EnableDeliveryPhaseDiagnostics()
        {
            if (_deliveryPhaseDiagnostics != null)
                return;
            _deliveryPhaseDiagnostics = new NetworkDeliveryPhaseRecorder();
            for (var index = 0; index < _peers.Count; index++)
                ConfigureDeliveryPhasePeer(_peers[index]);
        }

        /// <summary>Captures the opt-in immutable delivery-stage diagnostics.</summary>
        public NetworkDeliveryPhaseDiagnostics CaptureDeliveryPhaseDiagnostics() =>
            _deliveryPhaseDiagnostics?.Capture() ??
            NetworkDeliveryPhaseDiagnostics.Unavailable;

        private void ConfigureDeliveryPhasePeer(Peer peer)
        {
            if (_deliveryPhaseDiagnostics == null ||
                peer.DeliveryPhaseIndex >= 0 ||
                peer.Session.State != NetworkSessionState.Established)
                return;
            peer.DeliveryPhaseIndex = _deliveryPhaseDiagnostics.RegisterPeer(
                peer.PeerId, peer.Session.Epoch);
            if (peer.DeliveryPhaseIndex >= 0 &&
                peer.Transport is INetworkDeliveryPhaseTransport transport)
                transport.ConfigureDeliveryPhaseDiagnostics(
                    _deliveryPhaseDiagnostics, peer.DeliveryPhaseIndex);
        }

        /// <summary>Adds one transport-owned connection with server-assigned identity and scope.</summary>
        public NetworkSession<TWorld> AddConnection(INetworkTransport transport, uint peerId, uint epoch, ScopeId scope, INetworkObserver observer = null)
        {
            if (transport == null) throw new ArgumentNullException(nameof(transport));
            if (peerId == 0) throw new ArgumentOutOfRangeException(nameof(peerId), "Peer identity zero is reserved.");
            for (var i = 0; i < _peers.Count; i++)
            {
                if (_peers[i].Transport.Connection == transport.Connection)
                    throw new InvalidOperationException("Connection already exists.");
                if (_peers[i].PeerId == peerId)
                    throw new InvalidOperationException("Peer identity already exists.");
            }
            var session = new NetworkSession<TWorld>(transport.Connection,
                NetworkRole.Server, _schema, _bufferPool, observer ?? _observer);
            var peer = new Peer(transport, session, peerId, epoch, scope);
            _peers.Add(peer);
            peer.ConnectionCounted = true;
            _activeConnectionCount++;
            session.ReportSession(ServerTick, 0, 0, peer.PacketSequence);
            return session;
        }

        /// <summary>Closes and removes one connection while preserving scope-shared history.</summary>
        public bool RemoveConnection(ConnectionId connection)
        {
            for (var i = 0; i < _peers.Count; i++)
            {
                if (_peers[i].Transport.Connection != connection) continue;
                CleanupPeer(_peers[i]);
                return true;
            }
            return false;
        }

        /// <summary>Dequeues and decodes all currently available transport packets without advancing simulation time.</summary>
        public void Receive()
        {
            for (var i = 0; i < _peers.Count; i++)
            {
                var peer = _peers[i];
                while (true)
                {
                    var receiveStarted = Stopwatch.GetTimestamp();
                    NetworkBufferLease packet;
#if UNITY_EDITOR || GAME_DEBUG
                    using (NetworkDiagnosticMarkers.Measure(
                               NetworkDiagnosticPhase.ReceiveDequeue))
                    {
                        if (!peer.Transport.TryReceive(out packet))
                            break;
                    }
#else
                    if (!peer.Transport.TryReceive(out packet))
                        break;
#endif
                    var remove = false;
                    try
                    {
                        peer.Session.Trace(NetworkPhase.Receive, NetworkTraceKind.Point, NetworkResultCategory.Success, NetworkPacketKind.None, ServerTick, 0, packet.Length, 0, 0, unchecked((int)(ServerTick - peer.AcknowledgedSnapshotTick)), ElapsedNanoseconds(receiveStarted), activeConnections: ActiveConnectionCount, activePeers: ActivePeerCount);
                        remove = DecodePacket(peer, packet);
                    }
                    finally
                    {
                        packet.Dispose();
                    }
                    if (!remove)
                        continue;
                    var removedIndex = _peers.IndexOf(peer);
                    if (removedIndex >= 0)
                        _peers.RemoveAt(removedIndex);
                    i = removedIndex >= 0 ? removedIndex - 1 : -1;
                    break;
                }
            }
        }

        /// <summary>Advances exactly one authoritative tick around the supplied gameplay boundary.</summary>
        public void Tick(Action<uint> gameplay)
        {
            if (gameplay == null) throw new ArgumentNullException(nameof(gameplay));
            var serverTick = BeginTick();
            gameplay(serverTick);
            CompleteTick();
        }

        /// <summary>Dispatches due commands and begins one authoritative ECS tick.</summary>
        public uint BeginTick()
        {
            if (_activeTick != 0)
                throw new InvalidOperationException("A server tick is already active.");
            var stageStarted = Stopwatch.GetTimestamp();
            var serverTick = checked(ServerTick + 1);
            var dispatchStarted = Stopwatch.GetTimestamp();
            var dispatched = _coordinator.Dispatch(serverTick);
            TraceDispatch(serverTick, dispatched, ElapsedNanoseconds(dispatchStarted));
            for (var i = 0; i < _peers.Count; i++)
            {
                var peer = _peers[i];
                DispatchTransactions(peer);
                if (_coordinator.TryGetProcessedCommand(peer.Transport.Connection, out var cursor))
                {
                    peer.ServerProcessedCommandTick = cursor.Tick;
                    peer.ServerProcessedCommandSequence = cursor.Sequence;
                }
            }
            _activeTick = serverTick;
            _tickDispatchTicks = Stopwatch.GetTimestamp() - stageStarted;
            return serverTick;
        }

        /// <summary>Captures and sends authoritative state after gameplay systems complete.</summary>
        /// <param name="offerSnapshots">
        /// False completes the tick (transactions, receipts, scope refresh, tick advance) without
        /// capturing or offering snapshots. A runner catching up several ticks in one frame uses it
        /// for every tick but the last, so peers receive one snapshot of the newest state instead
        /// of a burst of intermediate ones that would only queue behind each other.
        /// </param>
        public void CompleteTick(bool offerSnapshots = true)
        {
            if (_activeTick == 0)
                throw new InvalidOperationException("No server tick is active.");
            var serverTick = _activeTick;
            long transactionTicks = 0;
            long scopeTicks = 0;
            long peerTicks = 0;
            long cleanupTicks = 0;
            try
            {
                var stageStarted = Stopwatch.GetTimestamp();
                for (var i = 0; i < _peers.Count; i++)
                {
                    CompleteTransactions(_peers[i], serverTick);
                    FlushTransactionReceipts(_peers[i]);
                }
                _captures.Clear();
                transactionTicks = Stopwatch.GetTimestamp() - stageStarted;
                // NCORE-15: rebuild the provider's spatial index once per tick, after gameplay has
                // moved every entity, then let it reassign each established peer's scope with
                // whatever hysteresis it implements. A reassignment always forces a keyframe: the
                // peer's existing baseline history belongs to its old scope and a delta against it
                // would be meaningless (or outright rejected) for the new one.
                if (_scopeProvider != null)
                {
                    stageStarted = Stopwatch.GetTimestamp();
                    var scopeRefreshStarted = Stopwatch.GetTimestamp();
                    try { _scopeProvider.RefreshTick(serverTick); }
                    finally
                    {
                        _scopeRefreshStopwatchTicks += Stopwatch.GetTimestamp() -
                            scopeRefreshStarted;
                    }
                    _departedScopes.Clear();
                    var scopeAssignmentStarted = Stopwatch.GetTimestamp();
                    for (var i = 0; i < _peers.Count; i++)
                    {
                        var peer = _peers[i];
                        if (peer.Session.State != NetworkSessionState.Established) continue;
                        var scope = peer.Scope;
                        if (_scopeProvider.TryUpdateScope(peer.PeerId, ref scope) &&
                            scope != peer.Scope)
                        {
                            _departedScopes.Add(peer.Scope);
                            peer.SetScope(scope);
                            peer.ScopeAssignedTick = serverTick;
                            _scopeChanges++;
                            if (_snapshotAdmissionMode ==
                                NetworkSnapshotAdmissionMode.ApplicationAck)
                                peer.ResetSnapshotAdmissionState();
                            peer.BeginSnapshotRecovery();
                        }
                    }
                    _scopeAssignmentStopwatchTicks += Stopwatch.GetTimestamp() -
                        scopeAssignmentStarted;
                    var scopeCleanupStarted = Stopwatch.GetTimestamp();
                    for (var i = 0; i < _departedScopes.Count; i++)
                        if (_coordinator.ForgetInactiveScope(_departedScopes[i]))
                            _replicator.ForgetScope(_departedScopes[i]);
                    _scopeCleanupStopwatchTicks += Stopwatch.GetTimestamp() -
                        scopeCleanupStarted;
                    scopeTicks = Stopwatch.GetTimestamp() - stageStarted;
                }
                if (offerSnapshots && _captureBudgetEnabled)
                {
                    _peerScopes.Clear();
                    for (var i = 0; i < _peers.Count; i++)
                        if (_peers[i].Session.State == NetworkSessionState.Established)
                            _peerScopes[_peers[i].PeerId] = _peers[i].Scope;
                    _replicator.BeginCaptureBudgetTick(serverTick, _peerScopes);
                }
                stageStarted = Stopwatch.GetTimestamp();
                if (!offerSnapshots)
                    IncrementSaturating(ref _catchUpTicksWithoutOffer);
                for (var i = 0; offerSnapshots && i < _peers.Count; i++)
                {
                    var peer = _peers[i];
                    if (peer.Session.State != NetworkSessionState.Established) continue;
                    _deliveryPhaseDiagnostics?.RecordSnapshotBatchOfferAttempt(
                        peer.DeliveryPhaseIndex);
                    // A stalled reliable channel must not let snapshots overtake
                    // terminal transaction receipts. Completed transactions stay
                    // counted until their receipt is actually accepted by transport.
                    if (peer.HasPendingReceiptWork)
                    {
                        IncrementSaturating(ref _pendingReceiptDeferrals);
                        _deliveryPhaseDiagnostics?.RecordPendingReceiptDeferral(
                            peer.DeliveryPhaseIndex);
                        continue;
                    }
                    // Snapshots are state, so only one snapshot batch should
                    // be in flight. Waiting for the native reliable backlog to drain
                    // prevents a fast ACK from immediately replacing a packet
                    // whose delivery callback and fragments are still pending.
                    // This keeps ordered delivery bounded without changing the
                    // snapshot baseline or transaction guarantees.
                    FlushOwnerState(peer, serverTick);
                    if (ShouldDeferSnapshot(peer, serverTick))
                        continue;
                    IncrementSaturating(ref _snapshotOfferAttempts);
                    using var snapshotScope = NetworkDiagnosticMarkers.Measure(NetworkDiagnosticPhase.Snapshot);
                    if (!_captures.TryGetValue(peer.Scope, out var capture))
                    {
                        var started = Stopwatch.GetTimestamp();
                        SnapshotCaptureResult captureResult;
                        using (NetworkDiagnosticMarkers.Measure(NetworkDiagnosticPhase.SnapshotCapture))
                            captureResult = _replicator.Capture(serverTick, peer.Scope, out capture);
                        _scopeCaptureCount++;
                        _scopeCaptureStopwatchTicks += Stopwatch.GetTimestamp() - started;
                        if (captureResult != SnapshotCaptureResult.Success)
                        {
                            IncrementSaturating(ref _captureFailures);
                            if ((uint)captureResult < (uint)_captureFailuresByReason.Length)
                                IncrementSaturating(ref _captureFailuresByReason[(int)captureResult]);
                            peer.Session.Trace(NetworkPhase.SnapshotCapture, NetworkTraceKind.Point, NetworkResultCategory.World, NetworkPacketKind.SnapshotChunk, serverTick, 0, 0, 0, 0, 0, ElapsedNanoseconds(started));
                            continue;
                        }
                        _captures.Add(peer.Scope, capture);
                        _scopeCaptureEntities.Add(capture.EntityCount);
                        _scopeCaptureBytes.Add(capture.ByteLength);
                        var historyStoreStarted = Stopwatch.GetTimestamp();
                        try { _coordinator.StoreCapture(peer.Scope, capture); }
                        finally
                        {
                            _historyStoreStopwatchTicks += Stopwatch.GetTimestamp() -
                                historyStoreStarted;
                        }
                        peer.Session.Trace(NetworkPhase.SnapshotCapture, NetworkTraceKind.Point, NetworkResultCategory.Success, NetworkPacketKind.SnapshotChunk, serverTick, 0, capture.ByteLength, _coordinator.HistoryCount(peer.Scope), _coordinator.HistoryByteCount(peer.Scope), unchecked((int)(serverTick - peer.AcknowledgedSnapshotTick)), ElapsedNanoseconds(started), capture.EntityCount, capture.RecordCount, activeConnections: ActiveConnectionCount, activePeers: ActivePeerCount);
                    }
                    using (NetworkDiagnosticMarkers.Measure(NetworkDiagnosticPhase.SnapshotDiagnostics))
                        peer.Session.ReportSnapshot(capture, _coordinator.History(peer.Scope));
                    SendSnapshot(peer, capture);
                }
                peerTicks = Stopwatch.GetTimestamp() - stageStarted;
                if (offerSnapshots && _replicationCensus != null &&
                    _replicationCensus.IsCensusTick(serverTick))
                    RunReplicationCensus(serverTick);
                ServerTick = serverTick;
            }
            finally
            {
                var cleanupStarted = Stopwatch.GetTimestamp();
                try
                {
                    ClearSnapshotDeltas();
                }
                finally
                {
                    ClearChunkPayloads();
                    _activeTick = 0;
                    cleanupTicks = Stopwatch.GetTimestamp() - cleanupStarted;
                    _lastTickStages = new NetworkServerTickStageDiagnostics(
                        _tickDispatchTicks, transactionTicks, scopeTicks,
                        peerTicks, cleanupTicks);
                }
            }
        }

        private void RunReplicationCensus(uint serverTick)
        {
            var started = Stopwatch.GetTimestamp();
            var failed = false;
            try
            {
                _replicationCensus.BeginTick();
                foreach (var pair in _captures)
                {
                    _coordinator.TryGetCapture(pair.Key, serverTick - 1, out var previous);
                    if (!_replicationCensus.ObserveScope(pair.Value, previous))
                        failed = true;
                }
                var peersWithoutCapture = 0;
                for (var i = 0; i < _peers.Count; i++)
                    if (_peers[i].Session.State == NetworkSessionState.Established &&
                        !_captures.ContainsKey(_peers[i].Scope))
                        peersWithoutCapture++;
                _replicationCensus.RecordPeersWithoutCapture(peersWithoutCapture);
            }
            catch
            {
                // Diagnostics must never fail the authority tick.
                failed = true;
            }
            _replicationCensus.CompleteTick(Stopwatch.GetTimestamp() - started, failed);
        }

        /// <summary>Copies one connection state without allocating a collection snapshot.</summary>
        public bool TryGetConnection(int index, out NetworkConnectionSnapshot snapshot)
        {
            if ((uint)index >= (uint)_peers.Count)
            {
                snapshot = default;
                return false;
            }
            var peer = _peers[index];
            snapshot = new NetworkConnectionSnapshot
            {
                Connection = new NetworkConnectionComponent
                {
                    Connection = peer.Transport.Connection,
                    Role = NetworkRole.Server,
                    State = peer.Session.State,
                    PeerId = peer.PeerId,
                    Epoch = peer.Epoch,
                    Scope = peer.Scope,
                },
                Ticks = new NetworkConnectionTickComponent
                {
                    ServerTick = ServerTick,
                    EstimatedServerTick = ServerTick,
                    AcknowledgedSnapshotTick = peer.AcknowledgedSnapshotTick,
                    ServerProcessedCommandTick = peer.ServerProcessedCommandTick,
                    ServerProcessedCommandSequence =
                        peer.ServerProcessedCommandSequence,
                },
            };
            return true;
        }

        /// <summary>Finds one immutable authoritative capture by scope and tick.</summary>
        public bool TryGetCapture(ScopeId scope, uint serverTick, out NetworkSnapshot snapshot)
            => _coordinator.TryGetCapture(scope, serverTick, out snapshot);

        public int PendingTransactionCount
        {
            get
            {
                var count = 0;
                for (var i = 0; i < _peers.Count; i++)
                    count += _peers[i].PendingTransactionCount;
                return count;
            }
        }

        /// <summary>Completes one pending transaction for the matching peer.</summary>
        public bool CompleteTransaction(NetworkTransactionId transactionId,
            NetworkTransactionStatus status = NetworkTransactionStatus.Applied)
        {
            if (status != NetworkTransactionStatus.Applied &&
                status != NetworkTransactionStatus.GameplayRejected)
                return false;
            NetworkServerTransaction match = null;
            for (var i = 0; i < _peers.Count; i++)
            {
                var peer = _peers[i];
                if (!peer.Transactions.TryGetValue(transactionId, out var transaction) ||
                    transaction.ReceiptSent || transaction.CompletionStatus.HasValue)
                    continue;
                if (match != null)
                    return false;
                match = transaction;
            }
            if (match == null)
                return false;
            match.CompletionStatus = status;
            return true;
        }

        /// <summary>Completes one transaction using its full connection-epoch key.</summary>
        public bool CompleteTransaction(uint peerId, uint epoch,
            NetworkTransactionId transactionId,
            NetworkTransactionStatus status = NetworkTransactionStatus.Applied)
        {
            if (status != NetworkTransactionStatus.Applied &&
                status != NetworkTransactionStatus.GameplayRejected)
                return false;
            for (var i = 0; i < _peers.Count; i++)
            {
                var peer = _peers[i];
                if (peer.PeerId != peerId || peer.Epoch != epoch ||
                    !peer.Transactions.TryGetValue(transactionId, out var transaction) ||
                    transaction.ReceiptSent || transaction.CompletionStatus.HasValue)
                    continue;
                transaction.CompletionStatus = status;
                return true;
            }
            return false;
        }

        /// <summary>Completes one pending transaction from its ECS request payload.</summary>
        public bool CompleteTransaction(in CompleteNetworkTransactionRequest request) =>
            CompleteTransaction(request.PeerId, request.Epoch,
                request.TransactionId, request.Status);

        /// <summary>Compatibility alias for ECS-facing transaction completion code.</summary>
        public bool CompleteNetworkTransaction(NetworkTransactionId transactionId,
            NetworkTransactionStatus status = NetworkTransactionStatus.Applied) =>
            CompleteTransaction(transactionId, status);

        private bool DecodePacket(Peer peer, NetworkBufferLease packet)
        {
            var started = Stopwatch.GetTimestamp();
            PacketHeader header;
            ReadOnlyMemory<byte> payload;
            bool decoded;
#if UNITY_EDITOR || GAME_DEBUG
            using (NetworkDiagnosticMarkers.Measure(
                       NetworkDiagnosticPhase.DecodeHeader))
            {
                decoded = NetworkPacket.TryDecode(packet, out header,
                    out payload);
            }
#else
            decoded = NetworkPacket.TryDecode(packet, out header, out payload);
#endif
            if (!decoded)
            {
                peer.Session.Trace(NetworkPhase.Decode, NetworkTraceKind.Point,
                    NetworkResultCategory.Protocol, NetworkPacketKind.None,
                    ServerTick, 0, packet.Length, 0, 0,
                    unchecked((int)(ServerTick - peer.AcknowledgedSnapshotTick)),
                    ElapsedNanoseconds(started), activeConnections: ActiveConnectionCount,
                    activePeers: ActivePeerCount);
                DisconnectPeer(peer);
                return true;
            }
            PacketValidationResult packetValidation;
            bool duplicateCommandPacket;
            bool duplicateTransactionPacket;
            bool validationRejected;
#if UNITY_EDITOR || GAME_DEBUG
            using (NetworkDiagnosticMarkers.Measure(
                       NetworkDiagnosticPhase.DecodeValidate))
            {
                packetValidation = peer.Session.ValidatePacket(in header);
                duplicateCommandPacket =
                    header.Kind == PacketKind.CommandBatch &&
                    packetValidation == PacketValidationResult.Duplicate;
                duplicateTransactionPacket =
                    header.Kind == PacketKind.TransactionCommand &&
                    packetValidation == PacketValidationResult.Duplicate;
                validationRejected = (header.Kind != PacketKind.Hello &&
                    header.CompatibilityTag != _compatibilityTag) ||
                    packetValidation != PacketValidationResult.Success &&
                    !duplicateCommandPacket && !duplicateTransactionPacket;
            }
#else
            packetValidation = peer.Session.ValidatePacket(in header);
            duplicateCommandPacket =
                header.Kind == PacketKind.CommandBatch &&
                packetValidation == PacketValidationResult.Duplicate;
            duplicateTransactionPacket =
                header.Kind == PacketKind.TransactionCommand &&
                packetValidation == PacketValidationResult.Duplicate;
            validationRejected = (header.Kind != PacketKind.Hello &&
                header.CompatibilityTag != _compatibilityTag) ||
                packetValidation != PacketValidationResult.Success &&
                !duplicateCommandPacket && !duplicateTransactionPacket;
#endif
            // Hello states the full fingerprints in its payload so admission can name the mismatch;
            // every later packet only carries their tag.
            var hello = default(HandshakeFingerprints);
            if (!validationRejected && header.Kind == PacketKind.Hello &&
                !HandshakeFingerprints.TryRead(payload.Span, out hello))
                validationRejected = true;
            if (validationRejected)
            {
                peer.Session.Trace(NetworkPhase.Decode, NetworkTraceKind.Point,
                    NetworkResultCategory.Protocol, DiagnosticKind(header.Kind),
                    ServerTick, 0, packet.Length, 0, 0,
                    unchecked((int)(ServerTick - peer.AcknowledgedSnapshotTick)),
                    ElapsedNanoseconds(started), activeConnections: ActiveConnectionCount,
                    activePeers: ActivePeerCount,
                    packetValidationResult: packetValidation);
                DisconnectPeer(peer);
                return true;
            }
            if (header.Kind == PacketKind.Hello && !Admit(peer,
                    hello.Schema, hello.Simulation, hello.Content))
                return true;
            NetworkCommandResult? commandResult = null;
            var decodeResult = NetworkResultCategory.Success;
            var resyncCorrelationId = 0u;
            var disconnect = false;
            if (header.Kind == PacketKind.CommandBatch)
            {
                commandResult = duplicateCommandPacket
                    ? NetworkCommandResult.Duplicate
                    : DecodeCommands(peer, packet, payload,
                        checked(ServerTick + 1));
                if (commandResult == NetworkCommandResult.LimitExceeded)
                    decodeResult = NetworkResultCategory.Limits;
                else if (commandResult != NetworkCommandResult.Queued &&
                         commandResult != NetworkCommandResult.Duplicate &&
                         commandResult != NetworkCommandResult.TickWindow)
                {
                    decodeResult = NetworkResultCategory.Protocol;
                    disconnect = true;
                }
            }
            else if (header.Kind == PacketKind.TransactionCommand)
            {
                commandResult = DecodeTransaction(peer, packet, payload,
                    checked(ServerTick + 1), duplicateTransactionPacket);
                if (duplicateTransactionPacket &&
                    commandResult != NetworkCommandResult.Duplicate)
                    disconnect = true;
                if (commandResult == NetworkCommandResult.PolicyRejected)
                    decodeResult = NetworkResultCategory.Policy;
                else if (commandResult == NetworkCommandResult.LimitExceeded)
                    decodeResult = NetworkResultCategory.Limits;
                else if (commandResult != NetworkCommandResult.Queued &&
                         commandResult != NetworkCommandResult.Duplicate)
                {
                    decodeResult = NetworkResultCategory.Protocol;
                    disconnect = true;
                }
            }
            else if (header.Kind == PacketKind.Ping)
                Send(peer, PacketKind.Pong, ServerTick, PacketHeader.NoneTick,
                    payload.Span);
            else if (header.Kind == PacketKind.Ack)
            {
                if (header.Flags == PacketFlags.UnreliableSequenced)
                    IncrementSaturating(ref _unreliableAcknowledgements);
                DecodeAcknowledgement(peer, header.AcknowledgedSnapshotTick);
            }
            else if (header.Kind == PacketKind.ResyncRequest)
            {
                if (!ResyncRequestPayload.TryRead(payload.Span, out var request))
                {
                    peer.Session.Trace(NetworkPhase.Decode,
                        NetworkTraceKind.Point, NetworkResultCategory.Malformed,
                        NetworkPacketKind.ResyncRequest, ServerTick, 0,
                        packet.Length, 0, 0,
                        unchecked((int)(ServerTick -
                                         peer.AcknowledgedSnapshotTick)),
                        ElapsedNanoseconds(started),
                        activeConnections: ActiveConnectionCount,
                        activePeers: ActivePeerCount,
                        packetValidationResult: packetValidation,
                        sequence: header.PacketSequence,
                        acknowledgedSnapshotTick: peer.AcknowledgedSnapshotTick,
                        oldestHistoryTick: _coordinator.OldestHistoryTick(peer.Scope),
                        newestHistoryTick: _coordinator.NewestHistoryTick(peer.Scope));
                    DisconnectPeer(peer);
                    return true;
                }
                resyncCorrelationId = request.CorrelationId;
                IncrementSaturating(ref _resyncRequests);
                // A client that rebuilt its replica world lost its owner state with it.
                peer.HasOwnerState = false;
                peer.ClearInFlightSnapshots();
                peer.BeginSnapshotRecovery();
                if (_snapshotAdmissionMode ==
                    NetworkSnapshotAdmissionMode.ApplicationAck)
                {
                    // A new explicit request supersedes a recovery batch the
                    // client could not apply. Keep its tick inert so a delayed
                    // ACK cannot release the replacement batch.
                    if (peer.RecoveryOutstandingTick != 0)
                    {
                        peer.SupersededSnapshotTick =
                            peer.RecoveryOutstandingTick;
                        peer.RecoveryOutstandingTick = 0;
                    }
                    peer.ResyncCorrelationId = request.CorrelationId;
                    peer.ResyncSnapshotTick = 0;
                }
                else if (peer.ResyncCorrelationId == 0)
                {
                    // A forced keyframe already in flight carries no correlation, so its ack must
                    // not end this recovery: the client waits for a keyframe answering the request.
                    peer.ResyncCorrelationId = request.CorrelationId;
                    peer.ResyncSnapshotTick = 0;
                }
            }
            else if (header.Kind == PacketKind.Disconnect)
                CleanupPeer(peer);
            peer.Session.Trace(NetworkPhase.Decode, NetworkTraceKind.Point,
                decodeResult, DiagnosticKind(header.Kind),
                ServerTick, 0, packet.Length, 0, 0,
                unchecked((int)(ServerTick - peer.AcknowledgedSnapshotTick)),
                ElapsedNanoseconds(started), activeConnections: ActiveConnectionCount,
                activePeers: ActivePeerCount,
                resyncCorrelationId: resyncCorrelationId,
                commandResult: commandResult,
                packetValidationResult: packetValidation,
                sequence: header.PacketSequence,
                acknowledgedSnapshotTick: peer.AcknowledgedSnapshotTick,
                oldestHistoryTick: _coordinator.OldestHistoryTick(peer.Scope),
                newestHistoryTick: _coordinator.NewestHistoryTick(peer.Scope));
            peer.Session.ReportSession(ServerTick, peer.AcknowledgedSnapshotTick,
                peer.ServerProcessedCommandSequence, peer.PacketSequence);
            if (disconnect)
            {
                DisconnectPeer(peer);
                return true;
            }
            if (header.Kind == PacketKind.Disconnect)
                return true;
            return false;
        }

        private bool Admit(Peer peer, SchemaFingerprint remoteFingerprint,
            ulong simulationFingerprint, ulong contentFingerprint)
        {
            if (remoteFingerprint != _schema.Fingerprint ||
                simulationFingerprint != _simulationFingerprint ||
                contentFingerprint != _contentFingerprint)
            {
                Send(peer, PacketKind.Disconnect, 0, PacketHeader.NoneTick, ReadOnlySpan<byte>.Empty);
                CloseSession(peer);
                return false;
            }

            var data = peer.Data();
            var policyInvoked = false;
            try
            {
                if (_admissionPolicy != null)
                {
                    policyInvoked = true;
                    if (!_admissionPolicy.TryAdmit(in data, out var rejection))
                    {
                        if (rejection == NetworkAdmissionRejection.None)
                            rejection = NetworkAdmissionRejection.Rejected;
                        TraceAdmissionFailure(peer, rejection);
                        TryRollbackAdmission(in data);
                        Send(peer, PacketKind.Disconnect, 0, PacketHeader.NoneTick, ReadOnlySpan<byte>.Empty);
                        CloseSession(peer);
                        return false;
                    }
                }

                var admission = peer.Session.Admit(remoteFingerprint,
                    peer.PeerId, peer.Epoch, peer.Scope);
                if (admission != NetworkAdmissionResult.Accepted)
                {
                    if (peer.Session.State == NetworkSessionState.Rejected)
                        CloseSession(peer);
                    throw new InvalidOperationException("Session rejected a validated peer admission.");
                }
                peer.PeerCounted = true;
                _activePeerCount++;
                ConfigureDeliveryPhasePeer(peer);

                _coordinator.Add(peer.Session);
                Span<byte> payload = stackalloc byte[12];
                Hashing.Write32(payload, 0, peer.PeerId);
                Hashing.Write64(payload, 4, peer.Scope.Value);
                if (!Send(peer, PacketKind.Ready, 0, PacketHeader.NoneTick, payload))
                    throw new InvalidOperationException("Ready packet could not be sent.");
            }
            catch
            {
                TraceAdmissionFailure(peer, NetworkAdmissionRejection.PolicyError);
                _coordinator.Remove(peer.Transport.Connection);
                CloseSession(peer);
                if (policyInvoked)
                    TryRollbackAdmission(in data);
                Send(peer, PacketKind.Disconnect, 0, PacketHeader.NoneTick, ReadOnlySpan<byte>.Empty);
                return false;
            }

            peer.AdmissionNotified = true;
            try
            {
                NotifyAdmitted(peer);
                return true;
            }
            catch
            {
                Send(peer, PacketKind.Disconnect, 0, PacketHeader.NoneTick, ReadOnlySpan<byte>.Empty);
                CleanupPeer(peer);
                return false;
            }
        }

        private void TryRollbackAdmission(in NetworkPeerData peer)
        {
            try
            {
                _admissionPolicy?.Rollback(in peer);
            }
            catch
            {
                // Admission is already rejected; rollback remains best effort and idempotent.
            }
        }

        private NetworkCommandResult DecodeCommands(Peer peer,
            NetworkBufferLease packet,
            ReadOnlyMemory<byte> payload, uint serverTick)
        {
            using var commandScope = NetworkDiagnosticMarkers.Measure(NetworkDiagnosticPhase.Command);
            if (payload.Length < 1)
                return NetworkCommandResult.Malformed;

            var bytes = payload.Span;
            int count = bytes[0];
            if (count < 1 || count > ProtocolLimits.MaxCommandsPerBatch)
                return NetworkCommandResult.Malformed;

            // Command redundancy resends the same sequence across more than one batch so it
            // survives a dropped packet, so most batches on a healthy link carry mostly commands
            // this session has already queued. NextCommandSequence only advances while this method
            // and the dispatch loop below run, so any sequence at or below the value read here is
            // guaranteed to still be a duplicate once Queue -> Validate would see it. Skipping the
            // pooled-buffer retain (a locked ref-count bump) and envelope slot for that guaranteed
            // outcome, while still walking its structural fields so framing errors are still
            // caught, is the only behavior change: every genuinely new command is decoded, sorted,
            // and validated exactly as before.
            var nextReceiveSequence = peer.Session.NextCommandSequence;

            int offset = 1;
            var commands = peer.DecodedCommands;
            var decoded = 0;
            for (var i = 0; i < count; i++)
            {
                if (offset > bytes.Length - 17)
                {
                    DisposeCommands(commands, decoded);
                    return NetworkCommandResult.Malformed;
                }

                uint sequence = Hashing.Read32(bytes, offset);
                uint targetTick = Hashing.Read32(bytes, offset + 4);
                uint idValue = Hashing.Read32(bytes, offset + 8);
                byte version = bytes[offset + 12];
                uint payloadLength = Hashing.Read32(bytes, offset + 13);
                offset += 17;
                if (sequence == 0 || idValue == 0 ||
                    payloadLength > ProtocolLimits.MaxCommandBytes ||
                    payloadLength > (uint)(bytes.Length - offset))
                {
                    DisposeCommands(commands, decoded);
                    return NetworkCommandResult.Malformed;
                }

                var exactLength = checked((int)payloadLength);
                if (sequence < nextReceiveSequence)
                {
                    // Already queued in an earlier tick: Queue -> Validate would report
                    // Duplicate for this exact sequence no matter where in the batch it sorts,
                    // so skip retaining its payload and only step past it.
                    offset += exactLength;
                    continue;
                }
                var exact = packet.RetainSlice(PacketHeader.Size + offset, exactLength);
                offset += exactLength;
                commands[decoded++] = new NetworkCommandEnvelope(
                    peer.Transport.Connection,
                    peer.PeerId,
                    peer.Epoch,
                    sequence,
                    targetTick,
                    new NetworkTypeId(idValue),
                    version,
                    exact);
            }

            if (offset != bytes.Length)
            {
                DisposeCommands(commands, decoded);
                return NetworkCommandResult.Malformed;
            }

            // A batch made entirely of already-processed sequences decoded above without
            // retaining or queuing anything; matches the pre-existing outcome for an
            // all-duplicate batch, which fell through the loop below untouched to this same
            // result.
            if (decoded == 0)
                return NetworkCommandResult.Queued;

            Array.Sort(commands, 0, decoded, NetworkCommandEnvelopeComparer.Instance);
            for (var i = 0; i < decoded; i++)
            {
                var result = _coordinator.Queue(commands[i], serverTick);
                if (result == NetworkCommandResult.TickWindow ||
                    result == NetworkCommandResult.Duplicate)
                {
                    commands[i].Dispose();
                    commands[i] = default;
                    continue;
                }
                if (result == NetworkCommandResult.LimitExceeded)
                {
                    for (var j = i; j < decoded; j++)
                    {
                        commands[j].Dispose();
                        commands[j] = default;
                    }
                    return result;
                }
                if (result != NetworkCommandResult.Queued)
                {
                    for (var j = i; j < decoded; j++)
                    {
                        commands[j].Dispose();
                        commands[j] = default;
                    }
                    return result;
                }
                commands[i] = default;
            }
            return NetworkCommandResult.Queued;
        }

        private static void DisposeCommands(NetworkCommandEnvelope[] commands,
            int count)
        {
            for (var i = 0; i < count; i++)
            {
                var command = commands[i];
                command.Dispose();
                commands[i] = default;
            }
        }

        private void DecodeAcknowledgement(Peer peer, uint acknowledgedTick)
        {
            if (_snapshotAdmissionMode ==
                NetworkSnapshotAdmissionMode.ApplicationAck)
            {
                DecodeApplicationAcknowledgement(peer, acknowledgedTick);
                return;
            }
            if (acknowledgedTick <= peer.AcknowledgedSnapshotTick)
                return;
            // Snapshots of the old scope were still on the wire when the scope changed. Their acks
            // say nothing about the keyframe already sent, so treating them as lost baselines would
            // free its window slot and send another whole keyframe per stale ack.
            if (acknowledgedTick < peer.ScopeAssignedTick)
            {
                IncrementSaturating(ref _staleScopeAcknowledgements);
                return;
            }
            if (acknowledgedTick > ServerTick ||
                !_coordinator.TryGetCapture(peer.Scope, acknowledgedTick,
                    out var baseline) ||
                baseline.Scope != peer.Scope ||
                baseline.SchemaFingerprint != _schema.Fingerprint)
            {
                IncrementSaturating(ref _lostBaselineAcknowledgements);
                peer.ClearInFlightSnapshots();
                peer.BeginSnapshotRecovery();
                return;
            }
            peer.AcknowledgedSnapshotTick = acknowledgedTick;
            peer.ReleaseInFlightSnapshots(acknowledgedTick);
            _deliveryPhaseDiagnostics?.RecordApplicationAck(
                peer.DeliveryPhaseIndex, acknowledgedTick, _monotonicTimestamp());
            RecordSnapshotApplicationAck(peer, acknowledgedTick, null);
            if (peer.ResyncRequested &&
                (peer.ResyncSnapshotTick == 0 ||
                 acknowledgedTick < peer.ResyncSnapshotTick))
                return;
            peer.ResyncRequested = false;
            peer.ResyncCorrelationId = 0;
            peer.ResyncSnapshotTick = 0;
        }

        private void DecodeApplicationAcknowledgement(Peer peer,
            uint acknowledgedTick)
        {
            if (acknowledgedTick <= peer.AcknowledgedSnapshotTick)
                return;

            var acknowledgesRecovery = peer.RecoveryOutstandingTick != 0 &&
                acknowledgedTick == peer.RecoveryOutstandingTick;
            var acknowledgesNormal = peer.ApplicationAckOutstandingTick != 0 &&
                acknowledgedTick == peer.ApplicationAckOutstandingTick;
            if (!acknowledgesRecovery && !acknowledgesNormal)
            {
                if (acknowledgedTick == peer.SupersededSnapshotTick)
                    return;
                RecordAcknowledgementGap(peer);
                return;
            }

            if (acknowledgedTick > ServerTick ||
                !_coordinator.TryGetCapture(peer.Scope, acknowledgedTick,
                    out var baseline) ||
                baseline.Scope != peer.Scope ||
                baseline.SchemaFingerprint != _schema.Fingerprint)
            {
                RecordAcknowledgementGap(peer);
                peer.ApplicationAckOutstandingTick = 0;
                peer.RecoveryOutstandingTick = 0;
                peer.SupersededSnapshotTick = 0;
                peer.ResyncSnapshotTick = 0;
                peer.ClearSnapshotSubmission();
                return;
            }

            peer.AcknowledgedSnapshotTick = acknowledgedTick;
            _deliveryPhaseDiagnostics?.RecordApplicationAck(
                peer.DeliveryPhaseIndex, acknowledgedTick, _monotonicTimestamp());
            RecordSnapshotApplicationAck(peer, acknowledgedTick,
                acknowledgesRecovery);
            peer.ApplicationAckOutstandingTick = 0;
            peer.SupersededSnapshotTick = 0;
            if (acknowledgesRecovery)
            {
                peer.RecoveryOutstandingTick = 0;
                peer.HasRecoveryAttempted = false;
                peer.LastRecoveryAttemptServerTick = 0;
            }

            if (peer.ResyncRequested &&
                (peer.ResyncSnapshotTick == 0 ||
                 acknowledgedTick < peer.ResyncSnapshotTick))
                return;
            peer.ResyncRequested = false;
            peer.ResyncCorrelationId = 0;
            peer.ResyncSnapshotTick = 0;
        }

        private void RecordSnapshotApplicationAck(Peer peer, uint acknowledgedTick,
            bool? recovery)
        {
            if (peer.TryConsumeSnapshotSubmission(acknowledgedTick,
                    recovery, out var elapsedStopwatchTicks))
                _applicationAck.AddStopwatchTicks(elapsedStopwatchTicks);
        }

        private bool ShouldDeferSnapshot(Peer peer, uint serverTick)
        {
            if (_snapshotAdmissionMode == NetworkSnapshotAdmissionMode.Windowed)
            {
                // Only a packet the transport could not yet submit means real backpressure. A
                // transport that reports no queue of its own falls back to its pending-reliable
                // flag, which for a callback-less transport is exactly that queue.
                var queued = peer.Transport is INetworkReliableBacklogState backlog
                    ? backlog.HasQueuedReliablePackets
                    : peer.Transport is INetworkReliableSendState sendState &&
                      sendState.HasPendingReliablePackets;
                if (queued)
                {
                    IncrementSaturating(ref _pendingReliableDeferrals);
                    _deliveryPhaseDiagnostics?.RecordPendingReliableDeferral(
                        peer.DeliveryPhaseIndex);
                    return true;
                }
                // A keyframe in flight (no acknowledged baseline yet, or recovery pending) is
                // the whole state: sending another one before its ACK only repeats it, so the
                // window shrinks to one until the peer confirms a baseline for deltas.
                var keyframePending = peer.AcknowledgedSnapshotTick == 0 || peer.ResyncRequested;
                var limit = keyframePending ? 1 : _maxSnapshotsInFlight;
                // An unreliable delta is never resent, so one the peer did not acknowledge in
                // time is lost, not in flight. While a keyframe is due no delta is what the peer
                // waits for, and its acknowledgement may never come: the peer drops without one
                // a reliable delta that a later unreliable one overtook. Left in the window any
                // of them would hold that keyframe back for good, so only a keyframe keeps the
                // slot then.
                if (_unreliableDeltaChunks > 0)
                    AddSaturating(ref _unreliableDeltaWindowExpirations,
                        peer.ExpireInFlightDeltas(serverTick, _unreliableDeltaLostTicks,
                            keyframePending));
                if (peer.InFlightSnapshotCount >= limit)
                {
                    IncrementSaturating(ref _snapshotWindowDeferrals);
                    return true;
                }
                return false;
            }
            if (_snapshotAdmissionMode == NetworkSnapshotAdmissionMode.Current)
            {
                var pendingReliable = peer.LastSnapshotSentTick != 0 &&
                    peer.Transport is INetworkReliableSendState currentState &&
                    currentState.HasPendingReliablePackets;
                if (pendingReliable)
                {
                    IncrementSaturating(ref _pendingReliableDeferrals);
                    _deliveryPhaseDiagnostics?.RecordPendingReliableDeferral(
                        peer.DeliveryPhaseIndex);
                }
                return pendingReliable;
            }

            var waitingForApplicationAck = peer.RecoveryOutstandingTick != 0 ||
                (peer.ApplicationAckOutstandingTick != 0 &&
                 !peer.ResyncRequested);
            var transportBackpressured =
                peer.Transport is INetworkReliableSendState reliableState &&
                reliableState.HasPendingReliablePackets;
            if (transportBackpressured)
            {
                IncrementSaturating(ref _pendingReliableDeferrals);
                _deliveryPhaseDiagnostics?.RecordPendingReliableDeferral(
                    peer.DeliveryPhaseIndex);
            }
            var recoveryRetryDelayed = peer.ResyncRequested &&
                peer.HasRecoveryAttempted &&
                unchecked(serverTick - peer.LastRecoveryAttemptServerTick) <
                    _recoveryRetryTicks;
            if (!waitingForApplicationAck && !transportBackpressured &&
                !recoveryRetryDelayed)
                return false;

            IncrementSaturating(ref _suppressedSnapshots);
            return true;
        }

        private void RecordAcknowledgementGap(Peer peer)
        {
            IncrementSaturating(ref _acknowledgementGaps);
            peer.BeginSnapshotRecovery();
            if (peer.RecoveryOutstandingTick == 0)
                peer.ResyncSnapshotTick = 0;
        }

        private static void IncrementSaturating(ref long value)
        {
            if (value < long.MaxValue)
                value++;
        }

        private static void AddSaturating(ref long value, long increment)
        {
            if (increment <= 0 || value == long.MaxValue)
                return;
            value = value > long.MaxValue - increment
                ? long.MaxValue
                : value + increment;
        }

        private void SendSnapshot(Peer peer, NetworkSnapshot snapshot)
        {
            var baselineTick = peer.AcknowledgedSnapshotTick;
            var keyframe = peer.ResyncRequested || baselineTick == 0;
            var applicationAckRecovery =
                _snapshotAdmissionMode ==
                    NetworkSnapshotAdmissionMode.ApplicationAck && keyframe;
            if (applicationAckRecovery)
            {
                peer.BeginSnapshotRecovery();
                peer.LastRecoveryAttemptServerTick = snapshot.ServerTick;
                peer.HasRecoveryAttempted = true;
                IncrementSaturating(ref _recoveryAttempts);
            }

            var reliableLimit = peer.Transport.MaxReliablePayloadBytes;
            if (reliableLimit <= PacketHeader.Size + SnapshotChunkHeader.Size)
            {
                IncrementSaturating(ref _preparationFailures);
                peer.BeginSnapshotRecovery();
                return;
            }
            // A transport that cannot currently accept even the smallest snapshot
            // chunk is transiently backpressured. Skip baseline lookup and delta
            // encoding without requesting recovery: no application chunk was
            // rejected, so the next tick can retry against the same baseline.
            // A delta that may travel unreliably is not held by the reliable channel; the chunk
            // send repeats the exact probe for whatever ends up reliable.
            if ((keyframe || _unreliableDeltaChunks == 0) &&
                peer.Transport is INetworkReliableSendPreflight preflight &&
                !preflight.CanAcceptReliablePacket(
                    PacketHeader.Size + SnapshotChunkHeader.Size + 1))
            {
                IncrementSaturating(ref _preflightRejections);
                return;
            }

            NetworkBufferLease delta = null;
            SnapshotDeltaAttribution deltaAttribution = default;
            NetworkSnapshot baseline = null;
            if (!keyframe)
            {
                _coordinator.TryGetCapture(peer.Scope, baselineTick,
                    out baseline);
                if (baseline == null || baseline.Scope != peer.Scope ||
                    baseline.SchemaFingerprint != _schema.Fingerprint)
                    baseline = null;
                var hasDelta = TryGetSnapshotDelta(peer.Scope, baselineTick,
                    baseline, snapshot, out delta, out deltaAttribution);
                if (!hasDelta)
                    keyframe = true;
            }
            if (keyframe)
            {
                peer.BeginSnapshotRecovery();
                if (peer.ResyncSnapshotTick == 0)
                    peer.ResyncSnapshotTick = snapshot.ServerTick;
            }

            var body = keyframe ? snapshot.Bytes.Span : delta.Span;
            var attributedRecordBytes = keyframe
                ? snapshot.AttributedCanonicalRecordBytes
                : deltaAttribution.AttributedRecordBytes;
            var attributionComplete = keyframe
                ? attributedRecordBytes >= 0 && attributedRecordBytes <= body.Length
                : deltaAttribution.IsReconciled;
            var reliableBody = Math.Min(
                reliableLimit - PacketHeader.Size - SnapshotChunkHeader.Size,
                ProtocolLimits.MaxWirePayloadBytes - SnapshotChunkHeader.Size);
            if (body.Length > ProtocolLimits.MaxDecodedPayloadBytes)
            {
                IncrementSaturating(ref _preparationFailures);
                peer.BeginSnapshotRecovery();
                return;
            }
            // Every peer of a transport gets the same chunk size, whatever its own trailer, so
            // peers on one baseline keep sharing the framed chunk payload.
            var unreliableBody = Math.Min(reliableBody,
                peer.Transport.MaxUnreliablePayloadBytes - PacketHeader.Size -
                SnapshotChunkHeader.Size);
            var unreliable = !keyframe && _unreliableDeltaChunks > 0 && unreliableBody > 0 &&
                             body.Length <= (long)unreliableBody * _unreliableDeltaChunks;
            var maxBody = unreliable ? unreliableBody : reliableBody;
            var chunkCountLong = (body.Length + (long)maxBody - 1L) / maxBody;
            if (chunkCountLong < 1 ||
                chunkCountLong > ProtocolLimits.MaxChunkMappings)
            {
                IncrementSaturating(ref _preparationFailures);
                peer.BeginSnapshotRecovery();
                return;
            }
            var chunkCount = checked((uint)chunkCountLong);
            // The current state of the peer's own entity, when the snapshot holds an older one,
            // rides at the end of the last chunk packet. One that does not fit there goes ahead of
            // the snapshot in a packet of its own; without it the peer would reconcile against the
            // older state, so the snapshot waits for a tick that can send both.
            var ownedLength = _captureBudgetEnabled
                ? WriteOwnedEntityRefresh(peer, snapshot.ServerTick)
                : 0;
            var trailerLength = 0;
            if (ownedLength > 0)
            {
                var lastBodyLength = body.Length -
                                     checked((int)((chunkCount - 1) * (long)maxBody));
                if (ownedLength <= byte.MaxValue && lastBodyLength + ownedLength <= maxBody)
                    trailerLength = ownedLength;
                else
                {
                    // The separate packet reaches the peer ahead of its snapshot only on the
                    // ordered channel, so this snapshot travels there as well.
                    if (unreliable)
                    {
                        unreliable = false;
                        maxBody = reliableBody;
                        chunkCount = checked((uint)((body.Length + (long)maxBody - 1L) / maxBody));
                        lastBodyLength = body.Length -
                                         checked((int)((chunkCount - 1) * (long)maxBody));
                    }
                    if (ownedLength <= byte.MaxValue && lastBodyLength + ownedLength <= maxBody)
                        trailerLength = ownedLength;
                    else if (!SendOwnedEntityRefreshPacket(peer, snapshot.ServerTick,
                                 ownedLength))
                        return;
                }
            }
            var acceptedChunk = false;
            long firstSubmissionStopwatchTicks = 0;
            for (uint chunkIndex = 0; chunkIndex < chunkCount; chunkIndex++)
            {
                var bodyOffset = checked((int)((long)chunkIndex *
                                               maxBody));
                var bodyLength = Math.Min(maxBody, body.Length - bodyOffset);
                var chunk = new SnapshotChunkHeader
                {
                    PayloadKind = keyframe
                        ? SnapshotPayloadKind.Keyframe
                        : SnapshotPayloadKind.Delta,
                    SnapshotTick = snapshot.ServerTick,
                    BaselineTick = keyframe ? 0 : baselineTick,
                    ScopeValue = peer.Scope.Value,
                    TotalLength = checked((uint)snapshot.ByteLength),
                    TotalHash = snapshot.PayloadHash,
                    ChunkIndex = chunkIndex,
                    ChunkCount = chunkCount,
                    ResyncCorrelationId = keyframe
                        ? peer.ResyncCorrelationId
                        : 0
                };
                var trailer = trailerLength > 0 && chunkIndex + 1 == chunkCount
                    ? new ReadOnlySpan<byte>(_ownerStatePayload, 0, trailerLength)
                    : default;
                if (!SendSnapshotChunk(peer, snapshot.ServerTick,
                        chunkIndex + 1, in chunk,
                        body.Slice(bodyOffset, bodyLength), trailer, unreliable))
                {
                    // A delta that is not resent leaves nothing behind when a chunk is
                    // refused: the next tick sends a whole one against the same baseline.
                    if (unreliable)
                        return;
                    // A rejected first chunk of a normal delta carries no
                    // recovery signal: no application chunk was accepted, so
                    // the client is not awaiting a keyframe and the next tick
                    // can retry the delta against the same baseline.
                    if (!keyframe && !acceptedChunk && chunkIndex == 0)
                        return;
                    if (acceptedChunk && _snapshotAdmissionMode ==
                        NetworkSnapshotAdmissionMode.ApplicationAck)
                        IncrementSaturating(ref _partialBatchFailures);
                    peer.BeginSnapshotRecovery();
                    return;
                }
                if (firstSubmissionStopwatchTicks == 0)
                    firstSubmissionStopwatchTicks = Stopwatch.GetTimestamp();
                acceptedChunk = true;
            }
            if (acceptedChunk)
            {
                if (trailerLength > 0)
                {
                    IncrementSaturating(ref _ownedEntityRefreshPackets);
                    IncrementSaturating(ref _ownedEntityRefreshTrailers);
                    AddSaturating(ref _ownedEntityRefreshBytes, trailerLength);
                }
                AddSaturating(ref _encodedSnapshotBytes, body.Length);
                AddSaturating(ref _snapshotFramingBytes,
                    checked((long)chunkCount *
                        (PacketHeader.Size + SnapshotChunkHeader.Size)));
                if (keyframe)
                {
                    IncrementSaturating(ref _keyframeSnapshots);
                    AddSaturating(ref _keyframeBytes, body.Length);
                    AddSaturating(ref _keyframeEntities, snapshot.EntityCount);
                }
                else
                {
                    IncrementSaturating(ref _deltaSnapshots);
                    AddSaturating(ref _deltaBytes, body.Length);
                    AddSaturating(ref _deltaTargetEntities, snapshot.EntityCount);
                    AddSaturating(ref _deltaEntityAdds, deltaAttribution.EntityAdds);
                    AddSaturating(ref _deltaEntityRemoves, deltaAttribution.EntityRemoves);
                    AddSaturating(ref _deltaEntityPatches, deltaAttribution.EntityPatches);
                    _deltaSnapshotBytes.Add(body.Length);
                    _deltaBaselineLagTicks.Add(unchecked(snapshot.ServerTick - baselineTick));
                    if (unreliable)
                        IncrementSaturating(ref _deltaSnapshotsUnreliable);
                    if (unreliable
                            ? chunkCount > 1
                            : PacketHeader.Size + SnapshotChunkHeader.Size +
                              Math.Min(maxBody, body.Length) +
                              (chunkCount == 1 ? trailerLength : 0) >
                              peer.Transport.MaxUnreliablePayloadBytes)
                        IncrementSaturating(ref _deltaSnapshotsOverDatagram);
                }
                if (attributionComplete)
                    AddSaturating(ref _passiveNpcEncodedRecordBytes,
                        attributedRecordBytes);
                else
                    IncrementSaturating(ref _passiveNpcByteAttributionFailures);
                IncrementSaturating(ref _snapshotBatchesAccepted);
                if (!unreliable)
                    _deliveryPhaseDiagnostics?.RecordSnapshotBatchAccepted(
                        peer.DeliveryPhaseIndex, snapshot.ServerTick,
                        peer.LastSnapshotSentTick, _monotonicTimestamp());
                if (peer.DeliveryObservationSnapshotTick != 0 &&
                    peer.Transport is INetworkReliableDeliveryObservation delivery)
                {
                    var observation = delivery.LastSnapshotDeliveryObservation;
                    var now = _monotonicTimestamp();
                    if (observation.SnapshotServerTick == peer.DeliveryObservationSnapshotTick &&
                        observation.ObservedStopwatchTicks > peer.ConsumedDeliveryStopwatchTicks &&
                        observation.ObservedStopwatchTicks > 0 &&
                        now >= observation.ObservedStopwatchTicks)
                    {
                        _callbackToSnapshotAdmission.AddStopwatchTicks(now - observation.ObservedStopwatchTicks);
                        peer.ConsumedDeliveryStopwatchTicks = observation.ObservedStopwatchTicks;
                    }
                }
                peer.LastSnapshotSentTick = snapshot.ServerTick;
                peer.DeliveryObservationSnapshotTick = snapshot.ServerTick;
                peer.RecordInFlightSnapshot(snapshot.ServerTick,
                    keyframe ? InFlightSnapshotKind.Keyframe
                    : unreliable ? InFlightSnapshotKind.UnreliableDelta
                    : InFlightSnapshotKind.ReliableDelta);
                peer.RecordSnapshotSubmission(snapshot.ServerTick, peer.Scope,
                    applicationAckRecovery, firstSubmissionStopwatchTicks);
                if (keyframe)
                    _keyframeBatches++;
                if (_snapshotAdmissionMode ==
                    NetworkSnapshotAdmissionMode.ApplicationAck)
                {
                    if (keyframe)
                    {
                        if (peer.ApplicationAckOutstandingTick != 0)
                            peer.SupersededSnapshotTick =
                                peer.ApplicationAckOutstandingTick;
                        peer.ApplicationAckOutstandingTick = 0;
                        peer.RecoveryOutstandingTick = snapshot.ServerTick;
                        IncrementSaturating(
                            ref _recoveryBatchesAccepted);
                    }
                    else
                    {
                        peer.ApplicationAckOutstandingTick =
                            snapshot.ServerTick;
                        peer.RecoveryOutstandingTick = 0;
                        peer.SupersededSnapshotTick = 0;
                    }
                }
            }
        }

        private bool TryGetSnapshotDelta(ScopeId scope, uint baselineTick,
            NetworkSnapshot baseline, NetworkSnapshot target,
            out NetworkBufferLease delta, out SnapshotDeltaAttribution attribution)
        {
            var key = new SnapshotDeltaKey(scope, baselineTick,
                target.ServerTick);
            if (_snapshotDeltas.TryGetValue(key, out var cached))
            {
                IncrementSaturating(ref _deltaCacheHits);
                delta = cached.Delta;
                attribution = cached.Attribution;
                return delta != null;
            }
            IncrementSaturating(ref _deltaCacheMisses);

            NetworkBufferLease lease = null;
            attribution = default;
            try
            {
                var encoded = false;
                if (baseline != null)
                {
                    var deltaEncodeStarted = Stopwatch.GetTimestamp();
                    using (NetworkDiagnosticMarkers.Measure(NetworkDiagnosticPhase.SnapshotDeltaEncode))
                    {
                        try
                        {
                            encoded = SnapshotDeltaCodec.TryEncode(_bufferPool,
                                baseline, target, out lease, _schema.DeltaHooks,
                                out attribution);
                        }
                        finally
                        {
                            _deltaEncodeStopwatchTicks +=
                                Stopwatch.GetTimestamp() - deltaEncodeStarted;
                        }
                    }
                }

                if (encoded)
                {
                    _snapshotDeltas.Add(key, new SnapshotDeltaEntry(lease, attribution));
                    delta = lease;
                    lease = null;
                    return true;
                }

                _snapshotDeltas.Add(key, default);
                delta = null;
                attribution = default;
                return false;
            }
            finally
            {
                lease?.Dispose();
            }
        }

        private void ClearSnapshotDeltas()
        {
            foreach (var delta in _snapshotDeltas.Values)
                delta.Delta?.Dispose();
            _snapshotDeltas.Clear();
        }

        // Peers acknowledging the same baseline tick for the same scope produce a
        // byte-identical chunk payload (header + body). Framing and hashing it once
        // per tick and reusing the result across those peers turns an O(peers) cost
        // into O(distinct baselines): every later peer only pays for the per-peer
        // packet header wrap and a body copy, not another chunk-header write or a
        // full-payload xxHash64 pass. The cache is keyed on every field that can
        // change the framed bytes, so peers on different baselines, chunks, or
        // resync correlations never share an entry.
        private bool TryGetChunkPayload(ScopeId scope, in SnapshotChunkHeader chunk,
            ReadOnlySpan<byte> body, out ReadOnlyMemory<byte> payload,
            out ulong payloadHash)
        {
            var key = new ChunkPayloadKey(scope, chunk);
            var cacheable = true;
            if (_chunkPayloads.TryGetValue(key, out var cached))
            {
                // The key names the chunk, not where the body was cut: a peer whose snapshot
                // was cut at another size shares the key and not the bytes.
                if (cached.Payload.Length == SnapshotChunkHeader.Size + body.Length)
                {
                    payload = cached.Payload.Memory;
                    payloadHash = cached.PayloadHash;
                    return true;
                }
                cacheable = false;
            }

            if (!SnapshotChunkEncoder.TryEncodePayload(_bufferPool, in chunk, body,
                    out var lease, out var hash))
            {
                payload = default;
                payloadHash = 0;
                return false;
            }

            if (!cacheable)
            {
                _uncachedChunkPayloads.Add(lease);
                payload = lease.Memory;
                payloadHash = hash;
                return true;
            }
            _chunkPayloads.Add(key, new ChunkPayloadEntry(lease, hash));
            payload = lease.Memory;
            payloadHash = hash;
            return true;
        }

        private void ClearChunkPayloads()
        {
            foreach (var entry in _chunkPayloads.Values)
                entry.Payload?.Dispose();
            _chunkPayloads.Clear();
            foreach (var lease in _uncachedChunkPayloads)
                lease.Dispose();
            _uncachedChunkPayloads.Clear();
        }

        private bool SendSnapshotChunk(Peer peer, uint serverTick,
            uint sequence, in SnapshotChunkHeader chunk,
            ReadOnlySpan<byte> body, ReadOnlySpan<byte> trailer = default,
            bool unreliable = false)
        {
            // Delivery phases pair a chunk with its transport callback, which an unreliable
            // send does not have.
            if (_deliveryPhaseDiagnostics != null && !unreliable)
                _deliveryPhaseDiagnostics.BeginSnapshotChunk(
                    peer.DeliveryPhaseIndex, serverTick, sequence,
                    sequence == 1, _monotonicTimestamp());
            // History lookups and the elapsed-time sample only feed the observer
            // trace event below; Trace itself is a no-op without one. Skipping
            // them when untraced avoids four scope-history dictionary lookups and
            // a Stopwatch sample on every peer send.
            var traceEnabled = peer.Session.IsTraceEnabled;
            var started = traceEnabled ? Stopwatch.GetTimestamp() : 0L;
            // A reliable transport may be transiently backpressured for this exact
            // encoded chunk even though it accepted the minimum probe. Reject it
            // before renting and encoding a packet; TrySend stays authoritative
            // after a true preflight.
            var exactPacketBytes = checked(PacketHeader.Size +
                SnapshotChunkHeader.Size + body.Length + trailer.Length);
            if (!unreliable && peer.Transport is INetworkReliableSendPreflight preflight &&
                !preflight.CanAcceptReliablePacket(exactPacketBytes))
            {
                IncrementSaturating(ref _preflightRejections);
                _deliveryPhaseDiagnostics?.ClearSnapshotIdentity(
                    peer.DeliveryPhaseIndex, serverTick, sequence);
                return false;
            }
            // Preparation work only starts after the exact preflight accepts, so a
            // rejection emits none of the preparation, encode, or send scopes.
            using var packetScope = NetworkDiagnosticMarkers.Measure(NetworkDiagnosticPhase.PacketPreparation);
            var header = new PacketHeader
            {
                Kind = PacketKind.SnapshotChunk,
                Flags = unreliable ? PacketFlags.UnreliableSequenced : PacketFlags.ReliableOrdered,
                Compression = NetworkCompression.None,
                SessionEpoch = peer.Session.Epoch,
                PacketSequence = sequence,
                ServerTick = serverTick,
                AcknowledgedSnapshotTick = PacketHeader.NoneTick,
                ServerProcessedCommandTick = peer.ServerProcessedCommandTick,
                ServerProcessedCommandSequence = peer.ServerProcessedCommandSequence,
                CompatibilityTag = _compatibilityTag
            };
            NetworkBufferLease packet = null;
            var encoded = false;
            using (NetworkDiagnosticMarkers.Measure(NetworkDiagnosticPhase.SnapshotChunkEncode))
            {
                // Peers acknowledging the same baseline share byte-identical chunk
                // framing (header + body). Encode and hash it once per tick and
                // reuse that result here; only the per-peer packet header and a
                // plain body copy are produced for every additional peer.
                if (!TryGetChunkPayload(peer.Scope, in chunk, body,
                        out var payload, out var payloadHash))
                {
                    encoded = false;
                }
                else if (trailer.IsEmpty)
                {
                    encoded = SnapshotChunkEncoder.TryEncodeFromPayload(_bufferPool,
                        header, payload, payloadHash, out packet);
                }
                else
                {
                    // The trailer is this peer's alone, so the shared hash cannot be reused.
                    var trailerEncodeStarted = Stopwatch.GetTimestamp();
                    encoded = SnapshotChunkEncoder.TryEncodeFromPayload(_bufferPool,
                        header, payload, trailer, out packet);
                    _ownedEntityRefreshEncodeStopwatchTicks += Stopwatch.GetTimestamp() -
                                                               trailerEncodeStarted;
                }
            }
            var packetBytes = packet?.Length ?? 0;
            var sent = false;
            if (encoded)
            {
                using var transportScope = NetworkDiagnosticMarkers.Measure(NetworkDiagnosticPhase.TransportTrySend);
                var transportSendStarted = Stopwatch.GetTimestamp();
                try { sent = peer.Transport.TrySend(packet); }
                finally
                {
                    _transportTrySendStopwatchTicks += Stopwatch.GetTimestamp() -
                        transportSendStarted;
                }
            }
            if (!encoded)
            {
                IncrementSaturating(ref _preparationFailures);
                _deliveryPhaseDiagnostics?.ClearSnapshotIdentity(
                    peer.DeliveryPhaseIndex, serverTick, sequence);
            }
            else if (!sent)
            {
                IncrementSaturating(ref _chunkSendFailures);
                if (encoded)
                    _deliveryPhaseDiagnostics?.RecordSnapshotChunkSendFailure(
                        peer.DeliveryPhaseIndex, serverTick, sequence);
            }
            if (traceEnabled)
                peer.Session.Trace(NetworkPhase.Send, NetworkTraceKind.Point,
                    sent ? NetworkResultCategory.Success : NetworkResultCategory.Transport,
                    NetworkPacketKind.SnapshotChunk, serverTick, PacketHeader.NoneTick,
                    packetBytes, _coordinator.HistoryCount(peer.Scope),
                    _coordinator.HistoryByteCount(peer.Scope),
                    unchecked((int)(serverTick - peer.AcknowledgedSnapshotTick)),
                    ElapsedNanoseconds(started), activeConnections: ActiveConnectionCount,
                    activePeers: ActivePeerCount,
                    resyncCorrelationId: peer.ResyncCorrelationId,
                    sequence: sequence,
                    acknowledgedSnapshotTick: peer.AcknowledgedSnapshotTick,
                    oldestHistoryTick: _coordinator.OldestHistoryTick(peer.Scope),
                    newestHistoryTick: _coordinator.NewestHistoryTick(peer.Scope));
            return sent;
        }

        private NetworkCommandResult DecodeTransaction(Peer peer,
            NetworkBufferLease packet, ReadOnlyMemory<byte> payload,
            uint applicationTick, bool duplicatePacket = false)
        {
            using var commandScope = NetworkDiagnosticMarkers.Measure(NetworkDiagnosticPhase.Command);
            if (packet.Length > peer.Transport.MaxReliablePayloadBytes)
                return NetworkCommandResult.LimitExceeded;
            if (!NetworkTransactionWire.TryReadCommand(payload.Span,
                    out var transactionId, out var typeId, out var version,
                    out var payloadOffset))
                return NetworkCommandResult.Malformed;
            if (peer.Transactions.ContainsKey(transactionId))
                return NetworkCommandResult.Duplicate;
            if (peer.ReceiptLedger.TryGetValue(transactionId, out var cached))
            {
                peer.QueueReceipt(in cached);
                return NetworkCommandResult.Duplicate;
            }
            if (transactionId.Value <= peer.HighestTransactionId)
            {
                // Evicted ids are never re-applied. Keep this fallback bounded while
                // preserving the monotonic high-water mark for exact-once safety.
                var evicted = new NetworkServerTransactionReceipt(transactionId,
                    NetworkTransactionStatus.Unhandled, applicationTick);
                return peer.QueueReceipt(in evicted)
                    ? NetworkCommandResult.Duplicate
                    : NetworkCommandResult.LimitExceeded;
            }
            if (duplicatePacket)
                return NetworkCommandResult.Sequence;
            if (peer.PendingTransactionCount >=
                NetworkTransactionWire.MaxPendingTransactions)
            {
                var rejected = new NetworkServerTransactionReceipt(transactionId,
                    NetworkTransactionStatus.PolicyRejected, applicationTick);
                peer.HighestTransactionId = transactionId.Value;
                return peer.QueueReceipt(in rejected)
                    ? NetworkCommandResult.PolicyRejected
                    : NetworkCommandResult.LimitExceeded;
            }
            peer.HighestTransactionId = transactionId.Value;

            var exactLength = payload.Length - payloadOffset;
            var exact = packet.RetainSlice(PacketHeader.Size + payloadOffset,
                exactLength);
            var envelope = new NetworkCommandEnvelope(peer.Transport.Connection,
                peer.PeerId, peer.Epoch, peer.LastReceivedPacketSequence,
                applicationTick, typeId, version, exact);
            var validation = peer.Session.ValidateTransaction(envelope,
                out var entry);
            if (validation != NetworkCommandResult.Queued)
            {
                envelope.Dispose();
                return validation;
            }
            peer.Transactions.Add(transactionId,
                new NetworkServerTransaction(transactionId, envelope, entry,
                    applicationTick));
            return NetworkCommandResult.Queued;
        }

        private static void DispatchTransactions(Peer peer)
        {
            using var commandScope = NetworkDiagnosticMarkers.Measure(NetworkDiagnosticPhase.Command);
            foreach (var transaction in peer.Transactions.Values)
            {
                if (transaction.Dispatched || transaction.ReceiptSent)
                    continue;
                transaction.Dispatched = true;
                try
                {
                    var result = peer.Session.Dispatch(transaction.Envelope,
                        transaction.Entry, NetworkCommandDelivery.Transaction,
                        transaction.TransactionId);
                    if (result == NetworkCommandResult.PolicyRejected)
                        transaction.CompletionStatus =
                            NetworkTransactionStatus.PolicyRejected;
                    else if (result != NetworkCommandResult.Dispatched)
                        transaction.CompletionStatus =
                            NetworkTransactionStatus.Unhandled;
                }
                catch
                {
                    transaction.CompletionStatus =
                        NetworkTransactionStatus.PolicyRejected;
                }
                finally
                {
                    transaction.Dispose();
                }
            }
        }

        private void CompleteTransactions(Peer peer, uint serverTick)
        {
            foreach (var transaction in peer.Transactions.Values)
            {
                if (!transaction.Dispatched || transaction.ReceiptSent)
                    continue;
                transaction.CompletionStatus ??=
                    NetworkTransactionStatus.Unhandled;
                var receipt = new NetworkServerTransactionReceipt(
                    transaction.TransactionId, transaction.CompletionStatus.Value,
                    transaction.ApplicationTick);
                peer.QueueReceipt(in receipt);
            }
        }

        private void FlushTransactionReceipts(Peer peer)
        {
            QueueCompletedTransactionReceipts(peer);
            while (peer.PendingReceipts.Count > 0)
            {
                var receipt = peer.PendingReceipts.Peek();
                Span<byte> payload = stackalloc byte[NetworkTransactionWire.ReceiptSize];
                if (!NetworkTransactionWire.TryWriteReceipt(payload,
                        receipt.TransactionId, receipt.Status,
                        receipt.ApplicationTick) ||
                    !Send(peer, PacketKind.TransactionReceipt,
                        receipt.ApplicationTick, PacketHeader.NoneTick, payload))
                    return;
                peer.PendingReceipts.Dequeue();
                peer.QueuedReceiptIds.Remove(receipt.TransactionId);
                if (peer.Transactions.TryGetValue(receipt.TransactionId,
                        out var transaction) &&
                    transaction.CompletionStatus.HasValue)
                {
                    transaction.ReceiptSent = true;
                    peer.Transactions.Remove(receipt.TransactionId);
                }
                QueueCompletedTransactionReceipts(peer);
            }
        }

        private static void QueueCompletedTransactionReceipts(Peer peer)
        {
            foreach (var transaction in peer.Transactions.Values)
            {
                if (!transaction.Dispatched || transaction.ReceiptSent ||
                    !transaction.CompletionStatus.HasValue)
                    continue;
                var receipt = new NetworkServerTransactionReceipt(
                    transaction.TransactionId,
                    transaction.CompletionStatus.Value,
                    transaction.ApplicationTick);
                peer.QueueReceipt(in receipt);
            }
        }

        // Serializing every owned entity every tick would cost more than the state changes, so each
        // peer is compared at a fixed cadence and sent bytes only when they differ.
        private void FlushOwnerState(Peer peer, uint serverTick)
        {
            if (unchecked(serverTick + peer.PeerId) % _ownerStateIntervalTicks != 0 ||
                !World<TWorld>.HasResource<NetworkOwnerStateResource<TWorld>>())
                return;
            _ownerStateScratch.Clear();
            World<TWorld>.GetResource<NetworkOwnerStateResource<TWorld>>().Provider
                .CollectOwned(peer.PeerId, _ownerStateScratch);
            if (!peer.HasOwnerState && _ownerStateScratch.Count == 0)
                return;

            if (_replicator.CaptureOwnerState(serverTick, peer.Scope, _ownerStateScratch,
                    out var snapshot) != SnapshotCaptureResult.Success)
            {
                IncrementSaturating(ref _ownerStateFailures);
                return;
            }
            using (snapshot)
            {
                if (peer.HasOwnerState && peer.OwnerStateHash == snapshot.PayloadHash)
                    return;
                var length = OwnerStatePayload.HeaderSize + snapshot.ByteLength;
                if (length > peer.Transport.MaxReliablePayloadBytes - PacketHeader.Size)
                {
                    IncrementSaturating(ref _ownerStateFailures);
                    return;
                }
                if (_ownerStatePayload.Length < length)
                    _ownerStatePayload = new byte[length];
                OwnerStatePayload.WriteHeader(_ownerStatePayload, snapshot.EntityCount, snapshot.RecordCount);
                snapshot.Bytes.Span.CopyTo(_ownerStatePayload.AsSpan(OwnerStatePayload.HeaderSize));
                if (!Send(peer, PacketKind.OwnerState, serverTick, PacketHeader.NoneTick,
                        _ownerStatePayload.AsSpan(0, length)))
                    return;
                peer.HasOwnerState = true;
                peer.OwnerStateHash = snapshot.PayloadHash;
                IncrementSaturating(ref _ownerStatePackets);
            }
        }

        // Writes the current state of the peer's own entity into _ownerStatePayload when the
        // snapshot of this tick holds an older one, and returns its length; zero when the snapshot
        // already holds the current state.
        private int WriteOwnedEntityRefresh(Peer peer, uint serverTick)
        {
            var started = Stopwatch.GetTimestamp();
            if (!_replicator.TryGetHeldOwnerBlock(peer.Scope, serverTick, peer.PeerId,
                    out var current, out var held, out var records))
            {
                _ownedEntityRefreshPatchStopwatchTicks += Stopwatch.GetTimestamp() - started;
                return 0;
            }
            var wholeLength = sizeof(byte) + OwnerStatePayload.HeaderSize + sizeof(int) +
                              current.Length;
            var capacity = Math.Max(wholeLength,
                sizeof(byte) + OwnedEntityRefreshPayload.MaximumPatchBytes(current));
            if (_ownerStatePayload.Length < capacity)
                _ownerStatePayload = new byte[capacity];
            var payload = _ownerStatePayload.AsSpan();
            int length;
            // The client rebuilds the block from the one the snapshot of this tick holds, so only
            // the records that differ from it travel.
            if (OwnedEntityRefreshPayload.TryWritePatch(held, current, payload.Slice(sizeof(byte)),
                    out var patchLength) && sizeof(byte) + patchLength < wholeLength)
            {
                payload[0] = OwnedEntityRefreshPayload.RecordPatch;
                length = sizeof(byte) + patchLength;
            }
            else
            {
                payload[0] = OwnedEntityRefreshPayload.WholeBlock;
                OwnerStatePayload.WriteHeader(payload.Slice(sizeof(byte)), 1, records);
                Hashing.Write32(payload, sizeof(byte) + OwnerStatePayload.HeaderSize, 1);
                current.CopyTo(payload.Slice(sizeof(byte) + OwnerStatePayload.HeaderSize +
                                             sizeof(int)));
                length = wholeLength;
            }
            _ownedEntityRefreshPatchStopwatchTicks += Stopwatch.GetTimestamp() - started;
            return length;
        }

        // Sent ahead of the snapshot chunks of the same tick on the same ordered channel: the client
        // keeps it until that snapshot applies and overwrites the held state in the same step.
        private bool SendOwnedEntityRefreshPacket(Peer peer, uint serverTick, int length)
        {
            var started = Stopwatch.GetTimestamp();
            if (length > peer.Transport.MaxReliablePayloadBytes - PacketHeader.Size)
            {
                IncrementSaturating(ref _ownedEntityRefreshFailures);
                return false;
            }
            var sent = Send(peer, PacketKind.OwnedEntityRefresh, serverTick, PacketHeader.NoneTick,
                _ownerStatePayload.AsSpan(0, length));
            _ownedEntityRefreshTransportStopwatchTicks += _lastSendTransportStopwatchTicks;
            _ownedEntityRefreshEncodeStopwatchTicks += Stopwatch.GetTimestamp() - started -
                                                       _lastSendTransportStopwatchTicks;
            if (!sent)
            {
                IncrementSaturating(ref _ownedEntityRefreshFailures);
                return false;
            }
            IncrementSaturating(ref _ownedEntityRefreshPackets);
            AddSaturating(ref _ownedEntityRefreshBytes, PacketHeader.Size + length);
            return true;
        }

        private bool Send(Peer peer, PacketKind kind, uint serverTick,
            uint acknowledgedTick, ReadOnlySpan<byte> payload,
            NetworkResyncReason resyncReason = NetworkResyncReason.None,
            NetworkResyncSource resyncSource = NetworkResyncSource.None,
            uint resyncCorrelationId = 0,
            NetworkCommandResult? commandResult = null)
        {
            var started = Stopwatch.GetTimestamp();
            var sequence = peer.PacketSequence;
            var header = new PacketHeader
            {
                Kind = kind, Flags = PacketFlags.ReliableOrdered, Compression = NetworkCompression.None,
                SessionEpoch = peer.Session.Epoch, PacketSequence = sequence, ServerTick = serverTick,
                AcknowledgedSnapshotTick = acknowledgedTick,
                ServerProcessedCommandTick = peer.ServerProcessedCommandTick,
                ServerProcessedCommandSequence = peer.ServerProcessedCommandSequence,
                CompatibilityTag = _compatibilityTag
            };
            NetworkBufferLease packet = null;
            var encoded = sequence != uint.MaxValue &&
                NetworkPacket.TryEncode(_bufferPool, header, payload,
                    out packet);
            var packetBytes = packet?.Length ?? 0;
            var transportStarted = Stopwatch.GetTimestamp();
            var sent = encoded && peer.Transport.TrySend(packet);
            _lastSendTransportStopwatchTicks = Stopwatch.GetTimestamp() - transportStarted;
            if (sent)
                peer.PacketSequence = sequence + 1;
            peer.Session.Trace(NetworkPhase.Send, NetworkTraceKind.Point, sent ? NetworkResultCategory.Success : NetworkResultCategory.Transport, DiagnosticKind(kind), serverTick, PacketHeader.NoneTick, packetBytes, _coordinator.HistoryCount(peer.Scope), _coordinator.HistoryByteCount(peer.Scope), unchecked((int)(serverTick - peer.AcknowledgedSnapshotTick)), ElapsedNanoseconds(started), activeConnections: ActiveConnectionCount, activePeers: ActivePeerCount, resyncReason: resyncReason, resyncSource: resyncSource, resyncCorrelationId: resyncCorrelationId, commandResult: commandResult, sequence: sequence, acknowledgedSnapshotTick: peer.AcknowledgedSnapshotTick, oldestHistoryTick: _coordinator.OldestHistoryTick(peer.Scope), newestHistoryTick: _coordinator.NewestHistoryTick(peer.Scope));
            var reportedTick = serverTick == PacketHeader.NoneTick ? ServerTick : Math.Max(ServerTick, serverTick);
            peer.Session.ReportSession(reportedTick, peer.AcknowledgedSnapshotTick, peer.ServerProcessedCommandSequence, peer.PacketSequence);
            return sent;
        }

        private int ActiveConnectionCount => _activeConnectionCount;
        private int ActivePeerCount => _activePeerCount;

        private void TraceDispatch(uint serverTick, NetworkDispatchSummary summary, long durationNanoseconds)
        {
            if (_observer == null) return;
            try
            {
                var value = new NetworkTraceEvent(NetworkPhase.CommandDispatch, NetworkTraceKind.Point, summary.Rejected > 0 ? NetworkResultCategory.Policy : NetworkResultCategory.Success,
                    NetworkRole.Server, 0, 0, 0, serverTick, 0, 0, 0, 0, 0, summary.Total, _coordinator.PendingCommandCount, 0,
                    ActiveConnectionCount, ActivePeerCount, Stopwatch.GetTimestamp(), NetworkPacketKind.CommandBatch, durationNanoseconds: durationNanoseconds,
                    fingerprint: _schema.Fingerprint, acceptedCommands: summary.Accepted, rejectedCommands: summary.Rejected);
                _observer.Observe(in value);
            }
            catch { }
        }

        private void DisconnectPeer(Peer peer)
        {
            Send(peer, PacketKind.Disconnect, ServerTick, PacketHeader.NoneTick,
                ReadOnlySpan<byte>.Empty);
            CleanupPeer(peer);
        }

        private void CloseSession(Peer peer)
        {
            ReleaseDeliveryPhasePeer(peer);
            peer.Session.Close();
            if (peer.PeerCounted)
            {
                peer.PeerCounted = false;
                _activePeerCount--;
            }
            if (peer.ConnectionCounted)
            {
                peer.ConnectionCounted = false;
                _activeConnectionCount--;
            }
        }

        private void ReleaseDeliveryPhasePeer(Peer peer)
        {
            var peerIndex = peer.DeliveryPhaseIndex;
            if (peerIndex < 0)
                return;
            if (peer.Transport is INetworkDeliveryPhaseTransport transport)
                transport.ConfigureDeliveryPhaseDiagnostics(null, -1);
            _deliveryPhaseDiagnostics?.ReleasePeer(peerIndex, peer.PeerId,
                peer.Session.Epoch);
            peer.DeliveryPhaseIndex = -1;
        }

        private void ClosePeer(Peer peer)
        {
            _peers.Remove(peer);
            CloseSession(peer);
            if (peer.AdmissionNotified && !peer.DisconnectNotified)
            {
                peer.DisconnectNotified = true;
                NotifyDisconnected(peer);
            }
            peer.Session.ReportSession(ServerTick, peer.AcknowledgedSnapshotTick,
                peer.ServerProcessedCommandSequence, peer.PacketSequence);
        }

        private void CleanupPeer(Peer peer)
        {
            try
            {
                ClosePeer(peer);
            }
            finally
            {
                foreach (var transaction in peer.Transactions.Values)
                    transaction.Dispose();
                peer.Transactions.Clear();
                peer.ReceiptLedger.Clear();
                peer.PendingReceipts.Clear();
                peer.QueuedReceiptIds.Clear();
                peer.ReceiptOrder.Clear();
                peer.CompletedTransactionIds.Clear();
                _coordinator.Remove(peer.Transport.Connection);
                // NCORE-16: mirrors the shared capture history's own scope cleanup (Remove() above
                // clears it the same way once no established peer references the scope anymore).
                // Checking HistoryCount is always safe here: a scope some other peer still uses has
                // already accumulated history by the time any peer disconnects from it, so this can
                // only ever drop a cache entry no capture will read again.
                if (_coordinator.HistoryCount(peer.Scope) == 0)
                    _replicator.ForgetScope(peer.Scope);
            }
        }

        private void NotifyAdmitted(Peer peer)
        {
            if (_peerObserver == null) return;
            var data = peer.Data();
            _peerObserver.Admitted(in data);
        }

        private void NotifyDisconnected(Peer peer)
        {
            if (_peerObserver == null) return;
            var data = peer.Data();
            try
            {
                _peerObserver.Disconnected(in data);
            }
            catch
            {
                // Transport/session cleanup must not be interrupted by game lifecycle hooks.
            }
        }

        private void TraceAdmissionFailure(Peer peer, NetworkAdmissionRejection rejection)
        {
            peer.Session.Trace(NetworkPhase.Decode, NetworkTraceKind.Point,
                NetworkResultCategory.Policy, NetworkPacketKind.Hello,
                ServerTick, 0, 0, _coordinator.HistoryCount(peer.Scope),
                _coordinator.HistoryByteCount(peer.Scope), 0, 0,
                activeConnections: ActiveConnectionCount, activePeers: ActivePeerCount,
                rejectedCommands: rejection == NetworkAdmissionRejection.None ? 0 : 1);
        }

        private static NetworkPacketKind DiagnosticKind(PacketKind kind) => (NetworkPacketKind)(byte)kind;

        /// <summary>Owner state packets accepted by transport.</summary>
        public long OwnerStatePackets => _ownerStatePackets;

        /// <summary>Owner states that could not be captured or did not fit one reliable packet.</summary>
        public long OwnerStateFailures => _ownerStateFailures;

        /// <summary>Times a peer received its own entity because the snapshot held an older state.</summary>
        public long OwnedEntityRefreshPackets => _ownedEntityRefreshPackets;

        /// <summary>Of those, the ones that rode at the end of the snapshot packet instead of a packet of their own.</summary>
        public long OwnedEntityRefreshTrailers => _ownedEntityRefreshTrailers;

        /// <summary>Own-entity packets the transport refused; the snapshot of that tick was not offered.</summary>
        public long OwnedEntityRefreshFailures => _ownedEntityRefreshFailures;

        private long _ownedEntityRefreshPackets;
        private long _ownedEntityRefreshTrailers;
        private long _ownedEntityRefreshBytes;
        private long _ownedEntityRefreshFailures;
        private long _ownedEntityRefreshPatchStopwatchTicks;
        private long _ownedEntityRefreshEncodeStopwatchTicks;
        private long _ownedEntityRefreshTransportStopwatchTicks;
        // Time the last Send spent inside the transport, for callers that attribute their own sends.
        private long _lastSendTransportStopwatchTicks;
        private readonly List<World<TWorld>.Entity> _ownerStateScratch = new List<World<TWorld>.Entity>();
        private byte[] _ownerStatePayload = new byte[1024];
        private long _ownerStatePackets;
        private long _ownerStateFailures;
        private static long ElapsedNanoseconds(long started) => (Stopwatch.GetTimestamp() - started) * 1000000000L / Stopwatch.Frequency;

        private readonly struct SnapshotDeltaKey : IEquatable<SnapshotDeltaKey>
        {
            internal SnapshotDeltaKey(ScopeId scope, uint baselineTick,
                uint targetTick)
            {
                Scope = scope;
                BaselineTick = baselineTick;
                TargetTick = targetTick;
            }

            private ScopeId Scope { get; }
            private uint BaselineTick { get; }
            private uint TargetTick { get; }

            public bool Equals(SnapshotDeltaKey other) => Scope == other.Scope &&
                BaselineTick == other.BaselineTick && TargetTick == other.TargetTick;

            public override bool Equals(object obj) =>
                obj is SnapshotDeltaKey other && Equals(other);

            public override int GetHashCode() => unchecked(
                (Scope.GetHashCode() * 397) ^ (int)BaselineTick ^
                ((int)TargetTick * 397));
        }

        private readonly struct SnapshotDeltaEntry
        {
            internal SnapshotDeltaEntry(NetworkBufferLease delta,
                SnapshotDeltaAttribution attribution)
            {
                Delta = delta;
                Attribution = attribution;
            }

            internal readonly NetworkBufferLease Delta;
            internal readonly SnapshotDeltaAttribution Attribution;
        }

        // Every field that participates in SnapshotChunkHeader.TryWrite (plus the
        // scope, since the header itself carries no scope) is included, so two
        // peers only ever share a cache entry when TryGetChunkPayload would have
        // framed identical bytes for both of them.
        private readonly struct ChunkPayloadKey : IEquatable<ChunkPayloadKey>
        {
            internal ChunkPayloadKey(ScopeId scope, in SnapshotChunkHeader chunk)
            {
                Scope = scope;
                PayloadKind = chunk.PayloadKind;
                SnapshotTick = chunk.SnapshotTick;
                BaselineTick = chunk.BaselineTick;
                TotalHash = chunk.TotalHash;
                ChunkIndex = chunk.ChunkIndex;
                ChunkCount = chunk.ChunkCount;
                ResyncCorrelationId = chunk.ResyncCorrelationId;
            }

            private ScopeId Scope { get; }
            private SnapshotPayloadKind PayloadKind { get; }
            private uint SnapshotTick { get; }
            private uint BaselineTick { get; }
            private ulong TotalHash { get; }
            private uint ChunkIndex { get; }
            private uint ChunkCount { get; }
            private uint ResyncCorrelationId { get; }

            public bool Equals(ChunkPayloadKey other) => Scope == other.Scope &&
                PayloadKind == other.PayloadKind &&
                SnapshotTick == other.SnapshotTick &&
                BaselineTick == other.BaselineTick &&
                TotalHash == other.TotalHash &&
                ChunkIndex == other.ChunkIndex &&
                ChunkCount == other.ChunkCount &&
                ResyncCorrelationId == other.ResyncCorrelationId;

            public override bool Equals(object obj) =>
                obj is ChunkPayloadKey other && Equals(other);

            public override int GetHashCode()
            {
                unchecked
                {
                    var hash = Scope.GetHashCode();
                    hash = (hash * 397) ^ (int)PayloadKind;
                    hash = (hash * 397) ^ (int)SnapshotTick;
                    hash = (hash * 397) ^ (int)BaselineTick;
                    hash = (hash * 397) ^ TotalHash.GetHashCode();
                    hash = (hash * 397) ^ (int)ChunkIndex;
                    hash = (hash * 397) ^ (int)ChunkCount;
                    hash = (hash * 397) ^ (int)ResyncCorrelationId;
                    return hash;
                }
            }
        }

        private readonly struct ChunkPayloadEntry
        {
            internal ChunkPayloadEntry(NetworkBufferLease payload, ulong payloadHash)
            {
                Payload = payload;
                PayloadHash = payloadHash;
            }

            internal readonly NetworkBufferLease Payload;
            internal readonly ulong PayloadHash;
        }

        /// <summary>What an in-flight window slot holds; decides when the slot may be given up without an acknowledgement.</summary>
        private enum InFlightSnapshotKind : byte
        {
            Keyframe,
            ReliableDelta,
            UnreliableDelta,
        }

        private sealed class Peer
        {
            internal Peer(INetworkTransport transport, NetworkSession<TWorld> session, uint peerId, uint epoch, ScopeId scope)
            { Transport = transport; Session = session; PeerId = peerId; Epoch = epoch; Scope = scope; PacketSequence = 1; }
            internal readonly INetworkTransport Transport;
            internal readonly NetworkSession<TWorld> Session;
            internal readonly uint PeerId;
            internal readonly uint Epoch;
            internal ScopeId Scope { get; private set; }
            internal uint ScopeAssignedTick;
            internal bool HasOwnerState;
            internal ulong OwnerStateHash;
            /// <summary>Reassigns this peer's scope (NCORE-15) and keeps its session in sync.</summary>
            internal void SetScope(ScopeId scope)
            {
                if (Scope != scope)
                {
                    ClearSnapshotSubmission();
                    DeliveryObservationSnapshotTick = 0;
                    ConsumedDeliveryStopwatchTicks = 0;
                    InFlightSnapshotCount = 0;
                }
                Scope = scope;
                Session.SetScope(scope);
            }
            internal void ResetSnapshotAdmissionState()
            {
                InFlightSnapshotCount = 0;
                AcknowledgedSnapshotTick = 0;
                LastSnapshotSentTick = 0;
                DeliveryObservationSnapshotTick = 0;
                ConsumedDeliveryStopwatchTicks = 0;
                ApplicationAckOutstandingTick = 0;
                RecoveryOutstandingTick = 0;
                SupersededSnapshotTick = 0;
                LastRecoveryAttemptServerTick = 0;
                HasRecoveryAttempted = false;
                ResyncCorrelationId = 0;
                ResyncSnapshotTick = 0;
                ClearSnapshotSubmission();
            }
            internal void BeginSnapshotRecovery()
            {
                ResyncRequested = true;
                DeliveryObservationSnapshotTick = 0;
                ConsumedDeliveryStopwatchTicks = 0;
            }

            /// <summary>
            /// Forgets the unacknowledged ticks so the keyframe a peer-side recovery needs is not
            /// held behind them by the windowed gate. Not part of <see cref="BeginSnapshotRecovery"/>:
            /// a keyframe the server sends on its own (no usable baseline yet) must still count
            /// toward the window, or a peer that never acknowledges would receive one every tick.
            /// </summary>
            internal void ClearInFlightSnapshots() => InFlightSnapshotCount = 0;

            /// <summary>Remembers one accepted snapshot tick until the peer acknowledges it or a later tick.</summary>
            internal void RecordInFlightSnapshot(uint tick, InFlightSnapshotKind kind)
            {
                if (InFlightSnapshotCount == InFlightSnapshotTicks.Length)
                {
                    Array.Copy(InFlightSnapshotTicks, 1, InFlightSnapshotTicks, 0,
                        InFlightSnapshotCount - 1);
                    Array.Copy(InFlightSnapshotKinds, 1, InFlightSnapshotKinds, 0,
                        InFlightSnapshotCount - 1);
                    InFlightSnapshotCount--;
                }
                InFlightSnapshotTicks[InFlightSnapshotCount] = tick;
                InFlightSnapshotKinds[InFlightSnapshotCount++] = kind;
            }

            /// <summary>Drops every in-flight tick at or below the cumulative acknowledgement.</summary>
            internal void ReleaseInFlightSnapshots(uint acknowledgedTick)
            {
                var kept = 0;
                for (var i = 0; i < InFlightSnapshotCount; i++)
                {
                    if (InFlightSnapshotTicks[i] <= acknowledgedTick)
                        continue;
                    InFlightSnapshotKinds[kept] = InFlightSnapshotKinds[i];
                    InFlightSnapshotTicks[kept++] = InFlightSnapshotTicks[i];
                }
                InFlightSnapshotCount = kept;
            }

            /// <summary>
            /// Drops the unreliable deltas sent at least <paramref name="lostTicks"/> ticks ago
            /// and, when <paramref name="keyframePending"/>, every delta. Returns the number of
            /// unreliable deltas dropped.
            /// </summary>
            internal int ExpireInFlightDeltas(uint serverTick, uint lostTicks,
                bool keyframePending)
            {
                var kept = 0;
                var unreliableDropped = 0;
                for (var i = 0; i < InFlightSnapshotCount; i++)
                {
                    var kind = InFlightSnapshotKinds[i];
                    var timedOut = kind == InFlightSnapshotKind.UnreliableDelta &&
                        unchecked(serverTick - InFlightSnapshotTicks[i]) >= lostTicks;
                    if (timedOut || keyframePending && kind != InFlightSnapshotKind.Keyframe)
                    {
                        if (kind == InFlightSnapshotKind.UnreliableDelta)
                            unreliableDropped++;
                        continue;
                    }
                    InFlightSnapshotKinds[kept] = kind;
                    InFlightSnapshotTicks[kept++] = InFlightSnapshotTicks[i];
                }
                InFlightSnapshotCount = kept;
                return unreliableDropped;
            }

            internal void RecordSnapshotSubmission(uint tick, ScopeId scope,
                bool recovery, long submittedStopwatchTicks)
            {
                SnapshotSubmissionTick = tick;
                SnapshotSubmissionScope = scope;
                SnapshotSubmissionEpoch = Epoch;
                SnapshotSubmissionRecovery = recovery;
                SnapshotSubmissionStopwatchTicks = submittedStopwatchTicks;
            }

            internal bool TryConsumeSnapshotSubmission(uint acknowledgedTick,
                bool? recovery, out long elapsedStopwatchTicks)
            {
                if (SnapshotSubmissionTick == 0 ||
                    SnapshotSubmissionTick != acknowledgedTick ||
                    SnapshotSubmissionEpoch != Epoch ||
                    SnapshotSubmissionScope != Scope ||
                    recovery.HasValue &&
                    SnapshotSubmissionRecovery != recovery.Value ||
                    SnapshotSubmissionStopwatchTicks == 0)
                {
                    elapsedStopwatchTicks = 0;
                    return false;
                }
                elapsedStopwatchTicks = Stopwatch.GetTimestamp() -
                    SnapshotSubmissionStopwatchTicks;
                ClearSnapshotSubmission();
                return elapsedStopwatchTicks >= 0;
            }

            internal void ClearSnapshotSubmission()
            {
                SnapshotSubmissionTick = 0;
                SnapshotSubmissionScope = default;
                SnapshotSubmissionEpoch = 0;
                SnapshotSubmissionRecovery = false;
                SnapshotSubmissionStopwatchTicks = 0;
            }

            internal uint PacketSequence;
            internal uint AcknowledgedSnapshotTick;
            internal uint LastSnapshotSentTick;
            internal readonly uint[] InFlightSnapshotTicks =
                new uint[MaxSnapshotsInFlightLimit];
            internal readonly InFlightSnapshotKind[] InFlightSnapshotKinds =
                new InFlightSnapshotKind[MaxSnapshotsInFlightLimit];
            internal int InFlightSnapshotCount;
            internal uint DeliveryObservationSnapshotTick;
            internal long ConsumedDeliveryStopwatchTicks;
            internal uint ApplicationAckOutstandingTick;
            internal uint RecoveryOutstandingTick;
            internal uint SupersededSnapshotTick;
            internal uint LastRecoveryAttemptServerTick;
            internal bool HasRecoveryAttempted;
            internal uint SnapshotSubmissionTick;
            internal ScopeId SnapshotSubmissionScope;
            internal uint SnapshotSubmissionEpoch;
            internal bool SnapshotSubmissionRecovery;
            internal long SnapshotSubmissionStopwatchTicks;
            internal uint ServerProcessedCommandTick;
            internal uint ServerProcessedCommandSequence;
            internal bool ResyncRequested;
            internal uint ResyncCorrelationId;
            internal uint ResyncSnapshotTick;
            internal bool ConnectionCounted;
            internal bool PeerCounted;
            internal int DeliveryPhaseIndex = -1;
            internal bool AdmissionNotified;
            internal bool DisconnectNotified;
            internal uint LastReceivedPacketSequence =>
                Session.LastReceivedPacketSequence;
            internal readonly Dictionary<NetworkTransactionId,
                NetworkServerTransaction> Transactions =
                new Dictionary<NetworkTransactionId, NetworkServerTransaction>();
            internal readonly Dictionary<NetworkTransactionId,
                NetworkServerTransactionReceipt> ReceiptLedger =
                new Dictionary<NetworkTransactionId, NetworkServerTransactionReceipt>();
            internal readonly Queue<NetworkServerTransactionReceipt> PendingReceipts =
                new Queue<NetworkServerTransactionReceipt>();
            internal readonly HashSet<NetworkTransactionId> QueuedReceiptIds =
                new HashSet<NetworkTransactionId>();
            internal readonly Queue<NetworkTransactionId> ReceiptOrder =
                new Queue<NetworkTransactionId>();
            internal readonly List<NetworkTransactionId> CompletedTransactionIds =
                new List<NetworkTransactionId>();
            internal ulong HighestTransactionId;
            internal int PendingTransactionCount
            {
                get
                {
                    return Transactions.Count;
                }
            }

            internal bool HasPendingReceiptWork
            {
                get
                {
                    if (PendingReceipts.Count != 0)
                        return true;
                    foreach (var transaction in Transactions.Values)
                    {
                        if (transaction.CompletionStatus.HasValue &&
                            !transaction.ReceiptSent)
                            return true;
                    }
                    return false;
                }
            }

            internal void CacheReceipt(in NetworkServerTransactionReceipt receipt)
            {
                if (ReceiptLedger.ContainsKey(receipt.TransactionId))
                    return;
                ReceiptLedger.Add(receipt.TransactionId, receipt);
                ReceiptOrder.Enqueue(receipt.TransactionId);
                while (ReceiptOrder.Count > NetworkTransactionWire.ReceiptLedgerCapacity)
                {
                    var evicted = ReceiptOrder.Dequeue();
                    ReceiptLedger.Remove(evicted);
                }
            }

            internal bool QueueReceipt(in NetworkServerTransactionReceipt receipt)
            {
                CacheReceipt(in receipt);
                if (!QueuedReceiptIds.Add(receipt.TransactionId))
                    return true;
                if (PendingReceipts.Count >=
                    NetworkTransactionWire.MaxPendingTransactions)
                {
                    QueuedReceiptIds.Remove(receipt.TransactionId);
                    return false;
                }
                PendingReceipts.Enqueue(receipt);
                return true;
            }
            internal readonly NetworkCommandEnvelope[] DecodedCommands =
                new NetworkCommandEnvelope[ProtocolLimits.MaxCommandsPerBatch];
            internal NetworkPeerData Data() => new NetworkPeerData
            {
                Connection = Transport.Connection,
                PeerId = PeerId,
                Epoch = Epoch,
                Scope = Scope
            };
        }

        private sealed class NetworkCommandEnvelopeComparer :
            IComparer<NetworkCommandEnvelope>
        {
            internal static readonly NetworkCommandEnvelopeComparer Instance =
                new NetworkCommandEnvelopeComparer();

            public int Compare(NetworkCommandEnvelope left,
                NetworkCommandEnvelope right) =>
                left.Sequence.CompareTo(right.Sequence);
        }
    }
}
