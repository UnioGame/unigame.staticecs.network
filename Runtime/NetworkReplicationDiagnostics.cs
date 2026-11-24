namespace UniGame.StaticEcs.Network
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using FFS.Libraries.StaticEcs;

    /// <summary>Copies one bounded log-linear histogram of non-negative integer values.</summary>
    /// <remarks>
    /// Values 0..15 have exact buckets; larger values use eight sub-buckets per power of two,
    /// so a bucket upper bound overstates its values by at most 12.5 %. Values above
    /// <see cref="int.MaxValue"/> are clamped into the last bucket; <see cref="Max"/> stays exact.
    /// </remarks>
    public readonly struct NetworkValueHistogram
    {
        private const int ExactBuckets = 16;
        private const int SubBuckets = 8;
        private const int FirstOctave = 4;
        private const int LastOctave = 30;

        /// <summary>Number of buckets in <see cref="Counts"/>.</summary>
        public const int BucketCount =
            ExactBuckets + (LastOctave - FirstOctave + 1) * SubBuckets;

        private readonly long[] _counts;

        public NetworkValueHistogram(long count, long sum, long max, long[] counts)
        {
            Count = count;
            Sum = sum;
            Max = max;
            _counts = counts;
        }

        public long Count { get; }
        public long Sum { get; }
        public long Max { get; }

        /// <summary>Per-bucket sample counts; empty when nothing was recorded.</summary>
        public IReadOnlyList<long> Counts => _counts ?? Array.Empty<long>();

        /// <summary>Returns the inclusive largest value that maps to one bucket.</summary>
        public static long UpperBound(int bucket)
        {
            if ((uint)bucket >= BucketCount)
                throw new ArgumentOutOfRangeException(nameof(bucket));
            if (bucket < ExactBuckets)
                return bucket;
            var octave = FirstOctave + (bucket - ExactBuckets) / SubBuckets;
            var sub = (bucket - ExactBuckets) % SubBuckets;
            return ((long)(SubBuckets + sub + 1) << (octave - 3)) - 1;
        }

        /// <summary>Returns the bucket index for one value.</summary>
        public static int BucketOf(long value)
        {
            if (value < ExactBuckets)
                return value < 0 ? 0 : (int)value;
            var clamped = value > int.MaxValue ? int.MaxValue : (int)value;
            var octave = 31 - LeadingZeroCount((uint)clamped);
            var sub = (clamped >> (octave - 3)) & (SubBuckets - 1);
            return ExactBuckets + (octave - FirstOctave) * SubBuckets + sub;
        }

        private static int LeadingZeroCount(uint value)
        {
            var count = 0;
            while ((value & 0x80000000u) == 0)
            {
                value <<= 1;
                count++;
            }
            return count;
        }
    }

    internal sealed class NetworkValueHistogramAccumulator
    {
        private readonly long[] _counts = new long[NetworkValueHistogram.BucketCount];
        private long _count;
        private long _sum;
        private long _max;

        internal void Add(long value)
        {
            if (value < 0)
                value = 0;
            _counts[NetworkValueHistogram.BucketOf(value)]++;
            _count++;
            _sum = value > long.MaxValue - _sum ? long.MaxValue : _sum + value;
            if (value > _max)
                _max = value;
        }

        internal NetworkValueHistogram Capture() =>
            new NetworkValueHistogram(_count, _sum, _max, (long[])_counts.Clone());
    }

    /// <summary>Cumulative stopwatch ticks of the work the update allowance adds to a server tick.</summary>
    /// <remarks>
    /// The capture stages run once per scope capture, in this order: Merge orders the cell blocks by
    /// entity, Compare finds the entities whose bytes changed, Weight ranks them, Select sorts the
    /// queue, Measure prices the entities the allowance can still take and spends it, Write copies
    /// the chosen blocks and keeps them for the next capture. The own-entity stages run once per
    /// state sent to its owner.
    /// </remarks>
    public readonly struct NetworkCaptureBudgetStageTicks
    {
        public NetworkCaptureBudgetStageTicks(long merge, long compare, long measure, long weight,
            long select, long write, long ownEntityPatch, long ownEntityEncode,
            long ownEntityTransport)
        {
            Merge = merge;
            Compare = compare;
            Measure = measure;
            Weight = weight;
            Select = select;
            Write = write;
            OwnEntityPatch = ownEntityPatch;
            OwnEntityEncode = ownEntityEncode;
            OwnEntityTransport = ownEntityTransport;
        }

        public long Merge { get; }
        public long Compare { get; }
        public long Measure { get; }
        public long Weight { get; }
        public long Select { get; }
        public long Write { get; }

        /// <summary>Finding the held block of the peer and writing the record patch.</summary>
        public long OwnEntityPatch { get; }

        /// <summary>
        /// Framing of the snapshot packet that carries the state as its trailer, or of a separate
        /// packet without its transport call.
        /// </summary>
        public long OwnEntityEncode { get; }

        /// <summary>The transport's reliable send of a separate own-entity packet.</summary>
        public long OwnEntityTransport { get; }
    }

    /// <summary>Copies cumulative server replication counters for bounded load reports.</summary>
    /// <remarks>
    /// Counters are cumulative since server creation; reports subtract a sample-start copy.
    /// Capturing allocates, so callers read it at sample boundaries, not per tick.
    /// </remarks>
    public sealed class NetworkReplicationDiagnostics
    {
        private readonly long[] _captureFailuresByReason;

        public NetworkReplicationDiagnostics(long[] captureFailuresByReason,
            NetworkValueHistogram scopeCaptureEntities, NetworkValueHistogram scopeCaptureBytes,
            long keyframeSnapshots, long keyframeBytes, long keyframeEntities,
            long deltaSnapshots, long deltaBytes, long deltaTargetEntities,
            long deltaEntityAdds, long deltaEntityRemoves, long deltaEntityPatches,
            NetworkReplicationCensusDiagnostics census,
            NetworkValueHistogram deltaSnapshotBytes = default, long deltaSnapshotsOverDatagram = 0,
            long captureBudgetRefreshedEntities = 0, long captureBudgetDeferredEntities = 0,
            long captureBudgetOverflowCaptures = 0, long ownedEntityRefreshPackets = 0,
            long ownedEntityRefreshBytes = 0, int captureBudgetBytes = 0,
            int captureBudgetMaximumWaitTicks = 0,
            bool captureBudgetRefreshesObserversPersonally = false,
            NetworkCaptureBudgetStageTicks captureBudgetStages = default,
            long ownedEntityRefreshTrailers = 0,
            NetworkValueHistogram deltaBaselineLagTicks = default,
            long deltaSnapshotsUnreliable = 0, long unreliableDeltaWindowExpirations = 0,
            long unreliableAcknowledgements = 0)
        {
            UnreliableAcknowledgements = unreliableAcknowledgements;
            DeltaBaselineLagTicks = deltaBaselineLagTicks;
            DeltaSnapshotsUnreliable = deltaSnapshotsUnreliable;
            UnreliableDeltaWindowExpirations = unreliableDeltaWindowExpirations;
            CaptureBudgetStages = captureBudgetStages;
            OwnedEntityRefreshTrailers = ownedEntityRefreshTrailers;
            _captureFailuresByReason = captureFailuresByReason ?? Array.Empty<long>();
            ScopeCaptureEntities = scopeCaptureEntities;
            ScopeCaptureBytes = scopeCaptureBytes;
            KeyframeSnapshots = keyframeSnapshots;
            KeyframeBytes = keyframeBytes;
            KeyframeEntities = keyframeEntities;
            DeltaSnapshots = deltaSnapshots;
            DeltaBytes = deltaBytes;
            DeltaTargetEntities = deltaTargetEntities;
            DeltaEntityAdds = deltaEntityAdds;
            DeltaEntityRemoves = deltaEntityRemoves;
            DeltaEntityPatches = deltaEntityPatches;
            Census = census;
            DeltaSnapshotBytes = deltaSnapshotBytes;
            DeltaSnapshotsOverDatagram = deltaSnapshotsOverDatagram;
            CaptureBudgetRefreshedEntities = captureBudgetRefreshedEntities;
            CaptureBudgetDeferredEntities = captureBudgetDeferredEntities;
            CaptureBudgetOverflowCaptures = captureBudgetOverflowCaptures;
            OwnedEntityRefreshPackets = ownedEntityRefreshPackets;
            OwnedEntityRefreshBytes = ownedEntityRefreshBytes;
            CaptureBudgetBytes = captureBudgetBytes;
            CaptureBudgetMaximumWaitTicks = captureBudgetMaximumWaitTicks;
            CaptureBudgetRefreshesObserversPersonally = captureBudgetRefreshesObserversPersonally;
        }

        /// <summary>Returns capture failures for one non-success capture result.</summary>
        public long CaptureFailures(SnapshotCaptureResult reason) =>
            (uint)reason < (uint)_captureFailuresByReason.Length
                ? _captureFailuresByReason[(int)reason]
                : 0;

        /// <summary>Entities in each canonical scope capture, one sample per scope per tick.</summary>
        public NetworkValueHistogram ScopeCaptureEntities { get; }

        /// <summary>Canonical bytes of each scope capture, one sample per scope per tick.</summary>
        public NetworkValueHistogram ScopeCaptureBytes { get; }

        /// <summary>Snapshot batches fully accepted by transport as keyframes.</summary>
        public long KeyframeSnapshots { get; }

        /// <summary>Encoded body bytes of accepted keyframes, excluding chunk framing.</summary>
        public long KeyframeBytes { get; }

        /// <summary>Entities carried by accepted keyframes.</summary>
        public long KeyframeEntities { get; }

        public long DeltaSnapshots { get; }

        /// <summary>Encoded body bytes of accepted deltas, excluding chunk framing.</summary>
        public long DeltaBytes { get; }

        /// <summary>Target-snapshot entities covered by accepted deltas.</summary>
        public long DeltaTargetEntities { get; }

        /// <summary>Entities an accepted delta adds to the peer's replica set (client replica creates).</summary>
        public long DeltaEntityAdds { get; }

        /// <summary>Entities an accepted delta removes from the peer's replica set (client replica destroys).</summary>
        public long DeltaEntityRemoves { get; }

        /// <summary>Entities whose canonical bytes differ from the peer's acknowledged baseline.</summary>
        public long DeltaEntityPatches { get; }

        /// <summary>Opt-in sampled change and observation census; null when not enabled.</summary>
        public NetworkReplicationCensusDiagnostics Census { get; }

        /// <summary>Encoded body bytes of each accepted delta, one sample per peer snapshot, excluding chunk framing.</summary>
        public NetworkValueHistogram DeltaSnapshotBytes { get; }

        /// <summary>
        /// Accepted deltas with a chunk packet larger than the peer transport's
        /// <see cref="INetworkTransportCapabilities.MaxUnreliablePayloadBytes"/>: the transport has to split
        /// such a packet into several datagrams, and losing any of them delays the whole snapshot.
        /// </summary>
        public long DeltaSnapshotsOverDatagram { get; }

        /// <summary>
        /// Server ticks between each accepted delta and the baseline it was encoded against, one
        /// sample per peer snapshot. A delta carries every change since its baseline, so a peer
        /// whose acknowledgement arrives one tick later gets the previous tick's changes again.
        /// </summary>
        public NetworkValueHistogram DeltaBaselineLagTicks { get; }

        /// <summary>
        /// Accepted deltas sent unreliably under <see cref="NetworkServerSettings.UnreliableDeltaChunks"/>.
        /// With them, <see cref="DeltaSnapshotsOverDatagram"/> counts the unreliable deltas of more
        /// than one chunk: losing any chunk loses that delta.
        /// </summary>
        public long DeltaSnapshotsUnreliable { get; }

        /// <summary>
        /// Unacknowledged deltas dropped from the in-flight window after
        /// <see cref="NetworkServerSettings.UnreliableDeltaLostTicks"/>: lost, or acknowledged later than that.
        /// </summary>
        public long UnreliableDeltaWindowExpirations { get; }

        /// <summary>
        /// Snapshot acknowledgements received on the unreliable channel: a client acknowledges a
        /// delta the way it arrived. Lost ones are not counted, so the gap to
        /// <see cref="DeltaSnapshotsUnreliable"/> is the loss in both directions.
        /// </summary>
        public long UnreliableAcknowledgements { get; }

        /// <summary>Changed entities a scope capture refreshed under <see cref="INetworkScopeCaptureBudgetPolicy{TWorld}"/>.</summary>
        public long CaptureBudgetRefreshedEntities { get; }

        /// <summary>Changed entities a scope capture held because the tick's allowance was spent, one count per capture.</summary>
        public long CaptureBudgetDeferredEntities { get; }

        /// <summary>Scope captures whose entities that may not wait already exceeded the allowance.</summary>
        public long CaptureBudgetOverflowCaptures { get; }

        /// <summary>Times a peer received its own entity because the scope snapshot held an older state.</summary>
        public long OwnedEntityRefreshPackets { get; }

        /// <summary>Of those, the ones that rode at the end of the snapshot packet instead of a packet of their own.</summary>
        public long OwnedEntityRefreshTrailers { get; }

        /// <summary>Wire bytes they added: the trailer alone, or a separate packet with its header.</summary>
        public long OwnedEntityRefreshBytes { get; }

        /// <summary>Changed-entity bytes a scope capture may refresh per tick; zero without an allowance.</summary>
        public int CaptureBudgetBytes { get; }

        /// <summary>Captures after which a waiting changed entity is refreshed; zero without an allowance.</summary>
        public int CaptureBudgetMaximumWaitTicks { get; }

        /// <summary>Whether a held entity reaches the peer that owns it in a personal packet.</summary>
        public bool CaptureBudgetRefreshesObserversPersonally { get; }

        public NetworkCaptureBudgetStageTicks CaptureBudgetStages { get; }
    }

    /// <summary>Copies the sampled replication census counters.</summary>
    /// <remarks>
    /// Every census tick compares each captured scope with the same scope's capture from the
    /// previous tick. Each entity is counted once per census tick, in the first scope that
    /// contains it. A record re-serialized with identical bytes is "unchanged": it is the upper
    /// bound of false <c>Changed</c> marks a dirty-on-access tracking model would produce for it.
    /// </remarks>
    public sealed class NetworkReplicationCensusDiagnostics
    {
        public NetworkReplicationCensusDiagnostics(int intervalTicks, long ticks,
            long stopwatchTicks, long failures, long scopeComparisons,
            long scopeComparisonsUnavailable, long peersWithoutCapture,
            NetworkReplicationKindCensus[] entityKinds,
            NetworkReplicationRecordCensus[] recordTypes)
        {
            IntervalTicks = intervalTicks;
            Ticks = ticks;
            StopwatchTicks = stopwatchTicks;
            Failures = failures;
            ScopeComparisons = scopeComparisons;
            ScopeComparisonsUnavailable = scopeComparisonsUnavailable;
            PeersWithoutCapture = peersWithoutCapture;
            EntityKinds = entityKinds;
            RecordTypes = recordTypes;
        }

        public int IntervalTicks { get; }

        /// <summary>Census ticks that completed.</summary>
        public long Ticks { get; }

        /// <summary>Total census cost in <see cref="Stopwatch"/> ticks.</summary>
        public long StopwatchTicks { get; }

        /// <summary>Census ticks abandoned because a capture could not be indexed.</summary>
        public long Failures { get; }

        /// <summary>Scope captures compared with the previous tick.</summary>
        public long ScopeComparisons { get; }

        /// <summary>Scope captures without a previous-tick capture in history.</summary>
        public long ScopeComparisonsUnavailable { get; }

        /// <summary>Established peers whose scope had no capture on a census tick, so their observed entities are missing.</summary>
        public long PeersWithoutCapture { get; }

        public IReadOnlyList<NetworkReplicationKindCensus> EntityKinds { get; }
        public IReadOnlyList<NetworkReplicationRecordCensus> RecordTypes { get; }
    }

    /// <summary>Census sums for one replicated entity kind across census ticks.</summary>
    public readonly struct NetworkReplicationKindCensus
    {
        public NetworkReplicationKindCensus(string name, uint typeId, long worldEntities,
            long observedEntities, long comparedEntities, long changedEntities,
            long addedEntities, long removedEntities)
        {
            Name = name;
            TypeId = typeId;
            WorldEntities = worldEntities;
            ObservedEntities = observedEntities;
            ComparedEntities = comparedEntities;
            ChangedEntities = changedEntities;
            AddedEntities = addedEntities;
            RemovedEntities = removedEntities;
        }

        public string Name { get; }
        public uint TypeId { get; }

        /// <summary>Entities of this kind in the authority world.</summary>
        public long WorldEntities { get; }

        /// <summary>Entities of this kind present in at least one peer scope capture.</summary>
        public long ObservedEntities { get; }

        /// <summary>Observed entities also present in their scope's previous-tick capture.</summary>
        public long ComparedEntities { get; }

        /// <summary>Compared entities whose canonical bytes changed since the previous tick.</summary>
        public long ChangedEntities { get; }

        /// <summary>Entities that entered a scope since that scope's previous-tick capture.</summary>
        public long AddedEntities { get; }

        /// <summary>Entities that left a scope since that scope's previous-tick capture.</summary>
        public long RemovedEntities { get; }
    }

    /// <summary>Census sums for one replicated record type across census ticks.</summary>
    public readonly struct NetworkReplicationRecordCensus
    {
        public NetworkReplicationRecordCensus(string name, uint typeId,
            long comparedRecords, long unchangedRecords)
        {
            Name = name;
            TypeId = typeId;
            ComparedRecords = comparedRecords;
            UnchangedRecords = unchangedRecords;
        }

        public string Name { get; }
        public uint TypeId { get; }

        /// <summary>Records of compared entities whose record set matched the previous tick.</summary>
        public long ComparedRecords { get; }

        /// <summary>Compared records re-serialized byte-identical to the previous tick.</summary>
        public long UnchangedRecords { get; }
    }

    /// <summary>Samples per-tick change and observation counts over the tick's scope captures.</summary>
    internal sealed class NetworkReplicationCensus<TWorld>
        where TWorld : struct, IWorldType
    {
        private const int MaximumKinds = 32;
        private const int MaximumRecordTypes = 128;

        private readonly int _intervalTicks;
        private readonly string[] _kindNames;
        private readonly uint[] _kindTypeIds;
        private readonly Dictionary<uint, int> _kindByTypeId = new Dictionary<uint, int>();
        private readonly int[] _kindByEntityType = new int[256];
        private readonly string[] _recordNames;
        private readonly uint[] _recordTypeIds;
        private readonly Dictionary<uint, int> _recordByTypeId = new Dictionary<uint, int>();
        private readonly HashSet<ulong> _seen = new HashSet<ulong>();
        private readonly long[] _worldEntities;
        private readonly long[] _observedEntities;
        private readonly long[] _comparedEntities;
        private readonly long[] _changedEntities;
        private readonly long[] _addedEntities;
        private readonly long[] _removedEntities;
        private readonly long[] _comparedRecords;
        private readonly long[] _unchangedRecords;
        private long _ticks;
        private long _stopwatchTicks;
        private long _failures;
        private long _scopeComparisons;
        private long _scopeComparisonsUnavailable;
        private long _peersWithoutCapture;

        internal NetworkReplicationCensus(NetworkSchema<TWorld> schema, int intervalTicks)
        {
            if (schema == null)
                throw new ArgumentNullException(nameof(schema));
            if (intervalTicks < 1)
                throw new ArgumentOutOfRangeException(nameof(intervalTicks));
            _intervalTicks = intervalTicks;
            for (var i = 0; i < _kindByEntityType.Length; i++)
                _kindByEntityType[i] = -1;
            var kindNames = new List<string>();
            var kindIds = new List<uint>();
            var recordNames = new List<string>();
            var recordIds = new List<uint>();
            foreach (var entry in schema.RetainedEntries)
            {
                if (entry.Kind == NetworkSchemaKind.Entity)
                {
                    if (kindNames.Count == MaximumKinds ||
                        !(entry.Invoker is IEntityNetworkInvoker<TWorld> invoker))
                        continue;
                    _kindByTypeId[entry.TypeId.Value] = kindNames.Count;
                    _kindByEntityType[invoker.EntityTypeId] = kindNames.Count;
                    kindNames.Add(entry.RuntimeType?.Name ?? entry.TypeId.Value.ToString());
                    kindIds.Add(entry.TypeId.Value);
                }
                else if (entry.Kind != NetworkSchemaKind.Command)
                {
                    if (recordNames.Count == MaximumRecordTypes)
                        continue;
                    _recordByTypeId[entry.TypeId.Value] = recordNames.Count;
                    recordNames.Add(entry.RuntimeType?.Name ?? entry.TypeId.Value.ToString());
                    recordIds.Add(entry.TypeId.Value);
                }
            }
            _kindNames = kindNames.ToArray();
            _kindTypeIds = kindIds.ToArray();
            _recordNames = recordNames.ToArray();
            _recordTypeIds = recordIds.ToArray();
            _worldEntities = new long[_kindNames.Length];
            _observedEntities = new long[_kindNames.Length];
            _comparedEntities = new long[_kindNames.Length];
            _changedEntities = new long[_kindNames.Length];
            _addedEntities = new long[_kindNames.Length];
            _removedEntities = new long[_kindNames.Length];
            _comparedRecords = new long[_recordNames.Length];
            _unchangedRecords = new long[_recordNames.Length];
        }

        internal bool IsCensusTick(uint serverTick) => serverTick % (uint)_intervalTicks == 0;

        internal void RecordPeersWithoutCapture(int count) => _peersWithoutCapture += count;

        /// <summary>Counts world population; call once per census tick before observing captures.</summary>
        internal void BeginTick()
        {
            _seen.Clear();
            foreach (var entity in World<TWorld>.Query().Entities(EntityStatusType.Any))
            {
                var kind = _kindByEntityType[entity.EntityType];
                if (kind >= 0)
                    _worldEntities[kind]++;
            }
        }

        /// <summary>Compares one scope's capture with its previous-tick capture, if retained.</summary>
        internal bool ObserveScope(NetworkSnapshot current, NetworkSnapshot previous)
        {
            if (!current.TryGetLayout(out var currentLayout))
                return false;
            var currentBytes = current.Bytes.Span;
            if (previous == null || !previous.TryGetLayout(out var previousLayout))
            {
                _scopeComparisonsUnavailable++;
                for (var i = 0; i < currentLayout.EntityCount; i++)
                {
                    var entity = currentLayout.Entities[i];
                    var kind = KindOf(currentBytes, in entity);
                    if (_seen.Add(entity.Gid) && kind >= 0)
                        _observedEntities[kind]++;
                }
                return true;
            }

            _scopeComparisons++;
            var previousBytes = previous.Bytes.Span;
            var previousIndex = 0;
            var currentIndex = 0;
            while (previousIndex < previousLayout.EntityCount ||
                   currentIndex < currentLayout.EntityCount)
            {
                var comparison = previousIndex == previousLayout.EntityCount ? 1 :
                    currentIndex == currentLayout.EntityCount ? -1 :
                    SnapshotDeltaCodec.CompareGid(previousLayout.Entities[previousIndex].Gid,
                        currentLayout.Entities[currentIndex].Gid);
                if (comparison < 0)
                {
                    var departed = KindOf(previousBytes,
                        in previousLayout.Entities[previousIndex]);
                    if (departed >= 0)
                        _removedEntities[departed]++;
                    previousIndex++;
                    continue;
                }

                var entity = currentLayout.Entities[currentIndex];
                var kind = KindOf(currentBytes, in entity);
                var first = _seen.Add(entity.Gid);
                if (first && kind >= 0)
                    _observedEntities[kind]++;
                if (comparison > 0)
                {
                    if (kind >= 0)
                        _addedEntities[kind]++;
                    currentIndex++;
                    continue;
                }

                if (first)
                    CompareEntity(previousBytes, previousLayout,
                        in previousLayout.Entities[previousIndex], currentBytes,
                        currentLayout, in entity, kind);
                previousIndex++;
                currentIndex++;
            }
            return true;
        }

        internal void CompleteTick(long stopwatchTicks, bool failed)
        {
            _stopwatchTicks += stopwatchTicks;
            if (failed)
                _failures++;
            else
                _ticks++;
        }

        internal NetworkReplicationCensusDiagnostics Capture()
        {
            var kinds = new NetworkReplicationKindCensus[_kindNames.Length];
            for (var i = 0; i < kinds.Length; i++)
                kinds[i] = new NetworkReplicationKindCensus(_kindNames[i], _kindTypeIds[i],
                    _worldEntities[i], _observedEntities[i], _comparedEntities[i],
                    _changedEntities[i], _addedEntities[i], _removedEntities[i]);
            var records = new NetworkReplicationRecordCensus[_recordNames.Length];
            for (var i = 0; i < records.Length; i++)
                records[i] = new NetworkReplicationRecordCensus(_recordNames[i],
                    _recordTypeIds[i], _comparedRecords[i], _unchangedRecords[i]);
            return new NetworkReplicationCensusDiagnostics(_intervalTicks, _ticks,
                _stopwatchTicks, _failures, _scopeComparisons,
                _scopeComparisonsUnavailable, _peersWithoutCapture, kinds, records);
        }

        private void CompareEntity(ReadOnlySpan<byte> previousBytes,
            SnapshotLayoutView previousLayout, in SnapshotEntityLayout previous,
            ReadOnlySpan<byte> currentBytes, SnapshotLayoutView currentLayout,
            in SnapshotEntityLayout current, int kind)
        {
            var unchanged = previousBytes.Slice(previous.RawOffset, previous.RawLength)
                .SequenceEqual(currentBytes.Slice(current.RawOffset, current.RawLength));
            if (kind >= 0)
            {
                _comparedEntities[kind]++;
                if (!unchanged)
                    _changedEntities[kind]++;
            }
            if (previous.RecordCount != current.RecordCount)
                return;
            for (var i = 0; i < current.RecordCount; i++)
            {
                var before = previousLayout.Records[previous.RecordStart + i];
                var after = currentLayout.Records[current.RecordStart + i];
                if (before.TypeId != after.TypeId || before.Kind != after.Kind)
                    return;
            }
            for (var i = 0; i < current.RecordCount; i++)
            {
                var before = previousLayout.Records[previous.RecordStart + i];
                var after = currentLayout.Records[current.RecordStart + i];
                if (!_recordByTypeId.TryGetValue(after.TypeId, out var record))
                    continue;
                _comparedRecords[record]++;
                if (unchanged || previousBytes.Slice(before.RawOffset, before.RawLength)
                        .SequenceEqual(currentBytes.Slice(after.RawOffset, after.RawLength)))
                    _unchangedRecords[record]++;
            }
        }

        private int KindOf(ReadOnlySpan<byte> bytes, in SnapshotEntityLayout entity)
        {
            var typeId = SnapshotDeltaCodec.ReadEntityKind(bytes, in entity);
            return _kindByTypeId.TryGetValue(typeId, out var kind) ? kind : -1;
        }
    }
}
