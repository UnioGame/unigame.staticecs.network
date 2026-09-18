namespace UniGame.StaticEcs.Network
{
    using System;
    using System.Buffers;
    using System.Collections.Generic;
    using FFS.Libraries.StaticEcs;
    using FFS.Libraries.StaticPack;

    /// <summary>Captures generated authority entities and applies transactional full snapshots.</summary>
    public sealed class NetworkReplicator<TWorld> : IDisposable
        where TWorld : struct, IWorldType
    {
        private readonly NetworkSchema<TWorld> _schema;
        private readonly NetworkBufferPool _bufferPool;
        private readonly bool _ownsBufferPool;
        private readonly Dictionary<EntityGID, NetworkReplicaEntry> _replicas =
            new Dictionary<EntityGID, NetworkReplicaEntry>();
        private readonly List<World<TWorld>.Entity> _captureEntities =
            new List<World<TWorld>.Entity>();
        private readonly HashSet<EntityGID> _captureSeen = new HashSet<EntityGID>();
        private readonly HashSet<EntityGID> _incoming = new HashSet<EntityGID>();
        private readonly List<EntityGID> _removed = new List<EntityGID>();
        private readonly List<NetworkReplicaEntry> _replicaScratch =
            new List<NetworkReplicaEntry>();
        private readonly object _owner = new object();
        private readonly NetworkScopeSelector<TWorld> _scopeSelector;
        private readonly INetworkScopeProvider<TWorld> _scopeProvider;
        private readonly INetworkCellularScopeProvider<TWorld> _cellularProvider;
        private readonly NetworkReplicaSkipPolicy<TWorld> _skipPolicy;
        private readonly INetworkUpdatePolicy<TWorld> _updatePolicy;
        private readonly Dictionary<ScopeId, ScopeCaptureCache> _updateCache =
            new Dictionary<ScopeId, ScopeCaptureCache>();
        // NCORE-15b: cell-level capture cache for the merge-capture path (see CaptureViaCells).
        // Reuses ScopeCaptureCache's shape (bytes + per-entity index for the NCORE-16 hold
        // policy) keyed by cell id instead of by peer scope id; CapturedTick lets a scope built
        // later in the same tick from an already-captured cell skip re-serializing it entirely.
        private readonly Dictionary<ScopeId, ScopeCaptureCache> _cellCache =
            new Dictionary<ScopeId, ScopeCaptureCache>();
        private readonly int[] _cellCursors = new int[NetworkCellularScopeLimits.MaxCellsPerScope];
        // Resolved once per cell per Capture call (see CaptureViaCells) so the k-way merge below
        // indexes a plain array instead of repeating a Dictionary<ScopeId, _> lookup (hash +
        // bucket walk) for every single entity it merges.
        private readonly ScopeCaptureCache[] _cellCacheRefs =
            new ScopeCaptureCache[NetworkCellularScopeLimits.MaxCellsPerScope];
        private readonly List<ScopeId> _staleCellIds = new List<ScopeId>();
        // Cells not touched by any scope for this many ticks are evicted from _cellCache so a
        // world with many transient cells (players passing through) does not grow it forever.
        private const uint CellCacheStaleAfterTicks = 64;
        private readonly NetworkSnapshotPool _snapshotPool;
        private int _captureCapacity = 4096;
        private int _cellCaptureCapacity = 4096;
        // NCORE-16: 4 (typeId) + 1 (schema kind) + 1 (version) + 1 (disabled) + 4 (payload length),
        // exactly the fixed record header Capture() itself writes below, before the payload bytes.
        private const int RecordHeaderSize = 11;

        /// <summary>Creates a client-side snapshot apply replicator.</summary>
        /// <param name="skipPolicy">
        /// Opt-in, off by default. When null (the default), every applied entity's records are
        /// always fully re-applied, matching this type's original, always-safe behavior exactly.
        /// Supply a policy only after confirming no client-side system writes the covered
        /// entities' replicated components between applies; see
        /// <see cref="NetworkReplicaSkipPolicy{TWorld}"/> for the exact contract.
        /// </param>
        public NetworkReplicator(NetworkSchema<TWorld> schema,
            ScopeId scope = default, int historyTicks = 64,
            long historyBytes = 32 * 1024 * 1024,
            NetworkBufferPool bufferPool = null,
            NetworkReplicaSkipPolicy<TWorld> skipPolicy = null)
        {
            _schema = schema ?? throw new ArgumentNullException(nameof(schema));
            _bufferPool = bufferPool ??
                new NetworkBufferPool(NetworkBufferPool.DefaultClientRetainedBytes);
            _ownsBufferPool = bufferPool == null;
            _skipPolicy = skipPolicy;
            _snapshotPool = new NetworkSnapshotPool(historyTicks + 2);
            Scope = scope;
            History = new NetworkHistory<NetworkSnapshot>(historyTicks, historyBytes,
                value => value.ByteLength, value => value.Dispose());
        }

        /// <summary>Creates an authority replicator with an active-scope selector.</summary>
        /// <param name="scopeProvider">
        /// Opt-in, off by default (NCORE-15). When supplied, <see cref="Capture"/> collects a
        /// scope's entities directly from <see cref="INetworkScopeProvider{TWorld}.CollectEntities"/>
        /// instead of collecting every generated entity and filtering it through
        /// <paramref name="scopeSelector"/>; <paramref name="scopeSelector"/> is still required but is
        /// not consulted while a provider is present. Leaving this null keeps capture behavior and
        /// cost byte-for-byte identical to before this parameter existed.
        /// </param>
        /// <param name="updatePolicy">
        /// Opt-in, off by default (NCORE-16). See <see cref="INetworkUpdatePolicy{TWorld}"/> for the
        /// exact contract. When supplied, <see cref="Capture"/> consults it once per entity and, for
        /// a "hold" answer on an entity this scope already captured previously, reuses that entity's
        /// previously captured record bytes verbatim instead of invoking
        /// <see cref="IRecordNetworkInvoker{TWorld}.Write"/> again. Leaving this null keeps capture
        /// behavior and cost byte-for-byte identical to before this parameter existed.
        /// </param>
        public NetworkReplicator(NetworkSchema<TWorld> schema,
            NetworkScopeSelector<TWorld> scopeSelector, ScopeId scope = default,
            int historyTicks = 64, long historyBytes = 32 * 1024 * 1024,
            NetworkBufferPool bufferPool = null,
            INetworkScopeProvider<TWorld> scopeProvider = null,
            INetworkUpdatePolicy<TWorld> updatePolicy = null)
            : this(schema, scope, historyTicks, historyBytes, bufferPool)
        {
            _scopeSelector = scopeSelector ??
                throw new ArgumentNullException(nameof(scopeSelector));
            _scopeProvider = scopeProvider;
            _cellularProvider = scopeProvider as INetworkCellularScopeProvider<TWorld>;
            _updatePolicy = updatePolicy;
        }

        /// <summary>
        /// Drops any cached previous-capture bytes/index this scope holds for the NCORE-16 update
        /// policy (a no-op when no policy is in use or the scope was never captured with one).
        /// Callers should invoke this once a scope has no established peer left, mirroring how its
        /// shared capture history is dropped, so an abandoned scope's held-entity cache does not
        /// linger for the life of the server.
        /// </summary>
        internal void ForgetScope(ScopeId scope) => _updateCache.Remove(scope);

        /// <summary>Gets the isolated replication scope.</summary>
        public ScopeId Scope { get; private set; }

        /// <summary>
        /// Adopts a new replication scope outside construction (NCORE-15). Used by
        /// <see cref="NetworkClient{TWorld}"/> to accept a scope carried by an incoming keyframe.
        /// Does not touch replica bookkeeping: the next <see cref="Apply"/> call's incoming/removed
        /// diff naturally destroys replicas absent from the new scope's keyframe and creates
        /// whatever is newly present, exactly like an entity leaving and re-entering relevance.
        /// </summary>
        internal void SetScope(ScopeId scope) => Scope = scope;
        /// <summary>Gets bounded snapshots successfully applied by this client.</summary>
        public NetworkHistory<NetworkSnapshot> History { get; }

        /// <inheritdoc />
        public void Dispose()
        {
            History.Clear();
            if (_ownsBufferPool)
                _bufferPool.Dispose();
        }

        /// <summary>Captures all generated authority entity kinds.</summary>
        public SnapshotCaptureResult Capture(uint serverTick,
            out NetworkSnapshot snapshot) => Capture(serverTick, Scope, out snapshot);

        /// <summary>Captures authority entities for one server replication scope.</summary>
        public SnapshotCaptureResult Capture(uint serverTick, ScopeId scope,
            out NetworkSnapshot snapshot)
        {
            if (_scopeSelector == null)
                throw new InvalidOperationException(
                    "Authority capture requires an explicit scope selector.");
            snapshot = null;
            if (World<TWorld>.Status != WorldStatus.Initialized)
                return SnapshotCaptureResult.WorldUnavailable;

            // NCORE-15b: a provider that can decompose a scope into its member cells lets
            // capture cost scale with occupied *cells* rather than occupied *scopes* -- see
            // CaptureViaCells's doc comment. Every other provider (or none) keeps the exact
            // capture path and cost that existed before this optimization.
            if (_cellularProvider != null)
                return CaptureViaCells(serverTick, scope, out snapshot);

            _captureEntities.Clear();
            _captureSeen.Clear();
            var entries = _schema.RetainedEntries;
            if (_scopeProvider != null)
            {
                // NCORE-15: a spatial (or otherwise position-aware) provider enumerates only this
                // scope's entities from its own index, so capture cost scales with one scope's
                // population instead of the whole world. The selector is not consulted here.
                _scopeProvider.CollectEntities(scope, _captureEntities, _captureSeen);
            }
            else
            {
                for (var i = 0; i < entries.Length; i++)
                    if (entries[i].Invoker is IEntityNetworkInvoker<TWorld> invoker)
                        invoker.Collect(_captureEntities, _captureSeen);
                for (var i = _captureEntities.Count - 1; i >= 0; i--)
                    if (!_scopeSelector(scope, _captureEntities[i]))
                        _captureEntities.RemoveAt(i);
            }
            if (_captureEntities.Count > ProtocolLimits.MaxEntities)
                return SnapshotCaptureResult.LimitExceeded;
            _captureEntities.Sort(EntityComparer.Instance);

            var buffer = _bufferPool.Rent(_captureCapacity);
            var writer = BinaryPackWriter.Create(buffer.Buffer);
            var records = 0;

            // NCORE-16: read side of the update-policy hold cache. `previousBytes`/`previousIndex`
            // are this scope's own last successful capture, untouched for the rest of this call, so
            // reading them while building this tick's `freshIndex` below is always safe -- the cache
            // object itself is only overwritten once, in bulk, after this entire loop finishes.
            ScopeCaptureCache updateCache = null;
            byte[] previousBytes = null;
            Dictionary<EntityGID, HeldRecordRange> previousIndex = null;
            Dictionary<EntityGID, HeldRecordRange> freshIndex = null;
            if (_updatePolicy != null)
            {
                if (!_updateCache.TryGetValue(scope, out updateCache))
                {
                    updateCache = new ScopeCaptureCache();
                    _updateCache.Add(scope, updateCache);
                }
                else
                {
                    previousBytes = updateCache.Bytes;
                    previousIndex = updateCache.Index;
                }
                freshIndex = new Dictionary<EntityGID, HeldRecordRange>(_captureEntities.Count);
            }

            try
            {
                writer.WriteInt(_captureEntities.Count);
                for (var i = 0; i < _captureEntities.Count; i++)
                {
                    var entity = _captureEntities[i];
                    if (i > 0 && Compare(_captureEntities[i - 1].GID, entity.GID) >= 0)
                        return FailCapture(buffer, writer.Buffer,
                            SnapshotCaptureResult.InvalidEntity);
                    NetworkSchemaEntry kind = null;
                    for (var j = 0; j < entries.Length; j++)
                    {
                        if (entries[j].Invoker is IEntityNetworkInvoker<TWorld> invoker &&
                            invoker.Matches(entity))
                        {
                            kind = entries[j];
                            break;
                        }
                    }
                    if (kind == null)
                        return FailCapture(buffer, writer.Buffer,
                            SnapshotCaptureResult.InvalidEntity);

                    writer.WriteUlong(entity.GID.Raw);
                    writer.WriteUint(kind.TypeId.Value);
                    writer.WriteBool(entity.IsDisabled);
                    var recordCountPosition = writer.MakePoint(sizeof(ushort));
                    var recordsStart = writer.Position;
                    var entityRecords = 0;

                    // NCORE-16: an entity the policy asks to hold, that this scope's previous
                    // capture already knows about under the same schema kind, becomes a candidate
                    // for reusing previously captured record bytes below. Anything else (no
                    // previous capture yet for this scope, entity new to it, kind mismatch, or the
                    // policy itself saying "refresh") always falls through to a fresh write per
                    // record, exactly like before this feature existed.
                    var holdCandidate = false;
                    var previousRecordCursor = 0;
                    var previousRecordsRemaining = 0;
                    if (previousIndex != null &&
                        previousIndex.TryGetValue(entity.GID, out var previousRange) &&
                        previousRange.Kind == kind.TypeId &&
                        _updatePolicy.ShouldHold(serverTick, scope, entity, entity.GID))
                    {
                        holdCandidate = true;
                        previousRecordCursor = previousRange.RecordsOffset;
                        previousRecordsRemaining = previousRange.RecordCount;
                    }

                    for (var j = 0; j < entries.Length; j++)
                    {
                        if (entries[j].Invoker is not IRecordNetworkInvoker<TWorld> invoker ||
                            !invoker.Has(entity))
                            continue;
                        if (entityRecords == ProtocolLimits.MaxRecordsPerEntity)
                            return FailCapture(buffer, writer.Buffer,
                                SnapshotCaptureResult.LimitExceeded);
                        var entry = entries[j];

                        // Both this loop and the one that captured `previousBytes` walk `entries`
                        // in the same fixed schema order, filtered by Has(); advancing a single
                        // forward cursor over the previous capture's records is therefore a plain
                        // merge join keyed by TypeId, never a re-scan from the start.
                        if (holdCandidate)
                        {
                            while (previousRecordsRemaining > 0 &&
                                   Hashing.Read32(previousBytes, previousRecordCursor) <
                                   entry.TypeId.Value)
                            {
                                var skipLength = unchecked((int)Hashing.Read32(previousBytes,
                                    previousRecordCursor + 7));
                                previousRecordCursor += RecordHeaderSize + skipLength;
                                previousRecordsRemaining--;
                            }
                            if (previousRecordsRemaining > 0 &&
                                Hashing.Read32(previousBytes, previousRecordCursor) ==
                                entry.TypeId.Value &&
                                previousBytes[previousRecordCursor + 4] == (byte)entry.Kind &&
                                previousBytes[previousRecordCursor + 5] == entry.Version)
                            {
                                var heldLength = unchecked((int)Hashing.Read32(previousBytes,
                                    previousRecordCursor + 7));
                                var blockLength = RecordHeaderSize + heldLength;
                                writer.EnsureSize((uint)blockLength);
                                Array.Copy(previousBytes, previousRecordCursor, writer.Buffer,
                                    (int)writer.Position, blockLength);
                                writer.Position += (uint)blockLength;
                                previousRecordCursor += blockLength;
                                previousRecordsRemaining--;
                                entityRecords++;
                                records++;
                                continue;
                            }
                        }

                        writer.WriteUint(entry.TypeId.Value);
                        writer.WriteByte((byte)entry.Kind);
                        writer.WriteByte(entry.Version);
                        writer.WriteBool(invoker.IsDisabled(entity));
                        var lengthPosition = writer.MakePoint(sizeof(uint));
                        var payloadStart = writer.Position;
                        invoker.Write(entity, ref writer, entry.MaxBytes);
                        writer.WriteUintAt(lengthPosition, writer.Position - payloadStart);
                        entityRecords++;
                        records++;
                    }
                    writer.WriteUshortAt(recordCountPosition,
                        checked((ushort)entityRecords));

                    if (freshIndex != null)
                        freshIndex[entity.GID] = new HeldRecordRange(kind.TypeId,
                            checked((int)recordsStart),
                            checked((ushort)entityRecords));
                }
            }
            catch
            {
                return FailCapture(buffer, writer.Buffer,
                    SnapshotCaptureResult.HookFailed);
            }

            if (writer.Position > ProtocolLimits.MaxDecodedPayloadBytes)
                return FailCapture(buffer, writer.Buffer,
                    SnapshotCaptureResult.LimitExceeded);

            // NCORE-16: publish this tick's bytes/index as the next capture's "previous" state.
            // `previousBytes`/`previousIndex` are never read again after this point, so reusing the
            // same backing array in place (growing it only when this tick's payload is larger) is
            // safe and avoids a per-tick allocation once a scope's capture size stabilizes.
            if (freshIndex != null)
            {
                var finalLength = checked((int)writer.Position);
                if (updateCache.Bytes.Length < finalLength)
                    updateCache.Bytes = new byte[finalLength];
                Array.Copy(writer.Buffer, 0, updateCache.Bytes, 0, finalLength);
                updateCache.Length = finalLength;
                updateCache.Index = freshIndex;
            }
            if (!ReferenceEquals(writer.Buffer, buffer.Buffer))
            {
                buffer.Dispose();
                buffer = _bufferPool.Adopt(writer.Buffer, checked((int)writer.Position));
            }
            else
            {
                buffer.SetLength(checked((int)writer.Position));
            }
            _captureCapacity = Math.Max(_captureCapacity, buffer.Capacity);
            snapshot = _snapshotPool.Rent(serverTick, _schema.Fingerprint, scope,
                buffer, _captureEntities.Count, records);
            return SnapshotCaptureResult.Success;
        }

        // NCORE-15b: builds one scope's canonical snapshot by k-way merging its neighbourhood
        // cells' already-encoded entity blocks, instead of re-collecting and re-serializing
        // every entity of every one of those cells on every single scope's capture. Cells
        // overlap between neighbouring scopes by construction (NCORE-15's 3x3 neighbourhood), so
        // in a spread population capture cost used to scale with occupied scopes times 9; here it
        // scales with occupied cells once, plus a cheap byte-copy merge per scope.
        //
        // Each occupied cell is serialized exactly once per tick, the first time any scope that
        // touches it is captured this tick (tracked by ScopeCaptureCache.CapturedTick); every
        // other scope sharing that cell this same tick reuses its bytes verbatim. The NCORE-16
        // update-hold policy still applies underneath -- CaptureCellInto consults it exactly like
        // the plain per-scope path used to, just keyed by cell instead of by scope, which is
        // strictly finer-grained (a cell is, at most, as large as a scope was before cells
        // existed) so held entities keep reusing their previously captured bytes.
        private SnapshotCaptureResult CaptureViaCells(uint serverTick, ScopeId scope,
            out NetworkSnapshot snapshot)
        {
            snapshot = null;
            Span<ScopeId> cellIds = stackalloc ScopeId[NetworkCellularScopeLimits.MaxCellsPerScope];
            var cellCount = _cellularProvider.CollectScopeCells(scope, cellIds);
            if ((uint)cellCount > (uint)NetworkCellularScopeLimits.MaxCellsPerScope)
                return SnapshotCaptureResult.LimitExceeded;

            EvictStaleCellCache(serverTick);

            var totalEntities = 0;
            var totalRecords = 0;
            for (var i = 0; i < cellCount; i++)
            {
                if (!_cellCache.TryGetValue(cellIds[i], out var cache))
                {
                    cache = new ScopeCaptureCache();
                    _cellCache.Add(cellIds[i], cache);
                }
                if (!cache.HasCapturedTick || cache.CapturedTick != serverTick)
                {
                    var cellResult = CaptureCellInto(serverTick, cellIds[i], cache);
                    if (cellResult != SnapshotCaptureResult.Success)
                        return cellResult;
                }
                _cellCacheRefs[i] = cache;
                totalEntities += cache.Blocks.Count;
                totalRecords += cache.RecordCount;
                _cellCursors[i] = 0;
            }
            if (totalEntities > ProtocolLimits.MaxEntities)
                return SnapshotCaptureResult.LimitExceeded;

            var buffer = _bufferPool.Rent(_captureCapacity);
            var writer = BinaryPackWriter.Create(buffer.Buffer);
            var merged = 0;
            try
            {
                writer.WriteInt(totalEntities);
                var hasPrevious = false;
                EntityGID previousGid = default;
                while (true)
                {
                    var bestSlot = -1;
                    for (var i = 0; i < cellCount; i++)
                    {
                        var cache = _cellCacheRefs[i];
                        if (_cellCursors[i] >= cache.Blocks.Count)
                            continue;
                        if (bestSlot < 0 || Compare(cache.Blocks[_cellCursors[i]].Gid,
                                _cellCacheRefs[bestSlot].Blocks[_cellCursors[bestSlot]].Gid) < 0)
                            bestSlot = i;
                    }
                    if (bestSlot < 0)
                        break;
                    var bestCache = _cellCacheRefs[bestSlot];
                    var block = bestCache.Blocks[_cellCursors[bestSlot]];
                    _cellCursors[bestSlot]++;
                    // Cells must partition entities (each entity belongs to exactly one cell);
                    // a duplicate or out-of-order GID here means a provider bug, not bad wire
                    // data, but is still reported the same way Capture always reports an
                    // internal invariant violation, rather than silently producing a corrupt
                    // canonical snapshot.
                    if (hasPrevious && Compare(previousGid, block.Gid) >= 0)
                        return FailCapture(buffer, writer.Buffer,
                            SnapshotCaptureResult.InvalidEntity);
                    previousGid = block.Gid;
                    hasPrevious = true;
                    writer.EnsureSize((uint)block.Length);
                    Array.Copy(bestCache.Bytes, block.Offset, writer.Buffer,
                        (int)writer.Position, block.Length);
                    writer.Position += (uint)block.Length;
                    merged++;
                }
            }
            catch
            {
                return FailCapture(buffer, writer.Buffer, SnapshotCaptureResult.HookFailed);
            }
            if (merged != totalEntities)
                return FailCapture(buffer, writer.Buffer, SnapshotCaptureResult.InvalidEntity);
            if (writer.Position > ProtocolLimits.MaxDecodedPayloadBytes)
                return FailCapture(buffer, writer.Buffer, SnapshotCaptureResult.LimitExceeded);

            if (!ReferenceEquals(writer.Buffer, buffer.Buffer))
            {
                buffer.Dispose();
                buffer = _bufferPool.Adopt(writer.Buffer, checked((int)writer.Position));
            }
            else
            {
                buffer.SetLength(checked((int)writer.Position));
            }
            _captureCapacity = Math.Max(_captureCapacity, buffer.Capacity);
            snapshot = _snapshotPool.Rent(serverTick, _schema.Fingerprint, scope,
                buffer, totalEntities, totalRecords);
            return SnapshotCaptureResult.Success;
        }

        // Serializes exactly one cell's entities into `cache`, applying the NCORE-16
        // update-hold policy (keyed by this cell id, if a policy is configured) against that
        // same cell's previous capture, then publishing this tick's bytes/index/blocks as its
        // new state. Mirrors the plain per-scope Capture body almost exactly -- the difference
        // is entirely in what gets cached (per cell, with an ascending-GID block index for
        // merging) rather than in how a single entity gets serialized.
        private SnapshotCaptureResult CaptureCellInto(uint serverTick, ScopeId cellId,
            ScopeCaptureCache cache)
        {
            _captureEntities.Clear();
            _captureSeen.Clear();
            _cellularProvider.CollectCellEntities(cellId, _captureEntities, _captureSeen);
            if (_captureEntities.Count > ProtocolLimits.MaxEntities)
                return SnapshotCaptureResult.LimitExceeded;

            // Fast path for an empty cell -- common in a spread population, where most of a
            // scope's 3x3 neighbourhood is unoccupied: publish "nothing here this tick" without
            // renting a buffer, creating a writer, or touching Blocks/Index at all. Cheap enough
            // that CaptureViaCells does not need to special-case unoccupied cells itself.
            if (_captureEntities.Count == 0)
            {
                cache.Blocks.Clear();
                cache.RecordCount = 0;
                cache.CapturedTick = serverTick;
                cache.HasCapturedTick = true;
                return SnapshotCaptureResult.Success;
            }
            _captureEntities.Sort(EntityComparer.Instance);

            var entries = _schema.RetainedEntries;
            var buffer = _bufferPool.Rent(_cellCaptureCapacity);
            var writer = BinaryPackWriter.Create(buffer.Buffer);
            var records = 0;
            var blocks = cache.Blocks;
            blocks.Clear();

            // NCORE-16: read side of the update-policy hold cache, exactly like the plain
            // per-scope path, except the "previous capture" is this cell's, not a peer scope's.
            // `previousBytes`/`previousIndex` are this cell's own last successful capture,
            // untouched for the rest of this call (the cache object itself is only overwritten
            // once, in bulk, after this entire loop finishes), so reading them into this tick's
            // `freshIndex` while writing into a *different* rented buffer is always safe.
            byte[] previousBytes = null;
            Dictionary<EntityGID, HeldRecordRange> previousIndex = null;
            Dictionary<EntityGID, HeldRecordRange> freshIndex = null;
            if (_updatePolicy != null)
            {
                if (cache.HasCapturedTick)
                {
                    previousBytes = cache.Bytes;
                    previousIndex = cache.Index;
                }
                freshIndex = new Dictionary<EntityGID, HeldRecordRange>(_captureEntities.Count);
            }

            try
            {
                for (var i = 0; i < _captureEntities.Count; i++)
                {
                    var entity = _captureEntities[i];
                    if (i > 0 && Compare(_captureEntities[i - 1].GID, entity.GID) >= 0)
                        return FailCapture(buffer, writer.Buffer,
                            SnapshotCaptureResult.InvalidEntity);
                    NetworkSchemaEntry kind = null;
                    for (var j = 0; j < entries.Length; j++)
                    {
                        if (entries[j].Invoker is IEntityNetworkInvoker<TWorld> invoker &&
                            invoker.Matches(entity))
                        {
                            kind = entries[j];
                            break;
                        }
                    }
                    if (kind == null)
                        return FailCapture(buffer, writer.Buffer,
                            SnapshotCaptureResult.InvalidEntity);

                    var blockStart = writer.Position;
                    writer.WriteUlong(entity.GID.Raw);
                    writer.WriteUint(kind.TypeId.Value);
                    writer.WriteBool(entity.IsDisabled);
                    var recordCountPosition = writer.MakePoint(sizeof(ushort));
                    var recordsStart = writer.Position;
                    var entityRecords = 0;

                    var holdCandidate = false;
                    var previousRecordCursor = 0;
                    var previousRecordsRemaining = 0;
                    if (previousIndex != null &&
                        previousIndex.TryGetValue(entity.GID, out var previousRange) &&
                        previousRange.Kind == kind.TypeId &&
                        _updatePolicy.ShouldHold(serverTick, cellId, entity, entity.GID))
                    {
                        holdCandidate = true;
                        previousRecordCursor = previousRange.RecordsOffset;
                        previousRecordsRemaining = previousRange.RecordCount;
                    }

                    for (var j = 0; j < entries.Length; j++)
                    {
                        if (entries[j].Invoker is not IRecordNetworkInvoker<TWorld> invoker ||
                            !invoker.Has(entity))
                            continue;
                        if (entityRecords == ProtocolLimits.MaxRecordsPerEntity)
                            return FailCapture(buffer, writer.Buffer,
                                SnapshotCaptureResult.LimitExceeded);
                        var entry = entries[j];

                        if (holdCandidate)
                        {
                            while (previousRecordsRemaining > 0 &&
                                   Hashing.Read32(previousBytes, previousRecordCursor) <
                                   entry.TypeId.Value)
                            {
                                var skipLength = unchecked((int)Hashing.Read32(previousBytes,
                                    previousRecordCursor + 7));
                                previousRecordCursor += RecordHeaderSize + skipLength;
                                previousRecordsRemaining--;
                            }
                            if (previousRecordsRemaining > 0 &&
                                Hashing.Read32(previousBytes, previousRecordCursor) ==
                                entry.TypeId.Value &&
                                previousBytes[previousRecordCursor + 4] == (byte)entry.Kind &&
                                previousBytes[previousRecordCursor + 5] == entry.Version)
                            {
                                var heldLength = unchecked((int)Hashing.Read32(previousBytes,
                                    previousRecordCursor + 7));
                                var blockLength = RecordHeaderSize + heldLength;
                                writer.EnsureSize((uint)blockLength);
                                Array.Copy(previousBytes, previousRecordCursor, writer.Buffer,
                                    (int)writer.Position, blockLength);
                                writer.Position += (uint)blockLength;
                                previousRecordCursor += blockLength;
                                previousRecordsRemaining--;
                                entityRecords++;
                                records++;
                                continue;
                            }
                        }

                        writer.WriteUint(entry.TypeId.Value);
                        writer.WriteByte((byte)entry.Kind);
                        writer.WriteByte(entry.Version);
                        writer.WriteBool(invoker.IsDisabled(entity));
                        var lengthPosition = writer.MakePoint(sizeof(uint));
                        var payloadStart = writer.Position;
                        invoker.Write(entity, ref writer, entry.MaxBytes);
                        writer.WriteUintAt(lengthPosition, writer.Position - payloadStart);
                        entityRecords++;
                        records++;
                    }
                    writer.WriteUshortAt(recordCountPosition,
                        checked((ushort)entityRecords));

                    if (freshIndex != null)
                        freshIndex[entity.GID] = new HeldRecordRange(kind.TypeId,
                            checked((int)recordsStart),
                            checked((ushort)entityRecords));
                    blocks.Add(new EntityBlockRange(entity.GID, checked((int)blockStart),
                        checked((int)(writer.Position - blockStart))));
                }
            }
            catch
            {
                return FailCapture(buffer, writer.Buffer, SnapshotCaptureResult.HookFailed);
            }

            if (writer.Position > ProtocolLimits.MaxDecodedPayloadBytes)
                return FailCapture(buffer, writer.Buffer, SnapshotCaptureResult.LimitExceeded);

            var finalLength = checked((int)writer.Position);
            if (cache.Bytes.Length < finalLength)
                cache.Bytes = new byte[finalLength];
            Array.Copy(writer.Buffer, 0, cache.Bytes, 0, finalLength);
            cache.Length = finalLength;
            // `blocks` is `cache.Blocks` itself (cleared and repopulated above), not a separate
            // list, so publishing it is already done -- nothing to reassign here.
            cache.RecordCount = records;
            cache.CapturedTick = serverTick;
            cache.HasCapturedTick = true;
            if (freshIndex != null)
                cache.Index = freshIndex;

            if (!ReferenceEquals(writer.Buffer, buffer.Buffer))
            {
                // The writer outgrew the rented scratch buffer -- remember the larger size so
                // the *next* capture of this cell rents enough up front instead of growing (and
                // reallocating) again every single tick.
                _cellCaptureCapacity = Math.Max(_cellCaptureCapacity, writer.Buffer.Length);
                var resized = _bufferPool.Adopt(writer.Buffer, 0);
                resized.Dispose();
            }
            else
            {
                _cellCaptureCapacity = Math.Max(_cellCaptureCapacity, buffer.Capacity);
            }
            buffer.Dispose();
            return SnapshotCaptureResult.Success;
        }

        private void EvictStaleCellCache(uint serverTick)
        {
            if (serverTick <= CellCacheStaleAfterTicks || _cellCache.Count == 0)
                return;
            var threshold = serverTick - CellCacheStaleAfterTicks;
            _staleCellIds.Clear();
            foreach (var pair in _cellCache)
                if (!pair.Value.HasCapturedTick || pair.Value.CapturedTick < threshold)
                    _staleCellIds.Add(pair.Key);
            for (var i = 0; i < _staleCellIds.Count; i++)
                _cellCache.Remove(_staleCellIds[i]);
        }

        internal NetworkSnapshot CreateSnapshot(uint serverTick,
            SchemaFingerprint fingerprint, ScopeId scope, NetworkBufferLease bytes,
            int entities, int records) => _snapshotPool.Rent(serverTick, fingerprint,
            scope, bytes, entities, records);

        /// <summary>
        /// Accepts an already schema-matched, hash-verified canonical snapshot as this
        /// replicator's new baseline without parsing a single entity or record, and without
        /// creating, mutating, destroying, or even requiring <c>World&lt;TWorld&gt;</c> to exist.
        /// Opt-in substitute for <see cref="Stage"/> + <see cref="Apply"/> for a caller that only
        /// needs ACK/baseline progression identical to a full client — e.g. NCORE-26b's light
        /// load-generator client, which must ACK every snapshot exactly when a full client would,
        /// but never needs gameplay state. The snapshot is stored in <see cref="History"/> exactly
        /// like <see cref="Apply"/> stores it at the end of a successful call (see its final
        /// <c>History.Store</c>), so it is a valid baseline for the next delta this replicator's
        /// owning <see cref="NetworkClient{TWorld}"/> receives.
        /// <para>
        /// Every replica-tracking invariant <see cref="Apply"/> maintains (the incoming/removed
        /// diff, per-entity <c>AppliedBytes</c>) exists only for entities this replicator actually
        /// created in an ECS world through <see cref="Apply"/>; a caller that only ever calls
        /// <see cref="AcceptCanonical"/> on this replicator has no such entities, so skipping that
        /// bookkeeping here changes nothing observable. Mixing <see cref="Apply"/> and
        /// <see cref="AcceptCanonical"/> calls on the same replicator instance is unsupported and
        /// not exercised by any caller in this package: <see cref="NetworkClient{TWorld}"/> only
        /// ever picks one apply mode for its whole lifetime (see its <c>canonicalOnlyApply</c>
        /// constructor parameter).
        /// </para>
        /// </summary>
        public SnapshotApplyResult AcceptCanonical(NetworkSnapshot snapshot)
        {
            if (snapshot == null || snapshot.ByteLength > ProtocolLimits.MaxDecodedPayloadBytes)
                return SnapshotApplyResult.LimitExceeded;
            if (snapshot.SchemaFingerprint != _schema.Fingerprint || snapshot.Scope != Scope)
                return SnapshotApplyResult.SchemaMismatch;
            History.Store(snapshot.ServerTick, snapshot);
            return SnapshotApplyResult.Success;
        }

        /// <summary>Validates bounds and schema without mutating ECS.</summary>
        public SnapshotApplyResult Stage(NetworkSnapshot snapshot,
            out StagedNetworkSnapshot staged)
        {
            staged = default;
            if (snapshot == null || snapshot.ByteLength > ProtocolLimits.MaxDecodedPayloadBytes)
                return SnapshotApplyResult.LimitExceeded;
            if (snapshot.SchemaFingerprint != _schema.Fingerprint ||
                snapshot.Scope != Scope)
                return SnapshotApplyResult.SchemaMismatch;
            if (Hashing.XxHash64(snapshot.Bytes.Span) != snapshot.PayloadHash)
                return SnapshotApplyResult.Malformed;

            var bytes = snapshot.Bytes.Span;
            var offset = 0;
            if (!TryReadInt(bytes, ref offset, out var count) || count < 0 ||
                count > ProtocolLimits.MaxEntities || count != snapshot.EntityCount)
                return SnapshotApplyResult.LimitExceeded;
            var entities = ArrayPool<StagedEntity>.Shared.Rent(Math.Max(1, count));
            var records = ArrayPool<StagedRecord>.Shared.Rent(
                Math.Max(1, snapshot.RecordCount));
            var recordIndex = 0;
            EntityGID previous = default;
            try
            {
                for (var i = 0; i < count; i++)
                {
                    if (!TryReadUlong(bytes, ref offset, out var raw) ||
                        !TryReadUint(bytes, ref offset, out var kindValue) ||
                        !TryReadByte(bytes, ref offset, out var disabledByte) ||
                        !TryReadUshort(bytes, ref offset, out var recordCount))
                        return FailStage(entities, i, records, recordIndex,
                            SnapshotApplyResult.Malformed);
                    var gid = new EntityGID(raw);
                    if (gid.Version == 0 || i > 0 && Compare(previous, gid) >= 0)
                        return FailStage(entities, i, records, recordIndex,
                            SnapshotApplyResult.Malformed);
                    previous = gid;
                    if (kindValue == 0 || !_schema.TryGet(new NetworkTypeId(kindValue),
                            out var kind) || kind.Kind != NetworkSchemaKind.Entity ||
                        kind.Invoker is not IEntityNetworkInvoker<TWorld>)
                        return FailStage(entities, i, records, recordIndex,
                            SnapshotApplyResult.SchemaMismatch);
                    if (recordCount > ProtocolLimits.MaxRecordsPerEntity ||
                        recordIndex > snapshot.RecordCount - recordCount)
                        return FailStage(entities, i, records, recordIndex,
                            SnapshotApplyResult.LimitExceeded);

                    var start = recordIndex;
                    var entityByteStart = offset;
                    NetworkSchemaEntry previousEntry = null;
                    for (var j = 0; j < recordCount; j++)
                    {
                        if (!TryReadUint(bytes, ref offset, out var idValue) ||
                            !TryReadByte(bytes, ref offset, out var wireKind) ||
                            !TryReadByte(bytes, ref offset, out var version) ||
                            !TryReadByte(bytes, ref offset, out var recordDisabled) ||
                            !TryReadInt(bytes, ref offset, out var length))
                            return FailStage(entities, i, records, recordIndex,
                                SnapshotApplyResult.Malformed);
                        if (idValue == 0 || length < 0 ||
                            length > ProtocolLimits.MaxComponentBytes ||
                            length > bytes.Length - offset)
                            return FailStage(entities, i, records, recordIndex,
                                SnapshotApplyResult.LimitExceeded);
                        if (!_schema.TryGet(new NetworkTypeId(idValue), out var entry) ||
                            entry.Kind != (NetworkSchemaKind)wireKind ||
                            entry.Version != version || length > entry.MaxBytes ||
                            entry.Invoker is not IRecordNetworkInvoker<TWorld> invoker)
                            return FailStage(entities, i, records, recordIndex,
                                SnapshotApplyResult.SchemaMismatch);
                        if (recordDisabled != 0 && !invoker.SupportsDisabled ||
                            previousEntry != null && Compare(previousEntry, entry) >= 0)
                            return FailStage(entities, i, records, recordIndex,
                                SnapshotApplyResult.Malformed);
                        previousEntry = entry;
                        records[recordIndex++] = new StagedRecord
                        {
                            Entry = entry,
                            Offset = offset,
                            Length = length,
                            Disabled = recordDisabled != 0,
                        };
                        offset += length;
                    }
                    entities[i] = new StagedEntity
                    {
                        Gid = gid,
                        Kind = kind,
                        Disabled = disabledByte != 0,
                        RecordStart = start,
                        RecordCount = recordCount,
                        ByteOffset = entityByteStart,
                        ByteLength = offset - entityByteStart,
                    };
                }
                if (offset != bytes.Length || recordIndex != snapshot.RecordCount)
                    return FailStage(entities, count, records, recordIndex,
                        SnapshotApplyResult.Malformed);
                for (var i = 0; i < count; i++)
                {
                    var source = entities[i];
                    if (!_replicas.TryGetValue(source.Gid, out var replica))
                        continue;
                    if (replica.KindId != source.Kind.TypeId ||
                        !replica.LocalGid.TryUnpack<TWorld>(out var existing) ||
                        !((IEntityNetworkInvoker<TWorld>)source.Kind.Invoker)
                            .Matches(existing))
                        return FailStage(entities, count, records, recordIndex,
                            SnapshotApplyResult.EntityConflict);
                }
                staged = new StagedNetworkSnapshot
                {
                    Owner = _owner,
                    Snapshot = snapshot,
                    Entities = entities,
                    Records = records,
                    EntityCount = count,
                    RecordCount = recordIndex,
                };
                return SnapshotApplyResult.Success;
            }
            catch
            {
                return FailStage(entities, count, records, recordIndex,
                    SnapshotApplyResult.Malformed);
            }
        }

        /// <summary>Applies a previously staged snapshot.</summary>
        public SnapshotApplyResult Apply(in StagedNetworkSnapshot staged)
        {
            if (staged.Snapshot == null || !ReferenceEquals(staged.Owner, _owner) ||
                staged.Fingerprint != _schema.Fingerprint || staged.Scope != Scope)
                return SnapshotApplyResult.SchemaMismatch;
            if (World<TWorld>.Status != WorldStatus.Initialized)
                return SnapshotApplyResult.Malformed;

            _incoming.Clear();
            for (var i = 0; i < staged.EntityCount; i++)
                _incoming.Add(staged.Entities[i].Gid);
            _removed.Clear();
            foreach (var pair in _replicas)
                if (!_incoming.Contains(pair.Key))
                    _removed.Add(pair.Key);
            for (var i = 0; i < _removed.Count; i++)
            {
                var sourceGid = _removed[i];
                var removedReplica = _replicas[sourceGid];
                if (removedReplica.LocalGid.TryUnpack<TWorld>(out var removed))
                    removed.Destroy();
                removedReplica.AppliedBytes?.Dispose();
                _replicas.Remove(sourceGid);
            }

            var buffer = staged.Snapshot.Buffer;
            var baseOffset = staged.Snapshot.Offset;
            var entries = _schema.RetainedEntries;
            for (var i = 0; i < staged.EntityCount; i++)
            {
                var source = staged.Entities[i];
                World<TWorld>.Entity entity;
                var hasReplica = _replicas.TryGetValue(source.Gid, out var replica);
                if (hasReplica)
                {
                    if (!replica.LocalGid.TryUnpack<TWorld>(out entity))
                        return SnapshotApplyResult.EntityConflict;
                }
                else
                {
                    entity = ((IEntityNetworkInvoker<TWorld>)source.Kind.Invoker).Create();
                }
                if (!((IEntityNetworkInvoker<TWorld>)source.Kind.Invoker).Matches(entity))
                    return SnapshotApplyResult.EntityConflict;

                // A canonical snapshot always carries every replicated entity's full current
                // record set, not only what changed on the wire (delta encoding only shrinks
                // the packet; SnapshotDeltaCodec reconstructs the complete per-entity byte
                // range before Stage() ever sees it). Re-decoding and re-applying that
                // unchanged byte-for-byte range every tick is the dominant per-snapshot
                // allocation on this path (NCORE-24): each record apply rents a scratch
                // buffer and re-runs the StaticPack read hook.
                //
                // Skipping that work when bytes are unchanged is only correct for an entity
                // no client-side system writes between applies: today's apply is also this
                // client's only correction mechanism for local writes (prediction,
                // reconciliation, interpolation) into replicated components, so skipping an
                // entity such a system can touch would let a local write persist uncorrected
                // until the server's bytes for it next change. _skipPolicy is null unless a
                // caller explicitly opted in per entity (see NetworkReplicaSkipPolicy), so by
                // default `unchanged` is always false here and every entity is fully
                // re-applied every tick, exactly as before this optimization existed.
                var unchanged = false;
                if (_skipPolicy != null && hasReplica &&
                    replica.Disabled == source.Disabled &&
                    replica.AppliedBytes != null &&
                    replica.AppliedBytes.Length == source.ByteLength)
                {
                    var sourceBytes = new ReadOnlySpan<byte>(buffer,
                        checked(baseOffset + source.ByteOffset), source.ByteLength);
                    unchanged = replica.AppliedBytes.Span.SequenceEqual(sourceBytes) &&
                        _skipPolicy(entity);
                }

                if (!unchanged)
                {
                    var sourceIndex = source.RecordStart;
                    var sourceEnd = source.RecordStart + source.RecordCount;
                    for (var j = 0; j < entries.Length; j++)
                    {
                        if (entries[j].Invoker is not IRecordNetworkInvoker<TWorld> invoker)
                            continue;
                        while (sourceIndex < sourceEnd &&
                               Compare(staged.Records[sourceIndex].Entry, entries[j]) < 0)
                            sourceIndex++;
                        if (sourceIndex >= sourceEnd ||
                            staged.Records[sourceIndex].Entry.TypeId != entries[j].TypeId)
                            invoker.Remove(entity);
                    }
                    for (var j = source.RecordStart; j < sourceEnd; j++)
                    {
                        var record = staged.Records[j];
                        ((IRecordNetworkInvoker<TWorld>)record.Entry.Invoker).Apply(entity,
                            buffer, checked(baseOffset + record.Offset), record.Length,
                            record.Entry.Version, record.Disabled);
                    }
                    if (source.Disabled)
                        entity.Disable();
                    else
                        entity.Enable();
                    entity.Set(new NetworkReplicaIdentityComponent
                    {
                        AuthorityGid = source.Gid,
                        KindId = source.Kind.TypeId,
                    });

                    // Only retain comparison bytes when a skip policy is actually in use:
                    // with no policy, `unchanged` above is always false, so this branch runs
                    // for every entity every tick, and retaining a lease here would add the
                    // exact per-tick lease churn NCORE-24 removes elsewhere for zero benefit.
                    // Once a policy is in use, only move the retained bytes forward when
                    // something actually applied — an unchanged entity's existing
                    // AppliedBytes are already byte-identical to this tick's bytes (that is
                    // what made it "unchanged"), so re-retaining here would be redundant.
                    NetworkBufferLease retainedBytes = null;
                    if (_skipPolicy != null)
                    {
                        retainedBytes = staged.Snapshot.RetainBytes(source.ByteOffset,
                            source.ByteLength);
                        if (hasReplica)
                            replica.AppliedBytes?.Dispose();
                    }
                    _replicas[source.Gid] = new NetworkReplicaEntry(entity.GID,
                        source.Kind.TypeId, source.Disabled, retainedBytes);
                }
            }
            History.Store(staged.ServerTick, staged.Snapshot);
            return SnapshotApplyResult.Success;
        }

        /// <summary>Destroys all client replicas and clears applied snapshot history.</summary>
        public void ClearReplicas()
        {
            if (World<TWorld>.Status != WorldStatus.Initialized)
            {
                foreach (var replica in _replicas.Values)
                    replica.AppliedBytes?.Dispose();
                _replicas.Clear();
                History.Clear();
                return;
            }
            _replicaScratch.Clear();
            foreach (var replica in _replicas.Values)
                _replicaScratch.Add(replica);
            for (var i = 0; i < _replicaScratch.Count; i++)
            {
                if (_replicaScratch[i].LocalGid.TryUnpack<TWorld>(out var entity))
                    entity.Destroy();
                _replicaScratch[i].AppliedBytes?.Dispose();
            }
            _replicas.Clear();
            History.Clear();
        }

        private SnapshotCaptureResult FailCapture(NetworkBufferLease buffer,
            byte[] writerBuffer, SnapshotCaptureResult result)
        {
            if (!ReferenceEquals(buffer.Buffer, writerBuffer))
            {
                var resized = _bufferPool.Adopt(writerBuffer, 0);
                resized.Dispose();
            }
            buffer.Dispose();
            return result;
        }

        private static SnapshotApplyResult FailStage(StagedEntity[] entities,
            int entityCount, StagedRecord[] records, int recordCount,
            SnapshotApplyResult result)
        {
            Array.Clear(entities, 0, Math.Min(entityCount, entities.Length));
            Array.Clear(records, 0, Math.Min(recordCount, records.Length));
            ArrayPool<StagedEntity>.Shared.Return(entities);
            ArrayPool<StagedRecord>.Shared.Return(records);
            return result;
        }

        private static int Compare(EntityGID left, EntityGID right)
        {
            var cluster = left.ClusterId.CompareTo(right.ClusterId);
            var id = left.Id.CompareTo(right.Id);
            return cluster != 0 ? cluster : id != 0 ? id :
                left.Version.CompareTo(right.Version);
        }

        private static int Compare(NetworkSchemaEntry left, NetworkSchemaEntry right)
        {
            // Enum.CompareTo(object) is the only overload NetworkSchemaKind has, so
            // calling it directly boxes the argument on every comparison; this runs
            // once per wire record on both Stage() and Apply()'s hot paths, so the
            // byte cast (whose CompareTo(byte) is a real, non-boxing IComparable<T>
            // implementation) is worth the small loss of readability.
            var kind = ((byte)left.Kind).CompareTo((byte)right.Kind);
            return kind != 0 ? kind : left.TypeId.CompareTo(right.TypeId);
        }

        private static bool TryReadByte(ReadOnlySpan<byte> bytes, ref int offset,
            out byte value)
        {
            if (offset >= bytes.Length)
            {
                value = 0;
                return false;
            }
            value = bytes[offset++];
            return true;
        }

        private static bool TryReadUshort(ReadOnlySpan<byte> bytes, ref int offset,
            out ushort value)
        {
            if (offset > bytes.Length - sizeof(ushort))
            {
                value = 0;
                return false;
            }
            value = (ushort)(bytes[offset] | bytes[offset + 1] << 8);
            offset += sizeof(ushort);
            return true;
        }

        private static bool TryReadInt(ReadOnlySpan<byte> bytes, ref int offset,
            out int value)
        {
            if (!TryReadUint(bytes, ref offset, out var raw))
            {
                value = 0;
                return false;
            }
            value = unchecked((int)raw);
            return true;
        }

        private static bool TryReadUint(ReadOnlySpan<byte> bytes, ref int offset,
            out uint value)
        {
            if (offset > bytes.Length - sizeof(uint))
            {
                value = 0;
                return false;
            }
            value = Hashing.Read32(bytes, offset);
            offset += sizeof(uint);
            return true;
        }

        private static bool TryReadUlong(ReadOnlySpan<byte> bytes, ref int offset,
            out ulong value)
        {
            if (offset > bytes.Length - sizeof(ulong))
            {
                value = 0;
                return false;
            }
            value = Hashing.Read64(bytes, offset);
            offset += sizeof(ulong);
            return true;
        }

        private sealed class EntityComparer : IComparer<World<TWorld>.Entity>
        {
            internal static readonly EntityComparer Instance = new EntityComparer();

            public int Compare(World<TWorld>.Entity left,
                World<TWorld>.Entity right) =>
                NetworkReplicator<TWorld>.Compare(left.GID, right.GID);
        }

        // NCORE-16: one scope's held-entity cache for the update-policy hold path. `Bytes[0..Length)`
        // is that scope's last successfully captured canonical buffer (the same wire format Capture
        // itself writes); `Index` locates each entity's record block within it. Both are replaced
        // wholesale at the end of every successful Capture call for this scope, so an entity that
        // left the scope is dropped from `Index` within one tick instead of accumulating forever.
        // NCORE-15b: the merge-capture path (CaptureViaCells) reuses this exact shape keyed by
        // cell id instead of by peer scope id, and additionally populates `Blocks` (each entity's
        // full wire block within `Bytes`, in the ascending-GID order Capture always writes them
        // in) and `CapturedTick`, neither of which the plain per-scope hold path uses.
        private sealed class ScopeCaptureCache
        {
            internal byte[] Bytes = Array.Empty<byte>();
            internal int Length;
            internal Dictionary<EntityGID, HeldRecordRange> Index =
                new Dictionary<EntityGID, HeldRecordRange>();
            internal List<EntityBlockRange> Blocks = new List<EntityBlockRange>();
            internal int RecordCount;
            internal uint CapturedTick;
            internal bool HasCapturedTick;
        }

        // One entity's full wire block inside a cell's ScopeCaptureCache.Bytes: from its GID
        // field through the end of its last record, ready to be byte-copied verbatim into a
        // merged scope buffer without touching a single record.
        private readonly struct EntityBlockRange
        {
            internal EntityBlockRange(EntityGID gid, int offset, int length)
            {
                Gid = gid;
                Offset = offset;
                Length = length;
            }

            internal readonly EntityGID Gid;
            internal readonly int Offset;
            internal readonly int Length;
        }

        // Locates one entity's record block (the bytes starting right after its 2-byte record-count
        // field) inside a ScopeCaptureCache's Bytes. RecordCount lets the merge scan in Capture know
        // when it has consumed every one of this entity's previously captured records without
        // reading past them into the next entity's header.
        private readonly struct HeldRecordRange
        {
            internal HeldRecordRange(NetworkTypeId kind, int recordsOffset, ushort recordCount)
            {
                Kind = kind;
                RecordsOffset = recordsOffset;
                RecordCount = recordCount;
            }

            internal readonly NetworkTypeId Kind;
            internal readonly int RecordsOffset;
            internal readonly ushort RecordCount;
        }
    }
}
