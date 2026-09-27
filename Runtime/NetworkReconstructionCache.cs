namespace UniGame.StaticEcs.Network
{
    using System;
    using System.Collections.Generic;

    /// <summary>Reports reconstruction cache hit/miss/eviction counters.</summary>
    public struct NetworkReconstructionCacheDiagnostics
    {
        /// <summary>Number of reconstructions served from a prior, already-verified result.</summary>
        public long Hits;

        /// <summary>Number of reconstructions that ran <see cref="SnapshotDeltaCodec.TryReconstruct"/>.</summary>
        public long Misses;

        /// <summary>Number of entries dropped to stay within capacity.</summary>
        public long Evictions;

        /// <summary>Entries currently retained.</summary>
        public int Entries;
    }

    /// <summary>
    /// Shares one delta-reconstruction result across every <see cref="NetworkClient{TWorld}"/> in a
    /// process that independently receives the exact same wire delta against the exact same baseline
    /// snapshot — the case for a thin load-generator process that runs many client slots against one
    /// server scope (NCORE-26). Opt-in: pass one shared instance to every
    /// <see cref="NetworkClient{TWorld}"/> constructor that should participate. Omitting it (the
    /// default) keeps every client's reconstruction fully independent, matching pre-NCORE-26
    /// behaviour byte-for-byte and allocation-for-allocation.
    /// <para>
    /// Not thread-safe: every sharing <see cref="NetworkClient{TWorld}"/> must be pumped from one
    /// thread, exactly like the shared <see cref="NetworkBufferPool"/> those clients already pass
    /// each other.
    /// </para>
    /// <para>
    /// Reconstructing and hash-verifying a delta against a baseline is a pure function of the
    /// baseline bytes, delta bytes, and declared snapshot header: the same inputs always produce
    /// the same canonical bytes or fail the same way. The key includes the verified baseline hash,
    /// the exact delta hash, and every wire header field, including the declared total hash and
    /// length. A modified declaration cannot hit a result verified under a different declaration.
    /// </para>
    /// </summary>
    public sealed class NetworkReconstructionCache
    {
        private readonly struct Key : IEquatable<Key>
        {
            private readonly SchemaFingerprint _schema;
            private readonly ScopeId _scope;
            private readonly uint _baselineTick;
            private readonly ulong _baselineHash;
            private readonly uint _snapshotTick;
            private readonly ulong _deltaHash;
            private readonly SnapshotPayloadKind _payloadKind;
            private readonly uint _totalLength;
            private readonly ulong _totalHash;
            private readonly uint _chunkIndex;
            private readonly uint _chunkCount;
            private readonly uint _resyncCorrelationId;
            private readonly ulong _scopeValue;

            internal Key(SchemaFingerprint schema, ScopeId scope, uint baselineTick,
                ulong baselineHash, in SnapshotChunkHeader header, ulong deltaHash)
            {
                _schema = schema;
                _scope = scope;
                _baselineTick = baselineTick;
                _baselineHash = baselineHash;
                _snapshotTick = header.SnapshotTick;
                _deltaHash = deltaHash;
                _payloadKind = header.PayloadKind;
                _totalLength = header.TotalLength;
                _totalHash = header.TotalHash;
                _chunkIndex = header.ChunkIndex;
                _chunkCount = header.ChunkCount;
                _resyncCorrelationId = header.ResyncCorrelationId;
                _scopeValue = header.ScopeValue;
            }

            public bool Equals(Key other) =>
                _schema.Equals(other._schema) && _scope.Equals(other._scope) &&
                _baselineTick == other._baselineTick && _baselineHash == other._baselineHash &&
                _snapshotTick == other._snapshotTick && _deltaHash == other._deltaHash &&
                _payloadKind == other._payloadKind && _totalLength == other._totalLength &&
                _totalHash == other._totalHash && _chunkIndex == other._chunkIndex &&
                _chunkCount == other._chunkCount &&
                _resyncCorrelationId == other._resyncCorrelationId &&
                _scopeValue == other._scopeValue;

            public override bool Equals(object obj) => obj is Key other && Equals(other);

            public override int GetHashCode()
            {
                unchecked
                {
                    var hash = _schema.GetHashCode();
                    hash = hash * 397 ^ _scope.GetHashCode();
                    hash = hash * 397 ^ (int)_baselineTick;
                    hash = hash * 397 ^ _baselineHash.GetHashCode();
                    hash = hash * 397 ^ (int)_snapshotTick;
                    hash = hash * 397 ^ _deltaHash.GetHashCode();
                    hash = hash * 397 ^ (int)_payloadKind;
                    hash = hash * 397 ^ (int)_totalLength;
                    hash = hash * 397 ^ _totalHash.GetHashCode();
                    hash = hash * 397 ^ (int)_chunkIndex;
                    hash = hash * 397 ^ (int)_chunkCount;
                    hash = hash * 397 ^ (int)_resyncCorrelationId;
                    return hash * 397 ^ _scopeValue.GetHashCode();
                }
            }
        }

        private sealed class Entry
        {
            internal NetworkBufferLease Lease;
            internal int EntityCount;
            internal int RecordCount;
        }

        private readonly Dictionary<Key, Entry> _entries = new Dictionary<Key, Entry>();
        private readonly Queue<Key> _order = new Queue<Key>();
        private readonly int _capacity;
        private long _hits;
        private long _misses;
        private long _evictions;

        /// <param name="capacity">
        /// Maximum distinct (baseline, delta) reconstructions retained at once. Bounds worst-case
        /// memory when a process briefly observes many distinct baseline/target tick pairs (a resync
        /// storm, or clients spread across several scopes/baselines). Oldest entries are evicted
        /// first. Must be positive.
        /// </param>
        public NetworkReconstructionCache(int capacity = 64)
        {
            if (capacity <= 0)
                throw new ArgumentOutOfRangeException(nameof(capacity));
            _capacity = capacity;
        }

        /// <summary>Captures current hit/miss/eviction counters.</summary>
        public NetworkReconstructionCacheDiagnostics CaptureDiagnostics() =>
            new NetworkReconstructionCacheDiagnostics
            {
                Hits = _hits,
                Misses = _misses,
                Evictions = _evictions,
                Entries = _entries.Count,
            };

        /// <summary>
        /// Reconstructs a delta against its baseline, reusing a prior result byte-for-byte when
        /// another caller already reconstructed and hash-verified the identical (baseline, delta)
        /// pair. Same contract as <see cref="SnapshotDeltaCodec.TryReconstruct"/> on both success and
        /// failure: on success the caller owns the returned lease and must dispose it exactly once;
        /// on failure (including a hash mismatch, whether observed by this call or by the miss that
        /// first populated the cache) no lease is returned and the caller's normal recovery path
        /// (resync) applies unchanged.
        /// </summary>
        internal bool TryReconstruct(NetworkBufferPool pool, NetworkSnapshot baseline,
            ReadOnlySpan<byte> delta, in SnapshotChunkHeader header, SchemaFingerprint schema,
            ScopeId scope, out NetworkBufferLease canonical, out int entityCount,
            out int recordCount, NetworkComponentDeltaHooks hooks)
        {
            canonical = null;
            entityCount = 0;
            recordCount = 0;
            if (!SnapshotDeltaCodec.IsValidReconstructionInput(pool, baseline,
                    delta.Length, in header, schema, scope))
                return false;

            var deltaHash = Hashing.XxHash64(delta);
            var key = new Key(schema, scope, header.BaselineTick, baseline.PayloadHash,
                in header, deltaHash);
            if (_entries.TryGetValue(key, out var cached))
            {
                _hits++;
                canonical = cached.Lease.Retain();
                entityCount = cached.EntityCount;
                recordCount = cached.RecordCount;
                return true;
            }

            _misses++;
            if (!SnapshotDeltaCodec.TryReconstruct(pool, baseline, delta, in header, schema,
                    scope, out var reconstructed, out entityCount, out recordCount, hooks))
            {
                canonical = null;
                return false;
            }

            EvictIfNeeded();
            var entry = new Entry
            {
                Lease = reconstructed.Retain(),
                EntityCount = entityCount,
                RecordCount = recordCount,
            };
            _entries.Add(key, entry);
            _order.Enqueue(key);
            canonical = reconstructed;
            return true;
        }

        private void EvictIfNeeded()
        {
            while (_entries.Count >= _capacity && _order.Count > 0)
            {
                var oldest = _order.Dequeue();
                if (!_entries.TryGetValue(oldest, out var entry))
                    continue;
                _entries.Remove(oldest);
                entry.Lease.Dispose();
                _evictions++;
            }
        }

        /// <summary>Releases every retained lease and clears the cache.</summary>
        public void Clear()
        {
            foreach (var entry in _entries.Values)
                entry.Lease.Dispose();
            _entries.Clear();
            _order.Clear();
        }
    }
}
