namespace UniGame.StaticEcs.Network
{
    using System;
    using System.Buffers;
    using System.Collections.Generic;
    using FFS.Libraries.StaticEcs;
    using FFS.Libraries.StaticPack;

    /// <summary>Selects whether one network entity belongs to an active replication scope.</summary>
    public delegate bool NetworkScopeSelector<TWorld>(ScopeId scope,
        World<TWorld>.Entity entity) where TWorld : struct, IWorldType;

    /// <summary>Reports full snapshot capture results.</summary>
    public enum SnapshotCaptureResult : byte
    {
        /// <summary>The snapshot was captured.</summary>
        Success,
        /// <summary>The Static ECS world is unavailable.</summary>
        WorldUnavailable,
        /// <summary>A protocol limit was exceeded.</summary>
        LimitExceeded,
        /// <summary>An entity identifier was invalid.</summary>
        InvalidEntity,
        /// <summary>A StaticPack hook failed.</summary>
        HookFailed,
    }

    /// <summary>Reports staged full snapshot application results.</summary>
    public enum SnapshotApplyResult : byte
    {
        /// <summary>The snapshot was staged or applied.</summary>
        Success,
        /// <summary>The snapshot schema was incompatible.</summary>
        SchemaMismatch,
        /// <summary>The snapshot payload was malformed.</summary>
        Malformed,
        /// <summary>A protocol limit was exceeded.</summary>
        LimitExceeded,
        /// <summary>Local entity state prevented application.</summary>
        EntityConflict,
    }

    /// <summary>Describes one validated canonical entity by primitive offsets.</summary>
    internal struct SnapshotEntityLayout
    {
        internal ulong Gid;
        internal ushort RecordCount;
        internal int RawOffset;
        internal int RawLength;
        internal int RecordStart;
    }

    /// <summary>Describes one validated canonical record by primitive offsets.</summary>
    internal struct SnapshotRecordLayout
    {
        internal uint TypeId;
        internal byte Kind;
        internal int RawOffset;
        internal int RawLength;
    }

    /// <summary>Rents immutable primitive layout arrays for the snapshot layout index.</summary>
    internal interface ISnapshotLayoutPool
    {
        SnapshotEntityLayout[] RentEntities(int length);
        void ReturnEntities(SnapshotEntityLayout[] array);
        SnapshotRecordLayout[] RentRecords(int length);
        void ReturnRecords(SnapshotRecordLayout[] array);
    }

    /// <summary>Production layout-array source backed by the shared array pool.</summary>
    internal sealed class SharedSnapshotLayoutPool : ISnapshotLayoutPool
    {
        internal static readonly SharedSnapshotLayoutPool Instance =
            new SharedSnapshotLayoutPool();

        public SnapshotEntityLayout[] RentEntities(int length) =>
            ArrayPool<SnapshotEntityLayout>.Shared.Rent(length);
        public void ReturnEntities(SnapshotEntityLayout[] array) =>
            ArrayPool<SnapshotEntityLayout>.Shared.Return(array, false);
        public SnapshotRecordLayout[] RentRecords(int length) =>
            ArrayPool<SnapshotRecordLayout>.Shared.Rent(length);
        public void ReturnRecords(SnapshotRecordLayout[] array) =>
            ArrayPool<SnapshotRecordLayout>.Shared.Return(array, false);
    }

    /// <summary>Selects the layout-array source; tests replace the shared seam.</summary>
    internal static class SnapshotLayoutMemory
    {
        internal static ISnapshotLayoutPool Pool = SharedSnapshotLayoutPool.Instance;
    }

    /// <summary>Views one immutable published layout index without allocating.</summary>
    internal readonly struct SnapshotLayoutView
    {
        internal SnapshotLayoutView(SnapshotEntityLayout[] entities,
            SnapshotRecordLayout[] records, int entityCount, int recordCount)
        {
            Entities = entities;
            Records = records;
            EntityCount = entityCount;
            RecordCount = recordCount;
        }

        internal SnapshotEntityLayout[] Entities { get; }
        internal SnapshotRecordLayout[] Records { get; }
        internal int EntityCount { get; }
        internal int RecordCount { get; }
    }

    /// <summary>Owns one immutable pooled canonical full-snapshot buffer.</summary>
    public sealed class NetworkSnapshot : IDisposable
    {
        private NetworkBufferLease _bytes;
        private NetworkSnapshotPool _pool;
        private SnapshotEntityLayout[] _layoutEntities;
        private SnapshotRecordLayout[] _layoutRecords;
        private ISnapshotLayoutPool _layoutPool;
        private int _layoutEntityCount;
        private int _layoutRecordCount;

        internal NetworkSnapshot()
        {
        }

        /// <summary>Creates a snapshot by consuming one exact canonical buffer lease.</summary>
        public NetworkSnapshot(uint tick, SchemaFingerprint fingerprint, ScopeId scope,
            NetworkBufferLease bytes, int entities, int records)
        {
            Initialize(null, tick, fingerprint, scope, bytes, entities, records);
        }

        internal void Initialize(NetworkSnapshotPool pool, uint tick,
            SchemaFingerprint fingerprint, ScopeId scope, NetworkBufferLease bytes,
            int entities, int records)
        {
            if (_bytes != null)
                throw new InvalidOperationException("Snapshot descriptor is already in use.");
            _pool = pool;
            ServerTick = tick;
            SchemaFingerprint = fingerprint;
            Scope = scope;
            _bytes = bytes ?? throw new ArgumentNullException(nameof(bytes));
            PayloadHash = Hashing.XxHash64(bytes.Span);
            EntityCount = entities;
            RecordCount = records;
        }

        /// <summary>Gets authoritative simulation time.</summary>
        public uint ServerTick { get; private set; }
        /// <summary>Gets the schema fingerprint that produced the snapshot.</summary>
        public SchemaFingerprint SchemaFingerprint { get; private set; }
        /// <summary>Gets the replication scope.</summary>
        public ScopeId Scope { get; private set; }
        /// <summary>Gets xxHash64 of the canonical bytes.</summary>
        public ulong PayloadHash { get; private set; }
        /// <summary>Gets immutable canonical bytes.</summary>
        public ReadOnlyMemory<byte> Bytes => _bytes?.Memory ?? ReadOnlyMemory<byte>.Empty;
        /// <summary>Gets the entity count.</summary>
        public int EntityCount { get; private set; }
        /// <summary>Gets the record count.</summary>
        public int RecordCount { get; private set; }
        /// <summary>Gets exact retained byte length.</summary>
        public int ByteLength => _bytes?.Length ?? 0;

        internal byte[] Buffer => _bytes?.Buffer;
        internal int Offset => _bytes?.Offset ?? 0;

        // Only pool-owned snapshots may lazily build and reuse the layout. Public
        // descriptors keep the parser/hash path so caller-supplied bytes are never
        // trusted without revalidation.
        internal bool HasLayout => _layoutEntities != null;

        internal bool TryReadCachedLayout(out SnapshotLayoutView layout)
        {
            if (_layoutEntities == null)
            {
                layout = default;
                return false;
            }
            layout = new SnapshotLayoutView(_layoutEntities, _layoutRecords,
                _layoutEntityCount, _layoutRecordCount);
            return true;
        }

        // Rents a validated layout without publishing it. The caller owns the
        // rentals and must either publish them or return them exactly once.
        internal bool TryBuildLayout(out SnapshotEntityLayout[] entities,
            out SnapshotRecordLayout[] records, out int entityCount,
            out int recordCount, out ISnapshotLayoutPool layoutPool)
        {
            entities = null;
            records = null;
            entityCount = 0;
            recordCount = 0;
            layoutPool = null;
            if (_pool == null || _layoutEntities != null)
                return false;
            return SnapshotDeltaCodec.TryBuildLayout(this, out entities,
                out records, out entityCount, out recordCount, out layoutPool);
        }

        internal void PublishLayout(SnapshotEntityLayout[] entities,
            SnapshotRecordLayout[] records, int entityCount, int recordCount,
            ISnapshotLayoutPool layoutPool)
        {
            _layoutEntities = entities;
            _layoutRecords = records;
            _layoutEntityCount = entityCount;
            _layoutRecordCount = recordCount;
            _layoutPool = layoutPool;
        }

        internal bool TryGetLayout(out SnapshotLayoutView layout)
        {
            if (TryReadCachedLayout(out layout))
                return true;
            if (!TryBuildLayout(out var entities, out var records,
                    out var entityCount, out var recordCount, out var layoutPool))
                return false;
            PublishLayout(entities, records, entityCount, recordCount,
                layoutPool);
            layout = new SnapshotLayoutView(entities, records, entityCount,
                recordCount);
            return true;
        }

        /// <inheritdoc />
        public void Dispose()
        {
            // Detach the index first so a double dispose can never return the
            // layout arrays twice, then release the payload and descriptor.
            var layoutPool = _layoutPool;
            var layoutEntities = _layoutEntities;
            var layoutRecords = _layoutRecords;
            _layoutPool = null;
            _layoutEntities = null;
            _layoutRecords = null;
            _layoutEntityCount = 0;
            _layoutRecordCount = 0;
            layoutPool?.ReturnEntities(layoutEntities);
            layoutPool?.ReturnRecords(layoutRecords);

            if (_bytes == null)
                return;
            var pool = _pool;
            _pool = null;
            _bytes.Dispose();
            _bytes = null;
            pool?.Return(this);
        }
    }
}
