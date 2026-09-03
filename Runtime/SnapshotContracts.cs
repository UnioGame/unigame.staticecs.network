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

    /// <summary>Owns one immutable pooled canonical full-snapshot buffer.</summary>
    public sealed class NetworkSnapshot : IDisposable
    {
        private NetworkBufferLease _bytes;
        private NetworkSnapshotPool _pool;

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

        /// <inheritdoc />
        public void Dispose()
        {
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
