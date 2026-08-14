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

    internal sealed class NetworkSnapshotPool
    {
        private readonly Stack<NetworkSnapshot> _snapshots;
        private readonly int _capacity;

        internal NetworkSnapshotPool(int capacity)
        {
            _capacity = Math.Max(1, capacity);
            _snapshots = new Stack<NetworkSnapshot>(_capacity);
        }

        internal NetworkSnapshot Rent(uint tick, SchemaFingerprint fingerprint,
            ScopeId scope, NetworkBufferLease bytes, int entities, int records)
        {
            var snapshot = _snapshots.Count > 0
                ? _snapshots.Pop()
                : new NetworkSnapshot();
            snapshot.Initialize(this, tick, fingerprint, scope, bytes, entities,
                records);
            return snapshot;
        }

        internal void Return(NetworkSnapshot snapshot)
        {
            if (_snapshots.Count < _capacity)
                _snapshots.Push(snapshot);
        }
    }

    /// <summary>Contains validated pooled metadata; applying it is the first ECS mutation.</summary>
    public struct StagedNetworkSnapshot : IDisposable
    {
        internal object Owner;
        internal NetworkSnapshot Snapshot;
        internal StagedEntity[] Entities;
        internal StagedRecord[] Records;
        internal int EntityCount;
        internal int RecordCount;

        /// <summary>Gets authoritative simulation time.</summary>
        public uint ServerTick => Snapshot?.ServerTick ?? 0;

        internal SchemaFingerprint Fingerprint => Snapshot?.SchemaFingerprint ?? default;
        internal ScopeId Scope => Snapshot?.Scope ?? default;

        /// <inheritdoc />
        public void Dispose()
        {
            if (Entities != null)
            {
                Array.Clear(Entities, 0, EntityCount);
                ArrayPool<StagedEntity>.Shared.Return(Entities);
            }
            if (Records != null)
            {
                Array.Clear(Records, 0, RecordCount);
                ArrayPool<StagedRecord>.Shared.Return(Records);
            }
            Owner = null;
            Snapshot = null;
            Entities = null;
            Records = null;
            EntityCount = 0;
            RecordCount = 0;
        }
    }

    internal struct StagedEntity
    {
        internal EntityGID Gid;
        internal NetworkSchemaEntry Kind;
        internal bool Disabled;
        internal int RecordStart;
        internal int RecordCount;
    }

    internal struct StagedRecord
    {
        internal NetworkSchemaEntry Entry;
        internal int Offset;
        internal int Length;
        internal bool Disabled;
    }

    internal readonly struct NetworkReplicaEntry
    {
        internal readonly EntityGID LocalGid;
        internal readonly NetworkTypeId KindId;

        internal NetworkReplicaEntry(EntityGID localGid, NetworkTypeId kindId)
        {
            LocalGid = localGid;
            KindId = kindId;
        }
    }

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
        private readonly NetworkSnapshotPool _snapshotPool;
        private int _captureCapacity = 4096;

        /// <summary>Creates a client-side snapshot apply replicator.</summary>
        public NetworkReplicator(NetworkSchema<TWorld> schema,
            ScopeId scope = default, int historyTicks = 64,
            long historyBytes = 32 * 1024 * 1024,
            NetworkBufferPool bufferPool = null)
        {
            _schema = schema ?? throw new ArgumentNullException(nameof(schema));
            _bufferPool = bufferPool ??
                new NetworkBufferPool(NetworkBufferPool.DefaultClientRetainedBytes);
            _ownsBufferPool = bufferPool == null;
            _snapshotPool = new NetworkSnapshotPool(historyTicks + 2);
            Scope = scope;
            History = new NetworkHistory<NetworkSnapshot>(historyTicks, historyBytes,
                value => value.ByteLength, value => value.Dispose());
        }

        /// <summary>Creates an authority replicator with an active-scope selector.</summary>
        public NetworkReplicator(NetworkSchema<TWorld> schema,
            NetworkScopeSelector<TWorld> scopeSelector, ScopeId scope = default,
            int historyTicks = 64, long historyBytes = 32 * 1024 * 1024,
            NetworkBufferPool bufferPool = null)
            : this(schema, scope, historyTicks, historyBytes, bufferPool)
        {
            _scopeSelector = scopeSelector ??
                throw new ArgumentNullException(nameof(scopeSelector));
        }

        /// <summary>Gets the isolated replication scope.</summary>
        public ScopeId Scope { get; }
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

            _captureEntities.Clear();
            _captureSeen.Clear();
            var entries = _schema.RetainedEntries;
            for (var i = 0; i < entries.Length; i++)
                if (entries[i].Invoker is IEntityNetworkInvoker<TWorld> invoker)
                    invoker.Collect(_captureEntities, _captureSeen);
            for (var i = _captureEntities.Count - 1; i >= 0; i--)
                if (!_scopeSelector(scope, _captureEntities[i]))
                    _captureEntities.RemoveAt(i);
            if (_captureEntities.Count > ProtocolLimits.MaxEntities)
                return SnapshotCaptureResult.LimitExceeded;
            _captureEntities.Sort(EntityComparer.Instance);

            var buffer = _bufferPool.Rent(_captureCapacity);
            var writer = BinaryPackWriter.Create(buffer.Buffer);
            var records = 0;
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
                    var entityRecords = 0;
                    for (var j = 0; j < entries.Length; j++)
                    {
                        if (entries[j].Invoker is not IRecordNetworkInvoker<TWorld> invoker ||
                            !invoker.Has(entity))
                            continue;
                        if (entityRecords == ProtocolLimits.MaxRecordsPerEntity)
                            return FailCapture(buffer, writer.Buffer,
                                SnapshotCaptureResult.LimitExceeded);
                        var entry = entries[j];
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

        internal NetworkSnapshot CreateSnapshot(uint serverTick,
            SchemaFingerprint fingerprint, ScopeId scope, NetworkBufferLease bytes,
            int entities, int records) => _snapshotPool.Rent(serverTick, fingerprint,
            scope, bytes, entities, records);

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
                var localGid = _replicas[sourceGid].LocalGid;
                if (localGid.TryUnpack<TWorld>(out var removed))
                    removed.Destroy();
                _replicas.Remove(sourceGid);
            }

            var buffer = staged.Snapshot.Buffer;
            var baseOffset = staged.Snapshot.Offset;
            var entries = _schema.RetainedEntries;
            for (var i = 0; i < staged.EntityCount; i++)
            {
                var source = staged.Entities[i];
                World<TWorld>.Entity entity;
                if (_replicas.TryGetValue(source.Gid, out var replica))
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
                _replicas[source.Gid] = new NetworkReplicaEntry(entity.GID,
                    source.Kind.TypeId);
            }
            History.Store(staged.ServerTick, staged.Snapshot);
            return SnapshotApplyResult.Success;
        }

        /// <summary>Destroys all client replicas and clears applied snapshot history.</summary>
        public void ClearReplicas()
        {
            if (World<TWorld>.Status != WorldStatus.Initialized)
            {
                _replicas.Clear();
                History.Clear();
                return;
            }
            _replicaScratch.Clear();
            foreach (var replica in _replicas.Values)
                _replicaScratch.Add(replica);
            for (var i = 0; i < _replicaScratch.Count; i++)
                if (_replicaScratch[i].LocalGid.TryUnpack<TWorld>(out var entity))
                    entity.Destroy();
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
            var kind = left.Kind.CompareTo(right.Kind);
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
    }
}
