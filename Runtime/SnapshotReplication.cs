using System;
using System.Collections.Generic;
using System.IO;
using FFS.Libraries.StaticEcs;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Selects whether one network entity belongs to an active replication scope.</summary>
    public delegate bool NetworkScopeSelector<TWorld>(ScopeId scope, World<TWorld>.Entity entity)
        where TWorld : struct, IWorldType;

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
        HookFailed
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
        EntityConflict
    }

    /// <summary>Owns immutable canonical full-snapshot bytes.</summary>
    public sealed class NetworkSnapshot
    {
        private readonly byte[] _bytes;

        /// <summary>Creates an immutable snapshot from exact canonical bytes.</summary>
        public NetworkSnapshot(uint tick, SchemaFingerprint fingerprint, ScopeId scope, byte[] bytes, int entities, int records)
        {
            ServerTick = tick;
            SchemaFingerprint = fingerprint;
            Scope = scope;
            _bytes = bytes == null ? throw new ArgumentNullException(nameof(bytes)) : (byte[])bytes.Clone();
            PayloadHash = Hashing.XxHash64(_bytes);
            EntityCount = entities;
            RecordCount = records;
        }
        /// <summary>Gets authoritative simulation time.</summary>
        public uint ServerTick { get; }
        /// <summary>Gets the schema fingerprint that produced the snapshot.</summary>
        public SchemaFingerprint SchemaFingerprint { get; }
        /// <summary>Gets the replication scope.</summary>
        public ScopeId Scope { get; }
        /// <summary>Gets xxHash64 of the canonical bytes.</summary>
        public ulong PayloadHash { get; }
        /// <summary>Gets immutable canonical bytes.</summary>
        public ReadOnlyMemory<byte> Bytes => _bytes;
        /// <summary>Gets the entity count.</summary>
        public int EntityCount { get; }
        /// <summary>Gets the record count.</summary>
        public int RecordCount { get; }
        /// <summary>Gets exact retained byte length.</summary>
        public int ByteLength => _bytes.Length;
        internal byte[] ExactBytes => _bytes;
    }

    /// <summary>Contains a fully validated snapshot; applying it is the first ECS mutation.</summary>
    public sealed class StagedNetworkSnapshot
    {
        internal StagedNetworkSnapshot(object owner, NetworkSnapshot snapshot, StagedEntity[] entities) { Owner = owner; Snapshot = snapshot; Entities = entities; }
        /// <summary>Gets authoritative simulation time.</summary>
        public uint ServerTick => Snapshot.ServerTick;
        internal object Owner { get; }
        internal SchemaFingerprint Fingerprint => Snapshot.SchemaFingerprint;
        internal ScopeId Scope => Snapshot.Scope;
        internal NetworkSnapshot Snapshot { get; }
        internal StagedEntity[] Entities { get; }
    }

    internal sealed class StagedEntity
    {
        internal EntityGID Gid;
        internal NetworkSchemaEntry Kind;
        internal bool Disabled;
        internal StagedRecord[] Records;
    }

    internal sealed class StagedRecord
    {
        internal NetworkSchemaEntry Entry;
        internal byte[] Payload;
        internal bool Disabled;
    }

    /// <summary>Captures authority NetworkTag entities and applies two-phase full snapshots.</summary>
    public sealed class NetworkReplicator<TWorld> where TWorld : struct, IWorldType
    {
        private readonly NetworkSchema<TWorld> _schema;
        private readonly Dictionary<EntityGID, NetworkTypeId> _replicas = new Dictionary<EntityGID, NetworkTypeId>();
        private readonly object _owner = new object();
        private readonly NetworkScopeSelector<TWorld> _scopeSelector;

        /// <summary>Creates a client-side snapshot apply replicator.</summary>
        public NetworkReplicator(NetworkSchema<TWorld> schema, ScopeId scope = default, int historyTicks = 64, long historyBytes = 32 * 1024 * 1024)
        {
            _schema = schema ?? throw new ArgumentNullException(nameof(schema));
            Scope = scope;
            History = new NetworkHistory<NetworkSnapshot>(historyTicks, historyBytes, value => value.ByteLength);
        }

        /// <summary>Creates an authority capture replicator with an explicit active-scope selector.</summary>
        public NetworkReplicator(NetworkSchema<TWorld> schema, NetworkScopeSelector<TWorld> scopeSelector, ScopeId scope = default, int historyTicks = 64, long historyBytes = 32 * 1024 * 1024)
            : this(schema, scope, historyTicks, historyBytes)
        {
            _scopeSelector = scopeSelector ?? throw new ArgumentNullException(nameof(scopeSelector));
        }
        /// <summary>Gets the isolated replication scope.</summary>
        public ScopeId Scope { get; }
        /// <summary>Gets bounded snapshots successfully applied by this client.</summary>
        public NetworkHistory<NetworkSnapshot> History { get; }

        /// <summary>Captures all authority entities marked with NetworkTag into an immutable full snapshot.</summary>
        public SnapshotCaptureResult Capture(uint serverTick, out NetworkSnapshot snapshot)
            => Capture(serverTick, Scope, out snapshot);

        /// <summary>Captures authority entities for an explicit server replication scope.</summary>
        public SnapshotCaptureResult Capture(uint serverTick, ScopeId scope, out NetworkSnapshot snapshot)
        {
            if (_scopeSelector == null) throw new InvalidOperationException("Authority capture requires an explicit scope selector.");
            snapshot = null;
            if (World<TWorld>.Status != WorldStatus.Initialized || !World<TWorld>.IsTagTypeRegistered<NetworkTag>()) return SnapshotCaptureResult.WorldUnavailable;
            var entities = new List<World<TWorld>.Entity>();
            foreach (var entity in World<TWorld>.Query<All<NetworkTag>>().Entities(EntityStatusType.Any))
            {
                if (!_scopeSelector(scope, entity)) continue;
                if (entities.Count == ProtocolLimits.MaxEntities) return SnapshotCaptureResult.LimitExceeded;
                entities.Add(entity);
            }
            entities.Sort((a, b) => Compare(a.GID, b.GID));
            using var stream = new MemoryStream();
            using var writer = new BinaryWriter(stream);
            writer.Write(entities.Count);
            var records = 0;
            try
            {
                for (var i = 0; i < entities.Count; i++)
                {
                    var entity = entities[i];
                    if (i > 0 && Compare(entities[i - 1].GID, entity.GID) >= 0) return SnapshotCaptureResult.InvalidEntity;
                    NetworkSchemaEntry kind = null;
                    var entries = _schema.RetainedEntries;
                    for (var j = 0; j < entries.Length; j++)
                        if (entries[j].Invoker is IEntityNetworkInvoker<TWorld> invoker && invoker.Matches(entity)) { kind = entries[j]; break; }
                    if (kind == null) return SnapshotCaptureResult.InvalidEntity;
                    WriteGid(writer, entity.GID);
                    writer.Write(kind.TypeId.Value);
                    writer.Write(entity.IsDisabled);
                    var present = new List<NetworkSchemaEntry>();
                    for (var j = 0; j < entries.Length; j++)
                        if (entries[j].Invoker is IRecordNetworkInvoker<TWorld> invoker && invoker.Has(entity)) present.Add(entries[j]);
                    if (present.Count > ProtocolLimits.MaxRecordsPerEntity) return SnapshotCaptureResult.LimitExceeded;
                    writer.Write((ushort)present.Count);
                    for (var j = 0; j < present.Count; j++)
                    {
                        var entry = present[j];
                        var invoker = (IRecordNetworkInvoker<TWorld>)entry.Invoker;
                        var payload = invoker.Capture(entity, entry.MaxBytes);
                        writer.Write(entry.TypeId.Value);
                        writer.Write((byte)entry.Kind);
                        writer.Write(entry.Version);
                        writer.Write(invoker.IsDisabled(entity));
                        writer.Write(payload.Length);
                        writer.Write(payload);
                        records++;
                    }
                }
            }
            catch { return SnapshotCaptureResult.HookFailed; }
            if (stream.Length > ProtocolLimits.MaxDecodedPayloadBytes) return SnapshotCaptureResult.LimitExceeded;
            snapshot = new NetworkSnapshot(serverTick, _schema.Fingerprint, scope, stream.ToArray(), entities.Count, records);
            return SnapshotCaptureResult.Success;
        }

        /// <summary>Validates all bounds, ordering, kinds, versions, and membership without mutating ECS.</summary>
        public SnapshotApplyResult Stage(NetworkSnapshot snapshot, out StagedNetworkSnapshot staged)
        {
            staged = null;
            if (snapshot == null || snapshot.ExactBytes.Length > ProtocolLimits.MaxDecodedPayloadBytes) return SnapshotApplyResult.LimitExceeded;
            if (snapshot.SchemaFingerprint != _schema.Fingerprint || snapshot.Scope != Scope) return SnapshotApplyResult.SchemaMismatch;
            if (Hashing.XxHash64(snapshot.ExactBytes) != snapshot.PayloadHash) return SnapshotApplyResult.Malformed;
            try
            {
                using var stream = new MemoryStream(snapshot.ExactBytes, false);
                using var reader = new BinaryReader(stream);
                var count = reader.ReadInt32();
                if (count < 0 || count > ProtocolLimits.MaxEntities) return SnapshotApplyResult.LimitExceeded;
                var entities = new StagedEntity[count];
                EntityGID previous = default;
                for (var i = 0; i < count; i++)
                {
                    var gid = ReadGid(reader);
                    if (gid.Version == 0 || i > 0 && Compare(previous, gid) >= 0) return SnapshotApplyResult.Malformed;
                    previous = gid;
                    var kindId = ReadTypeId(reader);
                    if (!_schema.TryGet(kindId, out var kind) || kind.Kind != NetworkSchemaKind.Entity || kind.Invoker is not IEntityNetworkInvoker<TWorld>) return SnapshotApplyResult.SchemaMismatch;
                    var disabled = reader.ReadBoolean();
                    var recordCount = reader.ReadUInt16();
                    if (recordCount > ProtocolLimits.MaxRecordsPerEntity) return SnapshotApplyResult.LimitExceeded;
                    var records = new StagedRecord[recordCount];
                    NetworkSchemaEntry previousEntry = null;
                    for (var j = 0; j < recordCount; j++)
                    {
                        var id = ReadTypeId(reader);
                        var wireKind = (NetworkSchemaKind)reader.ReadByte();
                        var version = reader.ReadByte();
                        var recordDisabled = reader.ReadBoolean();
                        var length = reader.ReadInt32();
                        if (length < 0 || length > ProtocolLimits.MaxComponentBytes || stream.Length - stream.Position < length) return SnapshotApplyResult.LimitExceeded;
                        if (!_schema.TryGet(id, out var entry) || entry.Kind != wireKind || entry.Version != version || length > entry.MaxBytes || entry.Invoker is not IRecordNetworkInvoker<TWorld> invoker) return SnapshotApplyResult.SchemaMismatch;
                        if (recordDisabled && !invoker.SupportsDisabled) return SnapshotApplyResult.Malformed;
                        if (previousEntry != null && Compare(previousEntry, entry) >= 0) return SnapshotApplyResult.Malformed;
                        previousEntry = entry;
                        records[j] = new StagedRecord { Entry = entry, Disabled = recordDisabled, Payload = reader.ReadBytes(length) };
                    }
                    entities[i] = new StagedEntity { Gid = gid, Kind = kind, Disabled = disabled, Records = records };
                }
                if (stream.Position != stream.Length || count != snapshot.EntityCount || CountRecords(entities) != snapshot.RecordCount) return SnapshotApplyResult.Malformed;
                for (var i = 0; i < entities.Length; i++)
                {
                    var source = entities[i];
                    if (!source.Gid.TryUnpack<TWorld>(out var existing)) continue;
                    if (!_replicas.TryGetValue(source.Gid, out var kindId) || kindId != source.Kind.TypeId || !((IEntityNetworkInvoker<TWorld>)source.Kind.Invoker).Matches(existing)) return SnapshotApplyResult.EntityConflict;
                }
                staged = new StagedNetworkSnapshot(_owner, snapshot, entities);
                return SnapshotApplyResult.Success;
            }
            catch (EndOfStreamException) { return SnapshotApplyResult.Malformed; }
            catch (IOException) { return SnapshotApplyResult.Malformed; }
            catch (Exception) { return SnapshotApplyResult.Malformed; }
        }

        /// <summary>Applies a previously staged snapshot. Hook and lifecycle exceptions are not rolled back.</summary>
        public SnapshotApplyResult Apply(StagedNetworkSnapshot staged)
        {
            if (staged == null || !ReferenceEquals(staged.Owner, _owner) || staged.Fingerprint != _schema.Fingerprint || staged.Scope != Scope) return SnapshotApplyResult.SchemaMismatch;
            if (World<TWorld>.Status != WorldStatus.Initialized) return SnapshotApplyResult.Malformed;
            var incoming = new HashSet<EntityGID>();
            for (var i = 0; i < staged.Entities.Length; i++) incoming.Add(staged.Entities[i].Gid);
            for (var i = 0; i < staged.Entities.Length; i++)
            {
                var source = staged.Entities[i];
                if (source.Gid.TryUnpack<TWorld>(out var existing) && (!_replicas.TryGetValue(source.Gid, out var kindId) || kindId != source.Kind.TypeId || !((IEntityNetworkInvoker<TWorld>)source.Kind.Invoker).Matches(existing))) return SnapshotApplyResult.EntityConflict;
            }
            var removedIds = new List<EntityGID>();
            foreach (var pair in _replicas) if (!incoming.Contains(pair.Key)) removedIds.Add(pair.Key);
            for (var i = 0; i < removedIds.Count; i++) { var gid = removedIds[i]; if (gid.TryUnpack<TWorld>(out var removed)) removed.Destroy(); _replicas.Remove(gid); }
            for (var i = 0; i < staged.Entities.Length; i++)
            {
                var source = staged.Entities[i];
                if (!source.Gid.TryUnpack<TWorld>(out var entity)) entity = ((IEntityNetworkInvoker<TWorld>)source.Kind.Invoker).Create(source.Gid);
                else if (!((IEntityNetworkInvoker<TWorld>)source.Kind.Invoker).Matches(entity)) return SnapshotApplyResult.EntityConflict;
                entity.Set<NetworkTag>();
                var entries = _schema.RetainedEntries;
                for (var j = 0; j < entries.Length; j++)
                {
                    if (entries[j].Invoker is not IRecordNetworkInvoker<TWorld> invoker) continue;
                    var found = false;
                    for (var k = 0; k < source.Records.Length; k++) if (source.Records[k].Entry.TypeId == entries[j].TypeId) { found = true; break; }
                    if (!found) invoker.Remove(entity);
                }
                for (var j = 0; j < source.Records.Length; j++)
                {
                    var record = source.Records[j];
                    ((IRecordNetworkInvoker<TWorld>)record.Entry.Invoker).Apply(entity, record.Payload, record.Entry.Version, record.Disabled);
                }
                if (source.Disabled) entity.Disable(); else entity.Enable();
                _replicas[source.Gid] = source.Kind.TypeId;
            }
            History.Store(staged.ServerTick, staged.Snapshot);
            return SnapshotApplyResult.Success;
        }

        private static int Compare(EntityGID left, EntityGID right)
        {
            var cluster = left.ClusterId.CompareTo(right.ClusterId);
            var id = left.Id.CompareTo(right.Id);
            return cluster != 0 ? cluster : id != 0 ? id : left.Version.CompareTo(right.Version);
        }
        private static int Compare(NetworkSchemaEntry left, NetworkSchemaEntry right) { var kind = left.Kind.CompareTo(right.Kind); return kind != 0 ? kind : left.TypeId.CompareTo(right.TypeId); }
        private static int CountRecords(StagedEntity[] entities) { var count = 0; for (var i = 0; i < entities.Length; i++) count += entities[i].Records.Length; return count; }
        private static void WriteGid(BinaryWriter writer, EntityGID gid) => writer.Write(gid.Raw);
        private static EntityGID ReadGid(BinaryReader reader) => new EntityGID(reader.ReadUInt64());
        private static NetworkTypeId ReadTypeId(BinaryReader reader) { var value = reader.ReadUInt32(); if (value == 0) throw new InvalidDataException("Zero network type id."); return new NetworkTypeId(value); }
    }
}
