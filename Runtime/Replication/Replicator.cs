using System;
using System.Buffers;
using FFS.Libraries.StaticEcs;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Captures and applies complete canonical snapshots inside one immutable replication scope.</summary>
    public sealed class Replicator<TWorld> : IDisposable where TWorld : struct, IWorldType
    {
        private readonly Schema _schema;
        private readonly ReplicaScope<TWorld> _scope;
        private bool _disposed;

        /// <summary>Creates a typed replicator for a frozen schema and role-bound scope.</summary>
        public Replicator(Schema schema, ReplicaScope<TWorld> scope)
        {
            _schema = schema ?? throw new ArgumentNullException(nameof(schema));
            _scope = scope ?? throw new ArgumentNullException(nameof(scope));
            _schema.EnsureWorld<TWorld>();
        }

        /// <summary>Captures all tagged authority entities into a caller-owned decoded payload lease.</summary>
        public CaptureResult Capture(out PacketLease payload)
        {
            payload = default;
            if (_disposed || _scope.IsDisposed || !_scope.ValidateCurrent()) return CaptureResult.ScopeInvalid;
            if (_scope.Role != ScopeRole.Authority) return CaptureResult.WrongRole;
            if (!World<TWorld>.IsTagTypeRegistered<ReplicatedTag>()) return CaptureResult.ScopeInvalid;

            var entities = ArrayPool<EntityGID>.Shared.Rent(ProtocolLimits.MaxEntities);
            var links = ArrayPool<EntityGID>.Shared.Rent(32768);
            PacketLease lease = default;
            var count = 0;
            try
            {
                foreach (var entity in World<TWorld>.Query<All<ReplicatedTag>>().Entities(EntityStatusType.Any, _scope.Clusters))
                {
                    if (count == ProtocolLimits.MaxEntities) return CaptureResult.LimitExceeded;
                    var gid = entity.GID;
                    if (gid.Version == 0 || !_scope.Contains(gid.Chunk, gid.ClusterId)) return CaptureResult.EntityConflict;
                    entities[count++] = gid;
                }
                var gids = entities.AsSpan(0, count);
                ReplicationSort.Sort(gids);
                for (var i = 1; i < count; i++) if (entities[i] == entities[i - 1]) return CaptureResult.EntityConflict;

                lease = PacketLease.Rent(ProtocolLimits.MaxDecodedPayloadBytes);
                var writer = new SnapshotWriter(lease.CapacitySpan);
                writer.U32((uint)count);
                var context = new CaptureContext { Entities = gids, LinkScratch = links.AsSpan(0, 32768) };
                var entries = _schema.RetainedEntries;
                for (var i = 0; i < count; i++)
                {
                    if (!entities[i].TryUnpack<TWorld>(out var entity) || !entity.Has<ReplicatedTag>()) return CaptureResult.InvalidEntity;
                    SchemaEntry kind = null;
                    for (var j = 0; j < entries.Length; j++)
                        if (entries[j].Invoker is IEntityInvoker<TWorld> invoker && invoker.Matches(entity)) { kind = entries[j]; break; }
                    if (kind == null) return CaptureResult.InvalidEntity;

                    writer.Entity(in entities[i]);
                    writer.Id(kind.TypeId);
                    writer.U16(entity.IsDisabled ? (ushort)EntityFlags.Disabled : (ushort)0);
                    var recordCountOffset = writer.ReserveU16();
                    ushort recordCount = 0;
                    for (var j = 0; j < entries.Length; j++)
                    {
                        if (entries[j].Invoker is not IRecordInvoker<TWorld> invoker) continue;
                        var result = invoker.Capture(entity, entries[j], ref writer, ref context);
                        if (result == CaptureRecordResult.Absent) continue;
                        if (result != CaptureRecordResult.Written) return Convert(result);
                        if (++recordCount > ProtocolLimits.MaxRecordsPerEntity) return CaptureResult.LimitExceeded;
                    }
                    writer.PatchU16(recordCountOffset, recordCount);
                    if (!writer.Valid) return CaptureResult.LimitExceeded;
                }
                lease.SetLength(writer.Position);
                payload = PacketLease.Transfer(ref lease);
                return CaptureResult.Success;
            }
            finally
            {
                if (lease.IsValid)
                {
                    lease.Dispose();
                    lease = default;
                }
                ArrayPool<EntityGID>.Shared.Return(entities);
                ArrayPool<EntityGID>.Shared.Return(links);
            }
        }

        /// <summary>Fully preflights and then applies one staged complete snapshot to the replica scope.</summary>
        public ApplyResult Apply(StagedPayload staged)
        {
            if (_disposed || _scope.IsDisposed || !_scope.ValidateCurrent()) return ApplyResult.ScopeInvalid;
            if (_scope.Role != ScopeRole.Replica) return ApplyResult.WrongRole;
            if (staged == null || !staged.IsActive || staged.Kind != PacketKind.FullSnapshot) return ApplyResult.WrongPayload;
            if (staged.SchemaHash != _schema.Hash) return ApplyResult.SchemaMismatch;
            if (!World<TWorld>.IsTagTypeRegistered<ReplicatedTag>()) return ApplyResult.ScopeInvalid;

            var stagedEntities = staged.Entities;
            if (stagedEntities.Length > ProtocolLimits.MaxEntities) return ApplyResult.LimitExceeded;
            var gidsArray = ArrayPool<EntityGID>.Shared.Rent(Math.Max(1, stagedEntities.Length));
            var kinds = ArrayPool<byte>.Shared.Rent(Math.Max(1, stagedEntities.Length));
            var existing = ArrayPool<ExistingEntity>.Shared.Rent(ProtocolLimits.MaxEntities);
            var existingCount = 0;
            try
            {
                var gids = gidsArray.AsSpan(0, stagedEntities.Length);
                var entries = _schema.RetainedEntries;

                for (var i = 0; i < stagedEntities.Length; i++)
                {
                    ref readonly var source = ref stagedEntities[i];
                    var wire = source.Entity;
                    if (wire.Version == 0 || !_scope.Contains(wire.Id >> Const.ENTITIES_IN_CHUNK_SHIFT, wire.ClusterId)) return ApplyResult.InvalidEntity;
                    var gid = new EntityGID(wire.Id, wire.Version, wire.ClusterId);
                    gids[i] = gid;
                    if (i > 0 && ReplicationSort.Compare(in gids[i - 1], in gid) >= 0) return ApplyResult.InvalidEntity;
                    if (i > 0 && gids[i - 1].Id == gid.Id) return ApplyResult.InvalidEntity;
                    if (!_schema.TryGet(source.KindId, out var kindEntry) || kindEntry.Invoker is not IEntityInvoker<TWorld> entityInvoker) return ApplyResult.InvalidEntity;
                    kinds[i] = entityInvoker.EntityTypeId;
                    if (i > 0 && (gids[i - 1].Id >> Const.ENTITIES_IN_SEGMENT_SHIFT) == (gid.Id >> Const.ENTITIES_IN_SEGMENT_SHIFT) && kinds[i - 1] != kinds[i]) return ApplyResult.InvalidEntity;

                }

                var context = new ApplyContext(gids);
                for (var i = 0; i < stagedEntities.Length; i++)
                {
                    ref readonly var source = ref stagedEntities[i];
                    for (var j = 0; j < source.RecordCount; j++)
                    {
                        ref readonly var record = ref staged.Records[source.FirstRecord + j];
                        if (!_schema.TryGet(record.TypeId, out var entry) || entry.Kind != (SchemaKind)record.Kind || entry.Version != record.Version ||
                            record.ElementCount > entry.MaxCount || entry.Invoker is not IRecordInvoker<TWorld> invoker || !invoker.Validate(staged.GetPayload(record), record.ElementCount))
                            return ApplyResult.InvalidEntity;
                        var result = invoker.Preflight(staged.GetPayload(record), record.ElementCount, record.Flags, ref context);
                        if (result != ApplyResult.Success) return result;
                    }
                }

                foreach (var entity in World<TWorld>.Query().Entities(EntityStatusType.Any, _scope.Clusters))
                {
                    var gid = entity.GID;
                    if (!_scope.Contains(gid.Chunk, gid.ClusterId)) continue;
                    if (existingCount == ProtocolLimits.MaxEntities) return ApplyResult.LimitExceeded;
                    existing[existingCount++] = new ExistingEntity(gid, entity.EntityType);
                    if (!_scope.Owns(in gid)) return ApplyResult.EntityConflict;
                }
                var currentLedger = _scope.Ledger;
                for (var i = 0; i < currentLedger.Length; i++)
                    if (!currentLedger[i].TryUnpack<TWorld>(out var ledgerEntity) || !ledgerEntity.Has<ReplicatedTag>() ||
                        !_scope.Contains(currentLedger[i].Chunk, currentLedger[i].ClusterId)) return ApplyResult.EntityConflict;
                if (existingCount != currentLedger.Length) return ApplyResult.EntityConflict;
                SortExisting(existing.AsSpan(0, existingCount));

                // Incoming entities already proved one kind per physical segment. Exact-GID survivors must retain
                // that incoming kind; every different-generation or absent ledger entity is outgoing and ignored here.
                for (var i = 0; i < stagedEntities.Length; i++)
                {
                    var occupant = FindExisting(existing.AsSpan(0, existingCount), gids[i].Id);
                    if (occupant >= 0 && existing[occupant].Gid == gids[i] && existing[occupant].Kind != kinds[i]) return ApplyResult.EntityConflict;
                }

                // No mutation occurs above this boundary.
                for (var i = 0; i < currentLedger.Length; i++)
                    if (!ReplicationSort.Contains(gids, in currentLedger[i]) && currentLedger[i].TryUnpack<TWorld>(out var outgoing)) outgoing.Destroy();

                for (var i = 0; i < stagedEntities.Length; i++)
                {
                    if (!gids[i].TryUnpack<TWorld>(out var entity))
                    {
                        if (!_schema.TryGet(stagedEntities[i].KindId, out var kindEntry) || kindEntry.Invoker is not IEntityInvoker<TWorld> creator) throw new InvalidOperationException("Preflighted entity kind disappeared.");
                        entity = creator.Create(gids[i]);
                    }
                    entity.Set<ReplicatedTag>();
                }

                ApplyPhase(staged, SchemaKind.Component, SchemaKind.Multi);
                ApplyPhase(staged, SchemaKind.Link, SchemaKind.Links);

                for (var i = 0; i < stagedEntities.Length; i++)
                {
                    var entity = gids[i].Unpack<TWorld>();
                    for (var j = 0; j < entries.Length; j++)
                    {
                        if (entries[j].Invoker is not IRecordInvoker<TWorld> invoker || HasRecord(staged, in stagedEntities[i], entries[j].TypeId)) continue;
                        invoker.Remove(entity);
                    }
                    for (var j = 0; j < stagedEntities[i].RecordCount; j++)
                    {
                        ref readonly var record = ref staged.Records[stagedEntities[i].FirstRecord + j];
                        ((IRecordInvoker<TWorld>)_schemaEntry(record.TypeId).Invoker).Normalize(entity, record.Flags);
                    }
                    if (stagedEntities[i].Flags == EntityFlags.Disabled) entity.Disable(); else entity.Enable();
                }

                _scope.ReplaceLedger(gids);
                return ApplyResult.Success;
            }
            finally
            {
                ArrayPool<EntityGID>.Shared.Return(gidsArray);
                ArrayPool<byte>.Shared.Return(kinds);
                ArrayPool<ExistingEntity>.Shared.Return(existing);
            }

            SchemaEntry _schemaEntry(TypeId id) { _schema.TryGet(id, out var entry); return entry; }
        }

        /// <summary>Stops this replicator from using its scope. The scope remains caller-owned.</summary>
        public void Dispose() => _disposed = true;

        private void ApplyPhase(StagedPayload staged, SchemaKind first, SchemaKind last)
        {
            var entities = staged.Entities;
            for (var i = 0; i < entities.Length; i++)
            {
                var wire = entities[i].Entity;
                var entity = new EntityGID(wire.Id, wire.Version, wire.ClusterId).Unpack<TWorld>();
                for (var j = 0; j < entities[i].RecordCount; j++)
                {
                    ref readonly var record = ref staged.Records[entities[i].FirstRecord + j];
                    var kind = (SchemaKind)record.Kind;
                    if (kind < first || kind > last || first == SchemaKind.Component && kind != SchemaKind.Component && kind != SchemaKind.Tag && kind != SchemaKind.Multi) continue;
                    _schema.TryGet(record.TypeId, out var entry);
                    ((IRecordInvoker<TWorld>)entry.Invoker).Apply(entity, staged.GetPayload(record), record.ElementCount);
                }
            }
        }

        private static bool HasRecord(StagedPayload staged, in StagedEntity entity, TypeId typeId)
        {
            for (var i = 0; i < entity.RecordCount; i++) if (staged.Records[entity.FirstRecord + i].TypeId == typeId) return true;
            return false;
        }

        private static int FindExisting(ReadOnlySpan<ExistingEntity> values, uint id)
        {
            var low = 0;
            var high = values.Length - 1;
            while (low <= high)
            {
                var middle = low + ((high - low) >> 1);
                if (values[middle].Gid.Id == id) return middle;
                if (values[middle].Gid.Id < id) low = middle + 1; else high = middle - 1;
            }
            return -1;
        }

        private static void SortExisting(Span<ExistingEntity> values)
        {
            if (values.Length > 1) SortExisting(values, 0, values.Length - 1);
        }

        private static void SortExisting(Span<ExistingEntity> values, int left, int right)
        {
            while (left < right)
            {
                var i = left;
                var j = right;
                var pivot = values[left + ((right - left) >> 1)].Gid.Id;
                while (i <= j)
                {
                    while (values[i].Gid.Id < pivot) i++;
                    while (values[j].Gid.Id > pivot) j--;
                    if (i <= j) { var value = values[i]; values[i++] = values[j]; values[j--] = value; }
                }
                if (j - left < right - i) { if (left < j) SortExisting(values, left, j); left = i; }
                else { if (i < right) SortExisting(values, i, right); right = j; }
            }
        }

        private static CaptureResult Convert(CaptureRecordResult result) => result switch
        {
            CaptureRecordResult.EntityConflict => CaptureResult.EntityConflict,
            CaptureRecordResult.DisabledUnsupported => CaptureResult.DisabledUnsupported,
            CaptureRecordResult.MissingTarget => CaptureResult.MissingTarget,
            CaptureRecordResult.LimitExceeded => CaptureResult.LimitExceeded,
            CaptureRecordResult.CodecFailed => CaptureResult.CodecFailed,
            _ => CaptureResult.InvalidEntity
        };

        private readonly struct ExistingEntity
        {
            internal ExistingEntity(EntityGID gid, byte kind) { Gid = gid; Kind = kind; }
            internal EntityGID Gid { get; }
            internal byte Kind { get; }
        }
    }
}
