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
        private readonly NetworkReplicaSkipPolicy<TWorld> _skipPolicy;
        private readonly NetworkSnapshotPool _snapshotPool;
        private int _captureCapacity = 4096;

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
    }
}
