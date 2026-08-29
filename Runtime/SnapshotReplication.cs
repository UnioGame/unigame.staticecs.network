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

    /// <summary>Encodes and reconstructs deterministic canonical snapshot deltas without ECS mutation.</summary>
    internal static class SnapshotDeltaCodec
    {
        private const int DeltaHeaderSize = sizeof(uint) * 3;
        private const int EntityHeaderSize = sizeof(ulong) + sizeof(uint) + sizeof(byte) + sizeof(ushort);

        private enum EntityOperation : byte
        {
            Add = 1,
            Remove = 2,
            Patch = 3,
        }

        private enum RecordOperation : byte
        {
            Add = 1,
            Remove = 2,
            Replace = 3,
        }

        internal static bool TryEncode(NetworkBufferPool pool,
            NetworkSnapshot baseline, NetworkSnapshot target,
            out NetworkBufferLease delta)
        {
            delta = null;
            if (pool == null || baseline == null || target == null ||
                baseline.ServerTick == 0 || target.ServerTick <= baseline.ServerTick ||
                baseline.SchemaFingerprint != target.SchemaFingerprint ||
                baseline.Scope != target.Scope)
                return false;

            var measure = new SnapshotWriter(true);
            if (!TryEncodeCore(baseline, target, ref measure, out var operationCount) ||
                measure.Length > ProtocolLimits.MaxDecodedPayloadBytes)
                return false;

            var lease = pool.Rent(checked((int)measure.Length));
            var writer = new SnapshotWriter(lease.WritableSpan);
            if (!TryEncodeCore(baseline, target, ref writer, out var writtenOperations) ||
                writtenOperations != operationCount || writer.Length != measure.Length)
            {
                lease.Dispose();
                return false;
            }

            delta = lease;
            return true;
        }

        internal static bool TryReconstruct(NetworkBufferPool pool,
            NetworkSnapshot baseline, ReadOnlySpan<byte> delta,
            in SnapshotChunkHeader header, SchemaFingerprint schema, ScopeId scope,
            out NetworkSnapshot snapshot)
        {
            snapshot = null;
            if (pool == null || baseline == null ||
                header.PayloadKind != SnapshotPayloadKind.Delta ||
                baseline.ServerTick == 0 || header.BaselineTick != baseline.ServerTick ||
                header.SnapshotTick <= baseline.ServerTick ||
                header.TotalLength == 0 ||
                header.TotalLength > ProtocolLimits.MaxDecodedPayloadBytes ||
                header.ChunkCount == 0 || header.ChunkIndex >= header.ChunkCount ||
                baseline.SchemaFingerprint != schema || baseline.Scope != scope ||
                delta.Length > ProtocolLimits.MaxDecodedPayloadBytes)
                return false;

            var measure = new SnapshotWriter(true);
            if (!TryReconstructCore(baseline, delta, ref measure,
                    out var entityCount, out var recordCount) ||
                measure.Length != header.TotalLength)
                return false;

            var lease = pool.Rent(checked((int)header.TotalLength));
            var writer = new SnapshotWriter(lease.WritableSpan);
            if (!TryReconstructCore(baseline, delta, ref writer,
                    out var writtenEntities, out var writtenRecords) ||
                writtenEntities != entityCount || writtenRecords != recordCount ||
                writer.Length != header.TotalLength ||
                Hashing.XxHash64(lease.Span) != header.TotalHash)
            {
                lease.Dispose();
                return false;
            }

            snapshot = new NetworkSnapshot(header.SnapshotTick, schema, scope,
                lease, entityCount, recordCount);
            return true;
        }

        internal static bool TryInspectCanonical(ReadOnlySpan<byte> bytes,
            out int entityCount, out int recordCount)
        {
            entityCount = 0;
            recordCount = 0;
            if (bytes.Length < sizeof(uint) ||
                bytes.Length > ProtocolLimits.MaxDecodedPayloadBytes)
                return false;
            var offset = 0;
            if (!TryReadUint(bytes, ref offset, out var rawEntityCount) ||
                rawEntityCount > (uint)ProtocolLimits.MaxEntities)
                return false;
            entityCount = checked((int)rawEntityCount);
            ulong previousGid = 0;
            var hasPrevious = false;
            for (var index = 0; index < entityCount; index++)
            {
                if (!TryReadCanonicalEntity(bytes, ref offset, out var entity) ||
                    hasPrevious && CompareGid(previousGid, entity.Gid) >= 0 ||
                    recordCount > ProtocolLimits.MaxEntities *
                        ProtocolLimits.MaxRecordsPerEntity - entity.RecordCount)
                    return false;
                previousGid = entity.Gid;
                hasPrevious = true;
                recordCount += entity.RecordCount;
            }
            return offset == bytes.Length;
        }

        private static bool TryEncodeCore(NetworkSnapshot baseline,
            NetworkSnapshot target, ref SnapshotWriter writer,
            out uint operationCount)
        {
            operationCount = 0;
            if (!TryOpenSnapshot(baseline, out var baselineCursor) ||
                !TryOpenSnapshot(target, out var targetCursor) ||
                !writer.TryWriteUint(checked((uint)target.EntityCount)) ||
                !writer.TryWriteUint(checked((uint)target.RecordCount)))
                return false;
            var countOffset = writer.Length;
            if (!writer.TryWriteUint(0))
                return false;

            if (!TryMoveNext(ref baselineCursor, out var baselineEntity,
                    out var hasBaseline) ||
                !TryMoveNext(ref targetCursor, out var targetEntity,
                    out var hasTarget))
                return false;

            while (hasBaseline || hasTarget)
            {
                var comparison = !hasBaseline ? 1 : !hasTarget ? -1 :
                    CompareGid(baselineEntity.Gid, targetEntity.Gid);
                if (comparison < 0)
                {
                    if (!writer.TryWriteByte((byte)EntityOperation.Remove) ||
                        !writer.TryWriteUlong(baselineEntity.Gid))
                        return false;
                    operationCount++;
                    if (!TryMoveNext(ref baselineCursor, out baselineEntity,
                            out hasBaseline))
                        return false;
                    continue;
                }
                if (comparison > 0)
                {
                    if (!writer.TryWriteByte((byte)EntityOperation.Add) ||
                        !writer.TryWrite(targetEntity.Raw))
                        return false;
                    operationCount++;
                    if (!TryMoveNext(ref targetCursor, out targetEntity,
                            out hasTarget))
                        return false;
                    continue;
                }

                if (!baselineEntity.Raw.SequenceEqual(targetEntity.Raw))
                {
                    if (!TryWritePatch(in baselineEntity, in targetEntity,
                            ref writer))
                        return false;
                    operationCount++;
                }
                if (!TryMoveNext(ref baselineCursor, out baselineEntity,
                        out hasBaseline) ||
                    !TryMoveNext(ref targetCursor, out targetEntity,
                        out hasTarget))
                    return false;
            }

            return baselineCursor.Complete && targetCursor.Complete &&
                   writer.TryWriteUintAt(countOffset, operationCount);
        }

        private static bool TryWritePatch(in CanonicalEntity baseline,
            in CanonicalEntity target, ref SnapshotWriter writer)
        {
            if (!writer.TryWriteByte((byte)EntityOperation.Patch) ||
                !writer.TryWrite(target.Header))
                return false;
            var countOffset = writer.Length;
            if (!writer.TryWriteUint(0))
                return false;

            var baselineCursor = new RecordCursor(baseline.Records,
                baseline.RecordCount);
            var targetCursor = new RecordCursor(target.Records,
                target.RecordCount);
            if (!TryMoveNext(ref baselineCursor, out var baselineRecord,
                    out var hasBaseline) ||
                !TryMoveNext(ref targetCursor, out var targetRecord,
                    out var hasTarget))
                return false;
            uint operationCount = 0;
            while (hasBaseline || hasTarget)
            {
                var comparison = !hasBaseline ? 1 : !hasTarget ? -1 :
                    CompareRecord(baselineRecord.Kind, baselineRecord.TypeId,
                        targetRecord.Kind, targetRecord.TypeId);
                if (comparison < 0)
                {
                    if (!writer.TryWriteByte((byte)RecordOperation.Remove) ||
                        !writer.TryWriteUint(baselineRecord.TypeId) ||
                        !writer.TryWriteByte(baselineRecord.Kind))
                        return false;
                    operationCount++;
                    if (!TryMoveNext(ref baselineCursor, out baselineRecord,
                            out hasBaseline))
                        return false;
                    continue;
                }
                if (comparison > 0)
                {
                    if (!writer.TryWriteByte((byte)RecordOperation.Add) ||
                        !writer.TryWrite(targetRecord.Raw))
                        return false;
                    operationCount++;
                    if (!TryMoveNext(ref targetCursor, out targetRecord,
                            out hasTarget))
                        return false;
                    continue;
                }
                if (!baselineRecord.Raw.SequenceEqual(targetRecord.Raw))
                {
                    if (!writer.TryWriteByte((byte)RecordOperation.Replace) ||
                        !writer.TryWrite(targetRecord.Raw))
                        return false;
                    operationCount++;
                }
                if (!TryMoveNext(ref baselineCursor, out baselineRecord,
                        out hasBaseline) ||
                    !TryMoveNext(ref targetCursor, out targetRecord,
                        out hasTarget))
                    return false;
            }
            return baselineCursor.Complete && targetCursor.Complete &&
                   writer.TryWriteUintAt(countOffset, operationCount);
        }

        private static bool TryReconstructCore(NetworkSnapshot baseline,
            ReadOnlySpan<byte> delta, ref SnapshotWriter writer,
            out int entityCount, out int recordCount)
        {
            entityCount = 0;
            recordCount = 0;
            if (!TryOpenSnapshot(baseline, out var baselineCursor) ||
                !TryOpenDelta(delta, out var targetEntities, out var targetRecords,
                    out var operationCursor) ||
                !writer.TryWriteUint(targetEntities) ||
                !TryMoveNext(ref baselineCursor, out var baselineEntity,
                    out var hasBaseline) ||
                !TryMoveNext(ref operationCursor, out var operation,
                    out var hasOperation))
                return false;

            while (hasBaseline || hasOperation)
            {
                var comparison = !hasBaseline ? 1 : !hasOperation ? -1 :
                    CompareGid(baselineEntity.Gid, operation.Gid);
                if (comparison < 0)
                {
                    if (!writer.TryWrite(baselineEntity.Raw) ||
                        !TryAdd(ref entityCount, 1) ||
                        !TryAdd(ref recordCount, baselineEntity.RecordCount) ||
                        !TryMoveNext(ref baselineCursor, out baselineEntity,
                            out hasBaseline))
                        return false;
                    continue;
                }
                if (comparison > 0)
                {
                    if (operation.Kind != EntityOperation.Add ||
                        !writer.TryWrite(operation.Added.Raw) ||
                        !TryAdd(ref entityCount, 1) ||
                        !TryAdd(ref recordCount, operation.Added.RecordCount) ||
                        !TryMoveNext(ref operationCursor, out operation,
                            out hasOperation))
                        return false;
                    continue;
                }

                if (operation.Kind == EntityOperation.Add)
                    return false;
                if (operation.Kind == EntityOperation.Patch)
                {
                    if (!TryReconstructPatch(in baselineEntity, in operation,
                            ref writer, out var patchedRecords) ||
                        !TryAdd(ref entityCount, 1) ||
                        !TryAdd(ref recordCount, patchedRecords))
                        return false;
                }
                if (!TryMoveNext(ref baselineCursor, out baselineEntity,
                        out hasBaseline) ||
                    !TryMoveNext(ref operationCursor, out operation,
                        out hasOperation))
                    return false;
            }

            return baselineCursor.Complete && operationCursor.Complete &&
                   entityCount == targetEntities && recordCount == targetRecords;
        }

        private static bool TryReconstructPatch(in CanonicalEntity baseline,
            in EntityDelta operation, ref SnapshotWriter writer,
            out int recordCount)
        {
            recordCount = 0;
            if (!writer.TryWrite(operation.Header))
                return false;
            var baselineCursor = new RecordCursor(baseline.Records,
                baseline.RecordCount);
            var operationCursor = new RecordDeltaCursor(operation.RecordOperations,
                operation.RecordOperationCount);
            if (!TryMoveNext(ref baselineCursor, out var baselineRecord,
                    out var hasBaseline) ||
                !TryMoveNext(ref operationCursor, out var recordOperation,
                    out var hasOperation))
                return false;

            while (hasBaseline || hasOperation)
            {
                var comparison = !hasBaseline ? 1 : !hasOperation ? -1 :
                    CompareRecord(baselineRecord.Kind, baselineRecord.TypeId,
                        recordOperation.Kind, recordOperation.TypeId);
                if (comparison < 0)
                {
                    if (!writer.TryWrite(baselineRecord.Raw) ||
                        !TryAdd(ref recordCount, 1) ||
                        !TryMoveNext(ref baselineCursor, out baselineRecord,
                            out hasBaseline))
                        return false;
                    continue;
                }
                if (comparison > 0)
                {
                    if (recordOperation.Operation != RecordOperation.Add ||
                        !writer.TryWrite(recordOperation.Raw) ||
                        !TryAdd(ref recordCount, 1) ||
                        !TryMoveNext(ref operationCursor, out recordOperation,
                            out hasOperation))
                        return false;
                    continue;
                }

                if (recordOperation.Operation == RecordOperation.Add)
                    return false;
                if (recordOperation.Operation == RecordOperation.Replace)
                {
                    if (!writer.TryWrite(recordOperation.Raw) ||
                        !TryAdd(ref recordCount, 1))
                        return false;
                }
                if (!TryMoveNext(ref baselineCursor, out baselineRecord,
                        out hasBaseline) ||
                    !TryMoveNext(ref operationCursor, out recordOperation,
                        out hasOperation))
                    return false;
            }

            return baselineCursor.Complete && operationCursor.Complete &&
                   recordCount == operation.RecordCount;
        }

        private static bool TryOpenSnapshot(NetworkSnapshot snapshot,
            out CanonicalCursor cursor)
        {
            cursor = default;
            if (snapshot == null || snapshot.ByteLength < sizeof(uint) ||
                snapshot.ByteLength > ProtocolLimits.MaxDecodedPayloadBytes ||
                snapshot.EntityCount < 0 || snapshot.EntityCount > ProtocolLimits.MaxEntities ||
                snapshot.RecordCount < 0 ||
                snapshot.RecordCount > ProtocolLimits.MaxEntities *
                    ProtocolLimits.MaxRecordsPerEntity ||
                Hashing.XxHash64(snapshot.Bytes.Span) != snapshot.PayloadHash)
                return false;
            var bytes = snapshot.Bytes.Span;
            var offset = 0;
            if (!TryReadUint(bytes, ref offset, out var entityCount) ||
                entityCount != snapshot.EntityCount)
                return false;
            cursor = new CanonicalCursor(bytes, offset, snapshot.EntityCount,
                snapshot.RecordCount);
            return true;
        }

        private static bool TryOpenDelta(ReadOnlySpan<byte> bytes,
            out uint entityCount, out uint recordCount,
            out EntityDeltaCursor cursor)
        {
            entityCount = 0;
            recordCount = 0;
            cursor = default;
            var offset = 0;
            if (bytes.Length < DeltaHeaderSize ||
                !TryReadUint(bytes, ref offset, out entityCount) ||
                !TryReadUint(bytes, ref offset, out recordCount) ||
                !TryReadUint(bytes, ref offset, out var operationCount) ||
                entityCount > (uint)ProtocolLimits.MaxEntities ||
                recordCount > (uint)(ProtocolLimits.MaxEntities *
                    ProtocolLimits.MaxRecordsPerEntity) ||
                operationCount > (uint)ProtocolLimits.MaxEntities * 2u)
            {
                return false;
            }

            cursor = new EntityDeltaCursor(bytes, offset,
                checked((int)operationCount));
            return true;
        }

        private static bool TryReadCanonicalEntity(ReadOnlySpan<byte> bytes,
            ref int offset, out CanonicalEntity entity)
        {
            entity = default;
            var start = offset;
            if (!TryReadEntityHeader(bytes, ref offset, out var gid, out var kind,
                    out var disabled, out var recordCount, out var header))
                return false;
            var recordsStart = offset;
            byte previousKind = 0;
            uint previousType = 0;
            var hasPrevious = false;
            for (var index = 0; index < recordCount; index++)
            {
                if (!TryReadCanonicalRecord(bytes, ref offset, out var record) ||
                    hasPrevious && CompareRecord(previousKind, previousType,
                        record.Kind, record.TypeId) >= 0)
                    return false;
                previousKind = record.Kind;
                previousType = record.TypeId;
                hasPrevious = true;
            }
            entity = new CanonicalEntity(gid, kind, disabled, recordCount,
                header, bytes.Slice(recordsStart, offset - recordsStart),
                bytes.Slice(start, offset - start));
            return true;
        }

        private static bool TryReadEntityHeader(ReadOnlySpan<byte> bytes,
            ref int offset, out ulong gid, out uint kind, out byte disabled,
            out ushort recordCount, out ReadOnlySpan<byte> header)
        {
            var start = offset;
            header = default;
            if (!TryReadUlong(bytes, ref offset, out gid) ||
                !TryReadUint(bytes, ref offset, out kind) ||
                !TryReadByte(bytes, ref offset, out disabled) ||
                !TryReadUshort(bytes, ref offset, out recordCount) ||
                new EntityGID(gid).Version == 0 || kind == 0 || disabled > 1 ||
                recordCount > ProtocolLimits.MaxRecordsPerEntity)
                return false;
            header = bytes.Slice(start, EntityHeaderSize);
            return true;
        }

        private static bool TryReadCanonicalRecord(ReadOnlySpan<byte> bytes,
            ref int offset, out CanonicalRecord record)
        {
            record = default;
            var start = offset;
            if (!TryReadUint(bytes, ref offset, out var typeId) ||
                !TryReadByte(bytes, ref offset, out var kind) ||
                !TryReadByte(bytes, ref offset, out _) ||
                !TryReadByte(bytes, ref offset, out var disabled) ||
                !TryReadUint(bytes, ref offset, out var length) ||
                typeId == 0 || kind < (byte)NetworkSchemaKind.Component ||
                kind > (byte)NetworkSchemaKind.Multi || disabled > 1 ||
                length > ProtocolLimits.MaxComponentBytes ||
                length > bytes.Length - offset)
                return false;
            offset += checked((int)length);
            record = new CanonicalRecord(typeId, kind,
                bytes.Slice(start, offset - start));
            return true;
        }

        private static bool TryMoveNext(ref CanonicalCursor cursor,
            out CanonicalEntity value, out bool hasValue)
        {
            value = default;
            hasValue = cursor.Remaining > 0;
            return !hasValue || cursor.TryRead(out value);
        }

        private static bool TryMoveNext(ref RecordCursor cursor,
            out CanonicalRecord value, out bool hasValue)
        {
            value = default;
            hasValue = cursor.Remaining > 0;
            return !hasValue || cursor.TryRead(out value);
        }

        private static bool TryMoveNext(ref EntityDeltaCursor cursor,
            out EntityDelta value, out bool hasValue)
        {
            value = default;
            hasValue = cursor.Remaining > 0;
            return !hasValue || cursor.TryRead(out value);
        }

        private static bool TryMoveNext(ref RecordDeltaCursor cursor,
            out RecordDelta value, out bool hasValue)
        {
            value = default;
            hasValue = cursor.Remaining > 0;
            return !hasValue || cursor.TryRead(out value);
        }

        private static bool TryAdd(ref int value, int addition)
        {
            if (addition < 0 || value > int.MaxValue - addition)
                return false;
            value += addition;
            return true;
        }

        private static int CompareGid(ulong leftRaw, ulong rightRaw)
        {
            var left = new EntityGID(leftRaw);
            var right = new EntityGID(rightRaw);
            var cluster = left.ClusterId.CompareTo(right.ClusterId);
            var id = left.Id.CompareTo(right.Id);
            return cluster != 0 ? cluster : id != 0 ? id :
                left.Version.CompareTo(right.Version);
        }

        private static int CompareRecord(byte leftKind, uint leftType,
            byte rightKind, uint rightType)
        {
            var kind = leftKind.CompareTo(rightKind);
            return kind != 0 ? kind : leftType.CompareTo(rightType);
        }

        private static bool TryReadByte(ReadOnlySpan<byte> bytes, ref int offset,
            out byte value)
        {
            if ((uint)offset >= (uint)bytes.Length)
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
            if (offset < 0 || offset > bytes.Length - sizeof(ushort))
            {
                value = 0;
                return false;
            }
            value = (ushort)(bytes[offset] | bytes[offset + 1] << 8);
            offset += sizeof(ushort);
            return true;
        }

        private static bool TryReadUint(ReadOnlySpan<byte> bytes, ref int offset,
            out uint value)
        {
            if (offset < 0 || offset > bytes.Length - sizeof(uint))
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
            if (offset < 0 || offset > bytes.Length - sizeof(ulong))
            {
                value = 0;
                return false;
            }
            value = Hashing.Read64(bytes, offset);
            offset += sizeof(ulong);
            return true;
        }

        private ref struct SnapshotWriter
        {
            private readonly Span<byte> _destination;
            private readonly bool _measure;
            private long _length;

            internal SnapshotWriter(bool measure)
            {
                _destination = Span<byte>.Empty;
                _measure = measure;
                _length = 0;
            }

            internal SnapshotWriter(Span<byte> destination)
            {
                _destination = destination;
                _measure = false;
                _length = 0;
            }

            internal long Length => _length;

            internal bool TryWriteByte(byte value)
            {
                if (!TryReserve(sizeof(byte), out var offset))
                    return false;
                if (!_measure)
                    _destination[offset] = value;
                return true;
            }

            internal bool TryWriteUint(uint value)
            {
                if (!TryReserve(sizeof(uint), out var offset))
                    return false;
                if (!_measure)
                    Hashing.Write32(_destination, offset, value);
                return true;
            }

            internal bool TryWriteUlong(ulong value)
            {
                if (!TryReserve(sizeof(ulong), out var offset))
                    return false;
                if (!_measure)
                    Hashing.Write64(_destination, offset, value);
                return true;
            }

            internal bool TryWrite(ReadOnlySpan<byte> value)
            {
                if (!TryReserve(value.Length, out var offset))
                    return false;
                if (!_measure)
                    value.CopyTo(_destination.Slice(offset, value.Length));
                return true;
            }

            internal bool TryWriteUintAt(long position, uint value)
            {
                if (position < 0 || position > _length - sizeof(uint))
                    return false;
                if (!_measure)
                    Hashing.Write32(_destination, checked((int)position), value);
                return true;
            }

            private bool TryReserve(int length, out int offset)
            {
                offset = 0;
                if (length < 0 || _length > int.MaxValue - length)
                    return false;
                offset = checked((int)_length);
                _length += length;
                return _measure || _length <= _destination.Length;
            }
        }

        private ref struct CanonicalCursor
        {
            private readonly ReadOnlySpan<byte> _bytes;
            private readonly int _expectedRecords;
            private int _offset;
            private int _records;
            private ulong _previousGid;
            private bool _hasPrevious;

            internal CanonicalCursor(ReadOnlySpan<byte> bytes, int offset,
                int entities, int expectedRecords)
            {
                _bytes = bytes;
                _offset = offset;
                Remaining = entities;
                _expectedRecords = expectedRecords;
                _records = 0;
                _previousGid = 0;
                _hasPrevious = false;
            }

            internal int Remaining { get; private set; }
            internal bool Complete => Remaining == 0 && _offset == _bytes.Length &&
                                      _records == _expectedRecords;

            internal bool TryRead(out CanonicalEntity entity)
            {
                entity = default;
                if (Remaining <= 0 ||
                    !TryReadCanonicalEntity(_bytes, ref _offset, out entity) ||
                    _hasPrevious && CompareGid(_previousGid, entity.Gid) >= 0 ||
                    _records > _expectedRecords - entity.RecordCount)
                    return false;
                _previousGid = entity.Gid;
                _hasPrevious = true;
                _records += entity.RecordCount;
                Remaining--;
                return true;
            }
        }

        private ref struct RecordCursor
        {
            private readonly ReadOnlySpan<byte> _bytes;
            private int _offset;

            internal RecordCursor(ReadOnlySpan<byte> bytes, int count)
            {
                _bytes = bytes;
                _offset = 0;
                Remaining = count;
            }

            internal int Remaining { get; private set; }
            internal bool Complete => Remaining == 0 && _offset == _bytes.Length;

            internal bool TryRead(out CanonicalRecord record)
            {
                record = default;
                if (Remaining <= 0 ||
                    !TryReadCanonicalRecord(_bytes, ref _offset, out record))
                    return false;
                Remaining--;
                return true;
            }
        }

        private ref struct EntityDeltaCursor
        {
            private readonly ReadOnlySpan<byte> _bytes;
            private int _offset;
            private ulong _previousGid;
            private bool _hasPrevious;

            internal EntityDeltaCursor(ReadOnlySpan<byte> bytes, int offset,
                int count)
            {
                _bytes = bytes;
                _offset = offset;
                Remaining = count;
                _previousGid = 0;
                _hasPrevious = false;
            }

            internal int Remaining { get; private set; }
            internal bool Complete => Remaining == 0 && _offset == _bytes.Length;

            internal bool TryRead(out EntityDelta operation)
            {
                operation = default;
                if (Remaining <= 0 || !TryReadByte(_bytes, ref _offset,
                        out var rawOperation))
                    return false;
                var kind = (EntityOperation)rawOperation;
                ulong gid;
                if (kind == EntityOperation.Add)
                {
                    if (!TryReadCanonicalEntity(_bytes, ref _offset,
                            out var added))
                        return false;
                    gid = added.Gid;
                    operation = EntityDelta.Add(in added);
                }
                else if (kind == EntityOperation.Remove)
                {
                    if (!TryReadUlong(_bytes, ref _offset, out gid) ||
                        new EntityGID(gid).Version == 0)
                        return false;
                    operation = EntityDelta.Remove(gid);
                }
                else if (kind == EntityOperation.Patch)
                {
                    if (!TryReadEntityHeader(_bytes, ref _offset, out gid,
                            out _, out _, out var recordCount, out var header) ||
                        !TryReadUint(_bytes, ref _offset, out var rawCount) ||
                        rawCount > ProtocolLimits.MaxRecordsPerEntity * 2u)
                        return false;
                    var count = checked((int)rawCount);
                    var operationStart = _offset;
                    var records = new RecordDeltaCursor(
                        _bytes.Slice(operationStart), count);
                    while (records.Remaining > 0)
                        if (!records.TryRead(out _))
                            return false;
                    _offset += records.Consumed;
                    operation = EntityDelta.Patch(gid, recordCount, header,
                        _bytes.Slice(operationStart, records.Consumed), count);
                }
                else
                {
                    return false;
                }
                if (_hasPrevious && CompareGid(_previousGid, gid) >= 0)
                    return false;
                _previousGid = gid;
                _hasPrevious = true;
                Remaining--;
                return true;
            }
        }

        private ref struct RecordDeltaCursor
        {
            private readonly ReadOnlySpan<byte> _bytes;
            private int _offset;
            private byte _previousKind;
            private uint _previousType;
            private bool _hasPrevious;

            internal RecordDeltaCursor(ReadOnlySpan<byte> bytes, int count)
            {
                _bytes = bytes;
                _offset = 0;
                Remaining = count;
                _previousKind = 0;
                _previousType = 0;
                _hasPrevious = false;
            }

            internal int Remaining { get; private set; }
            internal int Consumed => _offset;
            internal bool Complete => Remaining == 0 && _offset == _bytes.Length;

            internal bool TryRead(out RecordDelta operation)
            {
                operation = default;
                if (Remaining <= 0 || !TryReadByte(_bytes, ref _offset,
                        out var rawOperation))
                    return false;
                var kind = (RecordOperation)rawOperation;
                uint typeId;
                byte wireKind;
                if (kind == RecordOperation.Add ||
                    kind == RecordOperation.Replace)
                {
                    if (!TryReadCanonicalRecord(_bytes, ref _offset,
                            out var record))
                        return false;
                    typeId = record.TypeId;
                    wireKind = record.Kind;
                    operation = new RecordDelta(kind, typeId, wireKind,
                        record.Raw);
                }
                else if (kind == RecordOperation.Remove)
                {
                    if (!TryReadUint(_bytes, ref _offset, out typeId) ||
                        !TryReadByte(_bytes, ref _offset, out wireKind) ||
                        typeId == 0 || wireKind < (byte)NetworkSchemaKind.Component ||
                        wireKind > (byte)NetworkSchemaKind.Multi)
                        return false;
                    operation = new RecordDelta(kind, typeId, wireKind,
                        ReadOnlySpan<byte>.Empty);
                }
                else
                {
                    return false;
                }
                if (_hasPrevious && CompareRecord(_previousKind, _previousType,
                        wireKind, typeId) >= 0)
                    return false;
                _previousKind = wireKind;
                _previousType = typeId;
                _hasPrevious = true;
                Remaining--;
                return true;
            }
        }

        private readonly ref struct CanonicalEntity
        {
            internal CanonicalEntity(ulong gid, uint kind, byte disabled,
                ushort recordCount, ReadOnlySpan<byte> header,
                ReadOnlySpan<byte> records, ReadOnlySpan<byte> raw)
            {
                Gid = gid;
                Kind = kind;
                Disabled = disabled;
                RecordCount = recordCount;
                Header = header;
                Records = records;
                Raw = raw;
            }

            internal ulong Gid { get; }
            internal uint Kind { get; }
            internal byte Disabled { get; }
            internal ushort RecordCount { get; }
            internal ReadOnlySpan<byte> Header { get; }
            internal ReadOnlySpan<byte> Records { get; }
            internal ReadOnlySpan<byte> Raw { get; }
        }

        private readonly ref struct CanonicalRecord
        {
            internal CanonicalRecord(uint typeId, byte kind,
                ReadOnlySpan<byte> raw)
            {
                TypeId = typeId;
                Kind = kind;
                Raw = raw;
            }

            internal uint TypeId { get; }
            internal byte Kind { get; }
            internal ReadOnlySpan<byte> Raw { get; }
        }

        private readonly ref struct EntityDelta
        {
            private EntityDelta(EntityOperation kind, ulong gid,
                in CanonicalEntity added, ushort recordCount,
                ReadOnlySpan<byte> header, ReadOnlySpan<byte> recordOperations,
                int recordOperationCount)
            {
                Kind = kind;
                Gid = gid;
                Added = added;
                RecordCount = recordCount;
                Header = header;
                RecordOperations = recordOperations;
                RecordOperationCount = recordOperationCount;
            }

            internal EntityOperation Kind { get; }
            internal ulong Gid { get; }
            internal CanonicalEntity Added { get; }
            internal ushort RecordCount { get; }
            internal ReadOnlySpan<byte> Header { get; }
            internal ReadOnlySpan<byte> RecordOperations { get; }
            internal int RecordOperationCount { get; }

            internal static EntityDelta Add(in CanonicalEntity entity) =>
                new EntityDelta(EntityOperation.Add, entity.Gid, in entity, 0,
                    default, default, 0);

            internal static EntityDelta Remove(ulong gid)
            {
                var entity = default(CanonicalEntity);
                return new EntityDelta(EntityOperation.Remove, gid, in entity, 0,
                    default, default, 0);
            }

            internal static EntityDelta Patch(ulong gid, ushort recordCount,
                ReadOnlySpan<byte> header, ReadOnlySpan<byte> operations,
                int operationCount)
            {
                var entity = default(CanonicalEntity);
                return new EntityDelta(EntityOperation.Patch, gid, in entity,
                    recordCount, header, operations, operationCount);
            }
        }

        private readonly ref struct RecordDelta
        {
            internal RecordDelta(RecordOperation operation, uint typeId,
                byte kind, ReadOnlySpan<byte> raw)
            {
                Operation = operation;
                TypeId = typeId;
                Kind = kind;
                Raw = raw;
            }

            internal RecordOperation Operation { get; }
            internal uint TypeId { get; }
            internal byte Kind { get; }
            internal ReadOnlySpan<byte> Raw { get; }
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
