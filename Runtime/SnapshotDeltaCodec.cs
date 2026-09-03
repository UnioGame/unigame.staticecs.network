namespace UniGame.StaticEcs.Network
{
    using System;
    using System.Buffers;
    using System.Collections.Generic;
    using FFS.Libraries.StaticEcs;
    using FFS.Libraries.StaticPack;

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
            out NetworkBufferLease canonical, out int entityCount,
            out int recordCount)
        {
            canonical = null;
            entityCount = 0;
            recordCount = 0;
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
                    out var measuredEntities, out var measuredRecords) ||
                measure.Length != header.TotalLength)
                return false;

            var lease = pool.Rent(checked((int)header.TotalLength));
            try
            {
                var writer = new SnapshotWriter(lease.WritableSpan);
                if (!TryReconstructCore(baseline, delta, ref writer,
                        out var writtenEntities, out var writtenRecords) ||
                    writtenEntities != measuredEntities ||
                    writtenRecords != measuredRecords ||
                    writer.Length != header.TotalLength ||
                    Hashing.XxHash64(lease.Span) != header.TotalHash)
                    return false;

                canonical = lease;
                entityCount = measuredEntities;
                recordCount = measuredRecords;
                lease = null;
                return true;
            }
            finally
            {
                lease?.Dispose();
            }
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
            gid = 0;
            kind = 0;
            disabled = 0;
            recordCount = 0;
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
}
