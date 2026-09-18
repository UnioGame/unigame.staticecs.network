namespace UniGame.StaticEcs.Network
{
    using System;
    using System.Buffers;
    using System.Collections.Generic;
    using FFS.Libraries.StaticEcs;
    using FFS.Libraries.StaticPack;

    // NCORE-13 wire format (delta format version 1; gated by ProtocolLimits.Version 8):
    //
    // The canonical full-snapshot byte layout (entity header 15 B, record header 11 B,
    // TotalLength/TotalHash, client reconstruction) is UNCHANGED. Only the on-the-wire
    // DELTA encoding below changed; TryReconstruct always rebuilds the exact same
    // canonical bytes a keyframe would carry.
    //
    // Delta body:
    //   byte   formatVersion (=1)
    //   uint32 targetEntityCount
    //   uint32 targetRecordCount
    //   uint32 operationCount
    //   operationCount * {
    //     varint skip        -- baseline entities (in GID order) to copy unchanged
    //                           before this operation
    //     byte   opcode      -- 1=Remove 2=Add 3=PatchFast 4=PatchFull
    //     ...opcode-specific payload (see TryWritePatchFast/Full and
    //        TryReconstructPatchFast/Full)
    //   }
    //   -- any baseline entities left after the last operation are copied unchanged
    //      with no further framing (the decoder just drains the baseline cursor).
    //
    // Entity references are never sent as GID/8-byte headers for Remove/Patch: both
    // sides walk the baseline's GID-sorted entity list in lockstep, so a `skip` count
    // is a strictly cheaper equivalent of a delta-coded baseline index. PatchFast
    // covers the common case (record set unchanged, only payload/disabled bits
    // differ): a 2-bits-per-item mask (item 0 = entity Disabled flag, items 1..N =
    // baseline records 0..N-1 in canonical (kind,typeId) order) marks
    // changed/new-value, and only changed records carry a varint length + payload
    // (no typeId/kind/version/length-header — position implies identity). PatchFull
    // is the escape path for entities whose record set was added to or removed from;
    // it keeps the old, self-describing per-record Add/Remove/Replace encoding.
    internal static class SnapshotDeltaCodec
    {
        // NCORE-14 bumped the format version 1 -> 2: PatchFast's per-changed-record length
        // varint changed meaning for records whose type has a value-delta hook (see
        // NetworkComponentDeltaHooks). It now carries `(length << 1) | isDelta` instead of a
        // plain byte count; isDelta selects between a hook-produced value delta (decoded
        // against the baseline record's payload) and the NCORE-13 raw-payload encoding.
        // Records of hook-less types are completely unaffected (their length field is still a
        // plain byte count, byte-for-byte identical to format version 1). ProtocolLimits was
        // bumped 8 -> 9 alongside this so mismatched peers fail fast at the packet-framing
        // layer instead of misparsing the shifted length field.
        private const byte DeltaFormatVersion = 2;
        private const int DeltaHeaderSize = sizeof(byte) + sizeof(uint) * 3;
        private const int EntityHeaderSize =
            sizeof(ulong) + sizeof(uint) + sizeof(byte) + sizeof(ushort);
        private const int EntityDisabledOffset = sizeof(ulong) + sizeof(uint);
        private const int RecordHeaderSize =
            sizeof(uint) + sizeof(byte) + sizeof(byte) + sizeof(byte) + sizeof(uint);
        private const int RecordDisabledOffset = sizeof(uint) + sizeof(byte) + sizeof(byte);
        private const int MaxMaskBytes =
            (2 * (ProtocolLimits.MaxRecordsPerEntity + 1) + 7) / 8;
        private const int EntityLayoutBytes =
            sizeof(ulong) + sizeof(ushort) + sizeof(int) * 3;
        private const int RecordLayoutBytes =
            sizeof(uint) + sizeof(byte) + sizeof(int) * 2;
        private const long MaxLayoutBytes = 4L << 20;

#if UNITY_INCLUDE_TESTS
        [ThreadStatic]
        internal static Action AfterCandidateRentForTests;
#endif

        private enum EntityOperation : byte
        {
            Remove = 1,
            Add = 2,
            PatchFast = 3,
            PatchFull = 4,
        }

        private enum RecordOperation : byte
        {
            Add = 1,
            Remove = 2,
            Replace = 3,
        }

        internal static bool TryEncode(NetworkBufferPool pool,
            NetworkSnapshot baseline, NetworkSnapshot target,
            out NetworkBufferLease delta, NetworkComponentDeltaHooks hooks = null)
        {
            delta = null;
            hooks = hooks ?? NetworkComponentDeltaHooks.Empty;
            if (pool == null || baseline == null || target == null ||
                baseline.ServerTick == 0 || target.ServerTick <= baseline.ServerTick ||
                baseline.SchemaFingerprint != target.SchemaFingerprint ||
                baseline.Scope != target.Scope ||
                target.ByteLength <= 0 ||
                target.ByteLength > ProtocolLimits.MaxDecodedPayloadBytes)
                return false;

            NetworkBufferLease candidate = null;
            try
            {
                candidate = pool.Rent(target.ByteLength);
                try
                {
#if UNITY_INCLUDE_TESTS
                    AfterCandidateRentForTests?.Invoke();
#endif
                    var writer = new SnapshotWriter(candidate.WritableSpan);
                    if (!TryEncodePlan(baseline, target, hooks, candidate, ref writer, out _) ||
                        writer.Length >= target.ByteLength)
                        return false;

                    candidate.SetLength(checked((int)writer.Length));
                    delta = candidate;
                    candidate = null;
                    return true;
                }
                catch
                {
                    return false;
                }
            }
            finally
            {
                candidate?.Dispose();
            }
        }

        internal static bool TryReconstruct(NetworkBufferPool pool,
            NetworkSnapshot baseline, ReadOnlySpan<byte> delta,
            in SnapshotChunkHeader header, SchemaFingerprint schema, ScopeId scope,
            out NetworkBufferLease canonical, out int entityCount, out int recordCount,
            NetworkComponentDeltaHooks hooks = null)
        {
            canonical = null;
            entityCount = 0;
            recordCount = 0;
            hooks = hooks ?? NetworkComponentDeltaHooks.Empty;
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
            if (!TryReconstructCore(baseline, delta, hooks, ref measure,
                    out var measuredEntities, out var measuredRecords) ||
                measure.Length != header.TotalLength)
                return false;

            var lease = pool.Rent(checked((int)header.TotalLength));
            try
            {
                var writer = new SnapshotWriter(lease.WritableSpan);
                if (!TryReconstructCore(baseline, delta, hooks, ref writer,
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

        // Builds a primitive-only layout index for one pool-owned snapshot. The
        // build validates the payload hash, declared counts, ordering, bounds, and
        // exact byte coverage before publishing; every rental is returned exactly
        // once when validation fails.
        internal static bool TryBuildLayout(NetworkSnapshot snapshot,
            out SnapshotEntityLayout[] entities,
            out SnapshotRecordLayout[] records, out int entityCount,
            out int recordCount, out ISnapshotLayoutPool layoutPool)
        {
            entities = null;
            records = null;
            entityCount = 0;
            recordCount = 0;
            layoutPool = null;
            if (snapshot == null || snapshot.ByteLength < sizeof(uint) ||
                snapshot.ByteLength > ProtocolLimits.MaxDecodedPayloadBytes ||
                snapshot.EntityCount < 0 ||
                snapshot.EntityCount > ProtocolLimits.MaxEntities ||
                snapshot.RecordCount < 0 ||
                snapshot.RecordCount > ProtocolLimits.MaxEntities *
                    ProtocolLimits.MaxRecordsPerEntity ||
                Hashing.XxHash64(snapshot.Bytes.Span) != snapshot.PayloadHash)
                return false;

            var indexBytes = checked((long)snapshot.EntityCount *
                EntityLayoutBytes + (long)snapshot.RecordCount * RecordLayoutBytes);
            var bound = Math.Min(checked(2L * snapshot.ByteLength), MaxLayoutBytes);
            if (indexBytes > bound)
                return false;

            var pool = SnapshotLayoutMemory.Pool;
            SnapshotEntityLayout[] rentedEntities = null;
            SnapshotRecordLayout[] rentedRecords = null;
            var success = false;
            try
            {
                rentedEntities = pool.RentEntities(Math.Max(1,
                    snapshot.EntityCount));
                rentedRecords = pool.RentRecords(Math.Max(1,
                    snapshot.RecordCount));
                if (!TryFillLayout(snapshot.Bytes.Span, rentedEntities,
                        rentedRecords, snapshot.EntityCount,
                        snapshot.RecordCount))
                    return false;
                success = true;
            }
            catch
            {
                return false;
            }
            finally
            {
                if (!success)
                {
                    if (rentedEntities != null)
                        pool.ReturnEntities(rentedEntities);
                    if (rentedRecords != null)
                        pool.ReturnRecords(rentedRecords);
                }
            }

            entities = rentedEntities;
            records = rentedRecords;
            entityCount = snapshot.EntityCount;
            recordCount = snapshot.RecordCount;
            layoutPool = pool;
            return true;
        }

        private static bool TryFillLayout(ReadOnlySpan<byte> bytes,
            SnapshotEntityLayout[] entities, SnapshotRecordLayout[] records,
            int expectedEntities, int expectedRecords)
        {
            var offset = 0;
            if (!TryReadUint(bytes, ref offset, out var rawEntityCount) ||
                rawEntityCount != (uint)expectedEntities)
                return false;
            var entityIndex = 0;
            var recordIndex = 0;
            ulong previousGid = 0;
            var hasPrevious = false;
            while (offset < bytes.Length)
            {
                if (entityIndex >= expectedEntities)
                    return false;
                var entityStart = offset;
                if (!TryReadEntityHeader(bytes, ref offset, out var gid, out _,
                        out _, out var recordCount, out _) ||
                    hasPrevious && CompareGid(previousGid, gid) >= 0 ||
                    recordCount > expectedRecords - recordIndex)
                    return false;
                var recordStart = recordIndex;
                byte previousKind = 0;
                uint previousType = 0;
                var hasPreviousRecord = false;
                for (var index = 0; index < recordCount; index++)
                {
                    var recordOffset = offset;
                    if (!TryReadCanonicalRecord(bytes, ref offset,
                            out var record) ||
                        hasPreviousRecord && CompareRecord(previousKind,
                            previousType, record.Kind, record.TypeId) >= 0)
                        return false;
                    previousKind = record.Kind;
                    previousType = record.TypeId;
                    hasPreviousRecord = true;
                    records[recordIndex] = new SnapshotRecordLayout
                    {
                        TypeId = record.TypeId,
                        Kind = record.Kind,
                        RawOffset = recordOffset,
                        RawLength = offset - recordOffset,
                    };
                    recordIndex++;
                }
                entities[entityIndex] = new SnapshotEntityLayout
                {
                    Gid = gid,
                    RecordCount = recordCount,
                    RawOffset = entityStart,
                    RawLength = offset - entityStart,
                    RecordStart = recordStart,
                };
                entityIndex++;
                previousGid = gid;
                hasPrevious = true;
            }
            return entityIndex == expectedEntities &&
                   recordIndex == expectedRecords && offset == bytes.Length;
        }

        private static bool TryEncodePlan(NetworkSnapshot baseline,
            NetworkSnapshot target, NetworkComponentDeltaHooks hooks,
            NetworkBufferLease candidate,
            ref SnapshotWriter writer, out uint operationCount)
        {
            operationCount = 0;
            var baselineCached = baseline.TryReadCachedLayout(
                out var baselineLayout);
            var targetCached = target.TryReadCachedLayout(out var targetLayout);
            if (baselineCached && targetCached)
                return TryEncodeCoreIndexed(baseline, baselineLayout, target,
                    targetLayout, hooks, candidate, ref writer, out operationCount);

            // Build the missing side(s) before publishing anything so a pair that
            // cannot produce an indexed encode never leaves a lone layout behind.
            if (baselineCached)
            {
                if (!target.TryBuildLayout(out var targetEntities,
                        out var targetRecords, out var targetEntityCount,
                        out var targetRecordCount, out var targetPool))
                    return TryEncodeCore(baseline, target, ref writer,
                        out operationCount);
                target.PublishLayout(targetEntities, targetRecords,
                    targetEntityCount, targetRecordCount, targetPool);
                targetLayout = new SnapshotLayoutView(targetEntities,
                    targetRecords, targetEntityCount, targetRecordCount);
                return TryEncodeCoreIndexed(baseline, baselineLayout, target,
                    targetLayout, hooks, candidate, ref writer, out operationCount);
            }

            if (targetCached)
            {
                if (!baseline.TryBuildLayout(out var baselineEntities,
                        out var baselineRecords, out var baselineEntityCount,
                        out var baselineRecordCount, out var baselinePool))
                    return TryEncodeCore(baseline, target, ref writer,
                        out operationCount);
                baseline.PublishLayout(baselineEntities, baselineRecords,
                    baselineEntityCount, baselineRecordCount, baselinePool);
                baselineLayout = new SnapshotLayoutView(baselineEntities,
                    baselineRecords, baselineEntityCount, baselineRecordCount);
                return TryEncodeCoreIndexed(baseline, baselineLayout, target,
                    targetLayout, hooks, candidate, ref writer, out operationCount);
            }

            if (!baseline.TryBuildLayout(out var pendingBaselineEntities,
                    out var pendingBaselineRecords,
                    out var pendingBaselineEntityCount,
                    out var pendingBaselineRecordCount,
                    out var pendingBaselinePool))
                return TryEncodeCore(baseline, target, ref writer,
                    out operationCount);
            var published = false;
            try
            {
                if (!target.TryBuildLayout(out var pendingTargetEntities,
                        out var pendingTargetRecords,
                        out var pendingTargetEntityCount,
                        out var pendingTargetRecordCount,
                        out var pendingTargetPool))
                    return TryEncodeCore(baseline, target, ref writer,
                        out operationCount);
                baseline.PublishLayout(pendingBaselineEntities,
                    pendingBaselineRecords, pendingBaselineEntityCount,
                    pendingBaselineRecordCount, pendingBaselinePool);
                target.PublishLayout(pendingTargetEntities,
                    pendingTargetRecords, pendingTargetEntityCount,
                    pendingTargetRecordCount, pendingTargetPool);
                published = true;
                baselineLayout = new SnapshotLayoutView(pendingBaselineEntities,
                    pendingBaselineRecords, pendingBaselineEntityCount,
                    pendingBaselineRecordCount);
                targetLayout = new SnapshotLayoutView(pendingTargetEntities,
                    pendingTargetRecords, pendingTargetEntityCount,
                    pendingTargetRecordCount);
                return TryEncodeCoreIndexed(baseline, baselineLayout, target,
                    targetLayout, hooks, candidate, ref writer, out operationCount);
            }
            finally
            {
                if (!published)
                {
                    pendingBaselinePool.ReturnEntities(pendingBaselineEntities);
                    pendingBaselinePool.ReturnRecords(pendingBaselineRecords);
                }
            }
        }

        private static bool TryEncodeCoreIndexed(NetworkSnapshot baseline,
            SnapshotLayoutView baselineLayout, NetworkSnapshot target,
            SnapshotLayoutView targetLayout, NetworkComponentDeltaHooks hooks,
            NetworkBufferLease candidate,
            ref SnapshotWriter writer, out uint operationCount)
        {
            // NCORE-13 replaced the delta wire format (compact varint/bitmask
            // encoding below) but did not port SnapshotDeltaBurstBackend to it:
            // that backend still only knows how to emit the pre-NCORE-13 layout
            // (full entity/record headers, no skip/mask compaction). Calling it
            // here would silently produce bytes the new TryReconstructCore cannot
            // parse. Route every indexed encode through the portable path until a
            // follow-up ports the Burst backend; see the NCORE-13 report for the
            // CPU measurement (encode is ~3.5x/tick, dominated by bytes not CPU)
            // that justifies deferring the port instead of blocking this change on
            // it. SnapshotDeltaBurstBackend itself is left compiling and unused.
            _ = candidate;
            return TryEncodeCoreIndexedPortable(baseline, baselineLayout, target,
                targetLayout, hooks, ref writer, out operationCount);
        }

        private static bool TryEncodeCoreIndexedPortable(NetworkSnapshot baseline,
            SnapshotLayoutView baselineLayout, NetworkSnapshot target,
            SnapshotLayoutView targetLayout, NetworkComponentDeltaHooks hooks,
            ref SnapshotWriter writer,
            out uint operationCount)
        {
            operationCount = 0;
            if (!writer.TryWriteByte(DeltaFormatVersion) ||
                !writer.TryWriteUint(checked((uint)target.EntityCount)) ||
                !writer.TryWriteUint(checked((uint)target.RecordCount)))
                return false;
            var countOffset = writer.Length;
            if (!writer.TryWriteUint(0))
                return false;

            var baselineBytes = baseline.Bytes.Span;
            var targetBytes = target.Bytes.Span;
            var baselineEntities = baselineLayout.Entities;
            var targetEntities = targetLayout.Entities;
            var baselineCount = baselineLayout.EntityCount;
            var targetCount = targetLayout.EntityCount;
            var baselineIndex = 0;
            var targetIndex = 0;
            var hasBaseline = baselineIndex < baselineCount;
            var hasTarget = targetIndex < targetCount;
            uint pendingSkip = 0;

            while (hasBaseline || hasTarget)
            {
                var comparison = !hasBaseline ? 1 : !hasTarget ? -1 :
                    CompareGid(baselineEntities[baselineIndex].Gid,
                        targetEntities[targetIndex].Gid);
                if (comparison < 0)
                {
                    if (!TryWriteVarUInt(ref writer, pendingSkip) ||
                        !writer.TryWriteByte((byte)EntityOperation.Remove))
                        return false;
                    pendingSkip = 0;
                    operationCount++;
                    baselineIndex++;
                    hasBaseline = baselineIndex < baselineCount;
                    continue;
                }
                if (comparison > 0)
                {
                    var added = targetEntities[targetIndex];
                    if (!TryWriteVarUInt(ref writer, pendingSkip) ||
                        !writer.TryWriteByte((byte)EntityOperation.Add) ||
                        !writer.TryWrite(targetBytes.Slice(added.RawOffset,
                            added.RawLength)))
                        return false;
                    pendingSkip = 0;
                    operationCount++;
                    targetIndex++;
                    hasTarget = targetIndex < targetCount;
                    continue;
                }

                if (!EntityRawEqual(baselineBytes, baselineEntities[baselineIndex],
                        targetBytes, targetEntities[targetIndex]))
                {
                    if (!TryWriteVarUInt(ref writer, pendingSkip))
                        return false;
                    pendingSkip = 0;
                    if (!TryWritePatchIndexed(baselineBytes, baselineLayout,
                            baselineIndex, targetBytes, targetLayout,
                            targetIndex, hooks, ref writer))
                        return false;
                    operationCount++;
                }
                else
                {
                    pendingSkip++;
                }
                baselineIndex++;
                targetIndex++;
                hasBaseline = baselineIndex < baselineCount;
                hasTarget = targetIndex < targetCount;
            }

            // Any trailing unchanged baseline entities need no wire bytes at all:
            // TryReconstructCore drains whatever is left on the baseline cursor
            // once operationCount operations have been consumed.
            return writer.TryWriteUintAt(countOffset, operationCount);
        }

        private static bool EntityRawEqual(ReadOnlySpan<byte> baselineBytes,
            in SnapshotEntityLayout baseline, ReadOnlySpan<byte> targetBytes,
            in SnapshotEntityLayout target)
        {
            var left = baselineBytes.Slice(baseline.RawOffset, baseline.RawLength);
            var right = targetBytes.Slice(target.RawOffset, target.RawLength);
            return left.SequenceEqual(right);
        }

        private static bool TryWritePatchIndexed(ReadOnlySpan<byte> baselineBytes,
            SnapshotLayoutView baselineLayout, int baselineEntityIndex,
            ReadOnlySpan<byte> targetBytes, SnapshotLayoutView targetLayout,
            int targetEntityIndex, NetworkComponentDeltaHooks hooks,
            ref SnapshotWriter writer)
        {
            var baselineEntity = baselineLayout.Entities[baselineEntityIndex];
            var targetEntity = targetLayout.Entities[targetEntityIndex];
            if (RecordSequenceMatchesIndexed(baselineLayout, in baselineEntity,
                    targetLayout, in targetEntity))
                return writer.TryWriteByte((byte)EntityOperation.PatchFast) &&
                       TryWritePatchFastIndexed(baselineBytes, baselineLayout,
                           in baselineEntity, targetBytes, targetLayout,
                           in targetEntity, hooks, ref writer);
            return writer.TryWriteByte((byte)EntityOperation.PatchFull) &&
                   TryWritePatchFullIndexed(baselineBytes, baselineLayout,
                       in baselineEntity, targetBytes, targetLayout,
                       in targetEntity, ref writer);
        }

        // True when every baseline record of this entity has an exact (kind,
        // typeId) counterpart at the same position in the target: no record was
        // added or removed, only payload bytes and/or disabled bits may differ.
        // Callers may then use the compact positional PatchFast encoding.
        private static bool RecordSequenceMatchesIndexed(
            SnapshotLayoutView baselineLayout, in SnapshotEntityLayout baselineEntity,
            SnapshotLayoutView targetLayout, in SnapshotEntityLayout targetEntity)
        {
            if (baselineEntity.RecordCount != targetEntity.RecordCount)
                return false;
            var baselineRecords = baselineLayout.Records;
            var targetRecords = targetLayout.Records;
            var bStart = baselineEntity.RecordStart;
            var tStart = targetEntity.RecordStart;
            for (var i = 0; i < baselineEntity.RecordCount; i++)
            {
                var b = baselineRecords[bStart + i];
                var t = targetRecords[tStart + i];
                if (b.Kind != t.Kind || b.TypeId != t.TypeId)
                    return false;
            }
            return true;
        }

        private static bool TryWritePatchFastIndexed(
            ReadOnlySpan<byte> baselineBytes, SnapshotLayoutView baselineLayout,
            in SnapshotEntityLayout baselineEntity, ReadOnlySpan<byte> targetBytes,
            SnapshotLayoutView targetLayout, in SnapshotEntityLayout targetEntity,
            NetworkComponentDeltaHooks hooks, ref SnapshotWriter writer)
        {
            var count = baselineEntity.RecordCount;
            var maskBytes = MaskByteCount(count);
            Span<byte> maskBuffer = stackalloc byte[MaxMaskBytes];
            var mask = maskBuffer.Slice(0, maskBytes);
            mask.Clear();

            var baselineDisabled =
                baselineBytes[baselineEntity.RawOffset + EntityDisabledOffset];
            var targetDisabled =
                targetBytes[targetEntity.RawOffset + EntityDisabledOffset];
            if (baselineDisabled != targetDisabled)
            {
                SetBit(mask, 0);
                if (targetDisabled != 0)
                    SetBit(mask, 1);
            }

            var baselineRecords = baselineLayout.Records;
            var targetRecords = targetLayout.Records;
            var bStart = baselineEntity.RecordStart;
            var tStart = targetEntity.RecordStart;
            for (var i = 0; i < count; i++)
            {
                var b = baselineRecords[bStart + i];
                var t = targetRecords[tStart + i];
                var bRaw = baselineBytes.Slice(b.RawOffset, b.RawLength);
                var tRaw = targetBytes.Slice(t.RawOffset, t.RawLength);
                if (bRaw.SequenceEqual(tRaw))
                    continue;
                var bit = 2 * (i + 1);
                SetBit(mask, bit);
                if (targetBytes[t.RawOffset + RecordDisabledOffset] != 0)
                    SetBit(mask, bit + 1);
            }

            // A stackalloc'd mask cannot be passed to writer.TryWrite(ReadOnlySpan)
            // here: `writer` is itself a ref struct received by ref, so its
            // escape scope reaches the caller, wider than this stack buffer's.
            // Byte-at-a-time writes sidestep that (each argument is a plain
            // byte, not a span) without heap-allocating the mask.
            for (var i = 0; i < maskBytes; i++)
                if (!writer.TryWriteByte(mask[i]))
                    return false;

            for (var i = 0; i < count; i++)
            {
                if (!GetBit(mask, 2 * (i + 1)))
                    continue;
                var b = baselineRecords[bStart + i];
                var t = targetRecords[tStart + i];
                var payloadLength = t.RawLength - RecordHeaderSize;
                var payload = targetBytes.Slice(t.RawOffset + RecordHeaderSize,
                    payloadLength);
                if (!TryWritePatchFastRecordPayload(baselineBytes, b, payload,
                        hooks, ref writer))
                    return false;
            }
            return true;
        }

        // Writes one changed record's payload in PatchFast. Record types absent from
        // `hooks` (the common case pre-NCORE-14) keep the exact NCORE-13 wire encoding: a
        // plain varint byte count followed by the full payload. Record types present in
        // `hooks` (regardless of whether this particular value benefits) always use the
        // tagged NCORE-14 encoding instead, `(length << 1) | isDelta`: isDelta=1 selects a
        // hook-produced value delta (decoded against the baseline record's payload),
        // isDelta=0 falls back to the full payload when the hook declined or did not shrink
        // it. Both sides agree on which typeIds are tagged because both build `hooks` from
        // the same frozen schema (its fingerprint changes if that set differs).
        private static bool TryWritePatchFastRecordPayload(
            ReadOnlySpan<byte> baselineBytes, in SnapshotRecordLayout baselineRecord,
            ReadOnlySpan<byte> targetPayload, NetworkComponentDeltaHooks hooks,
            ref SnapshotWriter writer)
        {
            if (!hooks.TryGet(baselineRecord.TypeId, out var hook))
                return TryWriteVarUInt(ref writer, (uint)targetPayload.Length) &&
                       writer.TryWrite(targetPayload);

            if (baselineRecord.RawLength >= RecordHeaderSize)
            {
                var baselinePayload = baselineBytes.Slice(
                    baselineRecord.RawOffset + RecordHeaderSize,
                    baselineRecord.RawLength - RecordHeaderSize);
                var scratch = ArrayPool<byte>.Shared.Rent(targetPayload.Length);
                try
                {
                    var deltaLength = hook.TryWriteValueDelta(baselinePayload,
                        targetPayload, scratch.AsSpan(0, targetPayload.Length));
                    if (deltaLength >= 0 && deltaLength < targetPayload.Length)
                        return TryWriteVarUInt(ref writer,
                                   ((uint)deltaLength << 1) | 1u) &&
                               writer.TryWrite(scratch.AsSpan(0, deltaLength));
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(scratch);
                }
            }
            return TryWriteVarUInt(ref writer, (uint)targetPayload.Length << 1) &&
                   writer.TryWrite(targetPayload);
        }

        private static bool TryWritePatchFullIndexed(
            ReadOnlySpan<byte> baselineBytes, SnapshotLayoutView baselineLayout,
            in SnapshotEntityLayout baselineEntity, ReadOnlySpan<byte> targetBytes,
            SnapshotLayoutView targetLayout, in SnapshotEntityLayout targetEntity,
            ref SnapshotWriter writer)
        {
            var baselineDisabled =
                baselineBytes[baselineEntity.RawOffset + EntityDisabledOffset];
            var targetDisabled =
                targetBytes[targetEntity.RawOffset + EntityDisabledOffset];
            byte entityFlags = 0;
            if (baselineDisabled != targetDisabled)
            {
                entityFlags = 1;
                if (targetDisabled != 0)
                    entityFlags |= 2;
            }
            if (!writer.TryWriteByte(entityFlags) ||
                !TryWriteVarUInt(ref writer, (uint)targetEntity.RecordCount))
                return false;
            var countOffset = writer.Length;
            if (!writer.TryWriteUint(0))
                return false;

            var baselineRecords = baselineLayout.Records;
            var targetRecords = targetLayout.Records;
            var baselineIndex = baselineEntity.RecordStart;
            var baselineEnd = baselineIndex + baselineEntity.RecordCount;
            var targetIndex = targetEntity.RecordStart;
            var targetEnd = targetIndex + targetEntity.RecordCount;
            var hasBaseline = baselineIndex < baselineEnd;
            var hasTarget = targetIndex < targetEnd;
            uint operations = 0;

            while (hasBaseline || hasTarget)
            {
                var comparison = !hasBaseline ? 1 : !hasTarget ? -1 :
                    CompareRecord(baselineRecords[baselineIndex].Kind,
                        baselineRecords[baselineIndex].TypeId,
                        targetRecords[targetIndex].Kind,
                        targetRecords[targetIndex].TypeId);
                if (comparison < 0)
                {
                    var removed = baselineRecords[baselineIndex];
                    if (!writer.TryWriteByte((byte)RecordOperation.Remove) ||
                        !writer.TryWriteUint(removed.TypeId) ||
                        !writer.TryWriteByte(removed.Kind))
                        return false;
                    operations++;
                    baselineIndex++;
                    hasBaseline = baselineIndex < baselineEnd;
                    continue;
                }
                if (comparison > 0)
                {
                    var added = targetRecords[targetIndex];
                    if (!writer.TryWriteByte((byte)RecordOperation.Add) ||
                        !writer.TryWrite(targetBytes.Slice(added.RawOffset,
                            added.RawLength)))
                        return false;
                    operations++;
                    targetIndex++;
                    hasTarget = targetIndex < targetEnd;
                    continue;
                }

                var baselineRecord = baselineRecords[baselineIndex];
                var targetRecord = targetRecords[targetIndex];
                var left = baselineBytes.Slice(baselineRecord.RawOffset,
                    baselineRecord.RawLength);
                var right = targetBytes.Slice(targetRecord.RawOffset,
                    targetRecord.RawLength);
                if (!left.SequenceEqual(right))
                {
                    if (!writer.TryWriteByte((byte)RecordOperation.Replace) ||
                        !writer.TryWrite(right))
                        return false;
                    operations++;
                }
                baselineIndex++;
                targetIndex++;
                hasBaseline = baselineIndex < baselineEnd;
                hasTarget = targetIndex < targetEnd;
            }

            return writer.TryWriteUintAt(countOffset, operations);
        }

        private static bool TryEncodeCore(NetworkSnapshot baseline,
            NetworkSnapshot target, ref SnapshotWriter writer,
            out uint operationCount)
        {
            operationCount = 0;
            if (!TryOpenSnapshot(baseline, out var baselineCursor) ||
                !TryOpenSnapshot(target, out var targetCursor) ||
                !writer.TryWriteByte(DeltaFormatVersion) ||
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

            uint pendingSkip = 0;
            while (hasBaseline || hasTarget)
            {
                var comparison = !hasBaseline ? 1 : !hasTarget ? -1 :
                    CompareGid(baselineEntity.Gid, targetEntity.Gid);
                if (comparison < 0)
                {
                    if (!TryWriteVarUInt(ref writer, pendingSkip) ||
                        !writer.TryWriteByte((byte)EntityOperation.Remove))
                        return false;
                    pendingSkip = 0;
                    operationCount++;
                    if (!TryMoveNext(ref baselineCursor, out baselineEntity,
                            out hasBaseline))
                        return false;
                    continue;
                }
                if (comparison > 0)
                {
                    if (!TryWriteVarUInt(ref writer, pendingSkip) ||
                        !writer.TryWriteByte((byte)EntityOperation.Add) ||
                        !writer.TryWrite(targetEntity.Raw))
                        return false;
                    pendingSkip = 0;
                    operationCount++;
                    if (!TryMoveNext(ref targetCursor, out targetEntity,
                            out hasTarget))
                        return false;
                    continue;
                }

                if (!baselineEntity.Raw.SequenceEqual(targetEntity.Raw))
                {
                    if (!TryWriteVarUInt(ref writer, pendingSkip))
                        return false;
                    pendingSkip = 0;
                    if (!writer.TryWriteByte((byte)EntityOperation.PatchFull) ||
                        !TryWritePatch(in baselineEntity, in targetEntity,
                            ref writer))
                        return false;
                    operationCount++;
                }
                else
                {
                    pendingSkip++;
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

        // Streaming (non-indexed) patch fallback: always uses the self-describing
        // PatchFull record encoding. This path only runs when a snapshot's layout
        // index cannot be built or cached (pathologically large snapshots or pool
        // exhaustion); it favors simplicity/correctness over the compaction the
        // indexed path gives the hot path.
        private static bool TryWritePatch(in CanonicalEntity baseline,
            in CanonicalEntity target, ref SnapshotWriter writer)
        {
            byte entityFlags = 0;
            if (baseline.Disabled != target.Disabled)
            {
                entityFlags = 1;
                if (target.Disabled != 0)
                    entityFlags |= 2;
            }
            if (!writer.TryWriteByte(entityFlags) ||
                !TryWriteVarUInt(ref writer, target.RecordCount))
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
            ReadOnlySpan<byte> delta, NetworkComponentDeltaHooks hooks,
            ref SnapshotWriter writer,
            out int entityCount, out int recordCount)
        {
            entityCount = 0;
            recordCount = 0;
            if (!TryOpenSnapshot(baseline, out var baselineCursor) ||
                !TryReadDeltaHeader(delta, out var offset, out var targetEntities,
                    out var targetRecords, out var operationCount) ||
                !writer.TryWriteUint(targetEntities))
                return false;

            for (var opIndex = 0u; opIndex < operationCount; opIndex++)
            {
                if (!TryReadVarUInt(delta, ref offset, out var skip))
                    return false;
                for (var i = 0u; i < skip; i++)
                {
                    if (!TryMoveNext(ref baselineCursor, out var carried,
                            out var hasCarried) || !hasCarried ||
                        !writer.TryWrite(carried.Raw) ||
                        !TryAdd(ref entityCount, 1) ||
                        !TryAdd(ref recordCount, carried.RecordCount))
                        return false;
                }
                if (!TryReadByte(delta, ref offset, out var rawOpcode))
                    return false;
                var opcode = (EntityOperation)rawOpcode;
                if (opcode == EntityOperation.Remove)
                {
                    if (!TryMoveNext(ref baselineCursor, out _, out var hasValue) ||
                        !hasValue)
                        return false;
                }
                else if (opcode == EntityOperation.Add)
                {
                    if (!TryReadCanonicalEntity(delta, ref offset, out var added) ||
                        !writer.TryWrite(added.Raw) ||
                        !TryAdd(ref entityCount, 1) ||
                        !TryAdd(ref recordCount, added.RecordCount))
                        return false;
                }
                else if (opcode == EntityOperation.PatchFast)
                {
                    if (!TryMoveNext(ref baselineCursor, out var current,
                            out var hasValue) || !hasValue ||
                        !TryReconstructPatchFast(in current, delta, hooks, ref offset,
                            ref writer, out var patchedRecords) ||
                        !TryAdd(ref entityCount, 1) ||
                        !TryAdd(ref recordCount, patchedRecords))
                        return false;
                }
                else if (opcode == EntityOperation.PatchFull)
                {
                    if (!TryMoveNext(ref baselineCursor, out var current,
                            out var hasValue) || !hasValue ||
                        !TryReconstructPatchFull(in current, delta, ref offset,
                            ref writer, out var patchedRecords) ||
                        !TryAdd(ref entityCount, 1) ||
                        !TryAdd(ref recordCount, patchedRecords))
                        return false;
                }
                else
                {
                    return false;
                }
            }

            // Drain any remaining baseline entities: they carry no wire bytes.
            while (baselineCursor.Remaining > 0)
            {
                if (!TryMoveNext(ref baselineCursor, out var carried,
                        out var hasCarried) || !hasCarried ||
                    !writer.TryWrite(carried.Raw) ||
                    !TryAdd(ref entityCount, 1) ||
                    !TryAdd(ref recordCount, carried.RecordCount))
                    return false;
            }

            return baselineCursor.Complete && offset == delta.Length &&
                   entityCount == targetEntities && recordCount == targetRecords;
        }

        private static bool TryReconstructPatchFast(in CanonicalEntity baseline,
            ReadOnlySpan<byte> delta, NetworkComponentDeltaHooks hooks,
            ref int offset, ref SnapshotWriter writer,
            out int recordCount)
        {
            recordCount = 0;
            var count = baseline.RecordCount;
            var maskBytes = MaskByteCount(count);
            if (offset < 0 || maskBytes > delta.Length - offset)
                return false;
            var mask = delta.Slice(offset, maskBytes);
            offset += maskBytes;
            if (HasStrayBits(mask, 2 * (count + 1)))
                return false;

            var disabled = GetBit(mask, 0)
                ? GetBit(mask, 1) ? (byte)1 : (byte)0
                : baseline.Disabled;
            if (!writer.TryWriteUlong(baseline.Gid) ||
                !writer.TryWriteUint(baseline.Kind) ||
                !writer.TryWriteByte(disabled) ||
                !writer.TryWriteUshort(baseline.RecordCount))
                return false;

            var baselineRecords = new RecordCursor(baseline.Records, count);
            for (var i = 0; i < count; i++)
            {
                if (!TryMoveNext(ref baselineRecords, out var record,
                        out var hasRecord) || !hasRecord)
                    return false;
                var bit = 2 * (i + 1);
                if (!GetBit(mask, bit))
                {
                    if (!writer.TryWrite(record.Raw))
                        return false;
                }
                else
                {
                    var newDisabled = GetBit(mask, bit + 1) ? (byte)1 : (byte)0;
                    var version = record.Raw[RecordDisabledOffset - 1];
                    var tagged = hooks.TryGet(record.TypeId, out var hook);
                    if (!TryReadVarUInt(delta, ref offset, out var raw))
                        return false;
                    var isDelta = tagged && (raw & 1u) != 0;
                    var length = tagged ? raw >> 1 : raw;
                    if (length > ProtocolLimits.MaxComponentBytes ||
                        length > (uint)(delta.Length - offset))
                        return false;
                    if (isDelta)
                    {
                        if (record.Raw.Length < RecordHeaderSize)
                            return false;
                        var baselinePayload = record.Raw.Slice(RecordHeaderSize);
                        var deltaBytes = delta.Slice(offset, checked((int)length));
                        var scratch = ArrayPool<byte>.Shared.Rent(
                            ProtocolLimits.MaxComponentBytes);
                        try
                        {
                            if (!hook.TryReadValueDelta(baselinePayload, deltaBytes,
                                    scratch, out var written) ||
                                written < 0 || written > ProtocolLimits.MaxComponentBytes)
                                return false;
                            if (!writer.TryWriteUint(record.TypeId) ||
                                !writer.TryWriteByte(record.Kind) ||
                                !writer.TryWriteByte(version) ||
                                !writer.TryWriteByte(newDisabled) ||
                                !writer.TryWriteUint((uint)written) ||
                                !writer.TryWrite(scratch.AsSpan(0, written)))
                                return false;
                        }
                        finally
                        {
                            ArrayPool<byte>.Shared.Return(scratch);
                        }
                    }
                    else
                    {
                        if (!writer.TryWriteUint(record.TypeId) ||
                            !writer.TryWriteByte(record.Kind) ||
                            !writer.TryWriteByte(version) ||
                            !writer.TryWriteByte(newDisabled) ||
                            !writer.TryWriteUint(length) ||
                            !writer.TryWrite(delta.Slice(offset,
                                checked((int)length))))
                            return false;
                    }
                    offset += checked((int)length);
                }
                recordCount++;
            }
            if (!baselineRecords.Complete)
                return false;
            return true;
        }

        private static bool TryReconstructPatchFull(in CanonicalEntity baseline,
            ReadOnlySpan<byte> delta, ref int offset, ref SnapshotWriter writer,
            out int recordCount)
        {
            recordCount = 0;
            if (!TryReadByte(delta, ref offset, out var entityFlags) ||
                (entityFlags & ~0x3) != 0)
                return false;
            var disabledChanged = (entityFlags & 1) != 0;
            var newDisabled = (byte)((entityFlags >> 1) & 1);
            if (!TryReadVarUInt(delta, ref offset, out var targetRecordCount) ||
                targetRecordCount > (uint)ProtocolLimits.MaxRecordsPerEntity ||
                !TryReadUint(delta, ref offset, out var recordOpCount) ||
                recordOpCount > (uint)ProtocolLimits.MaxRecordsPerEntity * 2u)
                return false;

            var disabled = disabledChanged ? newDisabled : baseline.Disabled;
            if (!writer.TryWriteUlong(baseline.Gid) ||
                !writer.TryWriteUint(baseline.Kind) ||
                !writer.TryWriteByte(disabled) ||
                !writer.TryWriteUshort(checked((ushort)targetRecordCount)))
                return false;

            var baselineRecords = new RecordCursor(baseline.Records,
                baseline.RecordCount);
            if (offset < 0 || offset > delta.Length)
                return false;
            var opCursor = new RecordDeltaCursor(delta.Slice(offset),
                checked((int)recordOpCount));
            if (!TryMoveNext(ref baselineRecords, out var baselineRecord,
                    out var hasBaseline) ||
                !TryMoveNext(ref opCursor, out var recordOp, out var hasOp))
                return false;
            while (hasBaseline || hasOp)
            {
                var comparison = !hasBaseline ? 1 : !hasOp ? -1 :
                    CompareRecord(baselineRecord.Kind, baselineRecord.TypeId,
                        recordOp.Kind, recordOp.TypeId);
                if (comparison < 0)
                {
                    if (!writer.TryWrite(baselineRecord.Raw) ||
                        !TryAdd(ref recordCount, 1) ||
                        !TryMoveNext(ref baselineRecords, out baselineRecord,
                            out hasBaseline))
                        return false;
                    continue;
                }
                if (comparison > 0)
                {
                    if (recordOp.Operation != RecordOperation.Add ||
                        !writer.TryWrite(recordOp.Raw) ||
                        !TryAdd(ref recordCount, 1) ||
                        !TryMoveNext(ref opCursor, out recordOp, out hasOp))
                        return false;
                    continue;
                }

                if (recordOp.Operation == RecordOperation.Add)
                    return false;
                if (recordOp.Operation == RecordOperation.Replace)
                {
                    if (!writer.TryWrite(recordOp.Raw) ||
                        !TryAdd(ref recordCount, 1))
                        return false;
                }
                if (!TryMoveNext(ref baselineRecords, out baselineRecord,
                        out hasBaseline) ||
                    !TryMoveNext(ref opCursor, out recordOp, out hasOp))
                    return false;
            }
            if (!baselineRecords.Complete || recordCount != targetRecordCount)
                return false;
            offset += opCursor.Consumed;
            return true;
        }

        private static bool TryReadDeltaHeader(ReadOnlySpan<byte> bytes,
            out int offset, out uint entityCount, out uint recordCount,
            out uint operationCount)
        {
            offset = 0;
            entityCount = 0;
            recordCount = 0;
            operationCount = 0;
            if (bytes.Length < DeltaHeaderSize ||
                !TryReadByte(bytes, ref offset, out var formatVersion) ||
                formatVersion != DeltaFormatVersion ||
                !TryReadUint(bytes, ref offset, out entityCount) ||
                !TryReadUint(bytes, ref offset, out recordCount) ||
                !TryReadUint(bytes, ref offset, out operationCount) ||
                entityCount > (uint)ProtocolLimits.MaxEntities ||
                recordCount > (uint)(ProtocolLimits.MaxEntities *
                    ProtocolLimits.MaxRecordsPerEntity) ||
                operationCount > (uint)ProtocolLimits.MaxEntities * 2u)
            {
                offset = 0;
                return false;
            }
            return true;
        }

        private static int MaskByteCount(int recordCount) =>
            (2 * (recordCount + 1) + 7) / 8;

        private static bool GetBit(ReadOnlySpan<byte> mask, int bitIndex) =>
            (mask[bitIndex >> 3] & (1 << (bitIndex & 7))) != 0;

        private static void SetBit(Span<byte> mask, int bitIndex) =>
            mask[bitIndex >> 3] |= (byte)(1 << (bitIndex & 7));

        // Rejects a mask whose bits beyond the last meaningful item are non-zero:
        // the encoder always emits zero padding, so any set stray bit means the
        // delta was corrupted or hand-crafted.
        private static bool HasStrayBits(ReadOnlySpan<byte> mask, int usedBits)
        {
            var totalBits = mask.Length * 8;
            for (var bit = usedBits; bit < totalBits; bit++)
                if (GetBit(mask, bit))
                    return true;
            return false;
        }

        private static bool TryWriteVarUInt(ref SnapshotWriter writer, uint value)
        {
            do
            {
                var chunk = (byte)(value & 0x7F);
                value >>= 7;
                if (value != 0)
                    chunk |= 0x80;
                if (!writer.TryWriteByte(chunk))
                    return false;
            } while (value != 0);
            return true;
        }

        private static bool TryReadVarUInt(ReadOnlySpan<byte> bytes,
            ref int offset, out uint value)
        {
            value = 0;
            var shift = 0;
            while (true)
            {
                if (shift >= 35 || !TryReadByte(bytes, ref offset, out var b))
                {
                    value = 0;
                    return false;
                }
                var chunk = (uint)(b & 0x7F);
                if (shift == 28 && chunk > 0xFu)
                {
                    value = 0;
                    return false;
                }
                value |= chunk << shift;
                if ((b & 0x80) == 0)
                    return true;
                shift += 7;
            }
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

            internal bool TrySetLength(int length)
            {
                if (length < 0 || length > int.MaxValue ||
                    !_measure && length > _destination.Length)
                    return false;
                _length = length;
                return true;
            }

            internal bool TryWriteByte(byte value)
            {
                if (!TryReserve(sizeof(byte), out var offset))
                    return false;
                if (!_measure)
                    _destination[offset] = value;
                return true;
            }

            internal bool TryWriteUshort(ushort value)
            {
                if (!TryReserve(sizeof(ushort), out var offset))
                    return false;
                if (!_measure)
                    Hashing.Write16(_destination, offset, value);
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
