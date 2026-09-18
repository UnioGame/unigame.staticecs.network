namespace UniGame.StaticEcs.Network
{
    using System;
    using System.Threading;

#if UNITY_2022_2_OR_NEWER
    using Unity.Burst;
#endif

    internal enum SnapshotDeltaBurstResult : byte
    {
        Unavailable,
        Success,
        Failed,
    }

    /// <summary>
    /// Runs the structural half of the indexed delta merge (entity walk, skip counts,
    /// PatchFast/PatchFull classification, changed-record masks) through a synchronous
    /// Burst pointer. It never writes wire bytes: value-delta hooks
    /// (<see cref="INetworkComponentDelta"/>) are managed interface calls Burst cannot make,
    /// so <see cref="SnapshotDeltaCodec"/> always does the actual variable-length writing
    /// (including hookless raw-payload copies, to keep one write path), replaying the plan
    /// this backend produced. See <c>TryPlan</c>/<c>Plan</c> below for the split rationale.
    /// </summary>
    #if UNITY_2022_2_OR_NEWER
    [BurstCompile]
    #endif
    internal static unsafe class SnapshotDeltaBurstBackend
    {
#if UNITY_2022_2_OR_NEWER
        private const byte PlanRemove = 1;
        private const byte PlanAdd = 2;
        private const byte PlanPatchFast = 3;
        private const byte PlanPatchFull = 4;

        public delegate int PlanFunction(
            byte* baselineBytes, int baselineLength,
            SnapshotEntityLayout* baselineEntities, int baselineEntityCount,
            SnapshotRecordLayout* baselineRecords,
            byte* targetBytes, int targetLength,
            SnapshotEntityLayout* targetEntities, int targetEntityCount,
            SnapshotRecordLayout* targetRecords,
            SnapshotDeltaPlanOp* plan, int planCapacity,
            byte* maskBuffer, int maskBufferCapacity, int maskStride,
            int* opCount);

        public delegate int ProbeFunction();

        private static readonly object Sync = new object();
        private static FunctionPointer<PlanFunction> _plan;
        private static int _state;
        private static bool _probeFallback;

        internal static bool ForcePortableForTests;

        /// <summary>
        /// Fills <paramref name="planBuffer"/>/<paramref name="maskBuffer"/> with the
        /// structural diff between <paramref name="baseline"/> and <paramref name="target"/>.
        /// The caller (SnapshotDeltaCodec.TryEncodeCoreIndexedBurst) owns both arrays (rented
        /// from ArrayPool) and replays the plan into wire bytes; this method never touches
        /// hooks, wire opcodes' variable-length payloads, or the output buffer.
        /// </summary>
        internal static SnapshotDeltaBurstResult TryPlan(
            NetworkSnapshot baseline, SnapshotLayoutView baselineLayout,
            NetworkSnapshot target, SnapshotLayoutView targetLayout,
            SnapshotDeltaPlanOp[] planBuffer, int planCapacity,
            byte[] maskBuffer, int maskBufferCapacity, int maskStride,
            out int opCount)
        {
            opCount = 0;
            if (ForcePortableForTests)
                return SnapshotDeltaBurstResult.Unavailable;
            if (!EnsureBurst())
                return SnapshotDeltaBurstResult.Unavailable;
            if (!ValidateSnapshot(baseline, baselineLayout) ||
                !ValidateSnapshot(target, targetLayout) ||
                planBuffer == null || planCapacity <= 0 ||
                planCapacity > planBuffer.Length ||
                maskBuffer == null || maskBufferCapacity < 0 ||
                maskBufferCapacity > maskBuffer.Length || maskStride <= 0)
                return SnapshotDeltaBurstResult.Failed;

            var baselineBuffer = baseline.Buffer;
            var targetBuffer = target.Buffer;
            try
            {
                fixed (byte* baselinePointer = baselineBuffer)
                fixed (SnapshotEntityLayout* baselineEntities = baselineLayout.Entities)
                fixed (SnapshotRecordLayout* baselineRecords = baselineLayout.Records)
                fixed (byte* targetPointer = targetBuffer)
                fixed (SnapshotEntityLayout* targetEntities = targetLayout.Entities)
                fixed (SnapshotRecordLayout* targetRecords = targetLayout.Records)
                fixed (SnapshotDeltaPlanOp* planPointer = planBuffer)
                fixed (byte* maskPointer = maskBuffer)
                {
                    var ops = 0;
                    var result = _plan.Invoke(
                        baselinePointer + baseline.Offset, baseline.ByteLength,
                        baselineEntities, baselineLayout.EntityCount, baselineRecords,
                        targetPointer + target.Offset, target.ByteLength,
                        targetEntities, targetLayout.EntityCount, targetRecords,
                        planPointer, planCapacity,
                        maskPointer, maskBufferCapacity, maskStride,
                        &ops);
                    if (result != 1 || ops < 0 || ops > planCapacity)
                        return SnapshotDeltaBurstResult.Failed;
                    opCount = ops;
                    return SnapshotDeltaBurstResult.Success;
                }
            }
            catch
            {
                opCount = 0;
                return SnapshotDeltaBurstResult.Failed;
            }
        }

        internal static bool IsAvailable
        {
#if UNITY_2022_2_OR_NEWER
            get { return EnsureBurst(); }
#else
            get { return false; }
#endif
        }

        private static bool EnsureBurst()
        {
            var state = Volatile.Read(ref _state);
            if (state == 1)
                return true;
            if (state == -1)
                return false;
            lock (Sync)
            {
                if (_state == 1)
                    return true;
                if (_state == -1)
                    return false;
                try
                {
                    var probe = BurstCompiler.CompileFunctionPointer<ProbeFunction>(Probe);
                    _probeFallback = false;
                    if (probe.Invoke() != 1 || _probeFallback)
                    {
                        _state = -1;
                        return false;
                    }
                    _plan = BurstCompiler.CompileFunctionPointer<PlanFunction>(Plan);
                    _state = 1;
                    return true;
                }
                catch
                {
                    _state = -1;
                    return false;
                }
            }
        }

        [BurstCompile]
        private static int Probe()
        {
            MarkManagedProbe();
            return 1;
        }

        [BurstDiscard]
        private static void MarkManagedProbe() => _probeFallback = true;

        private static bool ValidateSnapshot(NetworkSnapshot snapshot,
            SnapshotLayoutView layout)
        {
            // Pool-owned layouts are built from the canonical parser before publication.
            // Only cheap ownership/count bounds remain on the hot path; public snapshots
            // never set HasLayout and therefore cannot reach this backend.
            return snapshot != null && snapshot.HasLayout &&
                   layout.Entities != null && layout.Records != null &&
                   layout.EntityCount == snapshot.EntityCount &&
                   layout.RecordCount == snapshot.RecordCount &&
                   layout.EntityCount >= 0 && layout.RecordCount >= 0 &&
                   layout.EntityCount <= layout.Entities.Length &&
                   layout.RecordCount <= layout.Records.Length &&
                   snapshot.ByteLength > 0 && snapshot.Buffer != null &&
                   snapshot.Offset >= 0 && snapshot.Offset <= snapshot.Buffer.Length &&
                   snapshot.ByteLength <= snapshot.Buffer.Length - snapshot.Offset;
        }

        // Pure structural diff: classifies each baseline/target entity pair (by GID,
        // mirroring SnapshotDeltaCodec's own merge-walk order) as an unchanged skip, a
        // Remove, an Add, a PatchFast (record set unchanged; only payload/disabled bits
        // differ -- mask computed here), or a PatchFull (record set itself changed --
        // SnapshotDeltaCodec.TryWritePatchFullIndexed re-diffs and writes it, unmodified).
        // Writes only compact plan entries and mask bytes; every entry that needs a
        // variable-length wire write leaves that write to managed code.
        [BurstCompile]
        private static int Plan(
            byte* baselineBytes, int baselineLength,
            SnapshotEntityLayout* baselineEntities, int baselineEntityCount,
            SnapshotRecordLayout* baselineRecords,
            byte* targetBytes, int targetLength,
            SnapshotEntityLayout* targetEntities, int targetEntityCount,
            SnapshotRecordLayout* targetRecords,
            SnapshotDeltaPlanOp* plan, int planCapacity,
            byte* maskBuffer, int maskBufferCapacity, int maskStride,
            int* opCount)
        {
            var baselineIndex = 0;
            var targetIndex = 0;
            var ops = 0;
            var pendingSkip = 0u;

            while (baselineIndex < baselineEntityCount ||
                   targetIndex < targetEntityCount)
            {
                var comparison = baselineIndex >= baselineEntityCount ? 1 :
                    targetIndex >= targetEntityCount ? -1 :
                    CompareGid(baselineEntities[baselineIndex].Gid,
                        targetEntities[targetIndex].Gid);
                if (comparison < 0)
                {
                    if (ops >= planCapacity)
                        return 0;
                    plan[ops] = new SnapshotDeltaPlanOp
                    {
                        Opcode = PlanRemove,
                        Skip = pendingSkip,
                        BaselineIndex = baselineIndex,
                        TargetIndex = -1,
                        MaskOffset = -1,
                        MaskLength = 0,
                    };
                    pendingSkip = 0;
                    ops++;
                    baselineIndex++;
                    continue;
                }
                if (comparison > 0)
                {
                    if (ops >= planCapacity)
                        return 0;
                    plan[ops] = new SnapshotDeltaPlanOp
                    {
                        Opcode = PlanAdd,
                        Skip = pendingSkip,
                        BaselineIndex = -1,
                        TargetIndex = targetIndex,
                        MaskOffset = -1,
                        MaskLength = 0,
                    };
                    pendingSkip = 0;
                    ops++;
                    targetIndex++;
                    continue;
                }

                var baselineEntity = baselineEntities[baselineIndex];
                var targetEntity = targetEntities[targetIndex];
                if (!RawSpansEqual(baselineBytes, baselineLength,
                        baselineEntity.RawOffset, baselineEntity.RawLength,
                        targetBytes, targetLength, targetEntity.RawOffset,
                        targetEntity.RawLength))
                {
                    if (ops >= planCapacity)
                        return 0;
                    if (RecordSequenceMatches(baselineRecords, baselineEntity,
                            targetRecords, targetEntity))
                    {
                        var maskBytes =
                            SnapshotDeltaCodec.MaskByteCount(baselineEntity.RecordCount);
                        var maskOffset = ops * maskStride;
                        if (maskBytes > maskStride || maskOffset < 0 ||
                            maskOffset > maskBufferCapacity - maskBytes ||
                            !ComputeMask(baselineBytes, baselineLength,
                                baselineEntity, baselineRecords, targetBytes,
                                targetLength, targetEntity, targetRecords,
                                maskBuffer, maskOffset, maskBytes))
                            return 0;
                        plan[ops] = new SnapshotDeltaPlanOp
                        {
                            Opcode = PlanPatchFast,
                            Skip = pendingSkip,
                            BaselineIndex = baselineIndex,
                            TargetIndex = targetIndex,
                            MaskOffset = maskOffset,
                            MaskLength = maskBytes,
                        };
                    }
                    else
                    {
                        plan[ops] = new SnapshotDeltaPlanOp
                        {
                            Opcode = PlanPatchFull,
                            Skip = pendingSkip,
                            BaselineIndex = baselineIndex,
                            TargetIndex = targetIndex,
                            MaskOffset = -1,
                            MaskLength = 0,
                        };
                    }
                    pendingSkip = 0;
                    ops++;
                }
                else
                {
                    pendingSkip++;
                }
                baselineIndex++;
                targetIndex++;
            }

            *opCount = ops;
            return 1;
        }

        // Mirrors SnapshotDeltaCodec.RecordSequenceMatchesIndexed exactly (same
        // iteration and comparison order): true when every baseline record of this
        // entity has an exact (kind, typeId) counterpart at the same position in the
        // target, so the compact positional PatchFast encoding applies.
        private static bool RecordSequenceMatches(SnapshotRecordLayout* baselineRecords,
            SnapshotEntityLayout baselineEntity, SnapshotRecordLayout* targetRecords,
            SnapshotEntityLayout targetEntity)
        {
            if (baselineEntity.RecordCount != targetEntity.RecordCount)
                return false;
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

        // Mirrors SnapshotDeltaCodec.TryWritePatchFastIndexed's mask computation
        // exactly (same bit numbering, same disabled-flag offsets): bit 0/1 encode the
        // entity's own Disabled flag change, bits 2*(i+1)/2*(i+1)+1 encode record i's
        // payload change and new Disabled flag. Only bit *placement* is decided here;
        // SnapshotDeltaCodec.TryWritePatchFastFromMask still owns every payload write
        // (including the hook lookup PatchFast's tagged length field depends on).
        private static bool ComputeMask(byte* baselineBytes, int baselineLength,
            SnapshotEntityLayout baselineEntity, SnapshotRecordLayout* baselineRecords,
            byte* targetBytes, int targetLength, SnapshotEntityLayout targetEntity,
            SnapshotRecordLayout* targetRecords, byte* maskBuffer, int maskOffset,
            int maskBytes)
        {
            for (var i = 0; i < maskBytes; i++)
                maskBuffer[maskOffset + i] = 0;

            var baselineDisabledIndex =
                baselineEntity.RawOffset + SnapshotDeltaCodec.EntityDisabledOffset;
            var targetDisabledIndex =
                targetEntity.RawOffset + SnapshotDeltaCodec.EntityDisabledOffset;
            if ((uint)baselineDisabledIndex >= (uint)baselineLength ||
                (uint)targetDisabledIndex >= (uint)targetLength)
                return false;
            var baselineDisabled = baselineBytes[baselineDisabledIndex];
            var targetDisabled = targetBytes[targetDisabledIndex];
            if (baselineDisabled != targetDisabled)
            {
                SetMaskBit(maskBuffer, maskOffset, 0);
                if (targetDisabled != 0)
                    SetMaskBit(maskBuffer, maskOffset, 1);
            }

            var count = baselineEntity.RecordCount;
            var bStart = baselineEntity.RecordStart;
            var tStart = targetEntity.RecordStart;
            for (var i = 0; i < count; i++)
            {
                var b = baselineRecords[bStart + i];
                var t = targetRecords[tStart + i];
                if (!RawSpansEqual(baselineBytes, baselineLength, b.RawOffset,
                        b.RawLength, targetBytes, targetLength, t.RawOffset,
                        t.RawLength))
                {
                    var bit = 2 * (i + 1);
                    SetMaskBit(maskBuffer, maskOffset, bit);
                    var targetRecordDisabledIndex =
                        t.RawOffset + SnapshotDeltaCodec.RecordDisabledOffset;
                    if ((uint)targetRecordDisabledIndex >= (uint)targetLength)
                        return false;
                    if (targetBytes[targetRecordDisabledIndex] != 0)
                        SetMaskBit(maskBuffer, maskOffset, bit + 1);
                }
            }
            return true;
        }

        private static void SetMaskBit(byte* maskBuffer, int baseOffset, int bitIndex) =>
            maskBuffer[baseOffset + (bitIndex >> 3)] |= (byte)(1 << (bitIndex & 7));

        private static int CompareGid(ulong left, ulong right)
        {
            var leftCluster = (ushort)(left >> 32);
            var rightCluster = (ushort)(right >> 32);
            if (leftCluster != rightCluster)
                return leftCluster < rightCluster ? -1 : 1;
            var leftId = (uint)left;
            var rightId = (uint)right;
            if (leftId != rightId)
                return leftId < rightId ? -1 : 1;
            var leftVersion = (ushort)(left >> 48);
            var rightVersion = (ushort)(right >> 48);
            return leftVersion == rightVersion ? 0 :
                leftVersion < rightVersion ? -1 : 1;
        }

        // left/right lengths must match first: unlike a plain memcmp, differing
        // lengths always mean "not equal" (mirrors ReadOnlySpan<byte>.SequenceEqual,
        // which SnapshotDeltaCodec's portable path calls for the same comparisons).
        private static bool RawSpansEqual(byte* left, int leftLength, int leftOffset,
            int leftLen, byte* right, int rightLength, int rightOffset, int rightLen)
        {
            if (leftLen != rightLen)
                return false;
            return BytesEqual(left, leftLength, leftOffset, right, rightLength,
                rightOffset, leftLen);
        }

        private static bool BytesEqual(byte* left, int leftLength, int leftOffset,
            byte* right, int rightLength, int rightOffset, int length)
        {
            if (length < 0 || leftOffset < 0 || rightOffset < 0 ||
                leftOffset > leftLength || rightOffset > rightLength ||
                length > leftLength - leftOffset || length > rightLength - rightOffset)
                return false;
            var index = 0;
            while (index <= length - 8)
            {
                if (*(ulong*)(left + leftOffset + index) !=
                    *(ulong*)(right + rightOffset + index))
                    return false;
                index += 8;
            }
            while (index < length)
            {
                if (left[leftOffset + index] != right[rightOffset + index])
                    return false;
                index++;
            }
            return true;
        }
#else
        internal static SnapshotDeltaBurstResult TryPlan(
            NetworkSnapshot baseline, SnapshotLayoutView baselineLayout,
            NetworkSnapshot target, SnapshotLayoutView targetLayout,
            SnapshotDeltaPlanOp[] planBuffer, int planCapacity,
            byte[] maskBuffer, int maskBufferCapacity, int maskStride,
            out int opCount)
        {
            opCount = 0;
            return SnapshotDeltaBurstResult.Unavailable;
        }

        internal static bool ForcePortableForTests;
        internal static bool IsAvailable => false;
#endif
    }
}
