namespace UniGame.StaticEcs.Network
{
    using System;
    using System.Runtime.InteropServices;
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

    /// <summary>Runs the validated indexed delta merge through a synchronous Burst pointer.</summary>
    #if UNITY_2022_2_OR_NEWER
    [BurstCompile]
    #endif
    internal static unsafe class SnapshotDeltaBurstBackend
    {
#if UNITY_2022_2_OR_NEWER
        private const int EntityLayoutStride = 22;
        private const int RecordLayoutStride = 13;
        private const int EntityHeaderSize = 15;
        private const byte EntityAdd = 1;
        private const byte EntityRemove = 2;
        private const byte EntityPatch = 3;
        private const byte RecordAdd = 1;
        private const byte RecordRemove = 2;
        private const byte RecordReplace = 3;

        public delegate int EncodeFunction(
            byte* baselineBytes, int baselineLength,
            SnapshotEntityLayout* baselineEntities, int baselineEntityCount,
            SnapshotRecordLayout* baselineRecords,
            byte* targetBytes, int targetLength,
            SnapshotEntityLayout* targetEntities, int targetEntityCount,
            SnapshotRecordLayout* targetRecords,
            byte* destination, int destinationCapacity,
            int targetEntityCountValue, int targetRecordCountValue,
            int* writtenLength, uint* operationCount);

        public delegate int ProbeFunction();

        private static readonly object Sync = new object();
        private static FunctionPointer<EncodeFunction> _encode;
        private static int _state;
        private static bool _probeFallback;

        internal static bool ForcePortableForTests;

        internal static SnapshotDeltaBurstResult TryEncode(
            NetworkSnapshot baseline, SnapshotLayoutView baselineLayout,
            NetworkSnapshot target, SnapshotLayoutView targetLayout,
            NetworkBufferLease candidate, out int writtenLength,
            out uint operationCount)
        {
            writtenLength = 0;
            operationCount = 0;
            if (ForcePortableForTests)
                return SnapshotDeltaBurstResult.Unavailable;
            if (!EnsureBurst())
                return SnapshotDeltaBurstResult.Unavailable;
            if (!ValidateSnapshot(baseline, baselineLayout) ||
                !ValidateSnapshot(target, targetLayout) ||
                !ValidateCandidate(candidate))
                return SnapshotDeltaBurstResult.Failed;

            var baselineBuffer = baseline.Buffer;
            var targetBuffer = target.Buffer;
            var outputBuffer = candidate.Buffer;
            try
            {
                fixed (byte* baselinePointer = baselineBuffer)
                fixed (SnapshotEntityLayout* baselineEntities = baselineLayout.Entities)
                fixed (SnapshotRecordLayout* baselineRecords = baselineLayout.Records)
                fixed (byte* targetPointer = targetBuffer)
                fixed (SnapshotEntityLayout* targetEntities = targetLayout.Entities)
                fixed (SnapshotRecordLayout* targetRecords = targetLayout.Records)
                fixed (byte* outputPointer = outputBuffer)
                {
                    var written = 0;
                    var operations = 0u;
                    var result = _encode.Invoke(
                        baselinePointer + baseline.Offset, baseline.ByteLength,
                        baselineEntities, baselineLayout.EntityCount, baselineRecords,
                        targetPointer + target.Offset, target.ByteLength,
                        targetEntities, targetLayout.EntityCount, targetRecords,
                        outputPointer + candidate.Offset, candidate.Length,
                        target.EntityCount, target.RecordCount,
                        &written, &operations);
                    if (result != 1 || written < 0 || written > candidate.Length)
                        return SnapshotDeltaBurstResult.Failed;
                    writtenLength = written;
                    operationCount = operations;
                    return SnapshotDeltaBurstResult.Success;
                }
            }
            catch
            {
                writtenLength = 0;
                operationCount = 0;
                return SnapshotDeltaBurstResult.Failed;
            }
        }

        internal static bool IsAvailableForTests
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
                    _encode = BurstCompiler.CompileFunctionPointer<EncodeFunction>(Encode);
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
        private static bool ValidateCandidate(NetworkBufferLease candidate)
        {
            if (candidate == null || candidate.Length < 12 ||
                candidate.Offset < 0 || candidate.Offset > candidate.Capacity ||
                candidate.Length > candidate.Capacity - candidate.Offset)
                return false;
            _ = candidate.Buffer;
            return true;
        }

        [BurstCompile]
        private static int Encode(
            byte* baselineBytes, int baselineLength,
            SnapshotEntityLayout* baselineEntities, int baselineEntityCount,
            SnapshotRecordLayout* baselineRecords,
            byte* targetBytes, int targetLength,
            SnapshotEntityLayout* targetEntities, int targetEntityCount,
            SnapshotRecordLayout* targetRecords,
            byte* destination, int destinationCapacity,
            int targetEntityCountValue, int targetRecordCountValue,
            int* writtenLength, uint* operationCount)
        {
            var position = 0;
            var operations = 0u;
            if (!WriteU32(destination, destinationCapacity, ref position,
                    (uint)targetEntityCountValue) ||
                !WriteU32(destination, destinationCapacity, ref position,
                    (uint)targetRecordCountValue))
                return 0;
            var countPosition = position;
            if (!WriteU32(destination, destinationCapacity, ref position, 0))
                return 0;

            var baselineIndex = 0;
            var targetIndex = 0;
            while (baselineIndex < baselineEntityCount ||
                   targetIndex < targetEntityCount)
            {
                var comparison = baselineIndex >= baselineEntityCount ? 1 :
                    targetIndex >= targetEntityCount ? -1 :
                    CompareGid(baselineEntities[baselineIndex].Gid,
                        targetEntities[targetIndex].Gid);
                if (comparison < 0)
                {
                    if (!WriteByte(destination, destinationCapacity, ref position,
                            EntityRemove) ||
                        !WriteU64(destination, destinationCapacity, ref position,
                            baselineEntities[baselineIndex].Gid))
                        return 0;
                    operations++;
                    baselineIndex++;
                    continue;
                }
                if (comparison > 0)
                {
                    var added = targetEntities[targetIndex];
                    if (!WriteByte(destination, destinationCapacity, ref position,
                            EntityAdd) ||
                        !CopyBytes(destination, destinationCapacity, ref position,
                            targetBytes, targetLength, added.RawOffset,
                            added.RawLength))
                        return 0;
                    operations++;
                    targetIndex++;
                    continue;
                }

                var baselineEntity = baselineEntities[baselineIndex];
                var targetEntity = targetEntities[targetIndex];
                if (!BytesEqual(baselineBytes, baselineLength,
                        baselineEntity.RawOffset, targetBytes, targetLength,
                        targetEntity.RawOffset,
                        baselineEntity.RawLength) ||
                    baselineEntity.RawLength != targetEntity.RawLength)
                {
                    if (!WritePatch(destination, destinationCapacity, ref position,
                            baselineBytes, baselineLength, baselineEntities,
                            baselineRecords, baselineEntity,
                            targetBytes, targetLength, targetEntities,
                            targetRecords, targetEntity))
                        return 0;
                    operations++;
                }
                baselineIndex++;
                targetIndex++;
            }

            if (!WriteU32At(destination, destinationCapacity, countPosition,
                    operations))
                return 0;
            *writtenLength = position;
            *operationCount = operations;
            return 1;
        }

        private static bool WritePatch(byte* destination, int capacity,
            ref int position, byte* baselineBytes, int baselineLength,
            SnapshotEntityLayout* baselineEntities,
            SnapshotRecordLayout* baselineRecords,
            SnapshotEntityLayout baselineEntity, byte* targetBytes,
            int targetLength, SnapshotEntityLayout* targetEntities,
            SnapshotRecordLayout* targetRecords, SnapshotEntityLayout targetEntity)
        {
            if (!WriteByte(destination, capacity, ref position, EntityPatch) ||
                !CopyBytes(destination, capacity, ref position, targetBytes,
                    targetLength, targetEntity.RawOffset, EntityHeaderSize))
                return false;
            var countPosition = position;
            if (!WriteU32(destination, capacity, ref position, 0))
                return false;

            var baselineIndex = baselineEntity.RecordStart;
            var baselineEnd = baselineIndex + baselineEntity.RecordCount;
            var targetIndex = targetEntity.RecordStart;
            var targetEnd = targetIndex + targetEntity.RecordCount;
            var operations = 0u;
            while (baselineIndex < baselineEnd || targetIndex < targetEnd)
            {
                var comparison = baselineIndex >= baselineEnd ? 1 :
                    targetIndex >= targetEnd ? -1 :
                    CompareRecord(baselineRecords[baselineIndex].Kind,
                        baselineRecords[baselineIndex].TypeId,
                        targetRecords[targetIndex].Kind,
                        targetRecords[targetIndex].TypeId);
                if (comparison < 0)
                {
                    var removed = baselineRecords[baselineIndex];
                    if (!WriteByte(destination, capacity, ref position,
                            RecordRemove) ||
                        !WriteU32(destination, capacity, ref position,
                            removed.TypeId) ||
                        !WriteByte(destination, capacity, ref position,
                            removed.Kind))
                        return false;
                    operations++;
                    baselineIndex++;
                    continue;
                }
                if (comparison > 0)
                {
                    var added = targetRecords[targetIndex];
                    if (!WriteByte(destination, capacity, ref position,
                            RecordAdd) ||
                        !CopyBytes(destination, capacity, ref position,
                            targetBytes, targetLength, added.RawOffset,
                            added.RawLength))
                        return false;
                    operations++;
                    targetIndex++;
                    continue;
                }

                var baselineRecord = baselineRecords[baselineIndex];
                var targetRecord = targetRecords[targetIndex];
                if (baselineRecord.RawLength != targetRecord.RawLength ||
                    !BytesEqual(baselineBytes, baselineLength,
                        baselineRecord.RawOffset, targetBytes, targetLength,
                        targetRecord.RawOffset, baselineRecord.RawLength))
                {
                    if (!WriteByte(destination, capacity, ref position,
                            RecordReplace) ||
                        !CopyBytes(destination, capacity, ref position,
                            targetBytes, targetLength, targetRecord.RawOffset,
                            targetRecord.RawLength))
                        return false;
                    operations++;
                }
                baselineIndex++;
                targetIndex++;
            }
            return WriteU32At(destination, capacity, countPosition, operations);
        }

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

        private static int CompareRecord(byte leftKind, uint leftType,
            byte rightKind, uint rightType)
        {
            if (leftKind != rightKind)
                return leftKind < rightKind ? -1 : 1;
            return leftType == rightType ? 0 : leftType < rightType ? -1 : 1;
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

        private static bool CopyBytes(byte* destination, int capacity,
            ref int position, byte* source, int sourceLength, int sourceOffset,
            int length)
        {
            if (length < 0 || sourceOffset < 0 || sourceOffset > sourceLength ||
                position < 0 || position > capacity ||
                length > sourceLength - sourceOffset ||
                length > capacity - position)
                return false;
            var index = 0;
            while (index <= length - 8)
            {
                *(ulong*)(destination + position + index) =
                    *(ulong*)(source + sourceOffset + index);
                index += 8;
            }
            while (index < length)
            {
                destination[position + index] = source[sourceOffset + index];
                index++;
            }
            position += length;
            return true;
        }
        private static bool WriteByte(byte* destination, int capacity,
            ref int position, byte value)
        {
            if (position < 0 || position >= capacity)
                return false;
            destination[position++] = value;
            return true;
        }

        private static bool WriteU32(byte* destination, int capacity,
            ref int position, uint value)
        {
            if (position < 0 || capacity - position < 4)
                return false;
            destination[position++] = (byte)value;
            destination[position++] = (byte)(value >> 8);
            destination[position++] = (byte)(value >> 16);
            destination[position++] = (byte)(value >> 24);
            return true;
        }

        private static bool WriteU64(byte* destination, int capacity,
            ref int position, ulong value)
        {
            if (position < 0 || capacity - position < 8)
                return false;
            for (var index = 0; index < 8; index++)
                destination[position++] = (byte)(value >> (index * 8));
            return true;
        }

        private static bool WriteU32At(byte* destination, int capacity,
            int position, uint value)
        {
            if (position < 0 || capacity - position < 4)
                return false;
            destination[position] = (byte)value;
            destination[position + 1] = (byte)(value >> 8);
            destination[position + 2] = (byte)(value >> 16);
            destination[position + 3] = (byte)(value >> 24);
            return true;
        }
#else
        internal static SnapshotDeltaBurstResult TryEncode(
            NetworkSnapshot baseline, SnapshotLayoutView baselineLayout,
            NetworkSnapshot target, SnapshotLayoutView targetLayout,
            NetworkBufferLease candidate, out int writtenLength,
            out uint operationCount)
        {
            writtenLength = 0;
            operationCount = 0;
            return SnapshotDeltaBurstResult.Unavailable;
        }
#endif
    }
}