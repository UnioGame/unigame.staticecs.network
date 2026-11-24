namespace UniGame.StaticEcs.Network
{
    using System;

    /// <summary>
    /// Payload of an <c>OwnedEntityRefresh</c> packet: a format tag, then either the whole entity
    /// block or only the records that differ from the block the scope snapshot of the same tick
    /// holds for that entity.
    /// </summary>
    /// <remarks>
    /// A record patch is: entity id (8), entity disabled flag (1), changed record count (2), then
    /// per changed record its index in the block (2) and the whole record. It applies only to a
    /// held block with the same entity, kind and record sequence, which is what the server compared
    /// it with; anything else is sent as the whole block.
    /// </remarks>
    internal static class OwnedEntityRefreshPayload
    {
        internal const byte WholeBlock = 0;
        internal const byte RecordPatch = 1;

        // Entity id (8), kind (4), disabled flag (1), record count (2).
        internal const int BlockHeaderSize = 15;
        private const int BlockKindOffset = 8;
        private const int BlockDisabledOffset = 12;
        private const int BlockRecordCountOffset = 13;

        // Type id (4), schema kind (1), version (1), disabled flag (1), payload length (4).
        private const int RecordHeaderSize = 11;
        private const int RecordIdentitySize = 6;
        private const int RecordLengthOffset = 7;

        private const int PatchHeaderSize = sizeof(ulong) + sizeof(byte) + sizeof(ushort);

        /// <summary>Bytes a patch of <paramref name="fresh"/> can take at most.</summary>
        internal static int MaximumPatchBytes(ReadOnlySpan<byte> fresh) =>
            fresh.Length < BlockHeaderSize
                ? 0
                : PatchHeaderSize + fresh.Length - BlockHeaderSize +
                  sizeof(ushort) * ReadUInt16(fresh, BlockRecordCountOffset);

        /// <summary>
        /// Writes the records of <paramref name="fresh"/> that differ from <paramref name="held"/>.
        /// Returns false when the two blocks do not share entity, kind and record sequence, or the
        /// target is too small.
        /// </summary>
        internal static bool TryWritePatch(ReadOnlySpan<byte> held, ReadOnlySpan<byte> fresh,
            Span<byte> target, out int written)
        {
            written = 0;
            if (held.Length < BlockHeaderSize || fresh.Length < BlockHeaderSize ||
                target.Length < PatchHeaderSize ||
                !held.Slice(0, BlockDisabledOffset).SequenceEqual(fresh.Slice(0, BlockDisabledOffset)))
                return false;
            var recordCount = ReadUInt16(fresh, BlockRecordCountOffset);
            if (recordCount != ReadUInt16(held, BlockRecordCountOffset))
                return false;

            Hashing.Write64(target, 0, Hashing.Read64(fresh, 0));
            target[sizeof(ulong)] = fresh[BlockDisabledOffset];
            var position = PatchHeaderSize;
            var changed = 0;
            var heldCursor = BlockHeaderSize;
            var freshCursor = BlockHeaderSize;
            for (var index = 0; index < recordCount; index++)
            {
                if (!TryMeasureRecord(held, heldCursor, out var heldLength) ||
                    !TryMeasureRecord(fresh, freshCursor, out var freshLength) ||
                    !held.Slice(heldCursor, RecordIdentitySize)
                        .SequenceEqual(fresh.Slice(freshCursor, RecordIdentitySize)))
                    return false;
                var freshRecord = fresh.Slice(freshCursor, freshLength);
                if (!freshRecord.SequenceEqual(held.Slice(heldCursor, heldLength)))
                {
                    if (target.Length - position < sizeof(ushort) + freshLength)
                        return false;
                    Hashing.Write16(target, position, (ushort)index);
                    freshRecord.CopyTo(target.Slice(position + sizeof(ushort)));
                    position += sizeof(ushort) + freshLength;
                    changed++;
                }
                heldCursor += heldLength;
                freshCursor += freshLength;
            }
            if (heldCursor != held.Length || freshCursor != fresh.Length)
                return false;
            Hashing.Write16(target, sizeof(ulong) + sizeof(byte), (ushort)changed);
            written = position;
            return true;
        }

        /// <summary>Entity id a patch was written for; zero when the patch is too short to carry one.</summary>
        internal static ulong PatchEntity(ReadOnlySpan<byte> patch) =>
            patch.Length < PatchHeaderSize ? 0UL : Hashing.Read64(patch, 0);

        /// <summary>
        /// Rebuilds the current block from the block the snapshot holds and a patch. Returns false
        /// for a patch that does not belong to <paramref name="held"/> or is malformed.
        /// </summary>
        internal static bool TryApplyPatch(ReadOnlySpan<byte> held, ReadOnlySpan<byte> patch,
            Span<byte> target, out int written, out int recordCount)
        {
            written = 0;
            recordCount = 0;
            if (held.Length < BlockHeaderSize || patch.Length < PatchHeaderSize ||
                target.Length < BlockHeaderSize ||
                Hashing.Read64(held, 0) != Hashing.Read64(patch, 0) ||
                patch[sizeof(ulong)] > 1)
                return false;
            recordCount = ReadUInt16(held, BlockRecordCountOffset);
            var changed = ReadUInt16(patch, sizeof(ulong) + sizeof(byte));
            if (changed > recordCount)
                return false;

            held.Slice(0, BlockHeaderSize).CopyTo(target);
            target[BlockDisabledOffset] = patch[sizeof(ulong)];
            var position = BlockHeaderSize;
            var heldCursor = BlockHeaderSize;
            var patchCursor = PatchHeaderSize;
            var applied = 0;
            for (var index = 0; index < recordCount; index++)
            {
                if (!TryMeasureRecord(held, heldCursor, out var heldLength))
                    return false;
                var source = held.Slice(heldCursor, heldLength);
                if (applied < changed)
                {
                    if (patch.Length - patchCursor < sizeof(ushort))
                        return false;
                    var patchedIndex = ReadUInt16(patch, patchCursor);
                    if (patchedIndex < index)
                        return false;
                    if (patchedIndex == index)
                    {
                        var recordStart = patchCursor + sizeof(ushort);
                        if (!TryMeasureRecord(patch, recordStart, out var patchedLength) ||
                            !patch.Slice(recordStart, RecordIdentitySize)
                                .SequenceEqual(held.Slice(heldCursor, RecordIdentitySize)))
                            return false;
                        source = patch.Slice(recordStart, patchedLength);
                        patchCursor = recordStart + patchedLength;
                        applied++;
                    }
                }
                if (target.Length - position < source.Length)
                    return false;
                source.CopyTo(target.Slice(position));
                position += source.Length;
                heldCursor += heldLength;
            }
            if (applied != changed || patchCursor != patch.Length || heldCursor != held.Length)
                return false;
            written = position;
            return true;
        }

        private static bool TryMeasureRecord(ReadOnlySpan<byte> bytes, int offset, out int length)
        {
            length = 0;
            if (offset < 0 || bytes.Length - offset < RecordHeaderSize)
                return false;
            var payload = Hashing.Read32(bytes, offset + RecordLengthOffset);
            if (payload > (uint)(bytes.Length - offset - RecordHeaderSize))
                return false;
            length = RecordHeaderSize + (int)payload;
            return true;
        }

        private static int ReadUInt16(ReadOnlySpan<byte> bytes, int offset) =>
            bytes[offset] | bytes[offset + 1] << 8;
    }
}
