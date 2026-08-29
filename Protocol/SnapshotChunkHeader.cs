using System;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Identifies the canonical snapshot payload encoding.</summary>
    public enum SnapshotPayloadKind : byte
    {
        /// <summary>Carries a complete independent snapshot.</summary>
        Keyframe = 1,
        /// <summary>Carries changes from an acknowledged snapshot.</summary>
        Delta = 2
    }

    /// <summary>Contains the fixed snapshot chunk payload header.</summary>
    public struct SnapshotChunkHeader
    {
        /// <summary>Fixed encoded header length.</summary>
        public const int Size = 29;

        /// <summary>Gets or sets the snapshot payload encoding.</summary>
        public SnapshotPayloadKind PayloadKind;
        /// <summary>Gets or sets the authoritative snapshot tick.</summary>
        public uint SnapshotTick;
        /// <summary>Gets or sets the delta baseline tick, or zero for a keyframe.</summary>
        public uint BaselineTick;
        /// <summary>Gets or sets the complete canonical payload length.</summary>
        public uint TotalLength;
        /// <summary>Gets or sets xxHash64 of the complete canonical payload.</summary>
        public ulong TotalHash;
        /// <summary>Gets or sets this chunk's zero-based index.</summary>
        public uint ChunkIndex;
        /// <summary>Gets or sets the total chunk count.</summary>
        public uint ChunkCount;

        /// <summary>Writes a complete validated header in little-endian order.</summary>
        public bool TryWrite(Span<byte> destination)
        {
            if (destination.Length < Size || !IsValid(this)) return false;
            var bytes = destination.Slice(0, Size);
            bytes[0] = (byte)PayloadKind;
            Hashing.Write32(bytes, 1, SnapshotTick);
            Hashing.Write32(bytes, 5, BaselineTick);
            Hashing.Write32(bytes, 9, TotalLength);
            Hashing.Write64(bytes, 13, TotalHash);
            Hashing.Write32(bytes, 21, ChunkIndex);
            Hashing.Write32(bytes, 25, ChunkCount);
            return true;
        }

        /// <summary>Reads and validates one fixed little-endian header.</summary>
        public static bool TryRead(ReadOnlySpan<byte> source,
            out SnapshotChunkHeader header)
        {
            header = default;
            if (source.Length < Size) return false;
            var value = new SnapshotChunkHeader
            {
                PayloadKind = (SnapshotPayloadKind)source[0],
                SnapshotTick = Hashing.Read32(source, 1),
                BaselineTick = Hashing.Read32(source, 5),
                TotalLength = Hashing.Read32(source, 9),
                TotalHash = Hashing.Read64(source, 13),
                ChunkIndex = Hashing.Read32(source, 21),
                ChunkCount = Hashing.Read32(source, 25)
            };
            if (!IsValid(value)) return false;
            header = value;
            return true;
        }

        private static bool IsValid(SnapshotChunkHeader value) =>
            IsKnownPayloadKind(value.PayloadKind) && value.TotalLength > 0 &&
            value.ChunkCount > 0 && value.ChunkIndex < value.ChunkCount &&
            (value.PayloadKind == SnapshotPayloadKind.Keyframe
                ? value.BaselineTick == 0
                : value.BaselineTick != 0 && value.BaselineTick < value.SnapshotTick);

        private static bool IsKnownPayloadKind(SnapshotPayloadKind kind) =>
            kind == SnapshotPayloadKind.Keyframe || kind == SnapshotPayloadKind.Delta;
    }
}
