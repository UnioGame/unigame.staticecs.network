using System;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Declares the only wire compression supported by Network v2.</summary>
    public enum NetworkCompression : byte
    {
        /// <summary>Leaves the canonical payload unchanged.</summary>
        None = 0
    }

    /// <summary>Contains the fixed Network v2 packet framing fields.</summary>
    public struct PacketHeader
    {
        /// <summary>Network v2 protocol number.</summary>
        public const ushort Version = 2;
        /// <summary>Fixed encoded header length.</summary>
        public const int Size = 68;
        /// <summary>Sentinel used when no tick exists.</summary>
        public const uint NoneTick = uint.MaxValue;

        /// <summary>Gets or sets the payload kind.</summary>
        public PacketKind Kind { get; set; }
        /// <summary>Gets or sets delivery flags.</summary>
        public PacketFlags Flags { get; set; }
        /// <summary>Gets or sets compression.</summary>
        public NetworkCompression Compression { get; set; }
        /// <summary>Gets or sets the session epoch.</summary>
        public uint SessionEpoch { get; set; }
        /// <summary>Gets or sets the packet sequence.</summary>
        public uint PacketSequence { get; set; }
        /// <summary>Gets or sets authoritative server time.</summary>
        public uint ServerTick { get; set; }
        /// <summary>Gets or sets the command target tick.</summary>
        public uint TargetTick { get; set; }
        /// <summary>Gets or sets the acknowledged snapshot tick.</summary>
        public uint AcknowledgedSnapshotTick { get; set; }
        /// <summary>Gets or sets the acknowledged command sequence.</summary>
        public uint AcknowledgedCommandSequence { get; set; }
        /// <summary>Gets or sets exact payload length.</summary>
        public uint PayloadLength { get; set; }
        /// <summary>Gets or sets the generated schema fingerprint.</summary>
        public SchemaFingerprint SchemaFingerprint { get; set; }
        /// <summary>Gets or sets xxHash64 of the canonical payload.</summary>
        public ulong PayloadHash { get; set; }

        /// <summary>Writes a complete validated header including CRC32.</summary>
        public bool TryWrite(Span<byte> destination)
        {
            if (destination.Length < Size || !IsValid(this)) return false;
            var bytes = destination.Slice(0, Size);
            bytes.Clear();
            Hashing.Write32(bytes, 0, 0x53434553);
            Hashing.Write16(bytes, 4, Version);
            Hashing.Write16(bytes, 6, Size);
            bytes[8] = (byte)Kind;
            bytes[9] = (byte)Flags;
            bytes[10] = (byte)Compression;
            Hashing.Write32(bytes, 12, SessionEpoch);
            Hashing.Write32(bytes, 16, PacketSequence);
            Hashing.Write32(bytes, 20, ServerTick);
            Hashing.Write32(bytes, 24, TargetTick);
            Hashing.Write32(bytes, 28, AcknowledgedSnapshotTick);
            Hashing.Write32(bytes, 32, AcknowledgedCommandSequence);
            Hashing.Write32(bytes, 36, PayloadLength);
            SchemaFingerprint.WriteBytes(bytes.Slice(40, 16));
            Hashing.Write64(bytes, 56, PayloadHash);
            Hashing.Write32(bytes, 64, Hashing.Crc32(bytes));
            return true;
        }

        /// <summary>Reads and validates a complete fixed header without touching payload bytes.</summary>
        public static bool TryRead(ReadOnlySpan<byte> source, out PacketHeader header)
        {
            header = default;
            if (source.Length < Size || Hashing.Read32(source, 0) != 0x53434553 ||
                Read16(source, 4) != Version || Read16(source, 6) != Size || source[11] != 0) return false;
            Span<byte> copy = stackalloc byte[Size];
            source.Slice(0, Size).CopyTo(copy);
            var expected = Hashing.Read32(copy, 64);
            copy.Slice(64, 4).Clear();
            if (Hashing.Crc32(copy) != expected) return false;
            header = new PacketHeader
            {
                Kind = (PacketKind)source[8], Flags = (PacketFlags)source[9], Compression = (NetworkCompression)source[10],
                SessionEpoch = Hashing.Read32(source, 12), PacketSequence = Hashing.Read32(source, 16),
                ServerTick = Hashing.Read32(source, 20), TargetTick = Hashing.Read32(source, 24),
                AcknowledgedSnapshotTick = Hashing.Read32(source, 28), AcknowledgedCommandSequence = Hashing.Read32(source, 32),
                PayloadLength = Hashing.Read32(source, 36), SchemaFingerprint = SchemaFingerprint.ReadBytes(source.Slice(40, 16)),
                PayloadHash = Hashing.Read64(source, 56)
            };
            return IsValid(header);
        }

        private static ushort Read16(ReadOnlySpan<byte> source, int offset) => (ushort)(source[offset] | source[offset + 1] << 8);

        private static bool IsValid(PacketHeader value) =>
            value.Kind >= PacketKind.Hello && value.Kind <= PacketKind.Disconnect &&
            ((byte)value.Flags & ~(byte)PacketFlags.ReliableOrdered) == 0 &&
            value.Compression == NetworkCompression.None && value.PayloadLength <= ProtocolLimits.MaxWirePayloadBytes;
    }
}
