using System;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Contains the fixed version-one packet framing fields.</summary>
    public struct PacketHeader
    {
        /// <summary>Version-one fixed header size.</summary>
        public const int Size = 72;
        /// <summary>Sentinel used when no tick exists.</summary>
        public const uint NoneTick = uint.MaxValue;
        /// <summary>Version-one protocol number.</summary>
        public const ushort Version = 1;
        /// <summary>Gets or sets the payload kind.</summary>
        public PacketKind Kind { get; set; }
        /// <summary>Gets or sets delivery flags.</summary>
        public PacketFlags Flags { get; set; }
        /// <summary>Gets or sets the bounded payload transform identifier.</summary>
        public byte TransformId { get; set; }
        /// <summary>Gets or sets the session epoch.</summary>
        public uint SessionEpoch { get; set; }
        /// <summary>Gets or sets the packet sequence, where zero means none.</summary>
        public uint PacketSequence { get; set; }
        /// <summary>Gets or sets the authoritative server tick.</summary>
        public uint ServerTick { get; set; }
        /// <summary>Gets or sets the baseline tick, which is always <see cref="NoneTick"/> in version one.</summary>
        public uint BaselineTick { get; set; }
        /// <summary>Gets or sets the acknowledged snapshot tick.</summary>
        public uint AcknowledgedSnapshotTick { get; set; }
        /// <summary>Gets or sets encoded payload length.</summary>
        public uint WirePayloadLength { get; set; }
        /// <summary>Gets or sets decoded canonical payload length.</summary>
        public uint DecodedPayloadLength { get; set; }
        /// <summary>Gets or sets the 16-byte schema hash.</summary>
        public TypeId SchemaHash { get; set; }
        /// <summary>Gets or sets xxHash64 of the decoded canonical payload.</summary>
        public ulong PayloadHash { get; set; }
        /// <summary>Gets or sets acknowledged command sequence.</summary>
        public uint AcknowledgedCommandSequence { get; set; }

        /// <summary>Writes the complete header and computed CRC into an exact destination.</summary>
        public bool TryWrite(Span<byte> destination)
        {
            if (destination.Length < Size || !IsValid(this)) return false;
            var header = destination.Slice(0, Size);
            header.Clear();
            Hashing.Write32(header, 0, 0x53434553);
            Hashing.Write16(header, 4, Version);
            Hashing.Write16(header, 6, Size);
            header[8] = (byte)Kind; header[9] = (byte)Flags; header[10] = TransformId;
            Hashing.Write32(header, 12, SessionEpoch); Hashing.Write32(header, 16, PacketSequence);
            Hashing.Write32(header, 20, ServerTick); Hashing.Write32(header, 24, BaselineTick);
            Hashing.Write32(header, 28, AcknowledgedSnapshotTick); Hashing.Write32(header, 32, WirePayloadLength);
            Hashing.Write32(header, 36, DecodedPayloadLength); SchemaHash.WriteBytes(header.Slice(40, 16));
            Hashing.Write64(header, 56, PayloadHash); Hashing.Write32(header, 68, AcknowledgedCommandSequence);
            Hashing.Write32(header, 64, Hashing.Crc32(header));
            return true;
        }

        /// <summary>Reads and validates a complete fixed header without reading payload bytes.</summary>
        public static bool TryRead(ReadOnlySpan<byte> source, out PacketHeader header)
        {
            header = default;
            if (source.Length < Size || Hashing.Read32(source, 0) != 0x53434553 ||
                Read16(source, 4) != Version || Read16(source, 6) != Size || source[11] != 0) return false;
            Span<byte> crcBytes = stackalloc byte[Size]; source.Slice(0, Size).CopyTo(crcBytes);
            var expected = Hashing.Read32(crcBytes, 64); crcBytes.Slice(64, 4).Clear();
            if (Hashing.Crc32(crcBytes) != expected) return false;
            header = new PacketHeader
            {
                Kind = (PacketKind)source[8], Flags = (PacketFlags)source[9], TransformId = source[10],
                SessionEpoch = Hashing.Read32(source, 12), PacketSequence = Hashing.Read32(source, 16),
                ServerTick = Hashing.Read32(source, 20), BaselineTick = Hashing.Read32(source, 24),
                AcknowledgedSnapshotTick = Hashing.Read32(source, 28), WirePayloadLength = Hashing.Read32(source, 32),
                DecodedPayloadLength = Hashing.Read32(source, 36), SchemaHash = TypeId.ReadBytes(source.Slice(40, 16)),
                PayloadHash = Hashing.Read64(source, 56), AcknowledgedCommandSequence = Hashing.Read32(source, 68)
            };
            return IsValid(header);
        }

        private static ushort Read16(ReadOnlySpan<byte> source, int offset) => (ushort)(source[offset] | source[offset + 1] << 8);
        private static bool IsValid(PacketHeader value)
        {
            if (value.Kind < PacketKind.Hello || value.Kind > PacketKind.Disconnect ||
                ((byte)value.Flags & ~(byte)PacketFlags.ReliableOrdered) != 0 || value.TransformId != 0 ||
                value.BaselineTick != NoneTick || value.WirePayloadLength > ProtocolLimits.MaxWirePayloadBytes ||
                value.DecodedPayloadLength > ProtocolLimits.MaxDecodedPayloadBytes) return false;
            var reliable = (value.Flags & PacketFlags.ReliableOrdered) != 0;
            return value.Kind == PacketKind.FullSnapshot ? !reliable : reliable;
        }
    }
}
