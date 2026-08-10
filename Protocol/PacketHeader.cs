using System;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Declares the only wire compression supported by Network v3.</summary>
    public enum NetworkCompression : byte
    {
        /// <summary>Leaves the canonical payload unchanged.</summary>
        None = 0
    }

    /// <summary>Contains the fixed Network v3 packet framing fields.</summary>
    public struct PacketHeader
    {
        /// <summary>Network v3 protocol number.</summary>
        public const ushort Version = 3;
        /// <summary>Fixed encoded header length.</summary>
        public const int Size = 92;
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
        /// <summary>Gets or sets the last input tick processed into this server state.</summary>
        public uint LastProcessedInputTick { get; set; }
        /// <summary>Gets or sets the last input sequence processed into this server state.</summary>
        public uint LastProcessedInputSequence { get; set; }
        /// <summary>Gets or sets exact payload length.</summary>
        public uint PayloadLength { get; set; }
        /// <summary>Gets or sets the generated schema fingerprint.</summary>
        public SchemaFingerprint SchemaFingerprint { get; set; }
        /// <summary>Gets or sets the deterministic simulation configuration fingerprint.</summary>
        public ulong SimulationFingerprint { get; set; }
        /// <summary>Gets or sets the baked grid and content fingerprint.</summary>
        public ulong ContentFingerprint { get; set; }
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
            Hashing.Write32(bytes, 36, LastProcessedInputTick);
            Hashing.Write32(bytes, 40, LastProcessedInputSequence);
            Hashing.Write32(bytes, 44, PayloadLength);
            SchemaFingerprint.WriteBytes(bytes.Slice(48, 16));
            Hashing.Write64(bytes, 64, SimulationFingerprint);
            Hashing.Write64(bytes, 72, ContentFingerprint);
            Hashing.Write64(bytes, 80, PayloadHash);
            Hashing.Write32(bytes, 88, Hashing.Crc32(bytes));
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
            var expected = Hashing.Read32(copy, 88);
            copy.Slice(88, 4).Clear();
            if (Hashing.Crc32(copy) != expected) return false;
            header = new PacketHeader
            {
                Kind = (PacketKind)source[8], Flags = (PacketFlags)source[9], Compression = (NetworkCompression)source[10],
                SessionEpoch = Hashing.Read32(source, 12), PacketSequence = Hashing.Read32(source, 16),
                ServerTick = Hashing.Read32(source, 20), TargetTick = Hashing.Read32(source, 24),
                AcknowledgedSnapshotTick = Hashing.Read32(source, 28), AcknowledgedCommandSequence = Hashing.Read32(source, 32),
                LastProcessedInputTick = Hashing.Read32(source, 36), LastProcessedInputSequence = Hashing.Read32(source, 40),
                PayloadLength = Hashing.Read32(source, 44), SchemaFingerprint = SchemaFingerprint.ReadBytes(source.Slice(48, 16)),
                SimulationFingerprint = Hashing.Read64(source, 64),
                ContentFingerprint = Hashing.Read64(source, 72),
                PayloadHash = Hashing.Read64(source, 80)
            };
            return IsValid(header);
        }

        private static ushort Read16(ReadOnlySpan<byte> source, int offset) => (ushort)(source[offset] | source[offset + 1] << 8);

        private static bool IsValid(PacketHeader value) =>
            value.Kind >= PacketKind.Hello && value.Kind <= PacketKind.Pong &&
            (value.Flags == PacketFlags.ReliableOrdered ||
             value.Flags == PacketFlags.UnreliableSequenced) &&
            value.Compression == NetworkCompression.None && value.PayloadLength <= ProtocolLimits.MaxWirePayloadBytes;
    }

    /// <summary>Encodes and validates exact Network v3 packets.</summary>
    public static class NetworkPacket
    {
        /// <summary>Frames one canonical payload with exact length and xxHash64.</summary>
        public static bool TryEncode(PacketHeader header, ReadOnlySpan<byte> payload, out byte[] packet)
        {
            packet = null;
            if (payload.Length > ProtocolLimits.MaxWirePayloadBytes) return false;
            header.PayloadLength = (uint)payload.Length;
            header.PayloadHash = Hashing.XxHash64(payload);
            var bytes = new byte[PacketHeader.Size + payload.Length];
            if (!header.TryWrite(bytes)) return false;
            payload.CopyTo(bytes.AsSpan(PacketHeader.Size));
            packet = bytes;
            return true;
        }

        /// <summary>Validates framing, exact length, fingerprint and payload hash before exposing payload bytes.</summary>
        public static bool TryDecode(byte[] packet, SchemaFingerprint expectedFingerprint, out PacketHeader header, out ReadOnlyMemory<byte> payload)
        {
            if (!TryDecode(packet, out header, out payload)) return false;
            if (header.SchemaFingerprint == expectedFingerprint) return true;
            header = default; payload = default; return false;
        }

        /// <summary>Validates framing, exact length and payload hash before handshake fingerprint admission.</summary>
        public static bool TryDecode(byte[] packet, out PacketHeader header, out ReadOnlyMemory<byte> payload)
        {
            header = default;
            payload = default;
            if (packet == null || packet.Length < PacketHeader.Size || !PacketHeader.TryRead(packet, out header) ||
                packet.Length != PacketHeader.Size + header.PayloadLength) return false;
            var body = new ReadOnlySpan<byte>(packet, PacketHeader.Size, (int)header.PayloadLength);
            if (Hashing.XxHash64(body) != header.PayloadHash) return false;
            payload = new ReadOnlyMemory<byte>(packet, PacketHeader.Size, (int)header.PayloadLength);
            return true;
        }
    }
}
