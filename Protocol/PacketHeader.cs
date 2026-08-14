using System;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Declares the only wire compression supported by Network v4.</summary>
    public enum NetworkCompression : byte
    {
        /// <summary>Leaves the canonical payload unchanged.</summary>
        None = 0
    }

    /// <summary>Contains the fixed Network v4 packet framing fields.</summary>
    public struct PacketHeader
    {
        /// <summary>Network v4 protocol number.</summary>
        public const ushort Version = 4;
        /// <summary>Fixed encoded header length.</summary>
        public const int Size = 84;
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
        /// <summary>Gets or sets the acknowledged snapshot tick.</summary>
        public uint AcknowledgedSnapshotTick { get; set; }
        /// <summary>Gets or sets the last command tick processed into this server state.</summary>
        public uint ServerProcessedCommandTick { get; set; }
        /// <summary>Gets or sets the last command sequence processed into this server state.</summary>
        public uint ServerProcessedCommandSequence { get; set; }
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
            Hashing.Write32(bytes, 24, AcknowledgedSnapshotTick);
            Hashing.Write32(bytes, 28, ServerProcessedCommandTick);
            Hashing.Write32(bytes, 32, ServerProcessedCommandSequence);
            Hashing.Write32(bytes, 36, PayloadLength);
            SchemaFingerprint.WriteBytes(bytes.Slice(40, 16));
            Hashing.Write64(bytes, 56, SimulationFingerprint);
            Hashing.Write64(bytes, 64, ContentFingerprint);
            Hashing.Write64(bytes, 72, PayloadHash);
            Hashing.Write32(bytes, 80, Hashing.Crc32(bytes));
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
            var expected = Hashing.Read32(copy, 80);
            copy.Slice(80, 4).Clear();
            if (Hashing.Crc32(copy) != expected) return false;
            header = new PacketHeader
            {
                Kind = (PacketKind)source[8], Flags = (PacketFlags)source[9], Compression = (NetworkCompression)source[10],
                SessionEpoch = Hashing.Read32(source, 12), PacketSequence = Hashing.Read32(source, 16),
                ServerTick = Hashing.Read32(source, 20), AcknowledgedSnapshotTick = Hashing.Read32(source, 24),
                ServerProcessedCommandTick = Hashing.Read32(source, 28),
                ServerProcessedCommandSequence = Hashing.Read32(source, 32),
                PayloadLength = Hashing.Read32(source, 36),
                SchemaFingerprint = SchemaFingerprint.ReadBytes(source.Slice(40, 16)),
                SimulationFingerprint = Hashing.Read64(source, 56),
                ContentFingerprint = Hashing.Read64(source, 64),
                PayloadHash = Hashing.Read64(source, 72)
            };
            return IsValid(header);
        }

        private static ushort Read16(ReadOnlySpan<byte> source, int offset) => (ushort)(source[offset] | source[offset + 1] << 8);

        private static bool IsValid(PacketHeader value) =>
            IsKnownKind(value.Kind) &&
            (value.Flags == PacketFlags.ReliableOrdered ||
             value.Flags == PacketFlags.UnreliableSequenced) &&
            value.Compression == NetworkCompression.None && value.PayloadLength <= ProtocolLimits.MaxWirePayloadBytes;

        private static bool IsKnownKind(PacketKind kind) => kind == PacketKind.Hello ||
            kind == PacketKind.Ready || kind == PacketKind.CommandBatch ||
            kind == PacketKind.FullSnapshot || kind == PacketKind.Ack ||
            kind == PacketKind.ResyncRequest || kind == PacketKind.Disconnect ||
            kind == PacketKind.Ping || kind == PacketKind.Pong;
    }

    /// <summary>Encodes and validates exact Network v4 packets.</summary>
    public static class NetworkPacket
    {
        /// <summary>Frames one canonical payload with exact length and xxHash64.</summary>
        public static bool TryEncode(NetworkBufferPool pool, PacketHeader header,
            ReadOnlySpan<byte> payload, out NetworkBufferLease packet)
        {
            packet = null;
            if (pool == null || payload.Length > ProtocolLimits.MaxWirePayloadBytes)
                return false;
            header.PayloadLength = (uint)payload.Length;
            header.PayloadHash = Hashing.XxHash64(payload);
            var lease = pool.Rent(checked(PacketHeader.Size + payload.Length));
            if (!header.TryWrite(lease.WritableSpan))
            {
                lease.Dispose();
                return false;
            }
            payload.CopyTo(lease.WritableSpan.Slice(PacketHeader.Size));
            packet = lease;
            return true;
        }

        /// <summary>Validates framing, exact length, fingerprint and payload hash before exposing payload bytes.</summary>
        public static bool TryDecode(NetworkBufferLease packet,
            SchemaFingerprint expectedFingerprint, out PacketHeader header,
            out ReadOnlyMemory<byte> payload)
        {
            if (!TryDecode(packet, out header, out payload)) return false;
            if (header.SchemaFingerprint == expectedFingerprint) return true;
            header = default; payload = default; return false;
        }

        /// <summary>Validates framing, exact length and payload hash before handshake fingerprint admission.</summary>
        public static bool TryDecode(NetworkBufferLease packet, out PacketHeader header,
            out ReadOnlyMemory<byte> payload)
        {
            header = default;
            payload = default;
            if (packet == null || packet.Length < PacketHeader.Size ||
                !PacketHeader.TryRead(packet.Span, out header) ||
                packet.Length != PacketHeader.Size + header.PayloadLength) return false;
            var body = packet.Span.Slice(PacketHeader.Size, (int)header.PayloadLength);
            if (Hashing.XxHash64(body) != header.PayloadHash) return false;
            payload = packet.Memory.Slice(PacketHeader.Size, (int)header.PayloadLength);
            return true;
        }
    }
}
