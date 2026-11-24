using System;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Declares the only wire compression supported by Network v7.</summary>
    public enum NetworkCompression : byte
    {
        /// <summary>Leaves the canonical payload unchanged.</summary>
        None = 0
    }

    /// <summary>Contains the fixed Network v7 packet framing fields.</summary>
    public struct PacketHeader
    {
        /// <summary>Network v7 protocol number.</summary>
        public const ushort Version = ProtocolLimits.Version;
        /// <summary>Fixed encoded header length.</summary>
        public const int Size = 56;
        /// <summary>Sentinel used when no tick exists.</summary>
        public const uint NoneTick = uint.MaxValue;

        /// <summary>Gets or sets the payload kind.</summary>
        public PacketKind Kind { get; set; }
        /// <summary>Gets or sets delivery flags.</summary>
        public PacketFlags Flags { get; set; }
        /// <summary>Gets or sets compression.</summary>
        public NetworkCompression Compression { get; set; }
        /// <summary>
        /// Gets or sets how many bytes at the end of the payload are addressed to this peer alone.
        /// The bytes before them are the payload its <see cref="Kind"/> describes, identical for
        /// every peer that shares it. Only a <see cref="PacketKind.SnapshotChunk"/> carries a trailer.
        /// </summary>
        public byte TrailerLength { get; set; }
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
        /// <summary>Gets or sets the tag of the compatibility fingerprints, see <see cref="CompatibilityTagOf"/>.</summary>
        public uint CompatibilityTag { get; set; }
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
            bytes[11] = TrailerLength;
            Hashing.Write32(bytes, 12, SessionEpoch);
            Hashing.Write32(bytes, 16, PacketSequence);
            Hashing.Write32(bytes, 20, ServerTick);
            Hashing.Write32(bytes, 24, AcknowledgedSnapshotTick);
            Hashing.Write32(bytes, 28, ServerProcessedCommandTick);
            Hashing.Write32(bytes, 32, ServerProcessedCommandSequence);
            Hashing.Write32(bytes, 36, PayloadLength);
            Hashing.Write32(bytes, 40, CompatibilityTag);
            Hashing.Write64(bytes, 44, PayloadHash);
            Hashing.Write32(bytes, 52, Hashing.Crc32(bytes));
            return true;
        }

        /// <summary>
        /// Folds the schema, simulation and content fingerprints into the tag every packet carries.
        /// The client states the full fingerprints once, in its <see cref="HandshakeFingerprints"/>
        /// payload, so admission still names the exact mismatch; later packets only prove they belong
        /// to the same compatible pair.
        /// </summary>
        public static uint CompatibilityTagOf(SchemaFingerprint schema, ulong simulation, ulong content)
        {
            Span<byte> bytes = stackalloc byte[HandshakeFingerprints.Size];
            new HandshakeFingerprints(schema, simulation, content).TryWrite(bytes);
            var hash = Hashing.XxHash64(bytes);
            return unchecked((uint)hash ^ (uint)(hash >> 32));
        }

        /// <summary>Checks whether a complete valid packet declares a different wire protocol version.</summary>
        internal static bool IsProtocolVersionMismatch(ReadOnlySpan<byte> source)
        {
            return TryReadFixedEnvelope(source, out var header, out var version) &&
                   version != Version && IsValid(header) &&
                   source.Length == Size + (int)header.PayloadLength &&
                   Hashing.XxHash64(source.Slice(Size, (int)header.PayloadLength)) == header.PayloadHash;
        }

        /// <summary>Checks the fixed magic and version prefix without trusting the remaining frame.</summary>
        internal static bool HasForeignProtocolVersion(ReadOnlySpan<byte> source) =>
            source.Length >= 6 && Hashing.Read32(source, 0) == 0x53434553 &&
            Read16(source, 4) != Version;

        /// <summary>Reads and validates a complete fixed header without touching payload bytes.</summary>
        public static bool TryRead(ReadOnlySpan<byte> source, out PacketHeader header)
        {
            if (!TryReadFixedEnvelope(source, out header, out var version) ||
                version != Version)
            {
                header = default;
                return false;
            }
            return IsValid(header);
        }

        private static bool TryReadFixedEnvelope(ReadOnlySpan<byte> source,
            out PacketHeader header, out ushort version)
        {
            header = default;
            version = default;
            if (source.Length < Size || Hashing.Read32(source, 0) != 0x53434553 ||
                Read16(source, 6) != Size) return false;
            Span<byte> copy = stackalloc byte[Size];
            source.Slice(0, Size).CopyTo(copy);
            var expected = Hashing.Read32(copy, 52);
            copy.Slice(52, 4).Clear();
            if (Hashing.Crc32(copy) != expected) return false;
            version = Read16(source, 4);
            header = new PacketHeader
            {
                Kind = (PacketKind)source[8], Flags = (PacketFlags)source[9], Compression = (NetworkCompression)source[10],
                TrailerLength = source[11],
                SessionEpoch = Hashing.Read32(source, 12), PacketSequence = Hashing.Read32(source, 16),
                ServerTick = Hashing.Read32(source, 20), AcknowledgedSnapshotTick = Hashing.Read32(source, 24),
                ServerProcessedCommandTick = Hashing.Read32(source, 28),
                ServerProcessedCommandSequence = Hashing.Read32(source, 32),
                PayloadLength = Hashing.Read32(source, 36),
                CompatibilityTag = Hashing.Read32(source, 40),
                PayloadHash = Hashing.Read64(source, 44)
            };
            return true;
        }

        private static ushort Read16(ReadOnlySpan<byte> source, int offset) => (ushort)(source[offset] | source[offset + 1] << 8);

        private static bool IsValid(PacketHeader value) =>
            IsKnownKind(value.Kind) &&
            (value.Flags == PacketFlags.ReliableOrdered ||
             value.Flags == PacketFlags.UnreliableSequenced) &&
            value.Compression == NetworkCompression.None && value.PayloadLength <= ProtocolLimits.MaxWirePayloadBytes &&
            (value.TrailerLength == 0 ||
             (value.Kind == PacketKind.SnapshotChunk && value.TrailerLength < value.PayloadLength));

        private static bool IsKnownKind(PacketKind kind) => kind == PacketKind.Hello ||
            kind == PacketKind.Ready || kind == PacketKind.CommandBatch ||
            kind == PacketKind.Ack ||
            kind == PacketKind.ResyncRequest || kind == PacketKind.Disconnect ||
            kind == PacketKind.SnapshotChunk || kind == PacketKind.Ping ||
            kind == PacketKind.Pong || kind == PacketKind.TransactionCommand ||
            kind == PacketKind.TransactionReceipt ||
            kind == PacketKind.OwnerState ||
            kind == PacketKind.OwnedEntityRefresh;
    }

    /// <summary>Encodes and validates exact Network v7 packets.</summary>
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
            uint expectedCompatibilityTag, out PacketHeader header,
            out ReadOnlyMemory<byte> payload)
        {
            if (!TryDecode(packet, out header, out payload)) return false;
            if (header.CompatibilityTag == expectedCompatibilityTag) return true;
            header = default; payload = default; return false;
        }

        /// <summary>Validates framing, exact length and payload hash before handshake fingerprint admission.</summary>
        public static bool TryDecode(NetworkBufferLease packet, out PacketHeader header,
            out ReadOnlyMemory<byte> payload)
        {
            header = default;
            payload = default;
            if (packet == null) return false;
            // The same received packet is commonly decoded twice: once by a transport's own
            // receive-time framing check, and again by NetworkServer/NetworkClient when it
            // actually processes the packet it queued. A lease's bytes are immutable once framed,
            // so the second call cannot see a different result -- reuse the first call's outcome
            // instead of repeating the CRC32 and xxHash64 passes over untrusted bytes that were
            // already fully verified a moment ago on this exact instance.
            if (packet.TryGetCachedDecode(out var cachedValid, out var cachedHeader))
            {
                if (!cachedValid) return false;
                header = cachedHeader;
                payload = packet.Memory.Slice(PacketHeader.Size, (int)header.PayloadLength);
                return true;
            }
            if (packet.Length < PacketHeader.Size ||
                !PacketHeader.TryRead(packet.Span, out header) ||
                packet.Length != PacketHeader.Size + header.PayloadLength)
            {
                packet.CacheDecode(false, default);
                return false;
            }
            var body = packet.Span.Slice(PacketHeader.Size, (int)header.PayloadLength);
            if (Hashing.XxHash64(body) != header.PayloadHash)
            {
                packet.CacheDecode(false, default);
                return false;
            }
            packet.CacheDecode(true, header);
            payload = packet.Memory.Slice(PacketHeader.Size, (int)header.PayloadLength);
            return true;
        }
    }
}
