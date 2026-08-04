using System;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Identifies a packet payload on the version-two wire.</summary>
    public enum PacketKind : byte
    {
        /// <summary>Begins protocol negotiation.</summary>
        Hello = 1,
        /// <summary>Completes protocol negotiation.</summary>
        HelloAck = 2,
        /// <summary>Carries an ordered batch of commands.</summary>
        CommandBatch = 3,
        /// <summary>Carries a complete independent world snapshot.</summary>
        FullSnapshot = 4,
        /// <summary>Acknowledges received state without a payload.</summary>
        Ack = 5,
        /// <summary>Requests recovery from a rejected state.</summary>
        ResyncRequest = 6,
        /// <summary>Ends a protocol session.</summary>
        Disconnect = 7
    }

    /// <summary>Declares packet delivery requirements.</summary>
    [Flags]
    public enum PacketFlags : byte
    {
        /// <summary>Requests exactly-once, in-order delivery.</summary>
        ReliableOrdered = 1
    }

    /// <summary>Declares replicated entity state.</summary>
    [Flags]
    public enum EntityFlags : ushort
    {
        /// <summary>Marks the entity disabled.</summary>
        Disabled = 1
    }

    /// <summary>Identifies a replicated record shape.</summary>
    public enum RecordKind : byte
    {
        /// <summary>A single component value.</summary>
        Component = 1,
        /// <summary>A zero-size tag.</summary>
        Tag = 2,
        /// <summary>A single entity relation.</summary>
        Link = 3,
        /// <summary>A canonical set of entity relations.</summary>
        Links = 4,
        /// <summary>An ordered collection of values.</summary>
        Multi = 5
    }

    /// <summary>Declares replicated record state.</summary>
    [Flags]
    public enum RecordFlags : byte
    {
        /// <summary>Marks a disableable component disabled.</summary>
        Disabled = 1
    }

    /// <summary>Declares command record options.</summary>
    [Flags]
    public enum CommandFlags : ushort
    {
        /// <summary>Version two defines no command options.</summary>
        None = 0
    }

    /// <summary>Reports protocol negotiation results.</summary>
    public enum ConnectResult : ushort
    {
        /// <summary>The connection was accepted.</summary>
        Accepted = 0,
        /// <summary>The protocol versions differ.</summary>
        ProtocolVersionMismatch = 1,
        /// <summary>The schemas differ.</summary>
        SchemaMismatch = 2,
        /// <summary>The requested tick rate is unsupported.</summary>
        TickRateUnsupported = 3,
        /// <summary>The requested limits are unsupported.</summary>
        LimitsRejected = 4,
        /// <summary>The chunk mapping is invalid.</summary>
        ChunkMapRejected = 5
    }

    /// <summary>Explains why a peer requested resynchronization.</summary>
    public enum ResyncReason : ushort
    {
        /// <summary>A canonical payload hash differed.</summary>
        HashMismatch = 1,
        /// <summary>A snapshot could not be applied.</summary>
        SnapshotRejected = 2,
        /// <summary>A bounded queue overflowed.</summary>
        QueueOverflow = 3,
        /// <summary>Local state prevented safe application.</summary>
        LocalStateConflict = 4,
        /// <summary>The session epoch was unexpected.</summary>
        UnexpectedEpoch = 5
    }

    /// <summary>Explains why a protocol session ended.</summary>
    public enum DisconnectReason : ushort
    {
        /// <summary>The peer violated the wire contract.</summary>
        ProtocolViolation = 1,
        /// <summary>The peer used an incompatible schema.</summary>
        SchemaMismatch = 2,
        /// <summary>The peer exceeded negotiated limits.</summary>
        LimitsExceeded = 3,
        /// <summary>The peer used an unexpected session epoch.</summary>
        UnexpectedEpoch = 4,
        /// <summary>The underlying transport closed.</summary>
        TransportClosed = 5,
        /// <summary>A sequence counter was exhausted.</summary>
        SequenceExhausted = 6,
        /// <summary>The server shut down.</summary>
        ServerShutdown = 7,
        /// <summary>The endpoint requested an orderly session close.</summary>
        Requested = 8
    }

    /// <summary>Defines immutable version-two protocol limits.</summary>
    public static class ProtocolLimits
    {
        /// <summary>Maximum encoded payload length.</summary>
        public const int MaxWirePayloadBytes = 8 * 1024 * 1024;
        /// <summary>Maximum decoded payload length.</summary>
        public const int MaxDecodedPayloadBytes = 32 * 1024 * 1024;
        /// <summary>Maximum entities in one snapshot.</summary>
        public const int MaxEntities = 65535;
        /// <summary>Maximum records on one entity.</summary>
        public const int MaxRecordsPerEntity = 256;
        /// <summary>Maximum commands in one batch.</summary>
        public const int MaxCommandsPerBatch = 256;
        /// <summary>Maximum bytes in one command.</summary>
        public const int MaxCommandBytes = 64 * 1024;
        /// <summary>Maximum bytes in one component value.</summary>
        public const int MaxComponentBytes = 1024 * 1024;
        /// <summary>Maximum negotiated chunk mappings.</summary>
        public const int MaxChunkMappings = 4096;
    }

    /// <summary>Identifies a generated wire type by a non-zero xxHash32 value.</summary>
    public readonly struct NetworkTypeId : IEquatable<NetworkTypeId>, IComparable<NetworkTypeId>
    {
        private readonly uint _value;

        /// <summary>Creates an identifier from a non-zero xxHash32 value.</summary>
        public NetworkTypeId(uint value)
        {
            if (value == 0) throw new ArgumentOutOfRangeException(nameof(value));
            _value = value;
        }

        /// <summary>Gets the hash value.</summary>
        public uint Value => _value;

        /// <inheritdoc />
        public bool Equals(NetworkTypeId other) => _value == other._value;
        /// <inheritdoc />
        public override bool Equals(object obj) => obj is NetworkTypeId other && Equals(other);
        /// <inheritdoc />
        public override int GetHashCode() => _value.GetHashCode();
        /// <inheritdoc />
        public int CompareTo(NetworkTypeId other) => _value.CompareTo(other._value);
        /// <inheritdoc />
        public override string ToString() => _value.ToString("x8");
        /// <summary>Tests identifier equality.</summary>
        public static bool operator ==(NetworkTypeId left, NetworkTypeId right) => left.Equals(right);
        /// <summary>Tests identifier inequality.</summary>
        public static bool operator !=(NetworkTypeId left, NetworkTypeId right) => !left.Equals(right);
    }

    /// <summary>Contains the 128-bit schema fingerprint carried by every packet.</summary>
    public readonly struct SchemaFingerprint : IEquatable<SchemaFingerprint>, IComparable<SchemaFingerprint>
    {
        private readonly ulong _low;
        private readonly ulong _high;

        /// <summary>Creates a fingerprint from two little-endian halves.</summary>
        public SchemaFingerprint(ulong low, ulong high) { _low = low; _high = high; }

        /// <summary>Gets the empty fingerprint.</summary>
        public static SchemaFingerprint Empty => default;

        /// <summary>Writes exactly 16 bytes in canonical little-endian order.</summary>
        public void WriteBytes(Span<byte> destination)
        {
            if (destination.Length < 16) throw new ArgumentException("A schema fingerprint requires 16 bytes.", nameof(destination));
            Hashing.Write64(destination, 0, _low);
            Hashing.Write64(destination, 8, _high);
        }

        /// <summary>Reads exactly 16 bytes in canonical little-endian order.</summary>
        public static SchemaFingerprint ReadBytes(ReadOnlySpan<byte> source)
        {
            if (source.Length < 16) throw new ArgumentException("A schema fingerprint requires 16 bytes.", nameof(source));
            return new SchemaFingerprint(Hashing.Read64(source, 0), Hashing.Read64(source, 8));
        }

        /// <inheritdoc />
        public bool Equals(SchemaFingerprint other) => _low == other._low && _high == other._high;
        /// <inheritdoc />
        public override bool Equals(object obj) => obj is SchemaFingerprint other && Equals(other);
        /// <inheritdoc />
        public override int GetHashCode() => unchecked(_low.GetHashCode() * 397 ^ _high.GetHashCode());
        /// <inheritdoc />
        public int CompareTo(SchemaFingerprint other) { var high = _high.CompareTo(other._high); return high != 0 ? high : _low.CompareTo(other._low); }
        /// <inheritdoc />
        public override string ToString() => $"{_high:x16}{_low:x16}";
        /// <summary>Tests identifier equality.</summary>
        public static bool operator ==(SchemaFingerprint left, SchemaFingerprint right) => left.Equals(right);
        /// <summary>Tests identifier inequality.</summary>
        public static bool operator !=(SchemaFingerprint left, SchemaFingerprint right) => !left.Equals(right);
    }
}
