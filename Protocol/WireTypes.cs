using System;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Identifies a packet payload on the version-one wire.</summary>
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
        /// <summary>Version one defines no command options.</summary>
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

    /// <summary>Defines immutable version-one protocol limits.</summary>
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

    /// <summary>Represents a stable RFC 4122 byte-ordered schema identifier.</summary>
    public readonly struct TypeId : IEquatable<TypeId>, IComparable<TypeId>
    {
        private readonly Guid _value;

        /// <summary>Creates an identifier from a UUID.</summary>
        public TypeId(Guid value) => _value = value;

        /// <summary>Creates an identifier from a canonical UUID string.</summary>
        public TypeId(string value) => _value = Guid.Parse(value);

        /// <summary>Gets the empty identifier.</summary>
        public static TypeId Empty => new(Guid.Empty);

        /// <summary>Gets the UUID value.</summary>
        public Guid Value => _value;

        /// <summary>Writes the identifier in RFC 4122 canonical byte order.</summary>
        public void WriteBytes(Span<byte> destination) => UuidBytes.Write(_value, destination);

        /// <summary>Reads an identifier in RFC 4122 canonical byte order.</summary>
        public static TypeId ReadBytes(ReadOnlySpan<byte> source) => new(UuidBytes.Read(source));

        /// <inheritdoc />
        public bool Equals(TypeId other) => _value.Equals(other._value);
        /// <inheritdoc />
        public override bool Equals(object obj) => obj is TypeId other && Equals(other);
        /// <inheritdoc />
        public override int GetHashCode() => _value.GetHashCode();
        /// <inheritdoc />
        public int CompareTo(TypeId other) => UuidBytes.Compare(_value, other._value);
        /// <inheritdoc />
        public override string ToString() => _value.ToString("D");
        /// <summary>Tests identifier equality.</summary>
        public static bool operator ==(TypeId left, TypeId right) => left.Equals(right);
        /// <summary>Tests identifier inequality.</summary>
        public static bool operator !=(TypeId left, TypeId right) => !left.Equals(right);
    }

    /// <summary>Represents a stable identifier for a bounded value codec.</summary>
    public readonly struct CodecId : IEquatable<CodecId>
    {
        private readonly TypeId _value;
        /// <summary>Creates a codec identifier from a UUID.</summary>
        public CodecId(Guid value) => _value = new TypeId(value);
        /// <summary>Creates a codec identifier from a canonical UUID string.</summary>
        public CodecId(string value) => _value = new TypeId(value);
        /// <summary>Gets the empty codec identifier.</summary>
        public static CodecId Empty => new(Guid.Empty);
        /// <summary>Writes the identifier in RFC 4122 canonical byte order.</summary>
        public void WriteBytes(Span<byte> destination) => _value.WriteBytes(destination);
        /// <inheritdoc />
        public bool Equals(CodecId other) => _value.Equals(other._value);
        /// <inheritdoc />
        public override bool Equals(object obj) => obj is CodecId other && Equals(other);
        /// <inheritdoc />
        public override int GetHashCode() => _value.GetHashCode();
        /// <inheritdoc />
        public override string ToString() => _value.ToString();
        /// <summary>Tests identifier equality.</summary>
        public static bool operator ==(CodecId left, CodecId right) => left.Equals(right);
        /// <summary>Tests identifier inequality.</summary>
        public static bool operator !=(CodecId left, CodecId right) => !left.Equals(right);
    }

    internal static class UuidBytes
    {
        internal static void Write(Guid value, Span<byte> destination)
        {
            if (destination.Length < 16) throw new ArgumentException("A UUID requires 16 bytes.", nameof(destination));
            Span<byte> bytes = stackalloc byte[16];
            if (!value.TryWriteBytes(bytes)) throw new InvalidOperationException("Unable to write UUID bytes.");
            destination[0] = bytes[3]; destination[1] = bytes[2]; destination[2] = bytes[1]; destination[3] = bytes[0];
            destination[4] = bytes[5]; destination[5] = bytes[4];
            destination[6] = bytes[7]; destination[7] = bytes[6];
            for (var i = 8; i < 16; i++) destination[i] = bytes[i];
        }

        internal static Guid Read(ReadOnlySpan<byte> source)
        {
            if (source.Length < 16) throw new ArgumentException("A UUID requires 16 bytes.", nameof(source));
            Span<byte> bytes = stackalloc byte[16];
            bytes[0] = source[3]; bytes[1] = source[2]; bytes[2] = source[1]; bytes[3] = source[0];
            bytes[4] = source[5]; bytes[5] = source[4]; bytes[6] = source[7]; bytes[7] = source[6];
            for (var i = 8; i < 16; i++) bytes[i] = source[i];
            return new Guid(bytes);
        }

        internal static int Compare(Guid left, Guid right)
        {
            Span<byte> a = stackalloc byte[16]; Span<byte> b = stackalloc byte[16];
            Write(left, a); Write(right, b);
            for (var i = 0; i < 16; i++) { var value = a[i].CompareTo(b[i]); if (value != 0) return value; }
            return 0;
        }
    }
}
