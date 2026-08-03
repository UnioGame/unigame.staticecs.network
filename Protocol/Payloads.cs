using System;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Represents an EntityGID in its canonical version-one wire layout.</summary>
    public readonly struct WireEntityId : IEquatable<WireEntityId>, IComparable<WireEntityId>
    {
        /// <summary>Creates a wire entity identifier.</summary>
        public WireEntityId(uint id, ushort clusterId, ushort version) { Id = id; ClusterId = clusterId; Version = version; }
        /// <summary>Gets the entity slot identifier.</summary>
        public uint Id { get; }
        /// <summary>Gets the cluster identifier.</summary>
        public ushort ClusterId { get; }
        /// <summary>Gets the entity generation.</summary>
        public ushort Version { get; }
        /// <inheritdoc />
        public int CompareTo(WireEntityId other) { var c = ClusterId.CompareTo(other.ClusterId); if (c != 0) return c; c = Id.CompareTo(other.Id); return c != 0 ? c : Version.CompareTo(other.Version); }
        /// <inheritdoc />
        public bool Equals(WireEntityId other) => Id == other.Id && ClusterId == other.ClusterId && Version == other.Version;
        /// <inheritdoc />
        public override bool Equals(object obj) => obj is WireEntityId other && Equals(other);
        /// <inheritdoc />
        public override int GetHashCode() => unchecked(((int)Id * 397 ^ ClusterId) * 397 ^ Version);
    }

    /// <summary>Contains a Hello negotiation payload.</summary>
    public struct HelloPayload
    {
        /// <summary>Gets or sets the peer nonce.</summary>
        public ulong Nonce { get; set; }
        /// <summary>Gets or sets the minimum supported tick rate.</summary>
        public ushort MinTickRate { get; set; }
        /// <summary>Gets or sets the maximum supported tick rate.</summary>
        public ushort MaxTickRate { get; set; }
        /// <summary>Gets or sets the maximum encoded payload size.</summary>
        public uint MaxWireBytes { get; set; }
        /// <summary>Gets or sets the maximum decoded payload size.</summary>
        public uint MaxDecodedBytes { get; set; }
        /// <summary>Gets or sets capability bits.</summary>
        public uint Capabilities { get; set; }
    }

    /// <summary>Maps an authoritative chunk to a cluster and role.</summary>
    public struct ChunkMapping
    {
        /// <summary>Gets or sets the chunk identifier.</summary>
        public uint Chunk { get; set; }
        /// <summary>Gets or sets the cluster identifier.</summary>
        public ushort Cluster { get; set; }
        /// <summary>Gets or sets the role, where one means AuthoritySelf.</summary>
        public byte Role { get; set; }
    }

    /// <summary>Contains a HelloAck negotiation payload.</summary>
    public sealed class HelloAckPayload
    {
        /// <summary>Gets or sets the connection result.</summary>
        public ConnectResult Result { get; set; }
        /// <summary>Gets or sets the selected tick rate.</summary>
        public ushort TickRate { get; set; }
        /// <summary>Gets or sets the trusted endpoint peer identifier.</summary>
        public uint PeerId { get; set; }
        /// <summary>Gets or sets the server nonce.</summary>
        public ulong ServerNonce { get; set; }
        /// <summary>Gets or sets chunk mappings.</summary>
        public ChunkMapping[] Chunks { get; set; } = Array.Empty<ChunkMapping>();
    }

    /// <summary>Contains one canonical command record.</summary>
    public struct CommandRecord
    {
        /// <summary>Gets or sets the stable command type.</summary>
        public TypeId TypeId { get; set; }
        /// <summary>Gets or sets the command schema version.</summary>
        public ushort Version { get; set; }
        /// <summary>Gets or sets version-one command flags.</summary>
        public CommandFlags Flags { get; set; }
        /// <summary>Gets or sets the ordered command sequence.</summary>
        public uint Sequence { get; set; }
        /// <summary>Gets or sets the originating client tick.</summary>
        public uint ClientTick { get; set; }
        /// <summary>Gets or sets the exactly bounded command bytes.</summary>
        public byte[] Payload { get; set; }
    }

    /// <summary>Contains an ordered command batch.</summary>
    public sealed class CommandBatchPayload
    {
        /// <summary>Gets or sets commands ordered by sequence.</summary>
        public CommandRecord[] Commands { get; set; } = Array.Empty<CommandRecord>();
    }

    /// <summary>Contains one canonical snapshot record.</summary>
    public struct SnapshotRecord
    {
        /// <summary>Gets or sets the stable record type.</summary>
        public TypeId TypeId { get; set; }
        /// <summary>Gets or sets the record shape.</summary>
        public RecordKind Kind { get; set; }
        /// <summary>Gets or sets record state flags.</summary>
        public RecordFlags Flags { get; set; }
        /// <summary>Gets or sets the schema version.</summary>
        public ushort Version { get; set; }
        /// <summary>Gets or sets logical element count.</summary>
        public uint ElementCount { get; set; }
        /// <summary>Gets or sets canonical record bytes.</summary>
        public byte[] Payload { get; set; }
    }

    /// <summary>Contains one canonical snapshot entity.</summary>
    public struct SnapshotEntity
    {
        /// <summary>Gets or sets the stable entity identity.</summary>
        public WireEntityId Entity { get; set; }
        /// <summary>Gets or sets the registered entity kind.</summary>
        public TypeId KindId { get; set; }
        /// <summary>Gets or sets entity state flags.</summary>
        public EntityFlags Flags { get; set; }
        /// <summary>Gets or sets records ordered by kind then type id.</summary>
        public SnapshotRecord[] Records { get; set; }
    }

    /// <summary>Contains a complete independent snapshot.</summary>
    public sealed class FullSnapshotPayload
    {
        /// <summary>Gets or sets entities in canonical identity order.</summary>
        public SnapshotEntity[] Entities { get; set; } = Array.Empty<SnapshotEntity>();
    }

    /// <summary>Contains a resynchronization request.</summary>
    public struct ResyncRequestPayload
    {
        /// <summary>Gets or sets the recovery reason.</summary>
        public ResyncReason Reason { get; set; }
        /// <summary>Gets or sets the last accepted server tick.</summary>
        public uint LastAcceptedTick { get; set; }
    }

    /// <summary>Contains a disconnect notification.</summary>
    public struct DisconnectPayload
    {
        /// <summary>Gets or sets the disconnect reason.</summary>
        public DisconnectReason Reason { get; set; }
    }

    /// <summary>Reads and writes canonical version-one packet payloads.</summary>
    public static class PayloadCodec
    {
        /// <summary>Writes a Hello payload.</summary>
        public static bool TryWrite(HelloPayload value, Span<byte> destination, out int written)
        {
            var w = new WireWriter(destination); w.U64(value.Nonce); w.U16(value.MinTickRate); w.U16(value.MaxTickRate); w.U32(value.MaxWireBytes); w.U32(value.MaxDecodedBytes); w.U32(value.Capabilities); written = w.Position;
            return w.Valid && value.MinTickRate <= value.MaxTickRate && value.MaxWireBytes <= ProtocolLimits.MaxWirePayloadBytes && value.MaxDecodedBytes <= ProtocolLimits.MaxDecodedPayloadBytes;
        }

        /// <summary>Reads an exactly framed Hello payload.</summary>
        public static bool TryReadHello(ReadOnlySpan<byte> source, out HelloPayload value)
        {
            var r = new WireReader(source); value = new HelloPayload { Nonce = r.U64(), MinTickRate = r.U16(), MaxTickRate = r.U16(), MaxWireBytes = r.U32(), MaxDecodedBytes = r.U32(), Capabilities = r.U32() };
            return r.Complete && value.MinTickRate <= value.MaxTickRate && value.MaxWireBytes <= ProtocolLimits.MaxWirePayloadBytes && value.MaxDecodedBytes <= ProtocolLimits.MaxDecodedPayloadBytes;
        }

        /// <summary>Writes a HelloAck payload.</summary>
        public static bool TryWrite(HelloAckPayload value, Span<byte> destination, out int written)
        {
            written = 0; if (value == null || !Known(value.Result) || value.Chunks == null || value.Chunks.Length > ProtocolLimits.MaxChunkMappings) return false;
            var w = new WireWriter(destination); w.U16((ushort)value.Result); w.U16(value.TickRate); w.U32(value.PeerId); w.U64(value.ServerNonce); w.U16((ushort)value.Chunks.Length); w.U16(0);
            for (var i = 0; i < value.Chunks.Length; i++) { var c = value.Chunks[i]; if (c.Role != 1) return false; w.U32(c.Chunk); w.U16(c.Cluster); w.U8(c.Role); w.U8(0); }
            written = w.Position; return w.Valid;
        }

        /// <summary>Reads an exactly framed HelloAck payload.</summary>
        public static bool TryReadHelloAck(ReadOnlySpan<byte> source, out HelloAckPayload value)
        {
            value = null; var r = new WireReader(source); var result = (ConnectResult)r.U16(); var tick = r.U16(); var peer = r.U32(); var nonce = r.U64(); var count = r.U16(); var reserved = r.U16();
            if (!r.Valid || !Known(result) || reserved != 0 || count > ProtocolLimits.MaxChunkMappings || r.Remaining != count * 8) return false;
            var chunks = new ChunkMapping[count];
            for (var i = 0; i < count; i++) { chunks[i] = new ChunkMapping { Chunk = r.U32(), Cluster = r.U16(), Role = r.U8() }; if (chunks[i].Role != 1 || r.U8() != 0) return false; }
            if (!r.Complete) return false; value = new HelloAckPayload { Result = result, TickRate = tick, PeerId = peer, ServerNonce = nonce, Chunks = chunks }; return true;
        }

        /// <summary>Writes a canonical command batch.</summary>
        public static bool TryWrite(CommandBatchPayload value, Span<byte> destination, out int written)
        {
            written = 0; if (value?.Commands == null || value.Commands.Length > ProtocolLimits.MaxCommandsPerBatch) return false;
            var w = new WireWriter(destination); w.U16((ushort)value.Commands.Length); w.U16(0); uint previous = 0;
            for (var i = 0; i < value.Commands.Length; i++) { var c = value.Commands[i]; var bytes = c.Payload ?? Array.Empty<byte>(); if (c.Flags != CommandFlags.None || c.Sequence == 0 || (i > 0 && c.Sequence <= previous) || bytes.Length > ProtocolLimits.MaxCommandBytes) return false; previous = c.Sequence; w.Id(c.TypeId); w.U16(c.Version); w.U16((ushort)c.Flags); w.U32(c.Sequence); w.U32(c.ClientTick); w.U32((uint)bytes.Length); w.Bytes(bytes); }
            written = w.Position; return w.Valid;
        }

        /// <summary>Reads an exactly framed canonical command batch.</summary>
        public static bool TryReadCommandBatch(ReadOnlySpan<byte> source, out CommandBatchPayload value)
        {
            value = null; if (!ValidateCommandBatchFraming(source)) return false; var r = new WireReader(source); var count = r.U16(); r.U16(); var commands = new CommandRecord[count]; uint previous = 0;
            for (var i = 0; i < count; i++) { var type = r.Id(); var version = r.U16(); var flags = (CommandFlags)r.U16(); var sequence = r.U32(); var tick = r.U32(); var length = r.U32(); if (!r.Valid || flags != CommandFlags.None || sequence == 0 || (i > 0 && sequence <= previous) || length > ProtocolLimits.MaxCommandBytes || length > r.Remaining) return false; previous = sequence; commands[i] = new CommandRecord { TypeId = type, Version = version, Flags = flags, Sequence = sequence, ClientTick = tick, Payload = r.Copy((int)length) }; }
            if (!r.Complete) return false; value = new CommandBatchPayload { Commands = commands }; return true;
        }

        /// <summary>Writes a canonical full snapshot.</summary>
        public static bool TryWrite(FullSnapshotPayload value, Span<byte> destination, out int written)
        {
            written = 0; if (value?.Entities == null || value.Entities.Length > ProtocolLimits.MaxEntities) return false; var w = new WireWriter(destination); w.U32((uint)value.Entities.Length); var previousEntity = default(WireEntityId);
            for (var i = 0; i < value.Entities.Length; i++) { var entity = value.Entities[i]; var records = entity.Records ?? Array.Empty<SnapshotRecord>(); if ((i > 0 && entity.Entity.CompareTo(previousEntity) <= 0) || ((ushort)entity.Flags & ~1) != 0 || records.Length > ProtocolLimits.MaxRecordsPerEntity) return false; previousEntity = entity.Entity; w.Entity(entity.Entity); w.Id(entity.KindId); w.U16((ushort)entity.Flags); w.U16((ushort)records.Length); SnapshotRecord previous = default;
                for (var j = 0; j < records.Length; j++) { var record = records[j]; var bytes = record.Payload ?? Array.Empty<byte>(); if ((j > 0 && Compare(record, previous) <= 0) || !ValidRecord(record, bytes)) return false; previous = record; w.Id(record.TypeId); w.U8((byte)record.Kind); w.U8((byte)record.Flags); w.U16(record.Version); w.U32(record.ElementCount); w.U32((uint)bytes.Length); w.Bytes(bytes); } }
            written = w.Position; return w.Valid;
        }

        /// <summary>Reads an exactly framed canonical full snapshot.</summary>
        public static bool TryReadFullSnapshot(ReadOnlySpan<byte> source, out FullSnapshotPayload value)
        {
            value = null; if (!ValidateSnapshotFraming(source)) return false; var r = new WireReader(source); var count = r.U32(); var entities = new SnapshotEntity[count]; var previousEntity = default(WireEntityId);
            for (var i = 0; i < count; i++) { var id = r.Entity(); var kind = r.Id(); var flags = (EntityFlags)r.U16(); var recordCount = r.U16(); if (!r.Valid || (i > 0 && id.CompareTo(previousEntity) <= 0) || ((ushort)flags & ~1) != 0 || recordCount > ProtocolLimits.MaxRecordsPerEntity) return false; previousEntity = id; var records = new SnapshotRecord[recordCount]; SnapshotRecord previous = default;
                for (var j = 0; j < recordCount; j++) { var record = new SnapshotRecord { TypeId = r.Id(), Kind = (RecordKind)r.U8(), Flags = (RecordFlags)r.U8(), Version = r.U16(), ElementCount = r.U32() }; var length = r.U32(); if (!r.Valid || length > ProtocolLimits.MaxComponentBytes || length > r.Remaining) return false; record.Payload = r.Copy((int)length); if ((j > 0 && Compare(record, previous) <= 0) || !ValidRecord(record, record.Payload)) return false; previous = record; records[j] = record; }
                entities[i] = new SnapshotEntity { Entity = id, KindId = kind, Flags = flags, Records = records }; }
            if (!r.Complete) return false; value = new FullSnapshotPayload { Entities = entities }; return true;
        }

        /// <summary>Writes an empty Ack payload.</summary>
        public static bool TryWriteAck(Span<byte> destination, out int written) { written = 0; return true; }
        /// <summary>Validates an empty Ack payload.</summary>
        public static bool TryReadAck(ReadOnlySpan<byte> source) => source.IsEmpty;

        /// <summary>Writes a resynchronization request.</summary>
        public static bool TryWrite(ResyncRequestPayload value, Span<byte> destination, out int written) { var w = new WireWriter(destination); w.U16((ushort)value.Reason); w.U16(0); w.U32(value.LastAcceptedTick); written = w.Position; return w.Valid && Known(value.Reason); }
        /// <summary>Reads an exactly framed resynchronization request.</summary>
        public static bool TryReadResyncRequest(ReadOnlySpan<byte> source, out ResyncRequestPayload value) { var r = new WireReader(source); value = new ResyncRequestPayload { Reason = (ResyncReason)r.U16() }; var reserved = r.U16(); value.LastAcceptedTick = r.U32(); return r.Complete && reserved == 0 && Known(value.Reason); }
        /// <summary>Writes a disconnect notification.</summary>
        public static bool TryWrite(DisconnectPayload value, Span<byte> destination, out int written) { var w = new WireWriter(destination); w.U16((ushort)value.Reason); w.U16(0); written = w.Position; return w.Valid && Known(value.Reason); }
        /// <summary>Reads an exactly framed disconnect notification.</summary>
        public static bool TryReadDisconnect(ReadOnlySpan<byte> source, out DisconnectPayload value) { var r = new WireReader(source); value = new DisconnectPayload { Reason = (DisconnectReason)r.U16() }; var reserved = r.U16(); return r.Complete && reserved == 0 && Known(value.Reason); }

        private static int Compare(SnapshotRecord left, SnapshotRecord right) { var c = left.Kind.CompareTo(right.Kind); return c != 0 ? c : left.TypeId.CompareTo(right.TypeId); }
        private static bool ValidRecord(SnapshotRecord record, byte[] bytes)
        {
            if (record.Kind < RecordKind.Component || record.Kind > RecordKind.Multi || ((byte)record.Flags & ~1) != 0 || bytes.Length > ProtocolLimits.MaxComponentBytes) return false;
            if (record.Kind != RecordKind.Component && record.Flags != 0) return false;
            if (record.Kind == RecordKind.Component) return record.ElementCount == 1;
            if (record.Kind == RecordKind.Tag) return record.ElementCount == 0 && bytes.Length == 0;
            if (record.Kind == RecordKind.Link) return record.ElementCount == 1 && bytes.Length == 8;
            if (record.Kind == RecordKind.Links) { if (bytes.Length != record.ElementCount * 8L) return false; var previous = default(WireEntityId); for (var i = 0; i < record.ElementCount; i++) { var current = ReadEntity(bytes, i * 8); if (i > 0 && current.CompareTo(previous) <= 0) return false; previous = current; } return true; }
            var offset = 0; for (var i = 0; i < record.ElementCount; i++) { if (offset > bytes.Length - 4) return false; var length = Hashing.Read32(bytes, offset); offset += 4; if (length > ProtocolLimits.MaxComponentBytes || length > bytes.Length - offset) return false; offset += (int)length; } return offset == bytes.Length;
        }
        private static WireEntityId ReadEntity(ReadOnlySpan<byte> bytes, int offset) => new(Hashing.Read32(bytes, offset), (ushort)(bytes[offset + 4] | bytes[offset + 5] << 8), (ushort)(bytes[offset + 6] | bytes[offset + 7] << 8));
        private static bool Known(ConnectResult v) => v >= ConnectResult.Accepted && v <= ConnectResult.ChunkMapRejected;
        private static bool Known(ResyncReason v) => v >= ResyncReason.HashMismatch && v <= ResyncReason.UnexpectedEpoch;
        private static bool Known(DisconnectReason v) => v >= DisconnectReason.ProtocolViolation && v <= DisconnectReason.Requested;

        internal static bool ValidateCommandBatchFraming(ReadOnlySpan<byte> source)
        {
            if (source.Length < 4) return false; var count = (ushort)(source[0] | source[1] << 8); if ((source[2] | source[3]) != 0 || count > ProtocolLimits.MaxCommandsPerBatch || source.Length < 4 + count * 32) return false;
            var offset = 4; uint previous = 0;
            for (var i = 0; i < count; i++) { if (offset > source.Length - 32) return false; var flags = (ushort)(source[offset + 18] | source[offset + 19] << 8); var sequence = Hashing.Read32(source, offset + 20); var length = Hashing.Read32(source, offset + 28); if (flags != 0 || sequence == 0 || i > 0 && sequence <= previous || length > ProtocolLimits.MaxCommandBytes || length > source.Length - offset - 32) return false; previous = sequence; offset += 32 + (int)length; }
            return offset == source.Length;
        }

        internal static bool ValidateSnapshotFraming(ReadOnlySpan<byte> source)
        {
            if (source.Length < 4) return false; var count = Hashing.Read32(source, 0); if (count > ProtocolLimits.MaxEntities || source.Length < 4L + count * 28L) return false;
            var offset = 4; var previousEntity = default(WireEntityId);
            for (var i = 0; i < count; i++) { if (offset > source.Length - 28) return false; var entity = ReadEntity(source, offset); var flags = (ushort)(source[offset + 24] | source[offset + 25] << 8); var recordCount = (ushort)(source[offset + 26] | source[offset + 27] << 8); if (i > 0 && entity.CompareTo(previousEntity) <= 0 || (flags & ~1) != 0 || recordCount > ProtocolLimits.MaxRecordsPerEntity || source.Length - offset - 28 < recordCount * 28) return false; previousEntity = entity; offset += 28; SnapshotRecord previous = default;
                for (var j = 0; j < recordCount; j++) { if (offset > source.Length - 28) return false; var length = Hashing.Read32(source, offset + 24); if (length > ProtocolLimits.MaxComponentBytes || length > source.Length - offset - 28) return false; var record = new SnapshotRecord { TypeId = TypeId.ReadBytes(source.Slice(offset, 16)), Kind = (RecordKind)source[offset + 16], Flags = (RecordFlags)source[offset + 17], Version = (ushort)(source[offset + 18] | source[offset + 19] << 8), ElementCount = Hashing.Read32(source, offset + 20) }; if (j > 0 && Compare(record, previous) <= 0 || !ValidRecord(record, source.Slice(offset + 28, (int)length))) return false; previous = record; offset += 28 + (int)length; } }
            return offset == source.Length;
        }

        private static bool ValidRecord(SnapshotRecord record, ReadOnlySpan<byte> bytes)
        {
            if (record.Kind < RecordKind.Component || record.Kind > RecordKind.Multi || ((byte)record.Flags & ~1) != 0 || bytes.Length > ProtocolLimits.MaxComponentBytes) return false;
            if (record.Kind != RecordKind.Component && record.Flags != 0) return false;
            if (record.Kind == RecordKind.Component) return record.ElementCount == 1;
            if (record.Kind == RecordKind.Tag) return record.ElementCount == 0 && bytes.IsEmpty;
            if (record.Kind == RecordKind.Link) return record.ElementCount == 1 && bytes.Length == 8;
            if (record.Kind == RecordKind.Links) { if (bytes.Length != record.ElementCount * 8L) return false; var previous = default(WireEntityId); for (var i = 0; i < record.ElementCount; i++) { var current = ReadEntity(bytes, i * 8); if (i > 0 && current.CompareTo(previous) <= 0) return false; previous = current; } return true; }
            var offset = 0; for (var i = 0; i < record.ElementCount; i++) { if (offset > bytes.Length - 4) return false; var length = Hashing.Read32(bytes, offset); offset += 4; if (length > ProtocolLimits.MaxComponentBytes || length > bytes.Length - offset) return false; offset += (int)length; } return offset == bytes.Length;
        }
    }

    internal ref struct WireWriter
    {
        private Span<byte> _data; internal int Position; internal bool Valid;
        internal WireWriter(Span<byte> data) { _data = data; Position = 0; Valid = true; }
        internal void U8(byte value) { if (!Take(1)) return; _data[Position - 1] = value; }
        internal void U16(ushort value) { if (!Take(2)) return; Hashing.Write16(_data, Position - 2, value); }
        internal void U32(uint value) { if (!Take(4)) return; Hashing.Write32(_data, Position - 4, value); }
        internal void U64(ulong value) { if (!Take(8)) return; Hashing.Write64(_data, Position - 8, value); }
        internal void Id(TypeId value) { if (!Take(16)) return; value.WriteBytes(_data.Slice(Position - 16, 16)); }
        internal void Entity(WireEntityId value) { U32(value.Id); U16(value.ClusterId); U16(value.Version); }
        internal void Bytes(ReadOnlySpan<byte> value) { if (!Take(value.Length)) return; value.CopyTo(_data.Slice(Position - value.Length)); }
        private bool Take(int count) { if (!Valid || count > _data.Length - Position) { Valid = false; return false; } Position += count; return true; }
    }

    internal ref struct WireReader
    {
        private ReadOnlySpan<byte> _data; private int _position; internal bool Valid; internal int Remaining => Valid ? _data.Length - _position : 0; internal bool Complete => Valid && _position == _data.Length;
        internal WireReader(ReadOnlySpan<byte> data) { _data = data; _position = 0; Valid = true; }
        internal byte U8() { if (!Take(1, out var p)) return 0; return _data[p]; }
        internal ushort U16() { if (!Take(2, out var p)) return 0; return (ushort)(_data[p] | _data[p + 1] << 8); }
        internal uint U32() { if (!Take(4, out var p)) return 0; return Hashing.Read32(_data, p); }
        internal ulong U64() { if (!Take(8, out var p)) return 0; return Hashing.Read64(_data, p); }
        internal TypeId Id() { if (!Take(16, out var p)) return default; return TypeId.ReadBytes(_data.Slice(p, 16)); }
        internal WireEntityId Entity() => new(U32(), U16(), U16());
        internal byte[] Copy(int count) { if (!Take(count, out var p)) return Array.Empty<byte>(); return _data.Slice(p, count).ToArray(); }
        private bool Take(int count, out int position) { position = _position; if (!Valid || count < 0 || count > _data.Length - _position) { Valid = false; return false; } _position += count; return true; }
    }
}
