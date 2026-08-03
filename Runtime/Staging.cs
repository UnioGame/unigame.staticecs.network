using System;
using System.Buffers;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Contains staged HelloAck scalars while chunk mappings remain pooled.</summary>
    public readonly struct StagedHelloAck
    {
        internal StagedHelloAck(ConnectResult result, ushort tickRate, uint peerId, ulong serverNonce)
        { Result = result; TickRate = tickRate; PeerId = peerId; ServerNonce = serverNonce; }
        /// <summary>Gets the connection result.</summary>
        public ConnectResult Result { get; }
        /// <summary>Gets the selected tick rate.</summary>
        public ushort TickRate { get; }
        /// <summary>Gets the trusted endpoint peer identifier.</summary>
        public uint PeerId { get; }
        /// <summary>Gets the server nonce.</summary>
        public ulong ServerNonce { get; }
    }

    /// <summary>Indexes one command payload inside an owned staged packet.</summary>
    public readonly struct StagedCommand
    {
        internal StagedCommand(TypeId typeId, ushort version, uint sequence, uint clientTick, int offset, int length)
        { TypeId = typeId; Version = version; Sequence = sequence; ClientTick = clientTick; Offset = offset; Length = length; }
        /// <summary>Gets the stable command type.</summary>
        public TypeId TypeId { get; }
        /// <summary>Gets the command schema version.</summary>
        public ushort Version { get; }
        /// <summary>Gets the ordered command sequence.</summary>
        public uint Sequence { get; }
        /// <summary>Gets the originating client tick.</summary>
        public uint ClientTick { get; }
        internal int Offset { get; }
        internal int Length { get; }
    }

    /// <summary>Indexes one canonical snapshot record inside an owned staged packet.</summary>
    public readonly struct StagedRecord
    {
        internal StagedRecord(TypeId typeId, RecordKind kind, RecordFlags flags, ushort version, uint count, int offset, int length)
        { TypeId = typeId; Kind = kind; Flags = flags; Version = version; ElementCount = count; Offset = offset; Length = length; }
        /// <summary>Gets the stable record type.</summary>
        public TypeId TypeId { get; }
        /// <summary>Gets the record shape.</summary>
        public RecordKind Kind { get; }
        /// <summary>Gets record state flags.</summary>
        public RecordFlags Flags { get; }
        /// <summary>Gets the record schema version.</summary>
        public ushort Version { get; }
        /// <summary>Gets logical element count.</summary>
        public uint ElementCount { get; }
        internal int Offset { get; }
        internal int Length { get; }
    }

    /// <summary>Indexes one canonical snapshot entity and its staged record range.</summary>
    public readonly struct StagedEntity
    {
        internal StagedEntity(WireEntityId entity, TypeId kindId, EntityFlags flags, int firstRecord, int recordCount)
        { Entity = entity; KindId = kindId; Flags = flags; FirstRecord = firstRecord; RecordCount = recordCount; }
        /// <summary>Gets the stable entity identity.</summary>
        public WireEntityId Entity { get; }
        /// <summary>Gets the registered entity kind.</summary>
        public TypeId KindId { get; }
        /// <summary>Gets entity state flags.</summary>
        public EntityFlags Flags { get; }
        /// <summary>Gets the first record index in <see cref="StagedPayload.Records"/>.</summary>
        public int FirstRecord { get; }
        /// <summary>Gets the entity record count.</summary>
        public int RecordCount { get; }
    }

    /// <summary>Owns decoded canonical bytes and pooled typed indexes until ECS consumption completes.</summary>
    public sealed class StagedPayload : IDisposable
    {
        private PacketLease _payload;
        private ChunkMapping[] _chunks;
        private StagedCommand[] _commands;
        private StagedEntity[] _entities;
        private StagedRecord[] _records;
        private int _chunkCount;
        private int _commandCount;
        private int _entityCount;
        private int _recordCount;

        internal StagedPayload(PacketKind kind, ref PacketLease payload) { Kind = kind; _payload = PacketLease.Transfer(ref payload); }
        /// <summary>Gets the staged payload kind.</summary>
        public PacketKind Kind { get; }
        /// <summary>Gets the exact schema hash that validated this stage, or an empty identifier for schema-less payloads.</summary>
        public TypeId SchemaHash { get; private set; }
        /// <summary>Gets a staged Hello value when <see cref="Kind"/> is Hello.</summary>
        public HelloPayload Hello { get; internal set; }
        /// <summary>Gets staged HelloAck scalars when <see cref="Kind"/> is HelloAck.</summary>
        public StagedHelloAck HelloAck { get; internal set; }
        /// <summary>Gets a staged resynchronization request when applicable.</summary>
        public ResyncRequestPayload ResyncRequest { get; internal set; }
        /// <summary>Gets a staged disconnect notification when applicable.</summary>
        public DisconnectPayload Disconnect { get; internal set; }
        /// <summary>Gets canonical decoded bytes borrowed until this stage is disposed.</summary>
        public ReadOnlyMemory<byte> Payload { get { EnsureActive(); return _payload.AsReadOnlyMemory(); } }
        /// <summary>Gets pooled staged chunk mappings borrowed until this stage is disposed.</summary>
        public ReadOnlySpan<ChunkMapping> Chunks { get { EnsureActive(); return _chunks == null ? ReadOnlySpan<ChunkMapping>.Empty : _chunks.AsSpan(0, _chunkCount); } }
        /// <summary>Gets pooled staged commands borrowed until this stage is disposed.</summary>
        public ReadOnlySpan<StagedCommand> Commands { get { EnsureActive(); return _commands == null ? ReadOnlySpan<StagedCommand>.Empty : _commands.AsSpan(0, _commandCount); } }
        /// <summary>Gets pooled staged entities borrowed until this stage is disposed.</summary>
        public ReadOnlySpan<StagedEntity> Entities { get { EnsureActive(); return _entities == null ? ReadOnlySpan<StagedEntity>.Empty : _entities.AsSpan(0, _entityCount); } }
        /// <summary>Gets pooled staged records borrowed until this stage is disposed.</summary>
        public ReadOnlySpan<StagedRecord> Records { get { EnsureActive(); return _records == null ? ReadOnlySpan<StagedRecord>.Empty : _records.AsSpan(0, _recordCount); } }
        /// <summary>Gets canonical bytes for one staged command, borrowed until this stage is disposed.</summary>
        public ReadOnlySpan<byte> GetPayload(in StagedCommand command) { EnsureActive(); return _payload.Span.Slice(command.Offset, command.Length); }
        /// <summary>Gets canonical bytes for one staged snapshot record, borrowed until this stage is disposed.</summary>
        public ReadOnlySpan<byte> GetPayload(in StagedRecord record) { EnsureActive(); return _payload.Span.Slice(record.Offset, record.Length); }
        /// <summary>Returns pooled indexes and decoded payload ownership.</summary>
        public void Dispose()
        {
            if (!_payload.IsValid) return;
            var payload = _payload;
            var chunks = _chunks;
            var commands = _commands;
            var entities = _entities;
            var records = _records;
            _payload = default;
            _chunks = null; _commands = null; _entities = null; _records = null;
            _chunkCount = 0; _commandCount = 0; _entityCount = 0; _recordCount = 0;
            try
            {
                if (chunks != null) ArrayPool<ChunkMapping>.Shared.Return(chunks);
                if (commands != null) ArrayPool<StagedCommand>.Shared.Return(commands);
                if (entities != null) ArrayPool<StagedEntity>.Shared.Return(entities);
                if (records != null) ArrayPool<StagedRecord>.Shared.Return(records);
            }
            finally
            {
                payload.Dispose();
            }
        }
        internal void SetChunks(ChunkMapping[] values, int count) { _chunks = values; _chunkCount = count; }
        internal void SetCommands(StagedCommand[] values, int count) { _commands = values; _commandCount = count; }
        internal void SetSnapshot(StagedEntity[] entities, int entityCount, StagedRecord[] records, int recordCount) { _entities = entities; _entityCount = entityCount; _records = records; _recordCount = recordCount; }
        internal void BindSchema(TypeId schemaHash) => SchemaHash = schemaHash;
        internal bool IsActive => _payload.IsValid;
        private void EnsureActive() { if (!_payload.IsValid) throw new ObjectDisposedException(nameof(StagedPayload)); }
    }

    internal static class PayloadStager
    {
        internal static bool TryStage(PacketKind kind, ref PacketLease payload, Schema schema, out StagedPayload staged)
        {
            staged = null;
            if (!payload.IsValid) return false;
            var result = new StagedPayload(kind, ref payload);
            try
            {
                var source = result.Payload.Span;
                var valid = false;
                switch (kind)
                {
                    case PacketKind.Hello:
                        valid = PayloadCodec.TryReadHello(source, out var hello); result.Hello = hello; break;
                    case PacketKind.HelloAck:
                        valid = TryStageHelloAck(source, result); break;
                    case PacketKind.CommandBatch:
                        valid = schema != null && TryStageCommands(source, result) && schema.Validate(result); break;
                    case PacketKind.FullSnapshot:
                        valid = schema != null && TryStageSnapshot(source, result) && schema.Validate(result); break;
                    case PacketKind.Ack:
                        valid = source.IsEmpty; break;
                    case PacketKind.ResyncRequest:
                        valid = PayloadCodec.TryReadResyncRequest(source, out var resync); result.ResyncRequest = resync; break;
                    case PacketKind.Disconnect:
                        valid = PayloadCodec.TryReadDisconnect(source, out var disconnect); result.Disconnect = disconnect; break;
                }
                if (!valid) return false;
                if (kind == PacketKind.CommandBatch || kind == PacketKind.FullSnapshot) result.BindSchema(schema.Hash);
                staged = result;
                result = null;
                return true;
            }
            finally
            {
                result?.Dispose();
            }
        }

        private static bool TryStageHelloAck(ReadOnlySpan<byte> source, StagedPayload staged)
        {
            if (source.Length < 20) return false;
            var result = (ConnectResult)Read16(source, 0); var count = Read16(source, 16);
            if (result < ConnectResult.Accepted || result > ConnectResult.ChunkMapRejected || Read16(source, 18) != 0 || count > ProtocolLimits.MaxChunkMappings || source.Length != 20 + count * 8) return false;
            ChunkMapping[] chunks = null;
            if (count > 0) chunks = ArrayPool<ChunkMapping>.Shared.Rent(count);
            for (var i = 0; i < count; i++) { var offset = 20 + i * 8; var role = source[offset + 6]; if (role != 1 || source[offset + 7] != 0) { if (chunks != null) ArrayPool<ChunkMapping>.Shared.Return(chunks); return false; } chunks[i] = new ChunkMapping { Chunk = Hashing.Read32(source, offset), Cluster = Read16(source, offset + 4), Role = role }; }
            staged.HelloAck = new StagedHelloAck(result, Read16(source, 2), Hashing.Read32(source, 4), Hashing.Read64(source, 8)); staged.SetChunks(chunks, count); return true;
        }

        private static bool TryStageCommands(ReadOnlySpan<byte> source, StagedPayload staged)
        {
            if (source.Length < 4) return false; var count = Read16(source, 0); if (Read16(source, 2) != 0 || count > ProtocolLimits.MaxCommandsPerBatch || source.Length < 4 + count * 32) return false;
            var offset = 4; uint previous = 0;
            for (var i = 0; i < count; i++) { if (offset > source.Length - 32) return false; var flags = Read16(source, offset + 18); var sequence = Hashing.Read32(source, offset + 20); var length = Hashing.Read32(source, offset + 28); if (flags != 0 || sequence == 0 || i > 0 && sequence <= previous || length > ProtocolLimits.MaxCommandBytes || length > source.Length - offset - 32) return false; previous = sequence; offset += 32 + (int)length; }
            if (offset != source.Length) return false;
            StagedCommand[] commands = null; if (count > 0) commands = ArrayPool<StagedCommand>.Shared.Rent(count); offset = 4;
            for (var i = 0; i < count; i++) { var length = (int)Hashing.Read32(source, offset + 28); commands[i] = new StagedCommand(TypeId.ReadBytes(source.Slice(offset, 16)), Read16(source, offset + 16), Hashing.Read32(source, offset + 20), Hashing.Read32(source, offset + 24), offset + 32, length); offset += 32 + length; }
            staged.SetCommands(commands, count); return true;
        }

        private static bool TryStageSnapshot(ReadOnlySpan<byte> source, StagedPayload staged)
        {
            if (source.Length < 4) return false; var entityCount = Hashing.Read32(source, 0); if (entityCount > ProtocolLimits.MaxEntities || source.Length < 4L + entityCount * 28L) return false;
            var offset = 4; var recordTotal = 0; var previousEntity = default(WireEntityId);
            for (var i = 0; i < entityCount; i++)
            {
                if (offset > source.Length - 28) return false; var entity = ReadEntity(source, offset); var flags = Read16(source, offset + 24); var count = Read16(source, offset + 26);
                if (i > 0 && entity.CompareTo(previousEntity) <= 0 || (flags & ~1) != 0 || count > ProtocolLimits.MaxRecordsPerEntity || source.Length - offset - 28 < count * 28) return false; previousEntity = entity; offset += 28; StagedRecord previous = default;
                for (var j = 0; j < count; j++) { if (offset > source.Length - 28) return false; var rawLength = Hashing.Read32(source, offset + 24); if (rawLength > ProtocolLimits.MaxComponentBytes || rawLength > source.Length - offset - 28) return false; var record = ReadRecord(source, offset); if (j > 0 && Compare(record, previous) <= 0 || !ValidRecord(record, source.Slice(offset + 28, record.Length))) return false; previous = record; offset += 28 + record.Length; recordTotal++; }
            }
            if (offset != source.Length) return false;
            StagedEntity[] entities = null; StagedRecord[] records = null; if (entityCount > 0) entities = ArrayPool<StagedEntity>.Shared.Rent((int)entityCount); if (recordTotal > 0) records = ArrayPool<StagedRecord>.Shared.Rent(recordTotal);
            offset = 4; var recordIndex = 0;
            for (var i = 0; i < entityCount; i++) { var entity = ReadEntity(source, offset); var kind = TypeId.ReadBytes(source.Slice(offset + 8, 16)); var flags = (EntityFlags)Read16(source, offset + 24); var count = Read16(source, offset + 26); entities[i] = new StagedEntity(entity, kind, flags, recordIndex, count); offset += 28; for (var j = 0; j < count; j++) { var record = ReadRecord(source, offset); records[recordIndex++] = record; offset += 28 + record.Length; } }
            staged.SetSnapshot(entities, (int)entityCount, records, recordTotal); return true;
        }

        private static StagedRecord ReadRecord(ReadOnlySpan<byte> source, int offset) => new(TypeId.ReadBytes(source.Slice(offset, 16)), (RecordKind)source[offset + 16], (RecordFlags)source[offset + 17], Read16(source, offset + 18), Hashing.Read32(source, offset + 20), offset + 28, (int)Hashing.Read32(source, offset + 24));
        private static bool ValidRecord(StagedRecord record, ReadOnlySpan<byte> bytes)
        {
            if (record.Kind < RecordKind.Component || record.Kind > RecordKind.Multi || ((byte)record.Flags & ~1) != 0 || bytes.Length > ProtocolLimits.MaxComponentBytes || record.Kind != RecordKind.Component && record.Flags != 0) return false;
            if (record.Kind == RecordKind.Component) return record.ElementCount == 1;
            if (record.Kind == RecordKind.Tag) return record.ElementCount == 0 && bytes.IsEmpty;
            if (record.Kind == RecordKind.Link) return record.ElementCount == 1 && bytes.Length == 8;
            if (record.Kind == RecordKind.Links) { if (bytes.Length != record.ElementCount * 8L) return false; var previous = default(WireEntityId); for (var i = 0; i < record.ElementCount; i++) { var current = ReadEntity(bytes, i * 8); if (i > 0 && current.CompareTo(previous) <= 0) return false; previous = current; } return true; }
            var offset = 0; for (var i = 0; i < record.ElementCount; i++) { if (offset > bytes.Length - 4) return false; var length = Hashing.Read32(bytes, offset); offset += 4; if (length > ProtocolLimits.MaxComponentBytes || length > bytes.Length - offset) return false; offset += (int)length; } return offset == bytes.Length;
        }
        private static int Compare(StagedRecord left, StagedRecord right) { var kind = left.Kind.CompareTo(right.Kind); return kind != 0 ? kind : left.TypeId.CompareTo(right.TypeId); }
        private static WireEntityId ReadEntity(ReadOnlySpan<byte> source, int offset) => new(Hashing.Read32(source, offset), Read16(source, offset + 4), Read16(source, offset + 6));
        private static ushort Read16(ReadOnlySpan<byte> source, int offset) => (ushort)(source[offset] | source[offset + 1] << 8);
    }
}
