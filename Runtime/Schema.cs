using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using FFS.Libraries.StaticEcs;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Identifies a canonical schema manifest record.</summary>
    public enum SchemaKind : byte
    {
        /// <summary>A replicated entity kind and creation path.</summary>
        Entity = 0,
        /// <summary>A single component value.</summary>
        Component = 1,
        /// <summary>A zero-size tag.</summary>
        Tag = 2,
        /// <summary>A single entity relation.</summary>
        Link = 3,
        /// <summary>A canonical relation set.</summary>
        Links = 4,
        /// <summary>An ordered multi-component value.</summary>
        Multi = 5,
        /// <summary>An ordered endpoint command.</summary>
        Command = 6
    }

    /// <summary>Defines a pure bounded codec whose successful reads and writes report exact consumption.</summary>
    public interface ICodec<T>
    {
        /// <summary>Writes one value without exceeding the destination.</summary>
        bool TryWrite(in T value, Span<byte> destination, out int written);
        /// <summary>Reads one value without exceeding the source.</summary>
        bool TryRead(ReadOnlySpan<byte> source, out T value, out int read);
    }

    /// <summary>Describes one frozen schema manifest record.</summary>
    public sealed class SchemaEntry
    {
        internal SchemaEntry(SchemaKind kind, byte flags, ushort version, TypeId typeId, CodecId codecId, uint maxPayload, uint maxCount, Type runtimeType, ISchemaInvoker invoker)
        { Kind = kind; Flags = flags; Version = version; TypeId = typeId; CodecId = codecId; MaxPayload = maxPayload; MaxCount = maxCount; RuntimeType = runtimeType; Invoker = invoker; }
        /// <summary>Gets the manifest record kind.</summary>
        public SchemaKind Kind { get; }
        /// <summary>Gets manifest flags.</summary>
        public byte Flags { get; }
        /// <summary>Gets the schema version.</summary>
        public ushort Version { get; }
        /// <summary>Gets the stable type identifier.</summary>
        public TypeId TypeId { get; }
        /// <summary>Gets the bounded codec identifier.</summary>
        public CodecId CodecId { get; }
        /// <summary>Gets maximum encoded value bytes.</summary>
        public uint MaxPayload { get; }
        /// <summary>Gets maximum element count.</summary>
        public uint MaxCount { get; }
        /// <summary>Gets the diagnostic runtime type.</summary>
        public Type RuntimeType { get; }
        /// <summary>Gets the retained typed command authorizer type, or null for non-command records.</summary>
        public Type AuthorizerType => CommandInvoker?.AuthorizerType;
        internal ISchemaInvoker Invoker { get; }
        internal IEntityInvoker EntityInvoker => Invoker as IEntityInvoker;
        internal IEntryCodec Codec => Invoker as IEntryCodec;
        internal ICommandInvoker CommandInvoker => Invoker as ICommandInvoker;
    }

    /// <summary>Contains an immutable deterministic network schema.</summary>
    public sealed class Schema
    {
        private readonly SchemaEntry[] _entries;
        internal Schema(TypeId hash, SchemaEntry[] entries, Type worldType) { Hash = hash; _entries = entries; WorldType = worldType; }
        /// <summary>Gets the first 16 bytes of the canonical manifest SHA-256.</summary>
        public TypeId Hash { get; }
        /// <summary>Gets manifest records ordered by kind then RFC UUID bytes.</summary>
        public IReadOnlyList<SchemaEntry> Entries => _entries;
        internal Type WorldType { get; }
        internal ReadOnlySpan<SchemaEntry> RetainedEntries => _entries;
        /// <summary>Finds a schema record by stable identifier.</summary>
        public bool TryGet(TypeId typeId, out SchemaEntry entry)
        {
            for (var i = 0; i < _entries.Length; i++) if (_entries[i].TypeId == typeId) { entry = _entries[i]; return true; }
            entry = null; return false;
        }

        internal void EnsureWorld<TWorld>() where TWorld : struct, IWorldType
        {
            if (WorldType != typeof(TWorld))
                throw new InvalidOperationException($"Schema for world `{WorldType.FullName}` cannot be used with world `{typeof(TWorld).FullName}`.");
        }

        internal bool TryGetCommand<T>(out SchemaEntry entry, out ICommandInvoker<T> invoker) where T : unmanaged
        {
            for (var i = 0; i < _entries.Length; i++)
            {
                var candidate = _entries[i];
                if (candidate.Kind != SchemaKind.Command || candidate.CommandInvoker is not ICommandInvoker<T> typed) continue;
                entry = candidate;
                invoker = typed;
                return true;
            }

            entry = null;
            invoker = null;
            return false;
        }

        internal bool Validate(StagedPayload staged)
        {
            if (staged.Kind == PacketKind.CommandBatch)
            {
                var commands = staged.Commands;
                for (var i = 0; i < commands.Length; i++) { ref readonly var command = ref commands[i]; var payload = staged.GetPayload(command); if (!TryGet(command.TypeId, out var entry) || entry.Kind != SchemaKind.Command || entry.Version != command.Version || payload.Length > entry.MaxPayload || entry.CommandInvoker == null || !entry.CommandInvoker.Validate(payload, 1)) return false; }
                return true;
            }
            if (staged.Kind != PacketKind.FullSnapshot) return true;
            var entities = staged.Entities; var records = staged.Records;
            for (var i = 0; i < entities.Length; i++) { ref readonly var entity = ref entities[i]; if (!TryGet(entity.KindId, out var kind) || kind.Kind != SchemaKind.Entity || kind.EntityInvoker == null) return false; for (var j = 0; j < entity.RecordCount; j++) { ref readonly var record = ref records[entity.FirstRecord + j]; if (!TryGet(record.TypeId, out var entry) || (byte)entry.Kind != (byte)record.Kind || entry.Version != record.Version || record.ElementCount > entry.MaxCount || entry.Kind == SchemaKind.Component && staged.GetPayload(record).Length > entry.MaxPayload || (record.Flags != 0 && (entry.Flags & 1) == 0) || entry.Codec == null || !entry.Codec.Validate(staged.GetPayload(record), record.ElementCount)) return false; } }
            return true;
        }

        /// <summary>Decodes and authorizes one retained typed command from trusted endpoint context without reparsing wire framing.</summary>
        public bool TryAuthorizeCommand<T>(StagedPayload staged, int commandIndex, in CommandContext context, out T command)
            where T : unmanaged
        {
            command = default;
            if (staged == null || staged.Kind != PacketKind.CommandBatch || staged.SchemaHash != Hash || !staged.IsActive ||
                (uint)commandIndex >= (uint)staged.Commands.Length) return false;
            ref readonly var record = ref staged.Commands[commandIndex];
            if (record.Sequence != context.Sequence || record.ClientTick != context.ClientTick || !TryGet(record.TypeId, out var entry) || entry.CommandInvoker is not ICommandInvoker<T> invoker) return false;
            return invoker.TryAuthorize(staged.GetPayload(record), in context, out command);
        }
    }

    /// <summary>Builds an AOT-safe typed schema for one Static ECS world.</summary>
    public sealed class SchemaBuilder<TWorld> where TWorld : struct, IWorldType
    {
        private const uint MaxCollectionCount = 32768;
        private readonly List<SchemaEntry> _entries = new();
        private readonly HashSet<TypeId> _ids = new();
        private readonly HashSet<Type> _types = new();
        private bool _frozen;

        /// <summary>Registers a replicated entity kind and its typed creation path.</summary>
        public SchemaBuilder<TWorld> EntityKind<TEntityType>(TypeId typeId) where TEntityType : unmanaged, IEntityType
        {
            Add(SchemaKind.Entity, 0, 0, typeId, CodecId.Empty, 0, 0, typeof(TEntityType), new EntityInvoker<TWorld, TEntityType>()); return this;
        }

        /// <summary>Registers a single component codec.</summary>
        public SchemaBuilder<TWorld> Component<T, TCodec>(TypeId typeId, ushort version, CodecId codecId, uint maxBytes)
            where T : unmanaged, IComponent where TCodec : struct, ICodec<T>
        { CheckPayload(maxBytes); Add(SchemaKind.Component, typeof(IDisableable).IsAssignableFrom(typeof(T)) ? (byte)1 : (byte)0, version, typeId, codecId, maxBytes, 1, typeof(T), new ComponentInvoker<TWorld, T, TCodec>()); return this; }

        /// <summary>Registers a zero-size tag.</summary>
        public SchemaBuilder<TWorld> Tag<T>(TypeId typeId, ushort version) where T : unmanaged, ITag
        { Add(SchemaKind.Tag, 0, version, typeId, CodecId.Empty, 0, 0, typeof(T), new TagInvoker<TWorld, T>()); return this; }

        /// <summary>Registers a single entity relation.</summary>
        public SchemaBuilder<TWorld> Link<T>(TypeId typeId, ushort version) where T : unmanaged, ILinkType
        { Add(SchemaKind.Link, 0, version, typeId, CodecId.Empty, 8, 1, typeof(T), new LinkInvoker<TWorld, T>()); return this; }

        /// <summary>Registers a canonical set of entity relations.</summary>
        public SchemaBuilder<TWorld> Links<T>(TypeId typeId, ushort version, uint maxCount) where T : unmanaged, ILinksType
        { CheckCollectionCount(maxCount); Add(SchemaKind.Links, 0, version, typeId, CodecId.Empty, checked(maxCount * 8), maxCount, typeof(T), new LinksInvoker<TWorld, T>()); return this; }

        /// <summary>Registers an ordered multi-component codec.</summary>
        public SchemaBuilder<TWorld> Multi<T, TCodec>(TypeId typeId, ushort version, CodecId codecId, uint maxCount, uint maxItemBytes)
            where T : unmanaged, IMultiComponent where TCodec : struct, ICodec<T>
        { CheckCollectionCount(maxCount); CheckPayload(maxItemBytes); Add(SchemaKind.Multi, 0, version, typeId, codecId, maxItemBytes, maxCount, typeof(T), new MultiInvoker<TWorld, T, TCodec>(maxItemBytes)); return this; }

        /// <summary>Registers a command codec and typed endpoint authorizer.</summary>
        public SchemaBuilder<TWorld> Command<T, TCodec, TAuthorizer>(TypeId typeId, ushort version, CodecId codecId, uint maxBytes)
            where T : unmanaged where TCodec : struct, ICodec<T> where TAuthorizer : struct, ICommandAuthorizer<TWorld, T>
        { if (maxBytes == 0 || maxBytes > ProtocolLimits.MaxCommandBytes) throw new ArgumentOutOfRangeException(nameof(maxBytes)); Add(SchemaKind.Command, 0, version, typeId, codecId, maxBytes, 1, typeof(T), new CommandInvoker<TWorld, T, TCodec, TAuthorizer>()); return this; }

        /// <summary>Freezes registrations and computes the deterministic canonical schema hash.</summary>
        public Schema Freeze()
        {
            if (_frozen) throw new InvalidOperationException("A schema builder can only be frozen once."); _frozen = true;
            _entries.Sort(static (a, b) => { var kind = ((byte)a.Kind).CompareTo((byte)b.Kind); return kind != 0 ? kind : a.TypeId.CompareTo(b.TypeId); });
            for (var i = 0; i < _entries.Count; i++) if (_entries[i].Invoker == null) throw new InvalidOperationException("Every schema record requires a retained typed invoker.");
            var prefix = Encoding.ASCII.GetBytes("SECS-SCHEMA-V1"); var bytes = new byte[prefix.Length + _entries.Count * 44]; prefix.CopyTo(bytes, 0); var offset = prefix.Length;
            for (var i = 0; i < _entries.Count; i++) { var e = _entries[i]; bytes[offset] = (byte)e.Kind; bytes[offset + 1] = e.Flags; Hashing.Write16(bytes, offset + 2, e.Version); e.TypeId.WriteBytes(bytes.AsSpan(offset + 4, 16)); e.CodecId.WriteBytes(bytes.AsSpan(offset + 20, 16)); Hashing.Write32(bytes, offset + 36, e.MaxPayload); Hashing.Write32(bytes, offset + 40, e.MaxCount); offset += 44; }
            using var sha = SHA256.Create(); var digest = sha.ComputeHash(bytes); return new Schema(TypeId.ReadBytes(digest.AsSpan(0, 16)), _entries.ToArray(), typeof(TWorld));
        }

        private void Add(SchemaKind kind, byte flags, ushort version, TypeId typeId, CodecId codecId, uint maxPayload, uint maxCount, Type type, ISchemaInvoker invoker)
        {
            if (_frozen) throw new InvalidOperationException("The schema is already frozen."); if (typeId == TypeId.Empty) throw new ArgumentException("Stable type identifiers cannot be empty.", nameof(typeId));
            if (type == typeof(ReplicatedTag)) throw new InvalidOperationException("ReplicatedTag is replication control state and cannot be registered as a schema record.");
            if (!_ids.Add(typeId)) throw new InvalidOperationException($"Duplicate schema type id `{typeId}`."); if (!_types.Add(type)) throw new InvalidOperationException($"Runtime type `{type.FullName}` is already registered.");
            _entries.Add(new SchemaEntry(kind, flags, version, typeId, codecId, maxPayload, maxCount, type, invoker));
        }
        private static void CheckPayload(uint value) { if (value == 0 || value > ProtocolLimits.MaxComponentBytes) throw new ArgumentOutOfRangeException(nameof(value)); }
        private static void CheckCollectionCount(uint value) { if (value == 0 || value > MaxCollectionCount) throw new ArgumentOutOfRangeException(nameof(value)); }
    }
}
