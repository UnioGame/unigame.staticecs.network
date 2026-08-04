using System;
using System.Buffers;
using System.Collections.Generic;
using System.ComponentModel;
using System.Security.Cryptography;
using FFS.Libraries.StaticEcs;
using FFS.Libraries.StaticPack;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Identifies a generated manifest record shape.</summary>
    public enum NetworkSchemaKind : byte
    {
        /// <summary>A network entity marker.</summary>
        Entity = 0,
        /// <summary>A component value.</summary>
        Component = 1,
        /// <summary>A zero-size tag.</summary>
        Tag = 2,
        /// <summary>A single entity relation.</summary>
        Link = 3,
        /// <summary>A set of entity relations.</summary>
        Links = 4,
        /// <summary>A multi-component value.</summary>
        Multi = 5,
        /// <summary>A client-to-server command event.</summary>
        Command = 6
    }

    /// <summary>Describes one immutable generated manifest record.</summary>
    public sealed class NetworkSchemaEntry
    {
        internal NetworkSchemaEntry(NetworkSchemaKind kind, NetworkTypeId id, byte version, uint maxBytes, uint maxCount, Type type, object invoker)
        { Kind = kind; TypeId = id; Version = version; MaxBytes = maxBytes; MaxCount = maxCount; RuntimeType = type; Invoker = invoker; }
        /// <summary>Gets the wire shape.</summary>
        public NetworkSchemaKind Kind { get; }
        /// <summary>Gets the generated xxHash32 identifier.</summary>
        public NetworkTypeId TypeId { get; }
        /// <summary>Gets the hook schema version.</summary>
        public byte Version { get; }
        /// <summary>Gets the maximum exact payload bytes.</summary>
        public uint MaxBytes { get; }
        /// <summary>Gets the maximum collection count.</summary>
        public uint MaxCount { get; }
        /// <summary>Gets the retained diagnostic type.</summary>
        public Type RuntimeType { get; }
        internal object Invoker { get; }
    }

    /// <summary>Contains an immutable generated schema closed on one Static ECS world.</summary>
    public sealed class NetworkSchema<TWorld> where TWorld : struct, IWorldType
    {
        private readonly NetworkSchemaEntry[] _entries;
        internal NetworkSchema(SchemaFingerprint fingerprint, NetworkSchemaEntry[] entries) { Fingerprint = fingerprint; _entries = entries; }
        /// <summary>Gets the first 128 bits of SHA-256 over the canonical sorted manifest.</summary>
        public SchemaFingerprint Fingerprint { get; }
        /// <summary>Gets canonical records ordered by kind and identifier.</summary>
        public IReadOnlyList<NetworkSchemaEntry> Entries => _entries;
        /// <summary>Finds one generated manifest record.</summary>
        public bool TryGet(NetworkTypeId id, out NetworkSchemaEntry entry)
        {
            for (var i = 0; i < _entries.Length; i++) if (_entries[i].TypeId == id) { entry = _entries[i]; return true; }
            entry = null;
            return false;
        }
        internal ReadOnlySpan<NetworkSchemaEntry> RetainedEntries => _entries;
    }

    /// <summary>Compiler-only entry point used by generated endpoint code.</summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public static class NetworkCompilerSupport
    {
        /// <summary>Creates a compiler-owned schema factory.</summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public static NetworkCompilerSchemaFactory<TWorld> Create<TWorld>() where TWorld : struct, IWorldType => new NetworkCompilerSchemaFactory<TWorld>();

        /// <summary>Computes the required non-zero xxHash32 wire identifier.</summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public static NetworkTypeId TypeId(string assemblyName, string metadataName)
        {
            if (string.IsNullOrEmpty(assemblyName)) throw new ArgumentException("Assembly name is required.", nameof(assemblyName));
            if (string.IsNullOrEmpty(metadataName)) throw new ArgumentException("Metadata name is required.", nameof(metadataName));
            var bytes = System.Text.Encoding.UTF8.GetBytes(assemblyName + ":" + metadataName);
            var hash = Hashing.XxHash32(bytes);
            if (hash == 0) throw new InvalidOperationException("The generated network type id is zero.");
            return new NetworkTypeId(hash);
        }

        /// <summary>Reads a component's declared Static ECS serialization version.</summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public static byte ComponentVersion<T>() where T : struct, FFS.Libraries.StaticEcs.IComponent, IComponentConfig<T>
            => default(T).Config().Version ?? 0;

        /// <summary>Reads an event's declared Static ECS serialization version.</summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public static byte EventVersion<T>() where T : struct, IEvent, IEventConfig<T>
            => default(T).Config().Version ?? 0;
    }

    /// <summary>Compiler-facing mutable factory; gameplay code uses generated endpoint classes.</summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public sealed class NetworkCompilerSchemaFactory<TWorld> where TWorld : struct, IWorldType
    {
        private readonly List<NetworkSchemaEntry> _entries = new List<NetworkSchemaEntry>();
        private readonly HashSet<uint> _ids = new HashSet<uint>();
        private bool _frozen;

        /// <summary>Adds a generated entity type.</summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Entity<TEntity>(NetworkTypeId id) where TEntity : struct, IEntityType, INetworkType => Add(NetworkSchemaKind.Entity, id, 0, 0, 0, typeof(TEntity), new EntityNetworkInvoker<TWorld, TEntity>());
        /// <summary>Adds a generated component type.</summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Component<T>(NetworkTypeId id, byte version = 0, uint maxBytes = ProtocolLimits.MaxComponentBytes) where T : struct, FFS.Libraries.StaticEcs.IComponent, INetworkType => Add(NetworkSchemaKind.Component, id, version, maxBytes, 1, typeof(T), new ComponentNetworkInvoker<TWorld, T>());
        /// <summary>Adds a generated disableable component type.</summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void DisableableComponent<T>(NetworkTypeId id, byte version = 0, uint maxBytes = ProtocolLimits.MaxComponentBytes) where T : struct, FFS.Libraries.StaticEcs.IComponent, IDisableable, INetworkType => Add(NetworkSchemaKind.Component, id, version, maxBytes, 1, typeof(T), new DisableableComponentNetworkInvoker<TWorld, T>());
        /// <summary>Adds a generated tag type.</summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Tag<T>(NetworkTypeId id, byte version = 0) where T : struct, ITag, INetworkType => Add(NetworkSchemaKind.Tag, id, version, 0, 0, typeof(T), new TagNetworkInvoker<TWorld, T>());
        /// <summary>Adds a generated link marker.</summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Link<T>(NetworkTypeId id, byte version = 0) where T : unmanaged, ILinkType, INetworkType => Add(NetworkSchemaKind.Link, id, version, 8, 1, typeof(T), new DisableableComponentNetworkInvoker<TWorld, World<TWorld>.Link<T>>());
        /// <summary>Adds a generated link-set marker.</summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Links<T>(NetworkTypeId id, byte version = 0, uint maxCount = 32768) where T : unmanaged, ILinksType, INetworkType => Add(NetworkSchemaKind.Links, id, version, checked(maxCount * 8), maxCount, typeof(T), new DisableableComponentNetworkInvoker<TWorld, World<TWorld>.Links<T>>());
        /// <summary>Adds a generated multi-component value.</summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Multi<T>(NetworkTypeId id, byte version = 0, uint maxBytes = ProtocolLimits.MaxComponentBytes, uint maxCount = 32768) where T : struct, IMultiComponent, INetworkType => Add(NetworkSchemaKind.Multi, id, version, maxBytes, maxCount, typeof(T), new DisableableComponentNetworkInvoker<TWorld, World<TWorld>.Multi<T>>());
        /// <summary>Adds a generated client command without a server policy.</summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Command<T>(NetworkTypeId id, byte version = 0, uint maxBytes = ProtocolLimits.MaxCommandBytes) where T : struct, IEvent, INetworkCommand => Add(NetworkSchemaKind.Command, id, version, maxBytes, 1, typeof(T), new CommandNetworkInvoker<TWorld, T>());
        /// <summary>Adds a generated server command policy.</summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Command<T, TPolicy>(NetworkTypeId id, byte version = 0, uint maxBytes = ProtocolLimits.MaxCommandBytes)
            where T : struct, IEvent, INetworkCommand where TPolicy : struct, INetworkCommandPolicy<TWorld, T> =>
            Add(NetworkSchemaKind.Command, id, version, maxBytes, 1, typeof(T), new CommandNetworkInvoker<TWorld, T, TPolicy>());

        /// <summary>Binds a server-only policy to an existing Shared command record.</summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Policy<T, TPolicy>()
            where T : struct, IEvent, INetworkCommand where TPolicy : struct, INetworkCommandPolicy<TWorld, T>
        {
            if (_frozen) throw new InvalidOperationException("The compiler schema factory is frozen.");
            for (var i = 0; i < _entries.Count; i++)
            {
                var entry = _entries[i];
                if (entry.Kind != NetworkSchemaKind.Command || entry.RuntimeType != typeof(T)) continue;
                _entries[i] = new NetworkSchemaEntry(entry.Kind, entry.TypeId, entry.Version, entry.MaxBytes, entry.MaxCount, entry.RuntimeType, new CommandNetworkInvoker<TWorld, T, TPolicy>());
                return;
            }
            throw new InvalidOperationException($"Command `{typeof(T).FullName}` is absent from aggregated Shared manifests.");
        }

        /// <summary>Freezes and defensively validates the generated schema.</summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public NetworkSchema<TWorld> Freeze()
        {
            if (_frozen) throw new InvalidOperationException("The compiler schema factory is already frozen.");
            _frozen = true;
            _entries.Sort((a, b) => { var kind = a.Kind.CompareTo(b.Kind); return kind != 0 ? kind : a.TypeId.CompareTo(b.TypeId); });
            var canonical = new byte[11 + _entries.Count * 14];
            System.Text.Encoding.ASCII.GetBytes("SECS-NET-V2").CopyTo(canonical, 0);
            var offset = 11;
            for (var i = 0; i < _entries.Count; i++)
            {
                var entry = _entries[i];
                canonical[offset++] = (byte)entry.Kind;
                canonical[offset++] = entry.Version;
                Hashing.Write32(canonical, offset, entry.TypeId.Value); offset += 4;
                Hashing.Write32(canonical, offset, entry.MaxBytes); offset += 4;
                Hashing.Write32(canonical, offset, entry.MaxCount); offset += 4;
            }
            using var sha = SHA256.Create();
            var digest = sha.ComputeHash(canonical);
            return new NetworkSchema<TWorld>(SchemaFingerprint.ReadBytes(digest), _entries.ToArray());
        }

        private void Add(NetworkSchemaKind kind, NetworkTypeId id, byte version, uint maxBytes, uint maxCount, Type type, object invoker)
        {
            if (_frozen) throw new InvalidOperationException("The compiler schema factory is frozen.");
            if (type == typeof(NetworkTag)) throw new InvalidOperationException("NetworkTag is control state, not a schema record.");
            if (!_ids.Add(id.Value)) throw new InvalidOperationException($"Duplicate network type id `{id}`.");
            _entries.Add(new NetworkSchemaEntry(kind, id, version, maxBytes, maxCount, type, invoker));
        }
    }

    internal interface IEntityNetworkInvoker<TWorld> where TWorld : struct, IWorldType
    {
        bool Matches(World<TWorld>.Entity entity);
        World<TWorld>.Entity Create(EntityGID gid);
    }

    internal interface IRecordNetworkInvoker<TWorld> where TWorld : struct, IWorldType
    {
        bool SupportsDisabled { get; }
        bool Has(World<TWorld>.Entity entity);
        bool IsDisabled(World<TWorld>.Entity entity);
        byte[] Capture(World<TWorld>.Entity entity, uint maxBytes);
        void Apply(World<TWorld>.Entity entity, byte[] payload, byte version, bool disabled);
        void Remove(World<TWorld>.Entity entity);
    }

    internal sealed class EntityNetworkInvoker<TWorld, TEntity> : IEntityNetworkInvoker<TWorld>
        where TWorld : struct, IWorldType where TEntity : struct, IEntityType
    {
        public bool Matches(World<TWorld>.Entity entity) => entity.EntityType == default(TEntity).Id();
        public World<TWorld>.Entity Create(EntityGID gid) => World<TWorld>.NewEntityByGID<TEntity>(gid);
    }

    internal class ComponentNetworkInvoker<TWorld, T> : IRecordNetworkInvoker<TWorld>
        where TWorld : struct, IWorldType where T : struct, FFS.Libraries.StaticEcs.IComponent
    {
        public virtual bool SupportsDisabled => false;
        public bool Has(World<TWorld>.Entity entity) => entity.Has<T>();
        public virtual bool IsDisabled(World<TWorld>.Entity entity) => false;
        public byte[] Capture(World<TWorld>.Entity entity, uint maxBytes)
        {
            var writer = BinaryPackWriter.CreateFromPool(Math.Min(maxBytes, 512));
            try
            {
                var value = entity.Read<T>();
                value.Write<TWorld>(ref writer, entity);
                if (writer.Position > maxBytes) throw new InvalidOperationException("Static ECS hook exceeded its generated protocol limit.");
                return writer.CopyToBytes();
            }
            finally { writer.Dispose(); }
        }
        public virtual void Apply(World<TWorld>.Entity entity, byte[] payload, byte version, bool disabled)
        {
            var reader = new BinaryPackReader(payload, (uint)payload.Length, 0);
            var value = default(T);
            value.Read<TWorld>(ref reader, entity, version, disabled);
            if (reader.Position != payload.Length) throw new InvalidOperationException("Static ECS hook did not consume the exact payload.");
            entity.Set(value);
            if (disabled) throw new InvalidOperationException("A non-disableable component cannot carry disabled state.");
        }
        public void Remove(World<TWorld>.Entity entity) { if (entity.Has<T>()) entity.Delete<T>(); }
    }

    internal sealed class DisableableComponentNetworkInvoker<TWorld, T> : ComponentNetworkInvoker<TWorld, T>
        where TWorld : struct, IWorldType where T : struct, FFS.Libraries.StaticEcs.IComponent, IDisableable
    {
        public override bool SupportsDisabled => true;
        public override bool IsDisabled(World<TWorld>.Entity entity) => entity.HasDisabled<T>();
        public override void Apply(World<TWorld>.Entity entity, byte[] payload, byte version, bool disabled)
        {
            base.Apply(entity, payload, version, false);
            if (disabled) entity.Disable<T>(); else entity.Enable<T>();
        }
    }

    internal sealed class TagNetworkInvoker<TWorld, T> : IRecordNetworkInvoker<TWorld>
        where TWorld : struct, IWorldType where T : struct, ITag
    {
        public bool SupportsDisabled => false;
        public bool Has(World<TWorld>.Entity entity) => entity.Has<T>();
        public bool IsDisabled(World<TWorld>.Entity entity) => false;
        public byte[] Capture(World<TWorld>.Entity entity, uint maxBytes) => Array.Empty<byte>();
        public void Apply(World<TWorld>.Entity entity, byte[] payload, byte version, bool disabled) { if (payload.Length != 0 || disabled) throw new InvalidOperationException("Tags have no payload or disabled state."); entity.Set<T>(); }
        public void Remove(World<TWorld>.Entity entity) { if (entity.Has<T>()) entity.Delete<T>(); }
    }

    internal interface ICommandNetworkInvoker<TWorld> where TWorld : struct, IWorldType
    {
        bool HasPolicy { get; }
        byte[] Capture(object command, uint maxBytes);
        NetworkCommandResult Dispatch(byte[] payload, byte version, in NetworkCommandContext context);
    }

    internal class CommandNetworkInvoker<TWorld, T> : ICommandNetworkInvoker<TWorld>
        where TWorld : struct, IWorldType where T : struct, IEvent, INetworkCommand
    {
        public virtual bool HasPolicy => false;
        public byte[] Capture(object command, uint maxBytes)
        {
            var value = (T)command;
            var writer = BinaryPackWriter.CreateFromPool(Math.Min(maxBytes, 256));
            try { value.Write(ref writer); if (writer.Position > maxBytes) throw new InvalidOperationException("Command hook exceeded its generated protocol limit."); return writer.CopyToBytes(); }
            finally { writer.Dispose(); }
        }
        public virtual NetworkCommandResult Dispatch(byte[] payload, byte version, in NetworkCommandContext context) => NetworkCommandResult.SchemaMismatch;
        protected static T Read(byte[] payload, byte version)
        {
            var reader = new BinaryPackReader(payload, (uint)payload.Length, 0);
            var value = default(T);
            value.Read(ref reader, version);
            if (reader.Position != payload.Length) throw new InvalidOperationException("Command hook did not consume the exact payload.");
            return value;
        }
    }

    internal sealed class CommandNetworkInvoker<TWorld, T, TPolicy> : CommandNetworkInvoker<TWorld, T>
        where TWorld : struct, IWorldType where T : struct, IEvent, INetworkCommand where TPolicy : struct, INetworkCommandPolicy<TWorld, T>
    {
        public override bool HasPolicy => true;
        public override NetworkCommandResult Dispatch(byte[] payload, byte version, in NetworkCommandContext context)
        {
            var command = Read(payload, version);
            var accepted = default(TPolicy).Authorize(in context, in command);
            if (accepted) World<TWorld>.SendEvent(new NetworkCommandAccepted<T> { Command = command, Context = context });
            else World<TWorld>.SendEvent(new NetworkCommandRejected<T> { Command = command, Context = context });
            return accepted ? NetworkCommandResult.Dispatched : NetworkCommandResult.PolicyRejected;
        }
    }
}
