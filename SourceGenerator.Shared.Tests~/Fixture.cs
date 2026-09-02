namespace FFS.Libraries.StaticEcs
{
    public interface IWorldType { }
    public interface IComponent { }
    public interface IDisableable { }
    public interface ITag { }
    public interface IEvent { }
    public interface IEntityType { }
    public interface ILinkType { }
    public interface ILinksType : ILinkType { }
    public interface IMultiComponent { }
    public readonly struct ComponentTypeConfig<T> where T : struct, IComponent { public readonly byte? Version; public ComponentTypeConfig(byte? version = null) { Version = version; } }
    public readonly struct EventTypeConfig<T> where T : struct, IEvent { public readonly byte? Version; public EventTypeConfig(byte? version = null) { Version = version; } }
    public interface IComponentConfig<T> where T : struct, IComponent { ComponentTypeConfig<T> Config(); }
    public interface IEventConfig<T> where T : struct, IEvent { EventTypeConfig<T> Config(); }
    public static class World<T> where T : struct, IWorldType { public struct Entity { } public struct TypeRegistrar { public TypeRegistrar Event<E>() where E : struct, IEvent => this; } }
}

namespace FFS.Libraries.StaticPack { public struct BinaryPackWriter { } public struct BinaryPackReader { } }

namespace UniGame.StaticEcs.Network
{
    public interface INetworkType { }
    public interface INetworkCommand { }
    public struct NetworkOwnerComponent : FFS.Libraries.StaticEcs.IComponent, INetworkType
    {
        public uint PeerId;
        public void Write<TWorld>(ref FFS.Libraries.StaticPack.BinaryPackWriter writer, FFS.Libraries.StaticEcs.World<TWorld>.Entity self) where TWorld : struct, FFS.Libraries.StaticEcs.IWorldType { }
        public void Read<TWorld>(ref FFS.Libraries.StaticPack.BinaryPackReader reader, FFS.Libraries.StaticEcs.World<TWorld>.Entity self, byte version, bool disabled) where TWorld : struct, FFS.Libraries.StaticEcs.IWorldType { }
    }
    public enum NetworkRole : byte { Client = 1, Server = 2 }
    public enum NetworkSchemaKind : byte { Entity, Component, Tag, Link, Links, Multi, Command }
    [System.AttributeUsage(System.AttributeTargets.Assembly, AllowMultiple = true)] public sealed class NetworkEndpointAttribute : System.Attribute { public NetworkEndpointAttribute(string name, System.Type world, NetworkRole role, params System.Type[] rootTypes) { } }
    [System.AttributeUsage(System.AttributeTargets.Class)] public sealed class NetworkManifestAttribute : System.Attribute { }
    [System.AttributeUsage(System.AttributeTargets.Assembly, AllowMultiple = true)] public sealed class NetworkManifestRecordAttribute : System.Attribute { public NetworkManifestRecordAttribute(uint id, NetworkSchemaKind kind, System.Type type, byte version = 1) { } }
    [System.AttributeUsage(System.AttributeTargets.Assembly, AllowMultiple = true)] public sealed class NetworkCommandPolicyManifestRecordAttribute : System.Attribute { public NetworkCommandPolicyManifestRecordAttribute(System.Type world, System.Type command, System.Type policy) { } }
    public readonly struct NetworkTypeId { public NetworkTypeId(uint value) { Value = value; } public uint Value { get; } }
    public sealed class NetworkSchema<T> where T : struct, FFS.Libraries.StaticEcs.IWorldType { public NetworkSchema(string fingerprint, byte[] versions) { Fingerprint = fingerprint; Versions = versions; } public string Fingerprint { get; } public byte[] Versions { get; } }
    public sealed class NetworkCompilerSchemaFactory<T> where T : struct, FFS.Libraries.StaticEcs.IWorldType
    {
        private readonly System.Collections.Generic.List<(uint Id, byte Version)> _entries = new System.Collections.Generic.List<(uint, byte)>();
        public void Entity<E>(NetworkTypeId id) where E : struct, FFS.Libraries.StaticEcs.IEntityType, INetworkType { }
        public void Component<C>(NetworkTypeId id, byte version = 1, uint max = 0) where C : struct, FFS.Libraries.StaticEcs.IComponent, INetworkType => _entries.Add((id.Value, version));
        public void DisableableComponent<C>(NetworkTypeId id, byte version = 1, uint max = 0) where C : struct, FFS.Libraries.StaticEcs.IComponent, FFS.Libraries.StaticEcs.IDisableable, INetworkType => _entries.Add((id.Value, version));
        public void Command<C>(NetworkTypeId id, byte version = 1, uint max = 0) where C : struct, FFS.Libraries.StaticEcs.IEvent, INetworkCommand => _entries.Add((id.Value, version));
        public void Policy<C, P>() where C : struct, FFS.Libraries.StaticEcs.IEvent, INetworkCommand where P : struct, INetworkCommandPolicy<T, C> { }
        public NetworkSchema<T> Freeze() { _entries.Sort((left, right) => left.Id.CompareTo(right.Id)); var versions = _entries.ConvertAll(value => value.Version).ToArray(); return new NetworkSchema<T>(string.Join(";", _entries), versions); }
    }
    public static class NetworkCompilerSupport { public static NetworkCompilerSchemaFactory<T> Create<T>() where T : struct, FFS.Libraries.StaticEcs.IWorldType => new NetworkCompilerSchemaFactory<T>(); public static byte ComponentVersion<T>() where T : struct, FFS.Libraries.StaticEcs.IComponent, FFS.Libraries.StaticEcs.IComponentConfig<T> => default(T).Config().Version ?? 0; public static byte EventVersion<T>() where T : struct, FFS.Libraries.StaticEcs.IEvent, FFS.Libraries.StaticEcs.IEventConfig<T> => default(T).Config().Version ?? 0; }
    public struct NetworkCommandAcceptedEvent<T> : FFS.Libraries.StaticEcs.IEvent where T : struct, FFS.Libraries.StaticEcs.IEvent, INetworkCommand { }
    public struct NetworkCommandRejectedEvent<T> : FFS.Libraries.StaticEcs.IEvent where T : struct, FFS.Libraries.StaticEcs.IEvent, INetworkCommand { }
    public interface INetworkCommandPolicy<TWorld, TCommand> where TWorld : struct, FFS.Libraries.StaticEcs.IWorldType where TCommand : struct, FFS.Libraries.StaticEcs.IEvent, INetworkCommand { }
}

namespace Shared
{
    public struct ClientWorld : FFS.Libraries.StaticEcs.IWorldType { }
    public struct ServerWorld : FFS.Libraries.StaticEcs.IWorldType { }
    public struct PlayerEntity : FFS.Libraries.StaticEcs.IEntityType, UniGame.StaticEcs.Network.INetworkType { }
    public struct NpcEntity : FFS.Libraries.StaticEcs.IEntityType, UniGame.StaticEcs.Network.INetworkType { }
    public struct Position : FFS.Libraries.StaticEcs.IComponent, FFS.Libraries.StaticEcs.IDisableable, FFS.Libraries.StaticEcs.IComponentConfig<Position>, UniGame.StaticEcs.Network.INetworkType
    {
        public FFS.Libraries.StaticEcs.ComponentTypeConfig<Position> Config() => new FFS.Libraries.StaticEcs.ComponentTypeConfig<Position>(Version);
#if NETWORK_VERSION_MISMATCH
        private const byte Version = 8;
#else
        private const byte Version = 7;
#endif
        public void Write<TWorld>(ref FFS.Libraries.StaticPack.BinaryPackWriter writer, FFS.Libraries.StaticEcs.World<TWorld>.Entity self) where TWorld : struct, FFS.Libraries.StaticEcs.IWorldType { }
        public void Read<TWorld>(ref FFS.Libraries.StaticPack.BinaryPackReader reader, FFS.Libraries.StaticEcs.World<TWorld>.Entity self, byte version, bool disabled) where TWorld : struct, FFS.Libraries.StaticEcs.IWorldType { }
    }
    public struct Move : FFS.Libraries.StaticEcs.IEvent, FFS.Libraries.StaticEcs.IEventConfig<Move>, UniGame.StaticEcs.Network.INetworkCommand
    {
        public FFS.Libraries.StaticEcs.EventTypeConfig<Move> Config() => new FFS.Libraries.StaticEcs.EventTypeConfig<Move>(9);
        public void Write(ref FFS.Libraries.StaticPack.BinaryPackWriter writer) { }
        public void Read(ref FFS.Libraries.StaticPack.BinaryPackReader reader, byte version) { }
    }
    public struct Health : FFS.Libraries.StaticEcs.IComponent, UniGame.StaticEcs.Network.INetworkType
    {
        public void Write<TWorld>(ref FFS.Libraries.StaticPack.BinaryPackWriter writer, FFS.Libraries.StaticEcs.World<TWorld>.Entity self) where TWorld : struct, FFS.Libraries.StaticEcs.IWorldType { }
        public void Read<TWorld>(ref FFS.Libraries.StaticPack.BinaryPackReader reader, FFS.Libraries.StaticEcs.World<TWorld>.Entity self, byte version, bool disabled) where TWorld : struct, FFS.Libraries.StaticEcs.IWorldType { }
    }
    public struct Ping : FFS.Libraries.StaticEcs.IEvent, UniGame.StaticEcs.Network.INetworkCommand
    {
        public void Write(ref FFS.Libraries.StaticPack.BinaryPackWriter writer) { }
        public void Read(ref FFS.Libraries.StaticPack.BinaryPackReader reader, byte version) { }
    }
    public struct MovePolicy : UniGame.StaticEcs.Network.INetworkCommandPolicy<ServerWorld, Move> { }
    public struct PingPolicy : UniGame.StaticEcs.Network.INetworkCommandPolicy<ServerWorld, Ping> { }
}
