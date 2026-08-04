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
    public static class World<T> where T : struct, IWorldType { public struct Entity { } public struct TypeRegistrar { public TypeRegistrar Event<E>() where E : struct, IEvent => this; } }
}

namespace FFS.Libraries.StaticPack { public struct BinaryPackWriter { } public struct BinaryPackReader { } }

namespace UniGame.StaticEcs.Network
{
    public interface INetworkType { }
    public interface INetworkCommand { }
    public enum NetworkRole : byte { Client = 1, Server = 2 }
    public enum NetworkSchemaKind : byte { Entity, Component, Tag, Link, Links, Multi, Command }
    [System.AttributeUsage(System.AttributeTargets.Assembly, AllowMultiple = true)] public sealed class NetworkEndpointAttribute : System.Attribute { public NetworkEndpointAttribute(string name, System.Type world, NetworkRole role) { } }
    [System.AttributeUsage(System.AttributeTargets.Class)] public sealed class NetworkManifestAttribute : System.Attribute { }
    [System.AttributeUsage(System.AttributeTargets.Assembly, AllowMultiple = true)] public sealed class NetworkManifestRecordAttribute : System.Attribute { public NetworkManifestRecordAttribute(uint id, NetworkSchemaKind kind, System.Type type, byte version = 1) { } }
    public readonly struct NetworkTypeId { public NetworkTypeId(uint value) { } }
    public sealed class NetworkSchema<T> where T : struct, FFS.Libraries.StaticEcs.IWorldType { }
    public sealed class NetworkCompilerSchemaFactory<T> where T : struct, FFS.Libraries.StaticEcs.IWorldType
    {
        public void Component<C>(NetworkTypeId id, byte version = 1, uint max = 0) where C : struct, FFS.Libraries.StaticEcs.IComponent, INetworkType { }
        public void DisableableComponent<C>(NetworkTypeId id, byte version = 1, uint max = 0) where C : struct, FFS.Libraries.StaticEcs.IComponent, FFS.Libraries.StaticEcs.IDisableable, INetworkType { }
        public void Command<C>(NetworkTypeId id, byte version = 1, uint max = 0) where C : struct, FFS.Libraries.StaticEcs.IEvent, INetworkCommand { }
        public void Policy<C, P>() where C : struct, FFS.Libraries.StaticEcs.IEvent, INetworkCommand where P : struct, INetworkCommandPolicy<T, C> { }
        public NetworkSchema<T> Freeze() => null;
    }
    public static class NetworkCompilerSupport { public static NetworkCompilerSchemaFactory<T> Create<T>() where T : struct, FFS.Libraries.StaticEcs.IWorldType => null; }
    public struct NetworkCommandAccepted<T> : FFS.Libraries.StaticEcs.IEvent where T : struct, FFS.Libraries.StaticEcs.IEvent, INetworkCommand { }
    public struct NetworkCommandRejected<T> : FFS.Libraries.StaticEcs.IEvent where T : struct, FFS.Libraries.StaticEcs.IEvent, INetworkCommand { }
    public interface INetworkCommandPolicy<TWorld, TCommand> where TWorld : struct, FFS.Libraries.StaticEcs.IWorldType where TCommand : struct, FFS.Libraries.StaticEcs.IEvent, INetworkCommand { }
}

namespace Shared
{
    public struct Position : FFS.Libraries.StaticEcs.IComponent, FFS.Libraries.StaticEcs.IDisableable, UniGame.StaticEcs.Network.INetworkType
    {
        public void Write<TWorld>(ref FFS.Libraries.StaticPack.BinaryPackWriter writer, FFS.Libraries.StaticEcs.World<TWorld>.Entity self) where TWorld : struct, FFS.Libraries.StaticEcs.IWorldType { }
        public void Read<TWorld>(ref FFS.Libraries.StaticPack.BinaryPackReader reader, FFS.Libraries.StaticEcs.World<TWorld>.Entity self, byte version, bool disabled) where TWorld : struct, FFS.Libraries.StaticEcs.IWorldType { }
    }
    public struct Move : FFS.Libraries.StaticEcs.IEvent, UniGame.StaticEcs.Network.INetworkCommand
    {
        public void Write(ref FFS.Libraries.StaticPack.BinaryPackWriter writer) { }
        public void Read(ref FFS.Libraries.StaticPack.BinaryPackReader reader, byte version) { }
    }
}
