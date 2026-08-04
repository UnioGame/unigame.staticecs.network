[assembly: UniGame.StaticEcs.Network.NetworkEndpointAttribute("Client", typeof(Demo.ClientWorld), UniGame.StaticEcs.Network.NetworkRole.Client)]
[assembly: UniGame.StaticEcs.Network.NetworkEndpointAttribute("Server", typeof(Demo.ServerWorld), UniGame.StaticEcs.Network.NetworkRole.Server)]

namespace FFS.Libraries.StaticEcs
{
    public interface IWorldType { }
    public interface IComponent { }
    public interface ITag { }
    public interface IEvent { }
    public interface IEntityType { }
    public interface ILinkType { }
    public interface ILinksType : ILinkType { }
    public interface IMultiComponent { }
    public static class World<T> where T : struct, IWorldType
    {
        public struct TypeRegistrar { public TypeRegistrar Event<E>() where E : struct, IEvent => this; }
    }
}

namespace UniGame.StaticEcs.Network
{
    public interface INetworkType { }
    public interface INetworkCommand { }
    public enum NetworkRole : byte { Client = 1, Server = 2 }
    public enum NetworkSchemaKind : byte { Entity, Component, Tag, Link, Links, Multi, Command }
    [System.AttributeUsage(System.AttributeTargets.Assembly, AllowMultiple = true)]
    public sealed class NetworkEndpointAttribute : System.Attribute { public NetworkEndpointAttribute(string name, System.Type world, NetworkRole role) { } }
    [System.AttributeUsage(System.AttributeTargets.Class)] public sealed class NetworkManifestAttribute : System.Attribute { }
    [System.AttributeUsage(System.AttributeTargets.Assembly, AllowMultiple = true)]
    public sealed class NetworkManifestRecordAttribute : System.Attribute { public NetworkManifestRecordAttribute(uint id, NetworkSchemaKind kind, System.Type type, byte version = 1) { } }
    public readonly struct NetworkTypeId { public NetworkTypeId(uint value) { } }
    public sealed class NetworkSchema<T> where T : struct, FFS.Libraries.StaticEcs.IWorldType { }
    public sealed class NetworkCompilerSchemaFactory<T> where T : struct, FFS.Libraries.StaticEcs.IWorldType
    {
        public void Component<C>(NetworkTypeId id, byte version = 1, uint max = 0) where C : struct, FFS.Libraries.StaticEcs.IComponent, INetworkType { }
        public void Command<C>(NetworkTypeId id, byte version = 1, uint max = 0) where C : struct, FFS.Libraries.StaticEcs.IEvent, INetworkCommand { }
        public void Policy<C, P>() where C : struct, FFS.Libraries.StaticEcs.IEvent, INetworkCommand where P : struct, INetworkCommandPolicy<T, C> { }
        public NetworkSchema<T> Freeze() => null;
    }
    public static class NetworkCompilerSupport { public static NetworkCompilerSchemaFactory<T> Create<T>() where T : struct, FFS.Libraries.StaticEcs.IWorldType => null; }
    public struct NetworkCommandAccepted<T> : FFS.Libraries.StaticEcs.IEvent where T : struct, FFS.Libraries.StaticEcs.IEvent, INetworkCommand { }
    public struct NetworkCommandRejected<T> : FFS.Libraries.StaticEcs.IEvent where T : struct, FFS.Libraries.StaticEcs.IEvent, INetworkCommand { }
    public interface INetworkCommandPolicy<TWorld, TCommand> where TWorld : struct, FFS.Libraries.StaticEcs.IWorldType where TCommand : struct, FFS.Libraries.StaticEcs.IEvent, INetworkCommand { }
}

namespace Demo
{
    public struct ClientWorld : FFS.Libraries.StaticEcs.IWorldType { }
    public struct ServerWorld : FFS.Libraries.StaticEcs.IWorldType { }
    public struct Position : FFS.Libraries.StaticEcs.IComponent, UniGame.StaticEcs.Network.INetworkType { }
    public struct Move : FFS.Libraries.StaticEcs.IEvent, UniGame.StaticEcs.Network.INetworkCommand { }
    public struct MovePolicy : UniGame.StaticEcs.Network.INetworkCommandPolicy<ServerWorld, Move> { }
}
