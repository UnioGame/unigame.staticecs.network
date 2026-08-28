[assembly: UniGame.StaticEcs.Network.NetworkEndpointAttribute("Client", typeof(Demo.ClientWorld), UniGame.StaticEcs.Network.NetworkRole.Client, typeof(Shared.Move))]
[assembly: UniGame.StaticEcs.Network.NetworkEndpointAttribute("Server", typeof(Demo.ServerWorld), UniGame.StaticEcs.Network.NetworkRole.Server, typeof(Shared.Move))]

namespace Demo
{
    public struct ClientWorld : FFS.Libraries.StaticEcs.IWorldType { }
    public struct ServerWorld : FFS.Libraries.StaticEcs.IWorldType { }
    public struct MovePolicy : UniGame.StaticEcs.Network.INetworkCommandPolicy<ServerWorld, Shared.Move> { }
    public struct PingPolicy : UniGame.StaticEcs.Network.INetworkCommandPolicy<ServerWorld, Shared.Ping> { }
}
