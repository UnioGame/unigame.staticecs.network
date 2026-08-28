[assembly: UniGame.StaticEcs.Network.NetworkEndpoint("Server", typeof(InvalidManifest.ServerWorld), UniGame.StaticEcs.Network.NetworkRole.Server, typeof(Shared.Move))]
namespace InvalidManifest
{
    public struct ServerWorld : FFS.Libraries.StaticEcs.IWorldType { }
    public struct MovePolicy : UniGame.StaticEcs.Network.INetworkCommandPolicy<ServerWorld, Shared.Move> { }
    public struct PingPolicy : UniGame.StaticEcs.Network.INetworkCommandPolicy<ServerWorld, Shared.Ping> { }
}
