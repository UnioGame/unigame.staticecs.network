[assembly: UniGame.StaticEcs.Network.NetworkEndpointAttribute("Server", typeof(Duplicate.ServerWorld), UniGame.StaticEcs.Network.NetworkRole.Server)]
namespace Duplicate
{
    public struct ServerWorld : FFS.Libraries.StaticEcs.IWorldType { }
    public struct FirstPolicy : UniGame.StaticEcs.Network.INetworkCommandPolicy<ServerWorld, Shared.Move> { }
    public struct SecondPolicy : UniGame.StaticEcs.Network.INetworkCommandPolicy<ServerWorld, Shared.Move> { }
}
