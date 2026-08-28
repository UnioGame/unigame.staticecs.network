[assembly: UniGame.StaticEcs.Network.NetworkEndpointAttribute("Server", typeof(Missing.ServerWorld), UniGame.StaticEcs.Network.NetworkRole.Server, typeof(Shared.Move))]
namespace Missing { public struct ServerWorld : FFS.Libraries.StaticEcs.IWorldType { } }
