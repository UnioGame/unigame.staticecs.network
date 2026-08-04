[assembly: UniGame.StaticEcs.Network.NetworkEndpoint("Local", typeof(InvalidSharedOnly.LocalWorld), UniGame.StaticEcs.Network.NetworkRole.Client)]
namespace InvalidSharedOnly
{
    public struct LocalWorld : FFS.Libraries.StaticEcs.IWorldType { }
    public struct LocalWireType : FFS.Libraries.StaticEcs.ITag, UniGame.StaticEcs.Network.INetworkType { }
}
