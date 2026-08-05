using FFS.Libraries.StaticEcs;

namespace UniGame.StaticEcs.Network.GeneratedTests.Shared
{
    /// <summary>
    /// Identifies the entity used by the generated owner-replication fixture.
    /// </summary>
    public struct GeneratedOwnerEntity : IEntityType, INetworkType
    {
        /// <summary>
        /// Returns the stable network entity type identifier.
        /// </summary>
        public byte Id() => 1;
    }
}
