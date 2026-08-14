namespace UniGame.StaticEcs.Network
{
    using FFS.Libraries.StaticEcs;

    /// <summary>Identifies one local ECS entity that represents a transport connection.</summary>
    public struct NetworkConnectionEntity : IEntityType
    {
        /// <inheritdoc />
        public byte Id() => byte.MaxValue;
    }

    /// <summary>Stores queryable connection identity and admission state.</summary>
    public struct NetworkConnectionComponent : IComponent
    {
        /// <summary>Transport-owned connection identifier.</summary>
        public ConnectionId Connection;
        /// <summary>Endpoint role represented by this entity.</summary>
        public NetworkRole Role;
        /// <summary>Current protocol session state.</summary>
        public NetworkSessionState State;
        /// <summary>Trusted server-assigned peer identifier.</summary>
        public uint PeerId;
        /// <summary>Trusted server-assigned session epoch.</summary>
        public uint Epoch;
        /// <summary>Server-assigned replication scope.</summary>
        public ScopeId Scope;
    }

    /// <summary>Stores queryable authoritative and acknowledgement tick cursors.</summary>
    public struct NetworkConnectionTickComponent : IComponent
    {
        /// <summary>Latest validated authoritative server tick.</summary>
        public uint ServerTick;
        /// <summary>Estimated authoritative tick including prediction lead.</summary>
        public uint EstimatedServerTick;
        /// <summary>Latest successfully applied snapshot tick.</summary>
        public uint AcknowledgedSnapshotTick;
        /// <summary>Latest command tick processed into authoritative state.</summary>
        public uint ServerProcessedCommandTick;
        /// <summary>Latest command sequence processed into authoritative state.</summary>
        public uint ServerProcessedCommandSequence;
    }

    /// <summary>Stores queryable clock-synchronization values for one connection.</summary>
    public struct NetworkConnectionClockComponent : IComponent
    {
        /// <summary>Smoothed round-trip duration in seconds.</summary>
        public double RoundTripSeconds;
        /// <summary>Timestamp associated with the latest validated server tick.</summary>
        public long LastServerTickTimestamp;
        /// <summary>Timestamp associated with the latest ping sample.</summary>
        public long LastPingTimestamp;
    }

    /// <summary>Marks the single local client connection in a replica world.</summary>
    public struct LocalNetworkConnectionTag : ITag
    {
    }

    /// <summary>Marks an admitted connection that may participate in gameplay.</summary>
    public struct NetworkEstablishedTag : ITag
    {
    }

    /// <summary>Marks a replica owned by the observer represented by the local connection.</summary>
    public struct LocalNetworkOwnerTag : ITag
    {
    }

    /// <summary>Contains a read-only endpoint view used to project service state into ECS.</summary>
    public struct NetworkConnectionSnapshot
    {
        /// <summary>Connection identity and session state.</summary>
        public NetworkConnectionComponent Connection;
        /// <summary>Tick and acknowledgement cursors.</summary>
        public NetworkConnectionTickComponent Ticks;
        /// <summary>Clock synchronization state.</summary>
        public NetworkConnectionClockComponent Clock;
    }
}
