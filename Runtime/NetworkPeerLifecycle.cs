namespace UniGame.StaticEcs.Network
{
    /// <summary>Contains server-trusted identity assigned to one admitted connection.</summary>
    public struct NetworkPeerData
    {
        /// <summary>Transport-owned connection identifier.</summary>
        public ConnectionId Connection;

        /// <summary>Server-assigned peer identifier.</summary>
        public uint PeerId;

        /// <summary>Server-assigned session epoch.</summary>
        public uint Epoch;

        /// <summary>Replication scope assigned by the server.</summary>
        public ScopeId Scope;
    }

    /// <summary>Receives exact-once server admission and disconnect lifecycle callbacks.</summary>
    public interface INetworkPeerObserver
    {
        /// <summary>Called after a connection becomes established.</summary>
        void Admitted(in NetworkPeerData peer);

        /// <summary>Called before an established connection is removed.</summary>
        void Disconnected(in NetworkPeerData peer);
    }
}
