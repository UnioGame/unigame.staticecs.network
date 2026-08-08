namespace UniGame.StaticEcs.Network
{
    /// <summary>Describes why a trusted server admission policy rejected one peer.</summary>
    public enum NetworkAdmissionRejection : byte
    {
        /// <summary>No rejection occurred.</summary>
        None,

        /// <summary>The peer was rejected by gameplay policy.</summary>
        Rejected,

        /// <summary>No gameplay capacity was available.</summary>
        Capacity,

        /// <summary>The server-owned gameplay entity could not be created.</summary>
        SpawnFailed,

        /// <summary>The admission policy failed unexpectedly.</summary>
        PolicyError
    }

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

    /// <summary>Atomically prepares trusted gameplay state before a network session becomes established.</summary>
    public interface INetworkPeerAdmissionPolicy
    {
        /// <summary>Prepares peer gameplay state or returns a stable rejection reason.</summary>
        bool TryAdmit(
            in NetworkPeerData peer,
            out NetworkAdmissionRejection reason);

        /// <summary>Rolls back prepared state when the protocol cannot commit admission.</summary>
        void Rollback(in NetworkPeerData peer);
    }
}
