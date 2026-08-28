namespace UniGame.StaticEcs.Network
{
    using System;
    using FFS.Libraries.StaticEcs;

    /// <summary>Identifies the client recovery phase projected into ECS.</summary>
    public enum NetworkRecoveryPhase : byte
    {
        /// <summary>No recovery is pending.</summary>
        None,
        /// <summary>The client is waiting for an authoritative keyframe.</summary>
        AwaitingKeyframe,
        /// <summary>The replica world must be recreated before processing continues.</summary>
        RecreateReplicaWorld,
        /// <summary>The session is incompatible and must be disconnected.</summary>
        DisconnectRequired,
    }

    /// <summary>Identifies why the client entered a recovery phase.</summary>
    public enum NetworkRecoveryReason : byte
    {
        /// <summary>No recovery reason.</summary>
        None,
        /// <summary>The local prediction history cannot satisfy reconciliation.</summary>
        PredictionHistoryUnavailable,
        /// <summary>A received snapshot was rejected before application.</summary>
        SnapshotRejected,
        /// <summary>Snapshot application failed after the apply path started.</summary>
        SnapshotApplyFailed,
        /// <summary>The remote protocol is incompatible with this endpoint.</summary>
        ProtocolIncompatible,
    }

    /// <summary>Stores the durable client recovery state on the local connection entity.</summary>
    public struct NetworkRecoveryComponent : IComponent
    {
        /// <summary>Current recovery phase.</summary>
        public NetworkRecoveryPhase Phase;
        /// <summary>Reason for the current recovery phase.</summary>
        public NetworkRecoveryReason Reason;
        /// <summary>Authoritative tick at which recovery was requested.</summary>
        public uint RequestedAtTick;
    }

    /// <summary>Transports one recovery request or successful-keyframe clear to ECS.</summary>
    public readonly struct NetworkRecoveryTransition
    {
        /// <summary>Creates one recovery transition.</summary>
        public NetworkRecoveryTransition(NetworkRecoveryPhase phase,
            NetworkRecoveryReason reason, uint requestedAtTick)
        {
            if (phase == NetworkRecoveryPhase.None && reason != NetworkRecoveryReason.None)
                throw new ArgumentException("A cleared recovery must have no reason.", nameof(reason));
            if (phase != NetworkRecoveryPhase.None && reason == NetworkRecoveryReason.None)
                throw new ArgumentException("A recovery phase requires a reason.", nameof(reason));
            Phase = phase;
            Reason = reason;
            RequestedAtTick = requestedAtTick;
        }

        /// <summary>Gets the recovery phase.</summary>
        public NetworkRecoveryPhase Phase { get; }
        /// <summary>Gets the recovery reason.</summary>
        public NetworkRecoveryReason Reason { get; }
        /// <summary>Gets the authoritative tick at which recovery was requested.</summary>
        public uint RequestedAtTick { get; }
    }
}
