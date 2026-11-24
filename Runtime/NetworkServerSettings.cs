namespace UniGame.StaticEcs.Network
{
    using System;

    /// <summary>
    /// Behaviour settings of <see cref="NetworkServer{TWorld}"/> that are not part of the wire
    /// format. A new instance holds the defaults; the server copies the values when it is created.
    /// </summary>
    public sealed class NetworkServerSettings
    {
        /// <summary>
        /// Ticks of snapshot history a scope keeps whatever its share of the history bytes. It must
        /// outlast an acknowledgement under load: an acknowledgement of an evicted tick costs a
        /// keyframe, whose size delays the next ones, and at 8 ticks that became a keyframe loop.
        /// </summary>
        public int MinimumHistoryTicksPerScope { get; set; } = 32;

        /// <summary>
        /// Bytes of snapshot history a scope keeps however many scopes share the history bytes, so a
        /// world of many small scopes does not evict every stored tick.
        /// </summary>
        public long MinimumHistoryBytesPerScope { get; set; } = 64 * 1024;

        /// <summary>Queued commands one peer may hold before its batches are rejected.</summary>
        public int MaximumPendingCommandsPerPeer { get; set; } = ProtocolLimits.MaxCommandsPerBatch * 3;

        /// <summary>
        /// Queued command bytes one peer may hold before its batches are rejected. Honest input
        /// commands are a few bytes each; a full wire payload per peer lets hostile clients pin memory.
        /// </summary>
        public int MaximumPendingCommandBytesPerPeer { get; set; } = 256 * 1024;

        /// <summary>Ticks behind the server tick a command's target tick may be and still queue.</summary>
        public int CommandPastWindowTicks { get; set; } = 2;

        /// <summary>Ticks ahead of the server tick a command's target tick may be and still queue.</summary>
        public int CommandFutureWindowTicks { get; set; } = 8;

        /// <summary>Ticks between two recovery snapshots offered to a peer that keeps asking for a resync.</summary>
        public int RecoveryRetryTicks { get; set; } = 10;

        /// <summary>
        /// Ticks between two comparisons of a peer's owner state; bytes are sent only when they
        /// differ. Peers are spread over the interval by their id.
        /// </summary>
        public int OwnerStateIntervalTicks { get; set; } = 5;

        /// <summary>Ticks after which the capture of a cell no scope asked for is dropped from the cache.</summary>
        public int CellCacheStaleTicks { get; set; } = 64;

        /// <summary>
        /// Largest number of datagram-sized chunks a delta may need and still travel unreliably;
        /// zero sends every snapshot on the reliable ordered channel. An unreliable delta that is
        /// lost is superseded by the next one, which is encoded against the baseline the peer last
        /// acknowledged, so a lost datagram does not hold later snapshots behind its resend.
        /// Keyframes, larger deltas and a delta whose own-entity state does not fit its last
        /// chunk stay reliable.
        /// </summary>
        public int UnreliableDeltaChunks { get; set; }

        /// <summary>
        /// Ticks after which a delta the peer did not acknowledge stops counting toward the
        /// in-flight window. Without it a peer that lost every delta of the window would never be
        /// sent another one. Used only with <see cref="UnreliableDeltaChunks"/> above zero.
        /// </summary>
        public int UnreliableDeltaLostTicks { get; set; } = 10;

        /// <summary>Largest value of <see cref="UnreliableDeltaChunks"/>.</summary>
        public const int MaximumUnreliableDeltaChunks = 4;

        /// <summary>Throws when a value is outside what the server can run with.</summary>
        public void Validate()
        {
            RequireAtLeast(MinimumHistoryTicksPerScope, 1, nameof(MinimumHistoryTicksPerScope));
            if (MinimumHistoryBytesPerScope < 1)
                throw new ArgumentOutOfRangeException(nameof(MinimumHistoryBytesPerScope));
            RequireAtLeast(MaximumPendingCommandsPerPeer, 1, nameof(MaximumPendingCommandsPerPeer));
            RequireAtLeast(MaximumPendingCommandBytesPerPeer, 1,
                nameof(MaximumPendingCommandBytesPerPeer));
            RequireAtLeast(CommandPastWindowTicks, 0, nameof(CommandPastWindowTicks));
            RequireAtLeast(CommandFutureWindowTicks, 0, nameof(CommandFutureWindowTicks));
            RequireAtLeast(RecoveryRetryTicks, 0, nameof(RecoveryRetryTicks));
            RequireAtLeast(OwnerStateIntervalTicks, 1, nameof(OwnerStateIntervalTicks));
            RequireAtLeast(CellCacheStaleTicks, 1, nameof(CellCacheStaleTicks));
            if (UnreliableDeltaChunks < 0 || UnreliableDeltaChunks > MaximumUnreliableDeltaChunks)
                throw new ArgumentOutOfRangeException(nameof(UnreliableDeltaChunks));
            RequireAtLeast(UnreliableDeltaLostTicks, 1, nameof(UnreliableDeltaLostTicks));
        }

        private static void RequireAtLeast(int value, int minimum, string name)
        {
            if (value < minimum)
                throw new ArgumentOutOfRangeException(name);
        }
    }
}
