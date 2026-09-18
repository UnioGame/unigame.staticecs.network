namespace UniGame.StaticEcs.Network
{
    using System;
    using System.Collections.Generic;
    using FFS.Libraries.StaticEcs;

    /// <summary>Bounds for <see cref="INetworkCellularScopeProvider{TWorld}.CollectScopeCells"/>.</summary>
    public static class NetworkCellularScopeLimits
    {
        /// <summary>
        /// Maximum number of cells one scope's neighbourhood can be made of (NCORE-15's 3x3
        /// grid needs 9; the extra headroom covers a coarser or irregular tiling without
        /// widening the contract again).
        /// </summary>
        public const int MaxCellsPerScope = 16;
    }

    /// <summary>
    /// Optional refinement of <see cref="INetworkScopeProvider{TWorld}"/> that exposes the
    /// individual cells a scope's neighbourhood is made of (NCORE-15b). A provider that also
    /// implements this interface lets <see cref="NetworkReplicator{TWorld}.Capture(uint, ScopeId, out NetworkSnapshot)"/>
    /// serialize each occupied cell's entities exactly once per tick and build every scope that
    /// touches it by copying that cell's already-encoded bytes, instead of re-collecting and
    /// re-serializing every one of a scope's cells (including cells shared with neighbouring
    /// scopes) on every single scope's capture.
    /// <para>
    /// A cell's identity doubles as its cache key and, for a peer alone in its own cell, as that
    /// peer's <see cref="ScopeId"/> (NCORE-15: "scope identity = cell id"). Implementing this
    /// interface is purely a performance opt-in: a provider that only implements
    /// <see cref="INetworkScopeProvider{TWorld}"/> keeps today's per-scope capture path, byte-for-byte
    /// identical to before this interface existed.
    /// </para>
    /// </summary>
    public interface INetworkCellularScopeProvider<TWorld> where TWorld : struct, IWorldType
    {
        /// <summary>
        /// Fills <paramref name="cells"/> with every distinct cell id that makes up
        /// <paramref name="scope"/>'s neighbourhood (e.g. the 3x3 block around a peer's own
        /// cell), and returns how many entries were written. <paramref name="cells"/> is at
        /// least <see cref="NetworkCellularScopeLimits.MaxCellsPerScope"/> long; returning more
        /// than that fails the capture with <see cref="SnapshotCaptureResult.LimitExceeded"/>.
        /// Called against the index built by the last <see cref="INetworkScopeProvider{TWorld}.RefreshTick"/>.
        /// An empty (unoccupied) cell is a normal, expected entry -- it is still cached so a
        /// later tick where it becomes occupied does not need special-casing.
        /// </summary>
        int CollectScopeCells(ScopeId scope, Span<ScopeId> cells);

        /// <summary>
        /// Appends every entity that belongs to exactly this one cell -- no neighbourhood
        /// expansion, unlike <see cref="INetworkScopeProvider{TWorld}.CollectEntities"/> -- into
        /// <paramref name="buffer"/>, deduplicating through <paramref name="seen"/>. Each entity
        /// must belong to exactly one cell so that merging already-sorted per-cell entity blocks
        /// by ascending GID (what <see cref="NetworkReplicator{TWorld}.Capture(uint, ScopeId, out NetworkSnapshot)"/>
        /// does with this method's output) produces the same globally GID-sorted sequence the
        /// non-cellular capture path produces.
        /// </summary>
        void CollectCellEntities(ScopeId cellId, List<World<TWorld>.Entity> buffer,
            HashSet<EntityGID> seen);
    }
}
