namespace UniGame.StaticEcs.Network
{
    using System.Collections.Generic;
    using FFS.Libraries.StaticEcs;

    /// <summary>
    /// Assigns per-peer replication scopes and enumerates one scope's entities directly from a
    /// caller-owned index, instead of the position-agnostic default of collecting every generated
    /// entity and filtering it with a per-entity <see cref="NetworkScopeSelector{TWorld}"/> (NCORE-15).
    /// <para>
    /// This package stays position-agnostic: it only calls this hook. A game/sandbox layer that
    /// knows about world position (e.g. a component such as <c>PositionComponent</c>) implements it
    /// with a spatial index (a hash grid keyed by cell) built once per tick, so a large, spread-out
    /// population produces many small scopes instead of one global scope covering everyone.
    /// </para>
    /// <para>
    /// Supplying a provider to <see cref="NetworkServer{TWorld}"/> / <see cref="NetworkReplicator{TWorld}"/>
    /// replaces the selector-based collect-then-filter capture path entirely (the selector argument
    /// is still required for API compatibility but is not consulted while a provider is present).
    /// Omitting a provider (the default) keeps today's single-global-scope behavior byte-for-byte
    /// identical, including capture cost and traffic.
    /// </para>
    /// </summary>
    public interface INetworkScopeProvider<TWorld> where TWorld : struct, IWorldType
    {
        /// <summary>
        /// Rebuilds this provider's index (e.g. a hash grid) from current world state. Called
        /// exactly once per server tick, after gameplay systems have run and before any peer's
        /// scope is reassigned or any scope is captured that tick.
        /// </summary>
        void RefreshTick(uint serverTick);

        /// <summary>
        /// Assigns or updates one peer's scope for the current tick. <paramref name="scope"/> holds
        /// the peer's current scope on entry; set it to a new value and return <c>true</c> only when
        /// the peer must move to a different scope this tick (the caller then forces a keyframe on
        /// the new scope, per NCORE-15's hysteresis contract). Returning <c>false</c>, or leaving
        /// <paramref name="scope"/> unchanged, keeps the peer on its current scope for this tick.
        /// <para>
        /// A provider that cannot yet resolve the peer (e.g. its owned entity has not spawned) should
        /// return <c>false</c> and leave <paramref name="scope"/> untouched, keeping the peer on its
        /// admission-time scope until it can be resolved.
        /// </para>
        /// </summary>
        bool TryUpdateScope(uint peerId, ref ScopeId scope);

        /// <summary>
        /// Appends every entity that belongs to <paramref name="scope"/> into <paramref name="buffer"/>,
        /// deduplicating through <paramref name="seen"/> exactly like
        /// <c>IEntityNetworkInvoker{TWorld}.Collect</c> does for the default capture path. Implementations
        /// should read directly from the index built by the last <see cref="RefreshTick"/> instead of
        /// scanning every replicated entity, so capture cost scales with one scope's population rather
        /// than the whole world.
        /// </summary>
        void CollectEntities(ScopeId scope, List<World<TWorld>.Entity> buffer,
            HashSet<EntityGID> seen);
    }
}
