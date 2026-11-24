namespace UniGame.StaticEcs.Network
{
    using FFS.Libraries.StaticEcs;

    /// <summary>
    /// Opt-in, off by default (NCORE-16). Decides, once per entity per tick, whether an entity's
    /// replicated records should be freshly serialized from current ECS state this capture, or held
    /// at their previously captured bytes instead. Held bytes are byte-for-byte identical to the
    /// last time this entity was captured for this scope, so <see cref="SnapshotDeltaCodec"/>'s raw
    /// byte comparison sees "unchanged" (0 wire bytes) for every peer sharing this scope's capture,
    /// no matter which baseline tick each peer has acknowledged.
    /// <para>
    /// This package stays gameplay-agnostic: it only calls this hook from
    /// <see cref="NetworkReplicator{TWorld}.Capture(uint, ScopeId, out NetworkSnapshot)"/>. A
    /// game/sandbox layer that knows about activity radii, ownership, or distance to players
    /// implements it -- e.g. a tiered policy where NPCs with no player nearby refresh every 4th
    /// tick, other non-near entities every 2nd tick, and everything else (including every peer's own
    /// entity, which prediction/reconciliation needs fresh) refreshes every tick, with each entity's
    /// phase offset by a hash of its GID so held entities do not all refresh on the same tick.
    /// </para>
    /// <para>
    /// The decision is per <b>entity</b>, never per peer: every peer sharing a scope's capture sees
    /// the exact same held-or-fresh bytes for a given entity this tick, so delta sharing across
    /// peers on the same baseline is unaffected -- a per-peer policy would defeat that sharing
    /// exactly like a per-peer capture would (see <see cref="INetworkScopeProvider{TWorld}"/>'s own
    /// remarks). An entity new to a scope (no previous capture of this scope contains it yet) is
    /// always freshly captured regardless of this policy's answer, and a held answer silently falls
    /// back to a fresh write for any record whose previously captured layout does not structurally
    /// match its current one (e.g. a component was added since the last capture). Supplying no
    /// policy (the default) keeps capture cost and traffic byte-for-byte identical to before this
    /// hook existed.
    /// </para>
    /// </summary>
    public interface INetworkUpdatePolicy<TWorld> where TWorld : struct, IWorldType
    {
        /// <summary>
        /// Returns <c>true</c> to hold this entity's replicated records at their previously captured
        /// bytes this tick instead of freshly serializing them, or <c>false</c> to refresh them now.
        /// Called once per entity, for every tick this entity's scope is captured, only while this
        /// policy is supplied to <see cref="NetworkReplicator{TWorld}"/>.
        /// </summary>
        bool ShouldHold(uint serverTick, ScopeId scope, in World<TWorld>.Entity entity,
            EntityGID gid);
    }

    /// <summary>
    /// Marks an update policy whose hold decision depends on the observing scope. The immutable
    /// epoch becomes part of the cellular capture-cache identity, so captures from distinct
    /// scopes never reuse bytes selected for another observer relationship.
    /// </summary>
    public interface INetworkScopeCaptureVariantPolicy<TWorld> : INetworkUpdatePolicy<TWorld>
        where TWorld : struct, IWorldType
    {
        /// <summary>Gets the immutable startup policy identity for cached capture variants.</summary>
        ulong CapturePolicyEpoch { get; }
    }

    /// <summary>
    /// Lets a game-owned update policy identify records for aggregate transport attribution.
    /// The shared network package only asks this question while capturing an authority snapshot;
    /// it never references gameplay types or components itself.
    /// </summary>
    public interface INetworkSnapshotAttributionPolicy<TWorld>
        where TWorld : struct, IWorldType
    {
        /// <summary>Returns whether this entity's encoded snapshot records belong to the metric.</summary>
        bool ShouldAttribute(in World<TWorld>.Entity entity, EntityGID gid);
    }

    /// <summary>Constants of <see cref="INetworkScopeCaptureBudgetPolicy{TWorld}"/>.</summary>
    public static class NetworkScopeCaptureBudget
    {
        /// <summary>A weight that refreshes the entity whatever the allowance of the tick.</summary>
        public const int AlwaysRefresh = int.MaxValue;
    }

    /// <summary>
    /// Opt-in. Limits how many bytes of changed entities one cellular scope snapshot refreshes per
    /// tick, so a crowded scope stays within the datagrams the game allows it. Changed entities
    /// compete by weight multiplied by the captures they have waited; the ones that do not fit keep
    /// the bytes of the scope's previous snapshot, which costs a delta nothing, and wait.
    /// <para>
    /// An entity whose fresh bytes equal the previous snapshot takes no allowance. An entity new to
    /// the scope and one that has waited <see cref="MaximumWaitTicks"/> captures are always
    /// refreshed and still count against the allowance. The decision is per entity and observing
    /// scope, never per peer, so peers of one scope keep sharing its capture.
    /// </para>
    /// <para>
    /// A peer predicts its own entity and compares it with every applied snapshot of the same tick,
    /// so it never sees that entity stale: either the scope it observes refreshes the entity
    /// always, or (<see cref="RefreshesObserversPersonally"/>) the entity waits like any other and
    /// the server sends its current block to the owner alone, ahead of the snapshot.
    /// </para>
    /// </summary>
    public interface INetworkScopeCaptureBudgetPolicy<TWorld> where TWorld : struct, IWorldType
    {
        /// <summary>Changed-entity bytes one scope capture may refresh per tick; zero or less lifts the limit.</summary>
        int BudgetBytes { get; }

        /// <summary>Captures after which a waiting changed entity is refreshed whatever the allowance.</summary>
        int MaximumWaitTicks { get; }

        /// <summary>
        /// Queued entities one capture may price and find too large for what is left of the
        /// allowance before the rest of its queue waits for the next capture; at least 1. A
        /// candidate that does not fit leaves room a cheaper one may still take, but every try
        /// walks the records of an entity.
        /// </summary>
        int MaximumMisses { get; }

        /// <summary>
        /// Whether an entity owned by a peer observing the scope may wait in the shared snapshot
        /// while its owner receives the current block in an <c>OwnedEntityRefresh</c> packet. False
        /// refreshes such an entity in every capture of that scope.
        /// </summary>
        bool RefreshesObserversPersonally { get; }

        /// <summary>Called once per tick before its first scope capture, after peers received their scopes.</summary>
        void BeginTick(uint serverTick);

        /// <summary>
        /// Returns the priority of one changed entity for the observing scope; a larger weight
        /// refreshes sooner and <see cref="NetworkScopeCaptureBudget.AlwaysRefresh"/> bypasses the queue.
        /// </summary>
        int Weight(ScopeId scope, in World<TWorld>.Entity entity, EntityGID gid);
    }
}
