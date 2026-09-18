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
}
