# Static ECS Network

Transport-neutral protocol and replication infrastructure for Static ECS.

## Capabilities

- Generates explicit AOT-safe Client and Server endpoint schemas without runtime reflection.
- Implements protocol v5 sessions, commands, canonical keyframes, baseline deltas, snapshot chunk assembly, and reason-aware recovery.
- Reconstructs keyframes and deltas into one canonical snapshot before the single transactional `Stage -> Apply` path.
- Owns bounded snapshot/command histories, deterministic adverse-link simulation, pooled packet leases, and complete-packet transport capabilities.
- Exposes queryable session, ownership, replica identity, tick, clock, and recovery contracts without game-specific policy.

## Usage

Declare explicit reachable roots in each composition assembly:

```csharp
[assembly: NetworkEndpoint("Authority", typeof(Main), NetworkRole.Server,
    typeof(MoveInput), typeof(NetworkOwnerComponent))]

[assembly: NetworkEndpoint("Client", typeof(ClientWorld), NetworkRole.Client,
    typeof(MoveInput), typeof(NetworkOwnerComponent))]
```

Generated schemas provide serialization and registration. Gameplay features still own
command cadence, server policy, deterministic simulation, prediction history, and
presentation. Follow the canonical
[network feature development guide](../../../docs/guides/network-feature-development.md)
instead of calling low-level packet APIs from gameplay.

## Configuration

- Both endpoints must agree on protocol v5 schema, simulation, and content fingerprints.
- `ScopeId` is currently fixed/global; spatial relevance is not modeled here.
- Command batches obey `MaxUnreliablePayloadBytes` and never fragment.
- Snapshot chunks obey `MaxReliablePayloadBytes`; canonical decoded state is bounded separately.
- Buffer leases have explicit consume/transfer/dispose ownership on every path.
- Compression, authentication, encryption, AOI/relevance cells, and server rewind are outside this package contract.
- See [network architecture](../../../docs/guides/network-static-ecs.md) for package and role composition.
