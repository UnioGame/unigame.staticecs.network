# Static ECS Network

Transport-neutral protocol, generated AOT-safe schemas, replication, and tick-indexed input contracts for Static ECS.

## Capabilities

- Generates endpoint schemas and typed serializers without runtime reflection.
- Implements protocol 3 with schema, simulation, and content fingerprints.
- Keeps admission, peer, epoch, ownership, packet sequencing, and snapshot history server controlled.
- Separates continuous `INetworkInput` from transactional `INetworkCommand`.
- Sends a bounded redundant input window and deduplicates frames by connection and input sequence.
- Acknowledges the last input tick and sequence actually processed by authority simulation in each snapshot.
- Estimates the current server tick from validated server ticks, ping/pong round-trip time, and a configured prediction lead.
- Maps authority GIDs to independently allocated replica GIDs and applies scoped snapshots transactionally.
- Provides typed bounded input/state timelines and a deterministic adverse-link `NetworkSimulator`.
- Provides stable `NetworkPrefabId` and `SceneObjectId` value types without Unity dependencies.

## Usage

Mark replicated state and the two kinds of client messages explicitly:

```csharp
public struct PositionComponent : IComponent, INetworkType
{
    public float X;
    public float Y;
    public float Z;

    public void Write<TWorld>(ref BinaryPackWriter writer, World<TWorld>.Entity self)
        where TWorld : struct, IWorldType
    {
        writer.WriteFloat(X, Y, Z);
    }
}

public struct MoveInput : IEvent, INetworkInput
{
    public sbyte X;
    public sbyte Z;
}

public struct UseAbilityCommand : IEvent, INetworkCommand
{
    public int AbilityId;
}
```

Input tick and sequence belong to the network envelope, not the input value. Send input for the estimated authoritative tick; the client repeats the newest configured frames:

```csharp
uint targetTick = client.EstimatedServerTick;
client.SendInput(new MoveInput { X = 1, Z = 0 }, targetTick, out uint sequence);
```

The authority pipeline owns its tick boundary:

```csharp
server.Receive();
server.Tick(serverTick => RunAuthorityGameplay(serverTick));
```

Due input events are emitted before the gameplay callback. A snapshot is captured after gameplay and carries `LastProcessedInputTick` and `LastProcessedInputSequence`. Command acknowledgement continues to mean protocol acceptance; it must not be used for prediction reconciliation.

Declare generated endpoints in composition assemblies:

```csharp
[assembly: NetworkEndpoint("Authority", typeof(Main), NetworkRole.Server)]
[assembly: NetworkEndpoint("Client", typeof(ClientWorld), NetworkRole.Client)]
```

Server policies authorize both input and commands from trusted session context. Payloads cannot claim ownership:

```csharp
public struct AuthorityMoveInputPolicy : INetworkCommandPolicy<Main, MoveInput>
{
    public bool Authorize(in NetworkCommandContext context, in MoveInput input) =>
        context.PeerId != 0;
}
```

`NetworkSimulator` exposes the same `INetworkTransport` pair used by endpoint code. `InputBatch` is unreliable-sequenced and can be lost, duplicated, or reordered; reliable ordered protocol traffic preserves delivery and ordering in the simulator.

```csharp
var config = NetworkSimulationPresets.Create(NetworkSimulationPreset.Unstable);
using var simulator = new NetworkSimulator(new ConnectionId(1), in config);
server.AddConnection(simulator.Server, peerId, epoch, scope);
var client = new NetworkClient<ClientWorld>(simulator.Client, schema, scope,
    ticksPerSecond: 20, predictionLeadTicks: 3, inputRedundancy: 3);
```

See the repository [network architecture guide](../../../docs/guides/network-static-ecs.md) for the implementation maturity, game integration, prediction flow, and production boundaries. Use [Developing Client-Server Static ECS Features](../../../docs/guides/network-feature-development.md) when adding a new game feature.

## Configuration

- Package version `2026.3.0` implements wire protocol `3` only.
- Every endpoint must use the same schema, simulation/config, and content/grid fingerprints.
- Default locomotion configuration is owned by the game: 20 Hz, 64 history ticks, 2 interpolation ticks, 3 redundant input frames, and a 2 ms replay budget.
- Snapshot history is bounded by both tick count and byte count.
- `ScopeId` is server assigned. It controls visibility, not authority.
- `NetworkOwnerComponent` is replicated metadata; policies trust only admitted `NetworkCommandContext` values.
- `NetworkPrefabId` identifies a dynamic prefab asset. `SceneObjectId` identifies one authored scene instance.
- Compression remains `None`; full snapshots remain the current baseline.
- Production sockets, authentication, encryption, AOI cells, acknowledged delta baselines, MTU fragmentation, pooling, and congestion control are intentionally outside this package slice.
