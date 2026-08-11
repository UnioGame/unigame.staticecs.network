# Static ECS Network

Transport-neutral protocol, generated AOT-safe schemas, replication, and tick-indexed command contracts for Static ECS.

## Capabilities

- Generates endpoint schemas and typed serializers without runtime reflection.
- Implements protocol 4 with schema, simulation, and content fingerprints.
- Keeps admission, peer, epoch, ownership, packet sequencing, and snapshot history server controlled.
- Uses one `INetworkCommand` contract and one redundant unreliable-sequenced command batch.
- Preserves every command event and deduplicates repeated envelopes by connection and command sequence.
- Exposes `ServerProcessedCommandTick` and `ServerProcessedCommandSequence` for the last command actually processed by authority simulation in each snapshot.
- Estimates the current server tick from validated server ticks, ping/pong round-trip time, and a configured prediction lead.
- Maps authority GIDs to independently allocated replica GIDs and applies scoped snapshots transactionally.
- Provides typed bounded command/state timelines and a deterministic adverse-link `NetworkSimulator`.
- Provides stable `NetworkPrefabId` and `SceneObjectId` value types without Unity dependencies.

## Usage

Mark replicated state and client commands explicitly:

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

public struct MoveInput : IEvent, INetworkCommand
{
    public sbyte X;
    public sbyte Z;
}

public struct UseAbilityCommand : IEvent, INetworkCommand
{
    public int AbilityId;
}
```

Tick and sequence belong to the command envelope, not the gameplay value. The following is the low-level package API for infrastructure, tests, and explicitly scheduled actions. Game integration should publish a typed ECS event and let its owning client feature assign the estimated tick and sequence:

```csharp
uint targetTick = client.EstimatedServerTick;
client.QueueCommand(new MoveInput { X = 1, Z = 0 }, targetTick, out uint sequence);
client.FlushCommands(targetTick);
```

For normal game integration, publish the command as a Static ECS event instead:

```csharp
World<ClientWorld>.SendEvent(new UseAbilityCommand
{
    AbilityId = abilityId,
});
```

The owning Client feature explicitly registers `NetworkCommandSentEvent<UseAbilityCommand>` and installs `SendNetworkCommandSystem<TWorld, UseAbilityCommand>` before the shared flush. The source generator supplies the wire schema, serializer, decoder, and typed acceptance contracts. Server authorization policy, continuous-command cadence, prediction history, and gameplay handling remain explicit domain behavior and are not generated.

The authority pipeline owns its tick boundary:

```csharp
server.Receive();
server.Tick(serverTick => RunAuthorityGameplay(serverTick));
```

Due command events are emitted before the gameplay callback. A snapshot is captured after gameplay, so its state belongs to `Snapshot.ServerTick`; it also carries `ServerProcessedCommandTick` and `ServerProcessedCommandSequence` as acknowledgements. The processed-command cursor is not a prediction reconciliation base.

Declare generated endpoints in composition assemblies:

```csharp
[assembly: NetworkEndpoint("Authority", typeof(Main), NetworkRole.Server)]
[assembly: NetworkEndpoint("Client", typeof(ClientWorld), NetworkRole.Client)]
```

Server policies authorize commands from trusted session context. Payloads cannot claim ownership:

```csharp
public struct AuthorityMoveInputPolicy : INetworkCommandPolicy<Main, MoveInput>
{
    public bool Authorize(in NetworkCommandContext context, in MoveInput input) =>
        context.PeerId != 0;
}
```

`NetworkSimulator` exposes the same `INetworkTransport` pair used by endpoint code. `CommandBatch` is unreliable-sequenced and repeats commands from the current and configured previous ticks. The server discards duplicate command sequences before publishing gameplay events. Reliable ordered protocol traffic carries admission, snapshots, acknowledgements, resync, and clock synchronization.

```csharp
var config = NetworkSimulationPresets.Create(NetworkSimulationPreset.Unstable);
using var simulator = new NetworkSimulator(new ConnectionId(1), in config);
server.AddConnection(simulator.Server, peerId, epoch, scope);
var client = new NetworkClient<ClientWorld>(simulator.Client, schema, scope,
    ticksPerSecond: 20, predictionLeadTicks: 3, commandRedundancy: 3);
```

See the repository [network architecture guide](../../../docs/guides/network-static-ecs.md) for the implementation maturity, game integration, prediction flow, and production boundaries. Use [Developing Client-Server Static ECS Features](../../../docs/guides/network-feature-development.md) when adding a new game feature. The [Russian data-flow guide](../../../docs/guides/static-ecs-network-data-flow-ru.md) walks through the complete client-server path and the current command-extension workflow.

## Configuration

- Package version `2026.4.0` implements wire protocol `4` only; v3 packets are rejected.
- Every endpoint must use the same schema, simulation/config, and content/grid fingerprints.
- Default locomotion configuration is owned by the game: 20 Hz, 64 history ticks, 2 interpolation ticks, 3 previous command ticks, and a 2 ms replay budget.
- Snapshot history is bounded by both tick count and byte count.
- `ScopeId` is server assigned. It controls visibility, not authority.
- `NetworkOwnerComponent` is replicated metadata; policies trust only admitted `NetworkCommandContext` values.
- `NetworkPrefabId` identifies a dynamic prefab asset. `SceneObjectId` identifies one authored scene instance.
- Compression remains `None`; full snapshots remain the current baseline.
- Production sockets, authentication, encryption, AOI cells, acknowledged delta baselines, MTU fragmentation, pooling, and congestion control are intentionally outside this package slice.
