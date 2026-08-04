# Static ECS Network

Transport-neutral Network v2 runtime and generated AOT-safe schemas for Static ECS.

## Capabilities

- Generates non-zero `uint` wire identifiers with xxHash32 over `<asmdef-name>:<metadata-type-name>` and a 16-byte SHA-256 schema fingerprint.
- Aggregates world-neutral Shared manifests into endpoint-specific `Generated{Name}Network` classes without runtime reflection or generated assets.
- Uses the existing Static ECS `Write` and `Read` hooks through generated typed invokers. Hook buffers are pooled while writing and copied to exact bounded payload arrays.
- Frames protocol version 2 packets with canonical hashes, CRC32, exact lengths, and `NetworkCompression.None`.
- Captures full snapshots from `NetworkTag` authority entities, validates the complete snapshot before mutation, creates replica tags automatically, and despawns absent replicas.
- Keeps `NetworkSession<TWorld>` state per connection while the server coordinator shares immutable captures only by `(ScopeId, ServerTick)`.
- Validates commands by connection, peer, epoch, generated schema, sequence, tick window, and server policy before ordering them by `(TargetTick, PeerId, Sequence)`.
- Provides bounded history, isolated two-client in-memory transport, privacy-safe observer events, and bounded NDJSON output.

## Usage

Declare wire types only in a Shared assembly. Each wire type is one concrete non-generic struct with one supported Static ECS shape.

```csharp
public struct PositionComponent : IComponent, INetworkType
{
    public float X;

    public void Write<TWorld>(ref BinaryPackWriter writer, World<TWorld>.Entity self)
        where TWorld : struct, IWorldType => writer.WriteFloat(X);

    public void Read<TWorld>(ref BinaryPackReader reader, World<TWorld>.Entity self, byte version, bool disabled)
        where TWorld : struct, IWorldType
    {
        X = reader.ReadFloat();
        self.Set(this);
    }
}

public struct MoveCommand : IEvent, INetworkCommand
{
    public float X;
    public void Write(ref BinaryPackWriter writer) => writer.WriteFloat(X);
    public void Read(ref BinaryPackReader reader, byte version) => X = reader.ReadFloat();
}
```

Declare every endpoint once at assembly level. A sandbox may declare both endpoints in one assembly with different world types.

```csharp
[assembly: NetworkEndpoint("Client", typeof(ClientWorld), NetworkRole.Client)]
[assembly: NetworkEndpoint("Server", typeof(ServerWorld), NetworkRole.Server)]
```

Bind authorization in the server assembly. Policies are endpoint behavior and do not change the Shared fingerprint.

```csharp
public struct MovePolicy : INetworkCommandPolicy<ServerWorld, MoveCommand>
{
    public bool Authorize(in NetworkCommandContext context, in MoveCommand command) =>
        context.PeerId != 0;
}
```

Register ordinary ECS types through the normal Static ECS registration path. Generated registration adds only required closed network result events. Create the schema after registration and before constructing endpoint runtime objects.

```csharp
World<ServerWorld>.Create(WorldConfig.Default());
World<ServerWorld>.Types().RegisterAll(typeof(PositionComponent).Assembly);
GeneratedServerNetwork.RegisterTypes(World<ServerWorld>.Types());
World<ServerWorld>.Types().Tag<NetworkTag>().Component<NetworkOwnerComponent>();
World<ServerWorld>.Initialize();

var schema = GeneratedServerNetwork.CreateSchema();
var replicator = new NetworkReplicator<ServerWorld>(schema);
var coordinator = new NetworkServerCoordinator<ServerWorld>(historyCapacity: 64);
```

For each accepted transport connection, create a separate session and complete fingerprint admission. `ConnectionId` comes from the transport; `PeerId` and epoch come from the server.

```csharp
var session = new NetworkSession<ServerWorld>(connectionId, NetworkRole.Server, schema, observer);
var result = session.Admit(remoteFingerprint, assignedPeerId, epoch, scopeId);
coordinator.Add(session);
```

The server sequence is receive, decode, validate and dispatch commands, gameplay, capture, then send. The client sequence is receive, decode, stage and apply snapshots, gameplay or presentation, then send. `ServerTick` is simulation time, Static ECS `World.CurrentTick` remains tracking time, and `Cycle` is only mock or replay call ordering.

```csharp
session.Tick(serverTick, cycle);
coordinator.Queue(commandEnvelope, serverTick);
coordinator.Dispatch(serverTick);

if (replicator.Capture(serverTick, out var capture) == SnapshotCaptureResult.Success)
    coordinator.StoreCapture(scopeId, capture);
```

On a client, stage every snapshot before applying it. A failed stage never mutates ECS. Hook or lifecycle exceptions after `Apply` starts are not rolled back.

```csharp
if (replicator.Stage(snapshot, out var staged) == SnapshotApplyResult.Success)
    replicator.Apply(staged);
```

## Configuration

- Package version `2026.1.0` implements wire protocol `2` only.
- Compression is fixed to `NetworkCompression.None`.
- Runtime limits may be reduced by endpoint orchestration but must not exceed `ProtocolLimits`.
- `NetworkTag` is control state and never appears in a generated manifest. `NetworkOwnerComponent` is written only from trusted server state.
- Endpoint names must be unique valid C# identifiers because they form `Generated{Name}Network`.
- The generator targets `netstandard2.0`, references Microsoft.CodeAnalysis.CSharp 4.3.1 at build time, and ships only `Analyzers/StaticEcs.Network.Generator.dll` with the `RoslynAnalyzer` label.
- `NetworkNdjsonLog` contains numeric metadata only. It never records packet payloads, command values, schema manifests, or replicated world bytes.
- See the repository [Static ECS knowledge base](../../../docs/knowledge/static-ecs/) for world lifecycle and type registration.
