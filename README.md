# Static ECS Network

Transport-neutral Network v2 runtime and generated AOT-safe schemas for Static ECS.

## Capabilities

- Generates non-zero `uint` wire identifiers with xxHash32 over `<asmdef-name>:<metadata-type-name>` and a 16-byte SHA-256 schema fingerprint.
- Aggregates world-neutral Shared manifests into endpoint-specific `Generated{Name}Network` classes without runtime reflection or generated assets.
- Uses the existing Static ECS `Write` and `Read` hooks through generated typed invokers. Hook buffers are pooled while writing and copied to exact bounded payload arrays.
- Frames protocol version 2 packets with canonical hashes, CRC32, exact lengths, and `NetworkCompression.None`.
- Captures every entity whose concrete `IEntityType` also implements `INetworkType`, applies scope selection, writes canonical GID order, and despawns absent ledger-owned replicas.
- Keeps `NetworkSession<TWorld>` state per connection while the server coordinator shares immutable captures only by `(ScopeId, ServerTick)`.
- Validates commands by connection, peer, epoch, generated schema, sequence, tick window, and server policy before ordering them by `(TargetTick, PeerId, Sequence)`.
- Classifies state, direction, epoch, and sequence failures without consuming rejected packet cursors, and reports accepted and policy-rejected command totals once per server tick.
- Provides bounded history, isolated two-client in-memory transport, privacy-safe observer events, and bounded NDJSON output.

## Usage

Declare wire types only in a Shared assembly. Each wire type is one concrete non-generic struct with one supported Static ECS shape.

```csharp
public struct PlayerEntity : IEntityType, INetworkType
{
    public byte Id() => 1;
}

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
World<ServerWorld>.Initialize();

var schema = GeneratedServerNetwork.CreateSchema();
var server = new NetworkServer<ServerWorld>(schema, (scope, entity) => IsInScope(scope, entity), historyTicks: 64, historyBytes: 32 * 1024 * 1024, observer: observer);
```

Add each server transport with its server-assigned peer, epoch, and scope. The framed Hello/Ready exchange admits the generated fingerprint; malformed, incompatible, or stale packets fail closed and request resynchronization.

```csharp
server.AddConnection(serverTransport, assignedPeerId, epoch, scopeId, observer);
```

The server `Receive()` dequeues and decodes current transport input without advancing time. `Tick(gameplay)` owns the authoritative clock, advances exactly once, dispatches commands, invokes gameplay, then captures and sends the resulting state. Clients expose `Process()` without a caller-provided tick; `ServerTick` comes only from validated packets. Static ECS `World.CurrentTick` remains tracking time.

```csharp
server.Receive();
server.Tick(serverTick => RunGameplay(serverTick));

var client = new NetworkClient<ClientWorld>(clientTransport, GeneratedClientNetwork.CreateSchema(), scopeId, observer);
client.BeginHandshake();
client.Process();
client.SendCommand(new MoveCommand { X = 1 }, targetTick);
```

Snapshot metadata includes tick, scope, schema fingerprint, canonical hash, bytes, entity count, and record count. Authority capture always requires an explicit scope selector; the client-only replicator constructor supports staging and apply but rejects capture. Client and server histories evict oldest ticks until both tick and byte budgets hold. Snapshot staging preflights canonical order, ledger ownership, entity kinds, bounds, and local occupancy before any ECS mutation; only ledger-owned replicas may be updated or despawned.

## Configuration

- Package version `2026.2.0` implements wire protocol `2` only.
- Compression is fixed to `NetworkCompression.None`.
- Runtime limits may be reduced by endpoint orchestration but must not exceed `ProtocolLimits`.
- Every entity kind marked with `INetworkType` is authority-replicated; scope selection runs after generated kind selection. `NetworkOwnerComponent` is written only from trusted server state.
- Endpoint names must be unique valid C# identifiers because they form `Generated{Name}Network`.
- The generator targets `netstandard2.0`, references Microsoft.CodeAnalysis.CSharp 4.3.1 at build time, and ships only `Analyzers/StaticEcs.Network.Generator.dll` with the `RoslynAnalyzer` label.
- `NetworkNdjsonLog` contains numeric metadata only. It never records packet payloads, command values, schema manifests, or replicated world bytes.
- Diagnostics use six measured phases. Decode includes framing and snapshot staging, SnapshotApply contains only ECS mutation, and acknowledgements are separate Send attempts. Snapshot failures retain distinct schema, malformed, limit, and world categories. Server dispatch totals are emitted through the server observer once per tick; connection gauges include live handshakes while peer gauges include only established sessions.
- Observers may additionally implement `INetworkDiagnosticsObserver` to receive immutable session cursors and snapshot/history metadata. These callbacks never expose payload bytes, command values, ECS handles, or Unity types.
- See the repository [Static ECS knowledge base](../../../docs/knowledge/static-ecs/) for world lifecycle and type registration.
