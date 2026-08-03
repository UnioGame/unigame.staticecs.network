# Static ECS Network

Transport-neutral protocol and replication foundations for deterministic Static ECS sessions.

## Capabilities

- Defines bounded version-one packet framing, canonical payload codecs, stable RFC UUID identifiers, CRC32, xxHash64, and schema hashing.
- Provides AOT-safe typed schema registration with retained entity, record, and command invokers, generation-checked pooled packet ownership, stepped transport and transform contracts, a bounded command outbox, command markers, and bounded tick history.
- Rejects unknown flags and enum values, unsupported transforms, malformed lengths, invalid hashes, reserved fields, and non-canonical ordering before ECS mutation.
- Binds schema-validated command and snapshot stages to the exact schema identity that accepted them.
- Dispatches commands through retained codecs and authorizers, then emits typed accepted or rejected Static ECS events.
- Captures deterministic full snapshots from tagged authority entities and applies them through retained AOT-safe invokers to an exact replica ledger.
- Preflights the complete snapshot, mapped topology, physical occupants, segment kinds, schema records, flags, codecs, and relation targets before any ECS mutation.
- Negotiates schema, tick rate, payload limits, epoch, peer identity, and canonical chunk topology through a deterministic stepped session handshake.
- Transfers ordered client commands, complete server snapshots, cumulative acknowledgements, and bounded resynchronization requests after admission.
- Retains independent canonical server-capture and client-apply records in bounded tick histories.
- Publishes optional privacy-safe operation events, cumulative session statistics, and retained canonical tick fingerprints without changing protocol behavior.
- Provides bounded strict NDJSON export plus explicit opt-in transport trace and deterministic replay.

## Usage

```csharp
var schema = new SchemaBuilder<ServerWorld>()
    .EntityKind<PlayerEntity>(new TypeId("3ee9226f-9459-48ef-a572-d567f297a997"))
    .Component<PositionComponent, PositionCodec>(
        new TypeId("f7da29ce-318f-4745-a01c-acf4fbd36c62"),
        version: 1,
        new CodecId("3a77e68f-799f-4425-9a90-2d5ea76b53d0"),
        maxBytes: 12)
    .Freeze();
```

Before `World<ServerWorld>.Initialize()`, register `ReplicatedTag` and every entity, component, tag, link, link-set, and multi-value type used by the schema through `World<ServerWorld>.Types()`. Freezing a schema retains codecs and invokers but does not register Static ECS storage.

Packet payloads are written with `PayloadCodec`, framed with `PacketFraming`, and passed as owned `PacketLease` instances through an `ITransport`. Successful decode returns a disposable `StagedPayload`; consume its pooled typed indexes and canonical payload slices before disposing it. Schema-bound stages expose the validating `SchemaHash`.

`PacketLease` is a value handle with one logical owner. Pass ownership only to APIs that consume it by `ref`; ordinary copies are borrowed aliases and become invalid when ownership transfers or returns. `Span` and aggregate `ReadOnlyMemory<byte>` views are borrowed and must not cross a transfer, disposal, or thread handoff. Call `Copy()` when bytes need independent retention.

```csharp
var packet = PacketLease.Rent(256);
try
{
    packet.SetLength(0);
    transport.TrySend(Channel.ReliableOrdered, ref packet);
}
finally
{
    if (packet.IsValid)
        packet.Dispose();
}
```

Create a `CommandDispatcher<TWorld>` from the same frozen schema and pass it only staged command batches. The dispatcher derives sequence and client tick from the stage, accepts the trusted peer id from the endpoint, and returns an exhaustive `DispatchResult` without transferring stage ownership.

Create one `CommandOutbox<TWorld>` per session epoch after negotiating its decoded command-batch capacity. Enqueue uses the schema's retained typed codec. `TryBuild` returns an owned canonical decoded payload and freezes its sequence prefix until the exact `MarkSent` call. Mark only after a successful reliable transport send; cumulative acknowledgements release sent entries.

```csharp
using var outbox = new CommandOutbox<ServerWorld>(schema, byteCapacity: negotiatedBytes);
var result = outbox.Enqueue(in command, clientTick);
if (result == EnqueueResult.Queued && outbox.TryBuild(out var commands, out var throughSequence))
{
    try
    {
        if (TryFrameAndSendReliably(commands.Span))
            outbox.MarkSent(throughSequence);
    }
    finally
    {
        if (commands.IsValid)
            commands.Dispose();
    }
}
```

When a transport also implements `ISteppedTransport`, call `BeginStep` once with the deterministic logical step index before receiving or sending session packets. `MemoryTransport` implements this barrier as a no-op.

`Session<TWorld>` owns its transport and the negotiated replication collaborators. A client sends `Hello`, the server replies with `Hello` and then `HelloAck`, and the client completes admission with `Ack`. Advance both endpoints with strictly increasing logical step indices until they become established or terminal.

```csharp
var clientConfig = SessionConfig.Client(
    nonce: 0x5E5510UL,
    minTickRate: 20,
    maxTickRate: 60);

using var client = new Session<ClientWorld>(clientConfig, schema, transport);
var work = client.Step(stepIndex);
if (client.State == SessionState.Established)
{
    var epoch = client.Epoch;
    var trustedPeer = client.PeerId;

    var move = new MoveCommand { X = 1 };
    client.Enqueue(in move, clientTick);
}
```

An established client retains enqueued commands until the server cumulatively acknowledges deterministic dispatch. An established server starts with `NeedsSnapshot` set and sets it again after a valid resynchronization request. Call `Capture` with strictly increasing authoritative ticks; each successful capture replaces any older unsent snapshot while history keeps an independent canonical copy.

```csharp
if (server.NeedsSnapshot)
    server.Capture(serverTick);

client.Step(stepIndex);
server.Step(stepIndex);
```

Each step receives at most one packet and attempts at most one send. Client send priority is resynchronization, command batch, then acknowledgement. Server send priority is full snapshot, then acknowledgement. A failed reliable send freezes the complete packet intent and retries byte-identically; a local or remote requested close cancels that transfer intent and reuses its uncommitted sequence for the orderly disconnect. Full snapshots use an independent unreliable-sequenced domain and may skip stale packets.

Servers use `SessionConfig.Server` with a non-zero epoch, trusted peer id, exact tick rate, and canonical authority chunk map. A rejecting server remains `Handshaking` while a false send retries the same `HelloAck`. After the rejection is queued it enters `Closing` with a null public result. The client must consume and publish that result, then dispose its session; a later server step observes `RemoteClosed`, publishes the same result, and closes. Do not dispose the rejecting server immediately after enqueue because `MemoryTransport` would drain the queued rejection. `Close()` requests an orderly disconnect. A session validates its scope again at send, receive, and established-step seams, so chunk ownership changes become terminal topology failures before replication work proceeds.

Register the negotiated chunks before creating a scope. The wire map always uses role `1` (`AuthoritySelf`); the local scope selects whether those chunks must be `Self` or `Other` owned.

```csharp
var map = new[]
{
    new ChunkMapping { Chunk = 7, Cluster = 2, Role = 1 }
};

using var scope = new ReplicaScope<ServerWorld>(ScopeRole.Authority, map);
using var replicator = new Replicator<ServerWorld>(schema, scope);

if (replicator.Capture(out var snapshot) == CaptureResult.Success)
{
    try
    {
        // Read snapshot.Span here, or transfer ownership to a consuming API by ref.
    }
    finally
    {
        if (snapshot.IsValid)
            snapshot.Dispose();
    }
}
```

On a replica world, stage the decoded `FullSnapshot` with the equivalent replica-world schema and pass it to `Replicator<TWorld>.Apply`. The scope ledger owns only exact entity GIDs created by successful applies. Missing ledger entities are despawned by later complete snapshots; unrelated entities never enter the ledger.

Pass an `ISessionObserver` when operation timing and structured diagnostics are required. The observer is caller-owned and is never disposed by the session. Observer exceptions are isolated and counted in `SessionStats.ObserverErrors`. Ordinary `SessionEvent` values contain bounded numeric metadata only: they never contain packet payloads, command or world bytes, nonces, epochs, peer identities, schema identities, exception text, or paths.

```csharp
using var output = File.Create(logPath);
using var log = new NdjsonLog(output, capacity: 4096, source: endpointId);
using var session = new Session<ClientWorld>(clientConfig, schema, transport, log);

session.Step(stepIndex);
log.Flush();

var stats = session.Stats;
if (session.TryGetFingerprint(serverTick, out var fingerprint) == HistoryLookup.Found)
{
    // Compare tick, canonical hash, and canonical byte length across endpoints.
}
```

`NdjsonLog.Observe` performs no I/O and retains a fixed SPSC ring. Call `Observe` from one producer and `Flush` from one consumer. Overflow drops newest events and writes one explicit gap after the retained prefix. A stream failure is terminal for that logger and is never allowed to affect the session.

Transport tracing is a different privacy boundary. `ReplayTape` records complete packet bytes and therefore can contain nonces, peer identities, commands, and replicated world state. Enable it only through an explicit diagnostic decision. Store tapes in access-controlled encrypted storage, enforce a small byte budget and retention window, and explicitly delete them when the investigation ends. Never attach raw tapes to ordinary telemetry or logs.

```csharp
using var tape = new ReplayTape(16 * 1024 * 1024);
using (var traced = new TraceTransport(connectedTransport, tape))
{
    // The session owns traced after successful construction.
}

using var protectedOutput = OpenProtectedTrace(tracePath);
tape.Save(protectedOutput);

using var replay = new ReplayTransport(tape);
// Replay requires the exact recorded BeginStep, receive, and send call sequence.
```

`TraceTransport` owns its wrapped transport but never the tape. A complete trace seals when the wrapper is disposed. Overflow or a wrapped transport exception makes the tape incomplete; incomplete tapes cannot be saved or replayed. `ReplayTransport` borrows a sealed complete tape, returns independently owned receive leases, and faults on any step, channel, byte, truncation, or transcript-end mismatch.

## Configuration

- Runtime limits may lower, but never raise, the constants in `ProtocolLimits`.
- Session wire and decoded limits are negotiated independently. Packet framing is checked against local configured limits before payload decode, and the accepted limits cannot exceed either endpoint's advertisement.
- Session transports must be connected, error-free, and implement `ISteppedTransport`; a successfully constructed session owns and disposes the transport.
- Session observers are optional, caller-owned, and must keep their own work bounded. Session event timestamps use `Stopwatch` ticks; strict NDJSON writes integer nanoseconds and stable lowercase tokens.
- `NdjsonLog` supports one producer and one consumer only. Its capacity is fixed, overflow is visible, and a faulted or disposed logger never resumes output.
- Replay tape byte capacity charges every 24-byte call record header plus payload. Saving requires a sealed complete unborrowed tape; loading validates the complete little-endian format, bounds, reserved bytes, and checksum before exposing a tape.
- Raw replay tapes are sensitive data. Protect storage, bound retention, restrict access, and explicitly delete files after use.
- Session step indices are strictly increasing. Each step begins the transport exactly once, receives at most one packet, and sends at most one packet. Failed sends retry the same semantic control packet and sequence.
- Only control packets are accepted during the handshake. Established sessions dispatch authorized commands, apply complete snapshots, exchange cumulative acknowledgements, and request full resynchronization without delta compression, prediction, rollback, or replay.
- `Enqueue` is available only to established clients. `Capture` is available only to established servers with valid authority collaborators; zero is a valid first tick and `NoneTick` is reserved.
- Acknowledgements are cumulative and may be stale or duplicated. Future command or snapshot acknowledgements are protocol failures and do not mutate retained state.
- Returned local snapshot conflicts queue a bounded resynchronization request. Terminal schema, protocol, or topology apply failures do not advance snapshot history or acknowledgement state.
- Version one supplies deterministic framing, not security. Nonces, epoch, CRC32, and xxHash do not authenticate peers, provide confidentiality, or prevent replay, and the client nonce is not echoed by the wire layout. Use a dedicated authenticated and integrity-protected transport across an untrusted boundary, and generate non-zero server nonce and epoch values that are not reused across live or restarted sessions.
- Packet ownership handoffs must be serialized; the handle does not permit concurrent mutation through borrowed aliases.
- Version one accepts only `NoOpTransform` with transform id zero.
- Schema values, markers, and commands must be unmanaged. Links and multi-value registrations are capped at 32,768 elements.
- `ReplicatedTag` is control state and cannot be registered as an ordinary schema record.
- Authority capture includes only `ReplicatedTag` entities in the exact mapped chunks. Every relation target must appear in the same snapshot.
- Replica chunks must be empty when `ReplicaScope<TWorld>` is created. Scope construction and replication never register, free, load, unload, or remap chunks.
- Version one preserves disabled entities and ordinary disableable components. FFS tag storage does not represent a disabled tag state; disabled links, link sets, and multi-components are rejected as `DisabledUnsupported`.
- Apply validates the full snapshot before mutation. Typed lifecycle hooks and user codecs run directly; exceptions propagate, and no rollback guarantee is made after mutation starts.
- Explicitly register both `CommandAcceptedEvent<T>` and `CommandRejectedEvent<T>` closed generic event types before initializing the world. `CommandDispatcher<TWorld>` construction rejects an uninitialized world or a missing result type. `ConfigurationError` remains a defensive result if world registration drifts after construction; a registered result event without a receiver returns `NoReceiver`.
- Command outboxes accept capacities from 36 bytes through `MaxWirePayloadBytes`. A codec always receives its complete registered command bound, so `CodecFailed`, standalone `TooLarge`, and current-capacity `Full` remain distinct outcomes.
- See the repository [Static ECS knowledge base](../../../docs/knowledge/static-ecs/) for world and marker lifecycle.
