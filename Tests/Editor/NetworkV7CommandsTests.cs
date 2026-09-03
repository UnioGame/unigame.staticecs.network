using System;
using System.Collections.Generic;
using System.IO;
using FFS.Libraries.StaticEcs;
using FFS.Libraries.StaticPack;
using NUnit.Framework;

namespace UniGame.StaticEcs.Network.Tests
{
    public sealed partial class NetworkV7Tests
    {

        [Test]
        public void RedundantCommandsAreDeduplicatedAndAcknowledgedOnAdverseLink()
        {
            World<InputWorld>.Create(WorldConfig.Default());
            var types = World<InputWorld>.Types();
            types.Event<TestInput>();
            types.Event<NetworkCommandAcceptedEvent<TestInput>>();
            types.Event<NetworkCommandRejectedEvent<TestInput>>();
            World<InputWorld>.Initialize();
            var receiver = World<InputWorld>
                .RegisterEventReceiver<NetworkCommandAcceptedEvent<TestInput>>();
            try
            {
                var clientFactory = NetworkCompilerSupport.Create<InputWorld>();
                clientFactory.Command<TestInput>(new NetworkTypeId(91));
                var serverFactory = NetworkCompilerSupport.Create<InputWorld>();
                serverFactory.Command<TestInput, AllowInputPolicy>(
                    new NetworkTypeId(91));
                var immediate = NetworkSimulationPresets.Create(
                    NetworkSimulationPreset.Immediate);
                using var simulator = new NetworkSimulator(new ConnectionId(90),
                    in immediate);
                var server = new NetworkServer<InputWorld>(serverFactory.Freeze(),
                    static (_, _) => false);
                server.AddConnection(simulator.Server, 7, 11, new ScopeId(1));
                var client = new NetworkClient<InputWorld>(simulator.Client,
                    clientFactory.Freeze(), new ScopeId(1), commandRedundancy: 3);

                Assert.That(client.BeginHandshake(), Is.True);
                simulator.Advance(0);
                server.Receive();
                server.Tick(_ => { });
                simulator.Advance(0);
                client.Process();
                Assert.That(client.Session.State,
                    Is.EqualTo(NetworkSessionState.Established));

                var adverse = NetworkSimulationPresets.Create(
                    NetworkSimulationPreset.Unstable);
                adverse.Seed = 771;
                adverse.LossProbability = 0.1f;
                adverse.DuplicateProbability = 0.1f;
                adverse.ReorderProbability = 0.1f;
                simulator.ApplyConfig(in adverse);

                var sequences = new HashSet<uint>();
                uint latestTick = 0;
                uint latestSequence = 0;
                for (var index = 0; index < 106; index++)
                {
                    var input = new TestInput { Value = index < 100 ? index + 1 : 0 };
                    Assert.That(client.SendCommand(in input, server.ServerTick + 4),
                        Is.EqualTo(NetworkCommandResult.Queued));
                    simulator.Advance(50);
                    server.Receive();
                    server.Tick(_ => { });
                    CollectAcceptedInputs(receiver, sequences, ref latestTick,
                        ref latestSequence);
                    World<InputWorld>.Tick();
                    simulator.Advance(50);
                    client.Process();
                }
                for (var index = 0; index < 8; index++)
                {
                    simulator.Advance(50);
                    server.Receive();
                    server.Tick(_ => { });
                    CollectAcceptedInputs(receiver, sequences, ref latestTick,
                        ref latestSequence);
                    World<InputWorld>.Tick();
                    simulator.Advance(50);
                    client.Process();
                }

                Assert.That(sequences.Count, Is.GreaterThanOrEqualTo(95));
                Assert.That(client.ServerProcessedCommandTick, Is.EqualTo(latestTick));
                Assert.That(client.ServerProcessedCommandSequence,
                    Is.EqualTo(latestSequence));
                var stats = simulator.CaptureStats();
                Assert.That(stats.ClientToServer.LostPackets, Is.GreaterThan(0));
                Assert.That(stats.ClientToServer.DuplicatePackets, Is.GreaterThan(0));
                Assert.That(stats.ClientToServer.ReorderedPackets, Is.GreaterThan(0));
            }
            finally
            {
                World<InputWorld>.DeleteEventReceiver(ref receiver);
                World<InputWorld>.Destroy();
            }
        }

        [Test]
        public void CommandQueueRejectsOverflowBeforeSendingPartialBatch()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            var receiver = World<AuthorityWorld>
                .RegisterEventReceiver<NetworkCommandAcceptedEvent<TestCommand>>();
            try
            {
                MemoryNetworkTransport.CreatePair(new ConnectionId(92),
                    out var clientTransport, out var serverTransport);
                using (clientTransport)
                using (serverTransport)
                {
                    var server = new NetworkServer<AuthorityWorld>(
                        Schema<AuthorityWorld>(true), static (_, _) => false);
                    server.AddConnection(serverTransport, 7, 12, new ScopeId(1));
                    var client = new NetworkClient<ClientAWorld>(clientTransport,
                        Schema<ClientAWorld>(false), new ScopeId(1));
                    Assert.That(client.BeginHandshake(), Is.True);
                    server.Receive();
                    server.Tick(_ => { });
                    client.Process();
                    server.Receive();

                    for (var i = 0; i < ProtocolLimits.MaxCommandsPerBatch; i++)
                    {
                        var command = new TestCommand { Value = i };
                        Assert.That(client.QueueCommand(in command, 2, out _),
                            Is.EqualTo(NetworkCommandResult.Queued));
                    }
                    var overflow = new TestCommand { Value = -1 };
                    Assert.That(client.QueueCommand(in overflow, 2, out _),
                        Is.EqualTo(NetworkCommandResult.LimitExceeded));
                    Assert.That(serverTransport.TryReceive(out _), Is.False,
                        "Queueing must not send a partial command batch.");

                    Assert.That(client.FlushCommands(2),
                        Is.EqualTo(NetworkCommandResult.Queued));
                    server.Receive();
                    server.Tick(_ => { });
                    var count = 0;
                    foreach (World<AuthorityWorld>
                                 .Event<NetworkCommandAcceptedEvent<TestCommand>> _ in receiver)
                        count++;
                    Assert.That(count, Is.EqualTo(ProtocolLimits.MaxCommandsPerBatch));
                }
            }
            finally
            {
                World<AuthorityWorld>.DeleteEventReceiver(ref receiver);
                World<AuthorityWorld>.Destroy();
                World<ClientAWorld>.Destroy();
            }
        }

        [Test]
        public void CommandQueueRejectsOneByteOverTransportLimitAndReleasesEnvelope()
        {
            CreateReplicationWorld<ClientAWorld>(false);
            try
            {
                var schema = Schema<ClientAWorld>(false);
                MemoryNetworkTransport.CreatePair(new ConnectionId(95),
                    out var clientEndpoint, out var serverTransport);
                var exactPacketBytes = PacketHeader.Size + 1 + 17 + sizeof(int);
                using (var clientTransport = new LimitedNetworkTransport(
                           clientEndpoint, exactPacketBytes - 1))
                using (serverTransport)
                using (var client = new NetworkClient<ClientAWorld>(clientTransport,
                           schema, new ScopeId(1)))
                {
                    Assert.That(client.Session.Admit(schema.Fingerprint, 1, 1,
                        new ScopeId(1)), Is.EqualTo(NetworkAdmissionResult.Accepted));
                    var command = new TestCommand { Value = 1 };

                    Assert.That(client.QueueCommand(in command, 2, out var rejectedSequence),
                        Is.EqualTo(NetworkCommandResult.LimitExceeded));
                    Assert.That(rejectedSequence, Is.Zero);
                    Assert.That(client.CaptureMemoryDiagnostics().PendingCommands, Is.Zero);
                    Assert.That(client.CaptureBufferDiagnostics().OutstandingLeases, Is.Zero);

                    clientTransport.MaxUnreliablePayloadBytes = exactPacketBytes;
                    Assert.That(client.QueueCommand(in command, 3, out var acceptedSequence),
                        Is.EqualTo(NetworkCommandResult.Queued));
                    Assert.That(acceptedSequence, Is.Not.Zero);
                    Assert.That(client.CaptureMemoryDiagnostics().PendingCommands, Is.EqualTo(1));
                    Assert.That(client.CaptureBufferDiagnostics().OutstandingLeases, Is.EqualTo(1));
                }
            }
            finally
            {
                World<ClientAWorld>.Destroy();
            }
        }

        [Test]
        public void CommandBatchRepeatsCurrentAndThreePreviousTicks()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            try
            {
                MemoryNetworkTransport.CreatePair(new ConnectionId(94),
                    out var clientTransport, out var serverTransport);
                using (clientTransport)
                using (serverTransport)
                {
                    var server = new NetworkServer<AuthorityWorld>(
                        Schema<AuthorityWorld>(true), static (_, _) => false);
                    server.AddConnection(serverTransport, 7, 14, new ScopeId(1));
                    var clientSchema = Schema<ClientAWorld>(false);
                    var client = new NetworkClient<ClientAWorld>(clientTransport,
                        clientSchema, new ScopeId(1), commandRedundancy: 3);
                    Assert.That(client.BeginHandshake(), Is.True);
                    server.Receive();
                    server.Tick(_ => { });
                    client.Process();
                    server.Receive();

                    NetworkBufferLease latestPacket = null;
                    for (uint tick = 2; tick <= 6; tick++)
                    {
                        var command = new TestCommand { Value = (int)tick };
                        Assert.That(client.QueueCommand(in command, tick, out _),
                            Is.EqualTo(NetworkCommandResult.Queued));
                        Assert.That(client.FlushCommands(tick),
                            Is.EqualTo(NetworkCommandResult.Queued));
                        Assert.That(serverTransport.TryReceive(out latestPacket), Is.True);
                    }

                    Assert.That(NetworkPacket.TryDecode(latestPacket,
                        clientSchema.Fingerprint, out _, out var payload), Is.True);
                    Assert.That(payload.Span[0], Is.EqualTo(4));
                    var ticks = new List<uint>();
                    int offset = 1;
                    for (var i = 0; i < payload.Span[0]; i++)
                    {
                        ticks.Add(Read32(payload.Span, offset + 4));
                        int commandBytes = checked((int)Read32(
                            payload.Span, offset + 13));
                        offset += 17 + commandBytes;
                    }
                    CollectionAssert.AreEqual(new uint[] { 3, 4, 5, 6 }, ticks);
                }
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
                World<ClientAWorld>.Destroy();
            }
        }

        [Test]
        public void MalformedAndOversizedCommandBatchesNeverQueueValidPrefix()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            var receiver = World<AuthorityWorld>
                .RegisterEventReceiver<NetworkCommandAcceptedEvent<TestCommand>>();
            try
            {
                MemoryNetworkTransport.CreatePair(new ConnectionId(93),
                    out var clientTransport, out var serverTransport);
                using (clientTransport)
                using (serverTransport)
                {
                    var serverSchema = Schema<AuthorityWorld>(true);
                    var clientSchema = Schema<ClientAWorld>(false);
                    var server = new NetworkServer<AuthorityWorld>(serverSchema,
                        static (_, _) => false);
                    server.AddConnection(serverTransport, 7, 13, new ScopeId(1));
                    var client = new NetworkClient<ClientAWorld>(clientTransport,
                        clientSchema, new ScopeId(1));
                    Assert.That(client.BeginHandshake(), Is.True);
                    server.Receive();
                    server.Tick(_ => { });
                    client.Process();
                    server.Receive();

                    var command = new TestCommand { Value = 7 };
                    Assert.That(client.QueueCommand(in command, 2, out _),
                        Is.EqualTo(NetworkCommandResult.Queued));
                    Assert.That(client.FlushCommands(2),
                        Is.EqualTo(NetworkCommandResult.Queued));
                    Assert.That(serverTransport.TryReceive(out var validPacket), Is.True);
                    Assert.That(NetworkPacket.TryDecode(validPacket, clientSchema.Fingerprint,
                        out var header, out var validPayload), Is.True);

                    var malformedPayload = new byte[validPayload.Length + 5];
                    validPayload.Span.CopyTo(malformedPayload);
                    malformedPayload[0] = 2;
                    Assert.That(NetworkPacket.TryEncode(Buffers, header, malformedPayload,
                        out var malformedPacket), Is.True);
                    Assert.That(clientTransport.TrySend(malformedPacket), Is.True);
                    server.Receive();

                    var oversizedPayload = new byte[18];
                    oversizedPayload[0] = 1;
                    Write32(oversizedPayload, 1, 2);
                    Write32(oversizedPayload, 5, 2);
                    Write32(oversizedPayload, 9, 10);
                    oversizedPayload[13] = 0;
                    Write32(oversizedPayload, 14,
                        ProtocolLimits.MaxCommandBytes + 1u);
                    header.PacketSequence = 2;
                    Assert.That(NetworkPacket.TryEncode(Buffers, header, oversizedPayload,
                        out var oversizedPacket), Is.True);
                    Assert.That(clientTransport.TrySend(oversizedPacket), Is.True);
                    server.Receive();
                    server.Tick(_ => { });

                    var count = 0;
                    foreach (World<AuthorityWorld>
                                 .Event<NetworkCommandAcceptedEvent<TestCommand>> _ in receiver)
                        count++;
                    Assert.That(count, Is.Zero);
                }
            }
            finally
            {
                World<AuthorityWorld>.DeleteEventReceiver(ref receiver);
                World<AuthorityWorld>.Destroy();
                World<ClientAWorld>.Destroy();
            }
        }

        [Test]
        public void ServerCommandRejectionsTraceEverySemanticResyncReason()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            try
            {
                MemoryNetworkTransport.CreatePair(new ConnectionId(94),
                    out var clientTransport, out var serverTransport);
                using (clientTransport)
                using (serverTransport)
                {
                    var observer = new TraceCollector();
                    var serverSchema = Schema<AuthorityWorld>(true);
                    var clientSchema = Schema<ClientAWorld>(false);
                    var server = new NetworkServer<AuthorityWorld>(serverSchema,
                        static (_, _) => false, observer: observer);
                    server.AddConnection(serverTransport, 7, 13,
                        new ScopeId(1), observer);
                    var client = new NetworkClient<ClientAWorld>(clientTransport,
                        clientSchema, new ScopeId(1));
                    Assert.That(client.BeginHandshake(), Is.True);
                    server.Receive();
                    server.Tick(_ => { });
                    client.Process();
                    server.Receive();

                    var command = new TestCommand { Value = 7 };
                    Assert.That(client.QueueCommand(in command, 2, out _),
                        Is.EqualTo(NetworkCommandResult.Queued));
                    Assert.That(client.FlushCommands(2),
                        Is.EqualTo(NetworkCommandResult.Queued));
                    Assert.That(serverTransport.TryReceive(out var validPacket), Is.True);
                    Assert.That(NetworkPacket.TryDecode(validPacket,
                        clientSchema.Fingerprint, out var header, out var validPayload), Is.True);

                    var invalidEnvelope = new byte[18];
                    invalidEnvelope[0] = 1;
                    var trailing = new byte[validPayload.Length + 1];
                    validPayload.Span.CopyTo(trailing);
                    var rejected = validPayload.ToArray();
                    Write32(rejected, 9, uint.MaxValue);
                    var payloads = new[]
                    {
                        Array.Empty<byte>(),
                        new byte[] { 0 },
                        new byte[] { 1 },
                        invalidEnvelope,
                        trailing,
                        rejected,
                    };
                    var expected = new[]
                    {
                        NetworkResyncReason.ServerEmptyPayload,
                        NetworkResyncReason.ServerInvalidCommandCount,
                        NetworkResyncReason.ServerTruncatedCommandHeader,
                        NetworkResyncReason.ServerInvalidCommandEnvelope,
                        NetworkResyncReason.ServerTrailingPayloadBytes,
                        NetworkResyncReason.ServerCommandQueueRejected,
                    };
                    observer.Events.Clear();
                    for (var index = 0; index < payloads.Length; index++)
                    {
                        header.PacketSequence = checked(header.PacketSequence +
                            (index == 0 ? 0u : 1u));
                        Assert.That(NetworkPacket.TryEncode(Buffers, header,
                            payloads[index], out var packet), Is.True);
                        Assert.That(clientTransport.TrySend(packet), Is.True);
                        server.Receive();
                    }

                    var actual = new List<NetworkResyncReason>();
                    var sources = new List<NetworkResyncSource>();
                    var commandResults = new List<NetworkCommandResult?>();
                    for (var index = 0; index < observer.Events.Count; index++)
                    {
                        var value = observer.Events[index];
                        if (value.Phase == NetworkPhase.Send &&
                            value.PacketKind == NetworkPacketKind.ResyncRequest)
                        {
                            actual.Add(value.ResyncReason);
                            sources.Add(value.ResyncSource);
                            commandResults.Add(value.CommandResult);
                        }
                    }
                    CollectionAssert.AreEqual(expected, actual);
                    CollectionAssert.AreEqual(
                        new[]
                        {
                            NetworkResyncSource.ServerCommandDecode,
                            NetworkResyncSource.ServerCommandDecode,
                            NetworkResyncSource.ServerCommandDecode,
                            NetworkResyncSource.ServerCommandDecode,
                            NetworkResyncSource.ServerCommandDecode,
                            NetworkResyncSource.ServerCommandDecode,
                        }, sources);
                    CollectionAssert.AreEqual(
                        new NetworkCommandResult?[]
                        {
                            NetworkCommandResult.Malformed,
                            NetworkCommandResult.Malformed,
                            NetworkCommandResult.Malformed,
                            NetworkCommandResult.Malformed,
                            NetworkCommandResult.Malformed,
                            NetworkCommandResult.SchemaMismatch,
                        }, commandResults);
                    validPacket.Dispose();
                }
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
                World<ClientAWorld>.Destroy();
            }
        }

        [Test]
        public void ServerDispatchReturnsPolicyRejectedWithoutRejectedEventReceiver()
        {
            World<RejectWorld>.Create(WorldConfig.Default());
            World<RejectWorld>.Types().Event<TestCommand>().Event<NetworkCommandAcceptedEvent<TestCommand>>().Event<NetworkCommandRejectedEvent<TestCommand>>();
            World<RejectWorld>.Initialize();
            try
            {
                var clientFactory = NetworkCompilerSupport.Create<RejectWorld>(); clientFactory.Command<TestCommand>(new NetworkTypeId(10)); var clientSchema = clientFactory.Freeze();
                var rawFactory = NetworkCompilerSupport.Create<RejectWorld>(); rawFactory.Command<TestCommand>(new NetworkTypeId(10)); var rawSchema = rawFactory.Freeze();
                var policyFactory = NetworkCompilerSupport.Create<RejectWorld>(); policyFactory.Command<TestCommand, RejectPolicy>(new NetworkTypeId(10)); var policySchema = policyFactory.Freeze();
                var client = new NetworkSession<RejectWorld>(new ConnectionId(1), NetworkRole.Client, clientSchema); client.Admit(clientSchema.Fingerprint, 1, 1, default);
                client.CreateCommand(new TestCommand { Value = 7 }, 1, out var command);
                var raw = new NetworkSession<RejectWorld>(new ConnectionId(1), NetworkRole.Server, rawSchema); raw.Admit(rawSchema.Fingerprint, 1, 1, default);
                var rawCoordinator = new NetworkServerCoordinator<RejectWorld>(); rawCoordinator.Add(raw);
                Assert.That(rawCoordinator.Queue(command, 1), Is.EqualTo(NetworkCommandResult.SchemaMismatch));
                var server = new NetworkSession<RejectWorld>(new ConnectionId(1), NetworkRole.Server, policySchema); server.Admit(policySchema.Fingerprint, 1, 1, default);
                Assert.That(server.Validate(command, 1, 2, 8, out var entry), Is.EqualTo(NetworkCommandResult.Queued));
                Assert.That(server.Dispatch(command, entry), Is.EqualTo(NetworkCommandResult.PolicyRejected));
            }
            finally { World<RejectWorld>.Destroy(); }
        }

        [Test]
        public void ServerIgnoresLateRedundantCommandButRejectsLateNewCommand()
        {
            World<RejectWorld>.Create(WorldConfig.Default());
            World<RejectWorld>.Types().Event<TestCommand>()
                .Event<NetworkCommandAcceptedEvent<TestCommand>>()
                .Event<NetworkCommandRejectedEvent<TestCommand>>();
            World<RejectWorld>.Initialize();
            try
            {
                var factory = NetworkCompilerSupport.Create<RejectWorld>();
                factory.Command<TestCommand, RejectPolicy>(new NetworkTypeId(10));
                var schema = factory.Freeze();
                var client = new NetworkSession<RejectWorld>(new ConnectionId(1),
                    NetworkRole.Client, schema);
                client.Admit(schema.Fingerprint, 1, 1, default);
                var server = new NetworkSession<RejectWorld>(new ConnectionId(1),
                    NetworkRole.Server, schema);
                server.Admit(schema.Fingerprint, 1, 1, default);

                client.CreateCommand(new TestCommand { Value = 1 }, 1,
                    out var first);
                client.CreateCommand(new TestCommand { Value = 2 }, 1,
                    out var second);
                try
                {
                    Assert.That(server.Validate(first, 1, 2, 8, out _),
                        Is.EqualTo(NetworkCommandResult.Queued));
                    Assert.That(server.Validate(first, 100, 2, 8, out _),
                        Is.EqualTo(NetworkCommandResult.Duplicate));
                    Assert.That(server.Validate(second, 100, 2, 8, out _),
                        Is.EqualTo(NetworkCommandResult.TickWindow));
                }
                finally
                {
                    first.Dispose();
                    second.Dispose();
                }
            }
            finally { World<RejectWorld>.Destroy(); }
        }

        [Test]
        public void ServerOwnsMonotonicTickAndGameplayPrecedesCapture()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            try
            {
                MemoryNetworkTransport.CreatePair(new ConnectionId(44), out var clientTransport, out var serverTransport);
                using (clientTransport) using (serverTransport)
                {
                    var serverObserver = new DiagnosticsCollector();
                    var clientObserver = new DiagnosticsCollector();
                    var server = new NetworkServer<AuthorityWorld>(Schema<AuthorityWorld>(true), (scope, entity) => true, observer: serverObserver);
                    server.AddConnection(serverTransport, 1, 9, new ScopeId(3), serverObserver);
                    var clientSchema = Schema<ClientAWorld>(false);
                    var client = new NetworkClient<ClientAWorld>(clientTransport, clientSchema, new ScopeId(3), clientObserver);
                    Assert.That(clientObserver.Sessions[0].NextSendPacketSequence, Is.EqualTo(1));
                    Assert.That(client.BeginHandshake(), Is.True);
                    Assert.That(clientObserver.Sessions[clientObserver.Sessions.Count - 1].NextSendPacketSequence, Is.EqualTo(2));
                    server.Receive();
                    EntityGID gid = default;
                    server.Tick(tick =>
                    {
                        Assert.That(tick, Is.EqualTo(1));
                        var entity = World<AuthorityWorld>.NewEntity<TestEntity>();
                        entity.Set(new TestComponent { Value = 42 }); gid = entity.GID;
                    });
                    client.Process();
                    Assert.That(server.ServerTick, Is.EqualTo(1));
                    Assert.That(gid.TryUnpack<ClientAWorld>(out var replica), Is.True);
                    Assert.That(replica.Read<TestComponent>().Value, Is.EqualTo(42));

                    var phases = new HashSet<NetworkPhase>();
                    foreach (var value in serverObserver.Events) phases.Add(value.Phase);
                    foreach (var value in clientObserver.Events) phases.Add(value.Phase);
                    Assert.That(phases, Is.EquivalentTo(new[] { NetworkPhase.Receive, NetworkPhase.Decode, NetworkPhase.CommandDispatch, NetworkPhase.SnapshotCapture, NetworkPhase.SnapshotApply, NetworkPhase.Send }));
                    Assert.That(serverObserver.Count(NetworkPhase.Send), Is.EqualTo(2));
                    Assert.That(clientObserver.Count(NetworkPhase.Send), Is.EqualTo(2));
                    var captured = serverObserver.Single(NetworkPhase.SnapshotCapture);
                    Assert.That(captured.Entities, Is.EqualTo(1)); Assert.That(captured.Records, Is.EqualTo(1));
                    Assert.That(captured.HistoryTicks, Is.EqualTo(1)); Assert.That(captured.HistoryBytes, Is.GreaterThan(0));
                    var applied = clientObserver.Single(NetworkPhase.SnapshotApply);
                    Assert.That(applied.Entities, Is.EqualTo(1)); Assert.That(applied.Records, Is.EqualTo(1));
                    var decodedSnapshot = clientObserver.Single(NetworkPhase.Decode, NetworkPacketKind.SnapshotChunk);
                    var ack = clientObserver.Single(NetworkPhase.Send, NetworkPacketKind.Ack);
                    Assert.That(decodedSnapshot.Timestamp, Is.LessThanOrEqualTo(applied.Timestamp));
                    Assert.That(applied.Timestamp, Is.LessThanOrEqualTo(ack.Timestamp));
                    Assert.That(clientObserver.Count(NetworkPhase.SnapshotApply), Is.EqualTo(1));
                    Assert.That(clientObserver.Count(NetworkPhase.Send, NetworkPacketKind.Ack), Is.EqualTo(1));
                    Assert.That(serverObserver.Snapshots.Count, Is.EqualTo(1));
                    Assert.That(clientObserver.Snapshots.Count, Is.EqualTo(1));
                    Assert.That(serverObserver.Snapshots[0].PayloadHash, Is.EqualTo(clientObserver.Snapshots[0].PayloadHash));
                    Assert.That(clientObserver.Sessions[clientObserver.Sessions.Count - 1].AcknowledgedSnapshotTick, Is.EqualTo(1));
                    Assert.That(serverObserver.Sessions[serverObserver.Sessions.Count - 1].NextSendPacketSequence, Is.EqualTo(2));

                    var nonSnapshot = new PacketHeader
                    {
                        Kind = PacketKind.ResyncRequest,
                        Flags = PacketFlags.ReliableOrdered,
                        SessionEpoch = 9,
                        PacketSequence = 2,
                        ServerTick = 7,
                        SchemaFingerprint = clientSchema.Fingerprint
                    };
                    Span<byte> resyncPayload = stackalloc byte[ResyncRequestPayload.Size];
                    Assert.That(new ResyncRequestPayload(55).TryWrite(
                        resyncPayload), Is.True);
                    Assert.That(NetworkPacket.TryEncode(Buffers, nonSnapshot,
                        resyncPayload, out var nonSnapshotPacket), Is.True);
                    Assert.That(serverTransport.TrySend(nonSnapshotPacket), Is.True);
                    client.Process();
                    Assert.That(client.ServerTick, Is.EqualTo(7));
                    Assert.That(client.AcknowledgedSnapshotTick, Is.EqualTo(1));
                    Assert.That(clientObserver.Single(NetworkPhase.Decode,
                        NetworkPacketKind.ResyncRequest).ResyncReason,
                        Is.EqualTo(NetworkResyncReason.None));
                    Assert.That(clientObserver.Single(NetworkPhase.Decode,
                        NetworkPacketKind.ResyncRequest).ResyncSource,
                        Is.EqualTo(NetworkResyncSource.None));
                    Assert.That(clientObserver.Single(NetworkPhase.Decode,
                        NetworkPacketKind.ResyncRequest).ResyncCorrelationId,
                        Is.EqualTo(55));
                    Assert.That(clientObserver.Single(NetworkPhase.Send,
                        NetworkPacketKind.ResyncRequest).ResyncReason,
                        Is.EqualTo(NetworkResyncReason.None));
                    Assert.That(clientObserver.Single(NetworkPhase.Send,
                        NetworkPacketKind.ResyncRequest).ResyncSource,
                        Is.EqualTo(NetworkResyncSource.ClientIncomingResyncEcho));
                    Assert.That(clientObserver.Single(NetworkPhase.Send,
                        NetworkPacketKind.ResyncRequest).ResyncCorrelationId,
                        Is.EqualTo(55));
                    nonSnapshot.PacketSequence = 3;
                    nonSnapshot.ServerTick = 3;
                    Assert.That(NetworkPacket.TryEncode(Buffers, nonSnapshot,
                        resyncPayload, out var olderTickPacket), Is.True);
                    Assert.That(serverTransport.TrySend(olderTickPacket), Is.True);
                    client.Process();
                    Assert.That(client.ServerTick, Is.EqualTo(7), "validated non-snapshot ticks must not regress the authoritative cursor");
                    Assert.That(client.SendCommand(new TestCommand { Value = 5 }, 8), Is.EqualTo(NetworkCommandResult.Queued));
                    var commandDiagnostics = clientObserver.Sessions[clientObserver.Sessions.Count - 1];
                    Assert.That(commandDiagnostics.ServerTick, Is.EqualTo(7));
                    Assert.That(commandDiagnostics.NextSendPacketSequence, Is.EqualTo(5));
                }
            }
            finally { World<AuthorityWorld>.Destroy(); World<ClientAWorld>.Destroy(); }
        }

        [Test]
        public void SessionsValidateAndDispatchCommandsInTargetPeerSequenceOrder()
        {
            World<TestWorld>.Create(WorldConfig.Default());
            World<TestWorld>.Types().Event<TestCommand>().Event<NetworkCommandAcceptedEvent<TestCommand>>().Event<NetworkCommandRejectedEvent<TestCommand>>();
            World<TestWorld>.Initialize();
            var receiver = World<TestWorld>.RegisterEventReceiver<NetworkCommandAcceptedEvent<TestCommand>>();
            try
            {
                var clientFactory = NetworkCompilerSupport.Create<TestWorld>();
                clientFactory.Command<TestCommand>(new NetworkTypeId(10));
                var clientSchema = clientFactory.Freeze();
                var serverFactory = NetworkCompilerSupport.Create<TestWorld>();
                serverFactory.Command<TestCommand, AllowPolicy>(new NetworkTypeId(10));
                var serverSchema = serverFactory.Freeze();
                Assert.That(clientSchema.Fingerprint, Is.EqualTo(serverSchema.Fingerprint));

                var clientA = new NetworkSession<TestWorld>(new ConnectionId(1), NetworkRole.Client, clientSchema);
                var clientB = new NetworkSession<TestWorld>(new ConnectionId(2), NetworkRole.Client, clientSchema);
                var serverA = new NetworkSession<TestWorld>(new ConnectionId(1), NetworkRole.Server, serverSchema);
                var serverB = new NetworkSession<TestWorld>(new ConnectionId(2), NetworkRole.Server, serverSchema);
                Assert.That(clientA.Admit(serverSchema.Fingerprint, 2, 5, new ScopeId(1)), Is.EqualTo(NetworkAdmissionResult.Accepted));
                Assert.That(serverA.Admit(clientSchema.Fingerprint, 2, 5, new ScopeId(1)), Is.EqualTo(NetworkAdmissionResult.Accepted));
                Assert.That(clientB.Admit(serverSchema.Fingerprint, 1, 6, new ScopeId(1)), Is.EqualTo(NetworkAdmissionResult.Accepted));
                Assert.That(serverB.Admit(clientSchema.Fingerprint, 1, 6, new ScopeId(1)), Is.EqualTo(NetworkAdmissionResult.Accepted));
                clientA.CreateCommand(new TestCommand { Value = 20 }, 10, out var a);
                clientB.CreateCommand(new TestCommand { Value = 10 }, 10, out var b);
                var coordinator = new NetworkServerCoordinator<TestWorld>();
                coordinator.Add(serverA); coordinator.Add(serverB);
                Assert.That(coordinator.Queue(a, 10), Is.EqualTo(NetworkCommandResult.Queued));
                Assert.That(coordinator.Queue(b, 10), Is.EqualTo(NetworkCommandResult.Queued));
                Assert.That(coordinator.Dispatch(10).Total, Is.EqualTo(2));
                var index = 0;
                foreach (var item in receiver)
                {
                    Assert.That(item.Value.Command.Value, Is.EqualTo(index == 0 ? 10 : 20));
                    index++;
                }
                Assert.That(index, Is.EqualTo(2));
            }
            finally
            {
                World<TestWorld>.DeleteEventReceiver(ref receiver);
                World<TestWorld>.Destroy();
            }
        }

        [Test]
        public void AdmissionPolicy_CommitsBeforeLifecycleNotification()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            try
            {
                MemoryNetworkTransport.CreatePair(new ConnectionId(71),
                    out var clientTransport, out var serverTransport);
                using (clientTransport)
                using (serverTransport)
                {
                    var order = new List<string>();
                    var policy = new RecordingAdmissionPolicy(order, true);
                    var observer = new OrderedPeerObserver(order);
                    var server = new NetworkServer<AuthorityWorld>(
                        Schema<AuthorityWorld>(true), static (_, _) => true,
                        peerObserver: observer, admissionPolicy: policy);
                    server.AddConnection(serverTransport, 3, 8, new ScopeId(1));
                    var client = new NetworkClient<ClientAWorld>(
                        clientTransport, Schema<ClientAWorld>(false), new ScopeId(1));

                    client.BeginHandshake();
                    server.Receive();
                    server.Tick(_ => { });
                    client.Process();

                    Assert.That(order, Is.EqualTo(new[] { "policy", "admitted" }));
                    Assert.That(policy.RollbackCount, Is.Zero);
                    Assert.That(client.Session.State, Is.EqualTo(NetworkSessionState.Established));
                }
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
                World<ClientAWorld>.Destroy();
            }
        }

        [Test]
        public void AdmissionPolicy_RejectionRollsBackWithoutLifecycleNotification()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            try
            {
                MemoryNetworkTransport.CreatePair(new ConnectionId(72),
                    out var clientTransport, out var serverTransport);
                using (clientTransport)
                using (serverTransport)
                {
                    var order = new List<string>();
                    var policy = new RecordingAdmissionPolicy(order, false);
                    var observer = new OrderedPeerObserver(order);
                    var server = new NetworkServer<AuthorityWorld>(
                        Schema<AuthorityWorld>(true), static (_, _) => true,
                        peerObserver: observer, admissionPolicy: policy);
                    server.AddConnection(serverTransport, 4, 9, new ScopeId(1));
                    var client = new NetworkClient<ClientAWorld>(
                        clientTransport, Schema<ClientAWorld>(false), new ScopeId(1));

                    client.BeginHandshake();
                    server.Receive();
                    client.Process();

                    Assert.That(order, Is.EqualTo(new[] { "policy", "rollback" }));
                    Assert.That(policy.RollbackCount, Is.EqualTo(1));
                    Assert.That(client.Session.State, Is.EqualTo(NetworkSessionState.Closed));
                }
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
                World<ClientAWorld>.Destroy();
            }
        }

        [Test]
        public void AdmissionObserverFailure_DisconnectsCommittedPeerAndRunsCleanup()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            try
            {
                MemoryNetworkTransport.CreatePair(new ConnectionId(73),
                    out var clientTransport, out var serverTransport);
                using (clientTransport)
                using (serverTransport)
                {
                    var order = new List<string>();
                    var policy = new RecordingAdmissionPolicy(order, true);
                    var observer = new ThrowingPeerObserver(order);
                    var server = new NetworkServer<AuthorityWorld>(
                        Schema<AuthorityWorld>(true), static (_, _) => true,
                        peerObserver: observer, admissionPolicy: policy);
                    server.AddConnection(serverTransport, 5, 10, new ScopeId(1));
                    var client = new NetworkClient<ClientAWorld>(
                        clientTransport, Schema<ClientAWorld>(false), new ScopeId(1));

                    client.BeginHandshake();
                    server.Receive();
                    client.Process();

                    Assert.That(order, Is.EqualTo(new[]
                    {
                        "policy",
                        "admitted",
                        "disconnected"
                    }));
                    Assert.That(client.Session.State, Is.EqualTo(NetworkSessionState.Closed));
                    Assert.That(server.RemoveConnection(new ConnectionId(73)), Is.False);
                }
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
                World<ClientAWorld>.Destroy();
            }
        }

    }
}