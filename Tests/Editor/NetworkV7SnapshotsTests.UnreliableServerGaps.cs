namespace UniGame.StaticEcs.Network.Tests
{
    using System;
    using System.Collections.Generic;
    using FFS.Libraries.StaticEcs;
    using NUnit.Framework;

    /// <summary>
    /// Covers what a server does with unreliable packets that arrive late, repeated or with a tick
    /// it never offered, and which peers and admission modes unreliable deltas work with.
    /// </summary>
    public sealed partial class NetworkV7Tests
    {
        [Test]
        public void OlderCommandBatchArrivingAfterANewerOneIsDroppedAsADuplicateWithoutDisconnect()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            var receiver = World<AuthorityWorld>
                .RegisterEventReceiver<NetworkCommandAcceptedEvent<TestCommand>>();
            try
            {
                using var pool = new NetworkBufferPool(1 << 20);
                MemoryNetworkTransport.CreatePair(new ConnectionId(951),
                    out var clientTransport, out var serverTransport);
                using (clientTransport)
                using (serverTransport)
                {
                    var observer = new TraceCollector();
                    var serverSchema = Schema<AuthorityWorld>(true);
                    var clientSchema = Schema<ClientAWorld>(false);
                    using var server = new NetworkServer<AuthorityWorld>(
                        serverSchema, static (_, _) => false,
                        observer: observer, bufferPool: pool);
                    server.AddConnection(serverTransport, 7, 1, new ScopeId(1), observer);
                    SendHello(clientTransport, pool, clientSchema.Fingerprint);
                    server.Receive();

                    var clientSession = new NetworkSession<ClientAWorld>(
                        clientTransport.Connection, NetworkRole.Client, clientSchema, pool);
                    Assert.That(clientSession.Admit(serverSchema.Fingerprint, 7, 1,
                        new ScopeId(1)), Is.EqualTo(NetworkAdmissionResult.Accepted));
                    Assert.That(clientSession.CreateCommand(
                        new TestCommand { Value = 1 }, 1, out var first),
                        Is.EqualTo(NetworkCommandResult.Queued));
                    Assert.That(clientSession.CreateCommand(
                        new TestCommand { Value = 2 }, 1, out var second),
                        Is.EqualTo(NetworkCommandResult.Queued));
                    // The client repeats recent commands: its second batch carries both.
                    var olderBatch = EncodeCommandBatch(first);
                    var newerBatch = EncodeCommandBatch(first, second);
                    first.Dispose();
                    second.Dispose();

                    SendCommandBatch(clientTransport, pool, serverSchema.Fingerprint, 1, 2,
                        newerBatch);
                    server.Receive();
                    server.Tick(_ => { });
                    CollectionAssert.AreEqual(new[] { 1, 2 }, DrainAcceptedCommands(receiver));

                    SendCommandBatch(clientTransport, pool, serverSchema.Fingerprint, 1, 1,
                        olderBatch);
                    server.Receive();
                    server.Tick(_ => { });

                    Assert.That(DrainAcceptedCommands(receiver), Is.Empty,
                        "The command of the late batch was already applied.");
                    Assert.That(server.ConnectionCount, Is.EqualTo(1));
                    Assert.That(observer.Count(NetworkPhase.Send, NetworkPacketKind.Disconnect),
                        Is.Zero);
                    var late = LastCommandBatchDecode(observer);
                    Assert.That(late.Sequence, Is.EqualTo(1));
                    Assert.That(late.PacketValidationResult,
                        Is.EqualTo(PacketValidationResult.Duplicate));
                    Assert.That(late.CommandResult, Is.EqualTo(NetworkCommandResult.Duplicate));

                    // The dropped packet did not disturb the numbering of the ones that follow.
                    Assert.That(clientSession.CreateCommand(new TestCommand { Value = 3 },
                        server.ServerTick + 1, out var third),
                        Is.EqualTo(NetworkCommandResult.Queued));
                    var nextBatch = EncodeCommandBatch(third);
                    third.Dispose();
                    SendCommandBatch(clientTransport, pool, serverSchema.Fingerprint, 1, 3,
                        nextBatch);
                    server.Receive();
                    server.Tick(_ => { });
                    CollectionAssert.AreEqual(new[] { 3 }, DrainAcceptedCommands(receiver));
                }
            }
            finally
            {
                World<AuthorityWorld>.DeleteEventReceiver(ref receiver);
                World<AuthorityWorld>.Destroy();
                World<ClientAWorld>.Destroy();
            }
        }

        // An acknowledgement names a tick the server has not reached. No capture of that tick
        // exists, so it cannot become a baseline: the peer gets a keyframe and stays connected.
        [Test]
        public void AcknowledgementAboveTheServerTickCostsAKeyframeAndNeverBecomesTheBaseline()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                MemoryNetworkTransport.CreatePair(new ConnectionId(952),
                    out var client, out var endpoint);
                using (client)
                using (endpoint)
                using (var server = new NetworkServer<AuthorityWorld>(schema,
                           static (_, _) => true,
                           settings: new NetworkServerSettings { UnreliableDeltaChunks = 1 }))
                {
                    EstablishUnreliablePeer(server, endpoint, client, schema);
                    ReceiveSnapshotPackets(client, out _);
                    SendPeerPacket(client, schema.Fingerprint, PacketKind.Ack, 1, 2, 1);
                    server.Receive();
                    server.Tick(_ => { });
                    ReceiveSnapshotPackets(client, out var delta);
                    Assert.That(delta.BaselineTick, Is.EqualTo(1));

                    var future = server.ServerTick + 5;
                    SendUnreliableAcknowledgement(client, schema.Fingerprint, 1, 1, future);
                    server.Receive();
                    Assert.That(server.ConnectionCount, Is.EqualTo(1));
                    Assert.That(server.CaptureScopeDiagnostics().LostBaselineAcknowledgements,
                        Is.EqualTo(1));

                    server.Tick(_ => { });
                    var recovery = ReceiveSnapshotPackets(client, out var keyframe);
                    Assert.That(keyframe.PayloadKind, Is.EqualTo(SnapshotPayloadKind.Keyframe));
                    Assert.That(recovery[0].Flags, Is.EqualTo(PacketFlags.ReliableOrdered));
                    var keyframeTick = keyframe.SnapshotTick;
                    SendPeerPacket(client, schema.Fingerprint, PacketKind.Ack, 1, 3, keyframeTick);
                    server.Receive();

                    // The server passes the tick the peer claimed; it was never remembered.
                    while (server.ServerTick <= future)
                    {
                        server.Tick(_ => { });
                        ReceiveSnapshotPackets(client, out var next);
                        Assert.That(next.PayloadKind, Is.EqualTo(SnapshotPayloadKind.Delta));
                        Assert.That(next.BaselineTick, Is.EqualTo(keyframeTick));
                    }
                    Assert.That(server.CaptureReplicationDiagnostics().KeyframeSnapshots,
                        Is.EqualTo(2));
                }
            }
            finally { World<AuthorityWorld>.Destroy(); }
        }

        // Unreliable acknowledgements are not ordered with the reliable keyframe of a new scope:
        // one for a delta of the old scope may arrive after the scope changed.
        [Test]
        public void UnreliableAcknowledgementOfTheOldScopeNeitherRepeatsTheKeyframeNorBecomesTheBaseline()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                var provider = new StubScopeProvider();
                MemoryNetworkTransport.CreatePair(new ConnectionId(953),
                    out var client, out var endpoint);
                using (client)
                using (var transport = new WindowedNetworkTransport(endpoint))
                using (var server = new NetworkServer<AuthorityWorld>(schema,
                           static (_, _) => true, scopeProvider: provider,
                           snapshotAdmissionMode: NetworkSnapshotAdmissionMode.Windowed,
                           maxSnapshotsInFlight: 4,
                           settings: new NetworkServerSettings
                           {
                               UnreliableDeltaChunks = 1,
                               UnreliableDeltaLostTicks = 100,
                           }))
                {
                    var oldScope = new ScopeId(71);
                    var newScope = new ScopeId(72);
                    server.AddConnection(transport, 1, 1, oldScope);
                    SendPeerPacket(client, schema.Fingerprint, PacketKind.Hello, 0, 1, 0);
                    server.Receive();
                    Assert.That(client.TryReceive(out var ready), Is.True);
                    ready.Dispose();
                    var entity = World<AuthorityWorld>.NewEntity<TestEntity>();
                    entity.Set(new TestComponent { Value = 1 });
                    provider.ScopeEntities[oldScope] =
                        new List<World<AuthorityWorld>.Entity> { entity };
                    provider.ScopeEntities[newScope] =
                        new List<World<AuthorityWorld>.Entity> { entity };
                    server.Tick(_ => { });
                    ReceiveSnapshotPackets(client, out _);
                    SendPeerPacket(client, schema.Fingerprint, PacketKind.Ack, 1, 2, 1);
                    server.Receive();

                    // Ticks 2 and 3 are unreliable deltas of the old scope, not yet acknowledged.
                    for (var tick = 2; tick <= 3; tick++)
                    {
                        server.Tick(_ => { });
                        var sent = ReceiveSnapshotPackets(client, out _);
                        Assert.That(sent[0].Flags, Is.EqualTo(PacketFlags.UnreliableSequenced));
                    }

                    provider.Assignments[1] = newScope;
                    server.Tick(_ => { });
                    ReceiveSnapshotPackets(client, out var keyframe);
                    Assert.That(keyframe.PayloadKind, Is.EqualTo(SnapshotPayloadKind.Keyframe));
                    Assert.That(keyframe.ScopeValue, Is.EqualTo(newScope.Value));
                    Assert.That(keyframe.SnapshotTick, Is.EqualTo(4));

                    SendUnreliableAcknowledgement(client, schema.Fingerprint, 1, 1, 3);
                    server.Receive();
                    server.Tick(_ => { });
                    AssertNoPacket(client,
                        "the late acknowledgement does not free the slot of the keyframe");
                    var scopes = server.CaptureScopeDiagnostics();
                    Assert.That(scopes.StaleScopeAcknowledgements, Is.EqualTo(1));
                    Assert.That(scopes.LostBaselineAcknowledgements, Is.Zero);
                    Assert.That(scopes.KeyframeBatches, Is.EqualTo(2));

                    SendPeerPacket(client, schema.Fingerprint, PacketKind.Ack, 1, 3, 4);
                    server.Receive();
                    server.Tick(_ => { });
                    ReceiveSnapshotPackets(client, out var resumed);
                    Assert.That(resumed.PayloadKind, Is.EqualTo(SnapshotPayloadKind.Delta));
                    Assert.That(resumed.BaselineTick, Is.EqualTo(4));
                    Assert.That(resumed.ScopeValue, Is.EqualTo(newScope.Value));
                    Assert.That(server.ConnectionCount, Is.EqualTo(1));
                }
            }
            finally { World<AuthorityWorld>.Destroy(); }
        }

        // The client asked for a keyframe; acknowledgements it sent before that for deltas it had
        // applied are still on their way. They must not end the recovery with a delta.
        [Test]
        public void UnreliableAcknowledgementArrivingAfterAResyncRequestDoesNotCancelTheKeyframe()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                const uint Correlation = 23;
                var schema = Schema<AuthorityWorld>(true);
                MemoryNetworkTransport.CreatePair(new ConnectionId(954),
                    out var client, out var endpoint);
                using (client)
                using (var transport = new WindowedNetworkTransport(endpoint))
                using (var server = new NetworkServer<AuthorityWorld>(schema,
                           static (_, _) => true,
                           snapshotAdmissionMode: NetworkSnapshotAdmissionMode.Windowed,
                           maxSnapshotsInFlight: 4,
                           settings: new NetworkServerSettings
                           {
                               UnreliableDeltaChunks = 1,
                               UnreliableDeltaLostTicks = 100,
                           }))
                {
                    EstablishUnreliablePeer(server, transport, client, schema);
                    ReceiveSnapshotPackets(client, out _);
                    SendPeerPacket(client, schema.Fingerprint, PacketKind.Ack, 1, 2, 1);
                    server.Receive();
                    for (var tick = 2; tick <= 3; tick++)
                    {
                        server.Tick(_ => { });
                        ReceiveSnapshotPackets(client, out _);
                    }

                    // The request overtakes the acknowledgement of tick 2.
                    SendResyncRequest(client, schema.Fingerprint, 1, 3, Correlation);
                    SendUnreliableAcknowledgement(client, schema.Fingerprint, 1, 1, 2);
                    server.Receive();
                    server.Tick(_ => { });
                    var recovery = ReceiveSnapshotPackets(client, out var keyframe);
                    Assert.That(keyframe.PayloadKind, Is.EqualTo(SnapshotPayloadKind.Keyframe));
                    Assert.That(keyframe.ResyncCorrelationId, Is.EqualTo(Correlation));
                    Assert.That(keyframe.SnapshotTick, Is.EqualTo(4));
                    Assert.That(recovery[0].Flags, Is.EqualTo(PacketFlags.ReliableOrdered));

                    // The acknowledgement of tick 3 arrives after the keyframe left.
                    SendUnreliableAcknowledgement(client, schema.Fingerprint, 1, 2, 3);
                    server.Receive();
                    server.Tick(_ => { });
                    AssertNoPacket(client,
                        "the keyframe keeps its slot and no delta against an older tick is sent");

                    SendPeerPacket(client, schema.Fingerprint, PacketKind.Ack, 1, 4, 4);
                    server.Receive();
                    server.Tick(_ => { });
                    ReceiveSnapshotPackets(client, out var resumed);
                    Assert.That(resumed.PayloadKind, Is.EqualTo(SnapshotPayloadKind.Delta));
                    Assert.That(resumed.BaselineTick, Is.EqualTo(4));
                    Assert.That(server.CaptureScopeDiagnostics().LostBaselineAcknowledgements,
                        Is.Zero);
                    Assert.That(server.CaptureReplicationDiagnostics().KeyframeSnapshots,
                        Is.EqualTo(2));
                    Assert.That(server.ConnectionCount, Is.EqualTo(1));
                }
            }
            finally { World<AuthorityWorld>.Destroy(); }
        }

        // Only application-ack admission is rejected with unreliable deltas. Current admission has
        // no window: it watches the reliable channel alone, which an unreliable delta never fills.
        [Test]
        public void CurrentAdmissionSendsUnreliableDeltasWithoutAWindowAndStillWaitsForTheReliableChannel()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                MemoryNetworkTransport.CreatePair(new ConnectionId(955),
                    out var client, out var endpoint);
                using (client)
                using (var transport = new WindowedNetworkTransport(endpoint))
                using (var server = new NetworkServer<AuthorityWorld>(schema,
                           static (_, _) => true,
                           snapshotAdmissionMode: NetworkSnapshotAdmissionMode.Current,
                           settings: new NetworkServerSettings
                           {
                               UnreliableDeltaChunks = 1,
                               UnreliableDeltaLostTicks = 2,
                           }))
                {
                    var entity = EstablishUnreliablePeer(server, transport, client, schema);
                    ReceiveSnapshotPackets(client, out _);
                    SendPeerPacket(client, schema.Fingerprint, PacketKind.Ack, 1, 2, 1);
                    server.Receive();

                    // More unacknowledged deltas than any window allows, each older than the
                    // lost-delta timeout by the end: Current admission counts neither.
                    var unacknowledged = NetworkServer<AuthorityWorld>.MaxSnapshotsInFlightLimit + 2;
                    for (var i = 0; i < unacknowledged; i++)
                    {
                        entity.Set(new TestComponent { Value = 10 + i });
                        server.Tick(_ => { });
                        var sent = ReceiveSnapshotPackets(client, out var delta);
                        Assert.That(sent[0].Flags, Is.EqualTo(PacketFlags.UnreliableSequenced));
                        Assert.That(delta.PayloadKind, Is.EqualTo(SnapshotPayloadKind.Delta));
                        Assert.That(delta.BaselineTick, Is.EqualTo(1));
                    }

                    transport.HasPendingReliablePackets = true;
                    entity.Set(new TestComponent { Value = 100 });
                    server.Tick(_ => { });
                    AssertNoPacket(client,
                        "a pending reliable packet holds the next snapshot, unreliable or not");
                    transport.HasPendingReliablePackets = false;
                    server.Tick(_ => { });
                    ReceiveSnapshotPackets(client, out var afterWait);
                    Assert.That(afterWait.PayloadKind, Is.EqualTo(SnapshotPayloadKind.Delta));
                    Assert.That(afterWait.BaselineTick, Is.EqualTo(1));

                    var admission = server.CaptureSnapshotAdmissionDiagnostics();
                    Assert.That(admission.Mode, Is.EqualTo(NetworkSnapshotAdmissionMode.Current));
                    Assert.That(admission.SnapshotWindowDeferrals, Is.Zero);
                    Assert.That(admission.PendingReliableDeferrals, Is.EqualTo(1));
                    var replication = server.CaptureReplicationDiagnostics();
                    Assert.That(replication.UnreliableDeltaWindowExpirations, Is.Zero);
                    Assert.That(replication.DeltaSnapshotsUnreliable,
                        Is.EqualTo(unacknowledged + 1));
                    Assert.That(replication.KeyframeSnapshots, Is.EqualTo(1));
                }
            }
            finally { World<AuthorityWorld>.Destroy(); }
        }

        // The nearest older protocol differs only in the version field of an otherwise well-formed
        // packet, which is what a peer that missed one release sends.
        [Test]
        public void ServerDisconnectsAPeerSpeakingThePreviousProtocolVersion()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                MemoryNetworkTransport.CreatePair(new ConnectionId(956),
                    out var client, out var endpoint);
                using (client)
                using (endpoint)
                using (var server = new NetworkServer<AuthorityWorld>(schema,
                           static (_, _) => true))
                {
                    var session = server.AddConnection(endpoint, 1, 1, new ScopeId(71));
                    var hello = Packet(PacketKind.Hello, 0, 1);
                    hello.CompatibilityTag = PacketHeader.CompatibilityTagOf(schema.Fingerprint, 0, 0);
                    Assert.That(NetworkPacket.TryEncode(Buffers, hello,
                        HelloPayload(schema.Fingerprint), out var packet), Is.True);
                    Assert.That(client.TrySend(WithPreviousProtocolVersion(packet)), Is.True);

                    server.Receive();

                    Assert.That(session.State, Is.EqualTo(NetworkSessionState.Closed));
                    Assert.That(server.ConnectionCount, Is.Zero);
                    while (client.TryReceive(out var answer))
                    {
                        try
                        {
                            Assert.That(NetworkPacket.TryDecode(answer, out var header, out _),
                                Is.True);
                            Assert.That(header.Kind, Is.EqualTo(PacketKind.Disconnect),
                                "The peer is never told it was admitted.");
                        }
                        finally { answer.Dispose(); }
                    }
                }
            }
            finally { World<AuthorityWorld>.Destroy(); }
        }

        [Test]
        public void ClientClosesTheSessionWhenTheServerSpeaksThePreviousProtocolVersion()
        {
            CreateReplicationWorld<ClientAWorld>(false);
            try
            {
                var schema = Schema<ClientAWorld>(false);
                MemoryNetworkTransport.CreatePair(new ConnectionId(957),
                    out var clientTransport, out var serverTransport);
                using (clientTransport)
                using (serverTransport)
                using (var client = new NetworkClient<ClientAWorld>(clientTransport,
                           schema, new ScopeId(1)))
                {
                    Assert.That(client.Session.Admit(schema.Fingerprint, 1, 1,
                        new ScopeId(1)), Is.EqualTo(NetworkAdmissionResult.Accepted));
                    var chunk = Packet(PacketKind.SnapshotChunk, 1, 1);
                    chunk.Flags = PacketFlags.UnreliableSequenced;
                    chunk.ServerTick = 1;
                    chunk.CompatibilityTag = PacketHeader.CompatibilityTagOf(schema.Fingerprint, 0, 0);
                    Assert.That(NetworkPacket.TryEncode(Buffers, chunk,
                        ReadOnlySpan<byte>.Empty, out var packet), Is.True);
                    Assert.That(serverTransport.TrySend(WithPreviousProtocolVersion(packet)),
                        Is.True);

                    client.Process();

                    Assert.That(client.Session.State, Is.EqualTo(NetworkSessionState.Closed));
                    Assert.That(client.TryConsumeRecoveryTransition(out var recovery), Is.True);
                    Assert.That(recovery.Phase,
                        Is.EqualTo(NetworkRecoveryPhase.DisconnectRequired));
                    Assert.That(recovery.Reason,
                        Is.EqualTo(NetworkRecoveryReason.ProtocolIncompatible));
                }
            }
            finally { World<ClientAWorld>.Destroy(); }
        }

        // Rewrites the version field and the header checksum, so the version is the only thing
        // wrong with the packet. Consumes the given lease.
        private static NetworkBufferLease WithPreviousProtocolVersion(NetworkBufferLease packet)
        {
            const ushort Previous = ProtocolLimits.Version - 1;
            var bytes = packet.Memory.ToArray();
            packet.Dispose();
            bytes[4] = (byte)Previous;
            bytes[5] = (byte)(Previous >> 8);
            Write32(bytes, PacketHeader.Size - 4, 0);
            Write32(bytes, PacketHeader.Size - 4, Crc32(bytes.AsSpan(0, PacketHeader.Size)));
            Assert.That(PacketHeader.TryRead(bytes, out _), Is.False);
            return Buffers.Copy(bytes);
        }

        private static List<int> DrainAcceptedCommands(
            EventReceiver<AuthorityWorld, NetworkCommandAcceptedEvent<TestCommand>> receiver)
        {
            var accepted = new List<int>();
            foreach (World<AuthorityWorld>
                         .Event<NetworkCommandAcceptedEvent<TestCommand>> item in receiver)
                accepted.Add(item.Value.Command.Value);
            return accepted;
        }

        private static NetworkTraceEvent LastCommandBatchDecode(TraceCollector observer)
        {
            for (var i = observer.Events.Count - 1; i >= 0; i--)
                if (observer.Events[i].Phase == NetworkPhase.Decode &&
                    observer.Events[i].PacketKind == NetworkPacketKind.CommandBatch)
                    return observer.Events[i];
            throw new InvalidOperationException("No command batch was decoded.");
        }
    }
}
