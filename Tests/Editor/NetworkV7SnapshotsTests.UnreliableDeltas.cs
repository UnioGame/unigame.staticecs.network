namespace UniGame.StaticEcs.Network.Tests
{
    using System;
    using System.Collections.Generic;
    using FFS.Libraries.StaticEcs;
    using NUnit.Framework;

    /// <summary>Covers <see cref="NetworkServerSettings.UnreliableDeltaChunks"/>: which snapshots leave the reliable channel and what a lost one costs.</summary>
    public sealed partial class NetworkV7Tests
    {
        [Test]
        public void UnreliableDeltaLeavesTheKeyframeOnTheReliableChannel()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                MemoryNetworkTransport.CreatePair(new ConnectionId(921),
                    out var client, out var endpoint);
                using (client)
                using (endpoint)
                using (var server = new NetworkServer<AuthorityWorld>(schema,
                           static (_, _) => true,
                           settings: new NetworkServerSettings { UnreliableDeltaChunks = 1 }))
                {
                    var entity = EstablishUnreliablePeer(server, endpoint, client, schema);
                    Assert.That(ReceiveSnapshotPackets(client, out var keyframe)[0].Flags,
                        Is.EqualTo(PacketFlags.ReliableOrdered));
                    Assert.That(keyframe.PayloadKind, Is.EqualTo(SnapshotPayloadKind.Keyframe));
                    SendPeerPacket(client, schema.Fingerprint, PacketKind.Ack, 1, 2, 1);
                    server.Receive();

                    entity.Set(new TestComponent { Value = 2 });
                    server.Tick(_ => { });
                    var packets = ReceiveSnapshotPackets(client, out var delta);
                    Assert.That(packets.Count, Is.EqualTo(1));
                    Assert.That(packets[0].Flags, Is.EqualTo(PacketFlags.UnreliableSequenced));
                    Assert.That(delta.PayloadKind, Is.EqualTo(SnapshotPayloadKind.Delta));
                    Assert.That(delta.BaselineTick, Is.EqualTo(1));

                    var diagnostics = server.CaptureReplicationDiagnostics();
                    Assert.That(diagnostics.DeltaSnapshotsUnreliable, Is.EqualTo(1));
                    Assert.That(diagnostics.DeltaSnapshotsOverDatagram, Is.Zero);
                }
            }
            finally { World<AuthorityWorld>.Destroy(); }
        }

        [Test]
        public void UnreliableDeltaIsCutToTheDatagramAndALargerOneStaysReliable()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                MemoryNetworkTransport.CreatePair(new ConnectionId(922),
                    out var client, out var endpoint);
                using (client)
                using (var transport = new LimitedNetworkTransport(endpoint,
                           endpoint.MaxUnreliablePayloadBytes))
                using (var server = new NetworkServer<AuthorityWorld>(schema,
                           static (_, _) => true,
                           settings: new NetworkServerSettings { UnreliableDeltaChunks = 2 }))
                {
                    var entity = EstablishUnreliablePeer(server, transport, client, schema);
                    ReceiveSnapshotPackets(client, out _);
                    SendPeerPacket(client, schema.Fingerprint, PacketKind.Ack, 1, 2, 1);
                    server.Receive();

                    entity.Set(new TestComponent { Value = 2 });
                    server.Tick(_ => { });
                    var whole = ReceiveSnapshotPackets(client, out _);
                    Assert.That(whole.Count, Is.EqualTo(1));
                    var headers = PacketHeader.Size + SnapshotChunkHeader.Size;
                    var body = (int)whole[0].PayloadLength - SnapshotChunkHeader.Size;
                    Assert.That(body, Is.GreaterThan(2));

                    // Half the body per datagram: the same delta needs both allowed chunks.
                    transport.MaxUnreliablePayloadBytes = headers + (body + 1) / 2;
                    entity.Set(new TestComponent { Value = 3 });
                    server.Tick(_ => { });
                    var halves = ReceiveSnapshotPackets(client, out var cut);
                    Assert.That(halves.Count, Is.EqualTo(2));
                    Assert.That(cut.ChunkCount, Is.EqualTo(2));
                    foreach (var half in halves)
                    {
                        Assert.That(half.Flags, Is.EqualTo(PacketFlags.UnreliableSequenced));
                        Assert.That(PacketHeader.Size + (int)half.PayloadLength,
                            Is.LessThanOrEqualTo(transport.MaxUnreliablePayloadBytes));
                    }

                    // One body byte per datagram is more chunks than allowed.
                    transport.MaxUnreliablePayloadBytes = headers + 1;
                    entity.Set(new TestComponent { Value = 4 });
                    server.Tick(_ => { });
                    var reliable = ReceiveSnapshotPackets(client, out var uncut);
                    Assert.That(reliable.Count, Is.EqualTo(1));
                    Assert.That(reliable[0].Flags, Is.EqualTo(PacketFlags.ReliableOrdered));
                    Assert.That(uncut.PayloadKind, Is.EqualTo(SnapshotPayloadKind.Delta));

                    var diagnostics = server.CaptureReplicationDiagnostics();
                    Assert.That(diagnostics.DeltaSnapshots, Is.EqualTo(3));
                    Assert.That(diagnostics.DeltaSnapshotsUnreliable, Is.EqualTo(2));
                    Assert.That(diagnostics.DeltaSnapshotsOverDatagram, Is.EqualTo(2),
                        "The two-chunk unreliable delta and the reliable one above the datagram.");
                }
            }
            finally { World<AuthorityWorld>.Destroy(); }
        }

        [Test]
        public void LostUnreliableDeltaIsSupersededByTheNextAndALateOneIsIgnored()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            try
            {
                MemoryNetworkTransport.CreatePair(new ConnectionId(923),
                    out var clientTransport, out var serverTransport);
                using (clientTransport)
                using (serverTransport)
                using (var server = new NetworkServer<AuthorityWorld>(
                           Schema<AuthorityWorld>(true), static (_, _) => true,
                           settings: new NetworkServerSettings { UnreliableDeltaChunks = 2 }))
                using (var client = new NetworkClient<ClientAWorld>(clientTransport,
                           Schema<ClientAWorld>(false), new ScopeId(1)))
                {
                    var entity = World<AuthorityWorld>.NewEntity<TestEntity>();
                    entity.Set(new TestComponent { Value = 1 });
                    server.AddConnection(serverTransport, 1, 1, new ScopeId(1));
                    Assert.That(client.BeginHandshake(), Is.True);
                    server.Receive();
                    server.Tick(_ => { });
                    client.Process();
                    Assert.That(ReadReplicaValue(), Is.EqualTo(1));
                    server.Receive();

                    // The delta of this tick never arrives; it is kept to arrive late below.
                    entity.Set(new TestComponent { Value = 2 });
                    server.Tick(_ => { });
                    Assert.That(clientTransport.TryReceive(out var lost), Is.True);

                    entity.Set(new TestComponent { Value = 3 });
                    server.Tick(_ => { });
                    client.Process();
                    Assert.That(client.AcknowledgedSnapshotTick, Is.EqualTo(server.ServerTick));
                    Assert.That(ReadReplicaValue(), Is.EqualTo(3));

                    Assert.That(serverTransport.TrySend(lost), Is.True);
                    client.Process();
                    Assert.That(ReadReplicaValue(), Is.EqualTo(3));
                    Assert.That(client.Session.State,
                        Is.EqualTo(NetworkSessionState.Established));
                    // The only transition is the one the first keyframe left behind.
                    Assert.That(client.TryConsumeRecoveryTransition(out var transition), Is.True);
                    Assert.That(transition.Phase, Is.EqualTo(NetworkRecoveryPhase.None),
                        "Neither the gap nor the late delta asks for a keyframe.");

                    server.Receive();
                    entity.Set(new TestComponent { Value = 4 });
                    server.Tick(_ => { });
                    client.Process();
                    Assert.That(ReadReplicaValue(), Is.EqualTo(4));
                    Assert.That(server.CaptureReplicationDiagnostics().KeyframeSnapshots,
                        Is.EqualTo(1));
                }
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
                World<ClientAWorld>.Destroy();
            }
        }

        [Test]
        public void LostChunkOfATwoChunkUnreliableDeltaCostsOnlyThatSnapshot()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            try
            {
                MemoryNetworkTransport.CreatePair(new ConnectionId(924),
                    out var clientTransport, out var memoryServer);
                using (clientTransport)
                using (var serverTransport = new LimitedNetworkTransport(memoryServer,
                           memoryServer.MaxUnreliablePayloadBytes))
                using (var server = new NetworkServer<AuthorityWorld>(
                           Schema<AuthorityWorld>(true), static (_, _) => true,
                           settings: new NetworkServerSettings
                           {
                               UnreliableDeltaChunks =
                                   NetworkServerSettings.MaximumUnreliableDeltaChunks,
                           }))
                using (var client = new NetworkClient<ClientAWorld>(clientTransport,
                           Schema<ClientAWorld>(false), new ScopeId(1)))
                {
                    var entity = World<AuthorityWorld>.NewEntity<TestEntity>();
                    entity.Set(new TestComponent { Value = 1 });
                    server.AddConnection(serverTransport, 1, 1, new ScopeId(1));
                    Assert.That(client.BeginHandshake(), Is.True);
                    server.Receive();
                    server.Tick(_ => { });
                    client.Process();
                    server.Receive();

                    // Learn the body of a one-value delta, then give a datagram half of it.
                    entity.Set(new TestComponent { Value = 2 });
                    server.Tick(_ => { });
                    var whole = ReceiveSnapshotPackets(clientTransport, out _);
                    Assert.That(whole.Count, Is.EqualTo(1));
                    var body = (int)whole[0].PayloadLength - SnapshotChunkHeader.Size;
                    serverTransport.MaxUnreliablePayloadBytes = PacketHeader.Size +
                        SnapshotChunkHeader.Size + (body + 1) / 2;

                    entity.Set(new TestComponent { Value = 3 });
                    server.Tick(_ => { });
                    Assert.That(clientTransport.TryReceive(out var firstHalf), Is.True);
                    firstHalf.Dispose();
                    client.Process();
                    Assert.That(ReadReplicaValue(), Is.EqualTo(1),
                        "Half a delta applies nothing.");

                    entity.Set(new TestComponent { Value = 4 });
                    server.Tick(_ => { });
                    client.Process();
                    Assert.That(client.AcknowledgedSnapshotTick, Is.EqualTo(server.ServerTick));
                    Assert.That(ReadReplicaValue(), Is.EqualTo(4));
                    Assert.That(client.TryConsumeRecoveryTransition(out var transition), Is.True);
                    Assert.That(transition.Phase, Is.EqualTo(NetworkRecoveryPhase.None),
                        "The half-received delta is superseded, not recovered with a keyframe.");
                    Assert.That(server.CaptureReplicationDiagnostics().KeyframeSnapshots,
                        Is.EqualTo(1));
                    var diagnostics = server.CaptureReplicationDiagnostics();
                    Assert.That(diagnostics.DeltaSnapshotsUnreliable, Is.EqualTo(3));
                    Assert.That(diagnostics.DeltaSnapshotsOverDatagram, Is.EqualTo(2));
                }
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
                World<ClientAWorld>.Destroy();
            }
        }

        [Test]
        public void WindowForgetsAnUnreliableDeltaThePeerDidNotAcknowledgeInTime()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                MemoryNetworkTransport.CreatePair(new ConnectionId(925),
                    out var client, out var endpoint);
                using (client)
                using (var transport = new WindowedNetworkTransport(endpoint))
                using (var server = new NetworkServer<AuthorityWorld>(schema,
                           static (_, _) => true,
                           snapshotAdmissionMode: NetworkSnapshotAdmissionMode.Windowed,
                           maxSnapshotsInFlight: 2,
                           settings: new NetworkServerSettings
                           {
                               UnreliableDeltaChunks = 1,
                               UnreliableDeltaLostTicks = 3,
                           }))
                {
                    EstablishUnreliablePeer(server, transport, client, schema);
                    ReceiveSnapshotPackets(client, out _);
                    SendPeerPacket(client, schema.Fingerprint, PacketKind.Ack, 1, 2, 1);
                    server.Receive();

                    // Ticks 2 and 3 fill the window and are never acknowledged.
                    server.Tick(_ => { });
                    ReceiveSnapshotPackets(client, out _);
                    server.Tick(_ => { });
                    ReceiveSnapshotPackets(client, out _);
                    server.Tick(_ => { });
                    AssertNoPacket(client, "two unacknowledged deltas fill the window");

                    // Three ticks after tick 2 it counts as lost and its slot is free again.
                    server.Tick(_ => { });
                    ReceiveSnapshotPackets(client, out var resumed);
                    Assert.That(resumed.SnapshotTick, Is.EqualTo(5));
                    Assert.That(resumed.BaselineTick, Is.EqualTo(1));
                    server.Tick(_ => { });
                    ReceiveSnapshotPackets(client, out var next);
                    Assert.That(next.SnapshotTick, Is.EqualTo(6));
                    Assert.That(server.CaptureReplicationDiagnostics()
                        .UnreliableDeltaWindowExpirations, Is.EqualTo(2));
                }
            }
            finally { World<AuthorityWorld>.Destroy(); }
        }

        [Test]
        public void WindowKeepsAnUnacknowledgedKeyframeHoweverLongItTakes()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                MemoryNetworkTransport.CreatePair(new ConnectionId(926),
                    out var client, out var endpoint);
                using (client)
                using (var transport = new WindowedNetworkTransport(endpoint))
                using (var server = new NetworkServer<AuthorityWorld>(schema,
                           static (_, _) => true,
                           snapshotAdmissionMode: NetworkSnapshotAdmissionMode.Windowed,
                           maxSnapshotsInFlight: 2,
                           settings: new NetworkServerSettings
                           {
                               UnreliableDeltaChunks = 1,
                               UnreliableDeltaLostTicks = 2,
                           }))
                {
                    EstablishUnreliablePeer(server, transport, client, schema);
                    ReceiveSnapshotPackets(client, out _);
                    for (var tick = 0; tick < 6; tick++)
                    {
                        server.Tick(_ => { });
                        AssertNoPacket(client, "the reliable keyframe is not repeated");
                    }
                    Assert.That(server.CaptureReplicationDiagnostics()
                        .UnreliableDeltaWindowExpirations, Is.Zero);
                }
            }
            finally { World<AuthorityWorld>.Destroy(); }
        }

        [Test]
        public void KeyframeIsNotHeldBehindUnacknowledgedUnreliableDeltas()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                MemoryNetworkTransport.CreatePair(new ConnectionId(929),
                    out var client, out var endpoint);
                using (client)
                using (var transport = new WindowedNetworkTransport(endpoint))
                using (var server = new NetworkServer<AuthorityWorld>(schema,
                           static (_, _) => true, historyTicks: 4,
                           snapshotAdmissionMode: NetworkSnapshotAdmissionMode.Windowed,
                           maxSnapshotsInFlight: 8,
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

                    // Ticks 2 and 3 travel unreliably and are never acknowledged.
                    for (var tick = 2; tick <= 3; tick++)
                    {
                        server.Tick(_ => { });
                        var sent = ReceiveSnapshotPackets(client, out var delta);
                        Assert.That(sent[0].Flags, Is.EqualTo(PacketFlags.UnreliableSequenced));
                        Assert.That(delta.SnapshotTick, Is.EqualTo((uint)tick));
                    }

                    // The transport accepts nothing while the baseline of tick 1 leaves the
                    // history, so the keyframe that replaces the deltas is refused as well.
                    transport.RefuseSends = true;
                    for (var tick = 4; tick <= 9; tick++)
                        server.Tick(_ => { });
                    AssertNoPacket(client, "the transport refused every packet");

                    transport.RefuseSends = false;
                    server.Tick(_ => { });
                    var recovered = ReceiveSnapshotPackets(client, out var keyframe);
                    Assert.That(keyframe.PayloadKind, Is.EqualTo(SnapshotPayloadKind.Keyframe));
                    Assert.That(keyframe.SnapshotTick, Is.EqualTo(10));
                    Assert.That(recovered[0].Flags, Is.EqualTo(PacketFlags.ReliableOrdered));
                    Assert.That(server.CaptureReplicationDiagnostics()
                        .UnreliableDeltaWindowExpirations, Is.EqualTo(2));

                    // The keyframe itself is reliable and keeps the one slot until acknowledged.
                    server.Tick(_ => { });
                    AssertNoPacket(client, "the keyframe is not repeated");
                    SendPeerPacket(client, schema.Fingerprint, PacketKind.Ack, 1, 3, 10);
                    server.Receive();
                    server.Tick(_ => { });
                    ReceiveSnapshotPackets(client, out var resumed);
                    Assert.That(resumed.PayloadKind, Is.EqualTo(SnapshotPayloadKind.Delta));
                    Assert.That(resumed.BaselineTick, Is.EqualTo(10));
                }
            }
            finally { World<AuthorityWorld>.Destroy(); }
        }

        [Test]
        public void WindowKeepsAnUnacknowledgedReliableDelta()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                MemoryNetworkTransport.CreatePair(new ConnectionId(930),
                    out var client, out var endpoint);
                using (client)
                using (var transport = new WindowedNetworkTransport(endpoint))
                using (var server = new NetworkServer<AuthorityWorld>(schema,
                           static (_, _) => true,
                           snapshotAdmissionMode: NetworkSnapshotAdmissionMode.Windowed,
                           maxSnapshotsInFlight: 2,
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

                    // One body byte per datagram: every delta needs more chunks than allowed and
                    // stays on the reliable channel.
                    transport.MaxUnreliablePayloadBytes =
                        PacketHeader.Size + SnapshotChunkHeader.Size + 1;
                    for (var value = 2; value <= 3; value++)
                    {
                        entity.Set(new TestComponent { Value = value });
                        server.Tick(_ => { });
                        var sent = ReceiveSnapshotPackets(client, out _);
                        Assert.That(sent[0].Flags, Is.EqualTo(PacketFlags.ReliableOrdered));
                    }

                    for (var tick = 0; tick < 6; tick++)
                    {
                        entity.Set(new TestComponent { Value = 10 + tick });
                        server.Tick(_ => { });
                        AssertNoPacket(client, "a reliable delta is resent, not lost, and keeps its slot");
                    }
                    Assert.That(server.CaptureReplicationDiagnostics()
                        .UnreliableDeltaWindowExpirations, Is.Zero);
                }
            }
            finally { World<AuthorityWorld>.Destroy(); }
        }

        // A peer drops, without acknowledging it, a reliable delta that a later unreliable one
        // overtook. Such a delta must not hold the single slot a due keyframe needs.
        [Test]
        public void KeyframeIsNotHeldBehindAnUnacknowledgedReliableDelta()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                MemoryNetworkTransport.CreatePair(new ConnectionId(933),
                    out var client, out var endpoint);
                using (client)
                using (var transport = new WindowedNetworkTransport(endpoint))
                using (var server = new NetworkServer<AuthorityWorld>(schema,
                           static (_, _) => true, historyTicks: 4,
                           snapshotAdmissionMode: NetworkSnapshotAdmissionMode.Windowed,
                           maxSnapshotsInFlight: 8,
                           settings: new NetworkServerSettings
                           {
                               UnreliableDeltaChunks = 1,
                               UnreliableDeltaLostTicks = 100,
                           }))
                {
                    var entity = EstablishUnreliablePeer(server, transport, client, schema);
                    ReceiveSnapshotPackets(client, out _);
                    SendPeerPacket(client, schema.Fingerprint, PacketKind.Ack, 1, 2, 1);
                    server.Receive();

                    // Tick 2 does not fit the unreliable budget and travels reliably, tick 3
                    // travels unreliably; neither is acknowledged.
                    var datagramBytes = transport.MaxUnreliablePayloadBytes;
                    transport.MaxUnreliablePayloadBytes =
                        PacketHeader.Size + SnapshotChunkHeader.Size + 1;
                    entity.Set(new TestComponent { Value = 2 });
                    server.Tick(_ => { });
                    var reliable = ReceiveSnapshotPackets(client, out _);
                    Assert.That(reliable[0].Flags, Is.EqualTo(PacketFlags.ReliableOrdered));
                    transport.MaxUnreliablePayloadBytes = datagramBytes;
                    server.Tick(_ => { });
                    var unreliable = ReceiveSnapshotPackets(client, out _);
                    Assert.That(unreliable[0].Flags, Is.EqualTo(PacketFlags.UnreliableSequenced));

                    // The baseline of tick 1 leaves the history while the transport refuses
                    // everything, so a keyframe becomes due with both deltas still in the window.
                    transport.RefuseSends = true;
                    for (var tick = 4; tick <= 9; tick++)
                        server.Tick(_ => { });
                    AssertNoPacket(client, "the transport refused every packet");

                    transport.RefuseSends = false;
                    server.Tick(_ => { });
                    ReceiveSnapshotPackets(client, out var keyframe);
                    Assert.That(keyframe.PayloadKind, Is.EqualTo(SnapshotPayloadKind.Keyframe));
                    Assert.That(keyframe.SnapshotTick, Is.EqualTo(10));
                    Assert.That(server.CaptureReplicationDiagnostics()
                        .UnreliableDeltaWindowExpirations, Is.EqualTo(1),
                        "the counter covers unreliable deltas only");

                    server.Tick(_ => { });
                    AssertNoPacket(client, "the keyframe keeps the one slot until acknowledged");
                }
            }
            finally { World<AuthorityWorld>.Destroy(); }
        }

        // Releasing and expiring compact the window; each remaining tick must keep its own kind,
        // or a reliable delta would time out as an unreliable one.
        [Test]
        public void WindowKeepsTheKindOfEachSnapshotAfterAPartialAcknowledgement()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                MemoryNetworkTransport.CreatePair(new ConnectionId(934),
                    out var client, out var endpoint);
                using (client)
                using (var transport = new WindowedNetworkTransport(endpoint))
                using (var server = new NetworkServer<AuthorityWorld>(schema,
                           static (_, _) => true,
                           snapshotAdmissionMode: NetworkSnapshotAdmissionMode.Windowed,
                           maxSnapshotsInFlight: 3,
                           settings: new NetworkServerSettings
                           {
                               UnreliableDeltaChunks = 1,
                               UnreliableDeltaLostTicks = 3,
                           }))
                {
                    var entity = EstablishUnreliablePeer(server, transport, client, schema);
                    ReceiveSnapshotPackets(client, out _);
                    SendPeerPacket(client, schema.Fingerprint, PacketKind.Ack, 1, 2, 1);
                    server.Receive();

                    // Ticks 2 and 4 travel unreliably, tick 3 reliably.
                    var datagramBytes = transport.MaxUnreliablePayloadBytes;
                    server.Tick(_ => { });
                    Assert.That(ReceiveSnapshotPackets(client, out _)[0].Flags,
                        Is.EqualTo(PacketFlags.UnreliableSequenced));
                    transport.MaxUnreliablePayloadBytes =
                        PacketHeader.Size + SnapshotChunkHeader.Size + 1;
                    entity.Set(new TestComponent { Value = 3 });
                    server.Tick(_ => { });
                    Assert.That(ReceiveSnapshotPackets(client, out _)[0].Flags,
                        Is.EqualTo(PacketFlags.ReliableOrdered));
                    transport.MaxUnreliablePayloadBytes = datagramBytes;
                    server.Tick(_ => { });
                    Assert.That(ReceiveSnapshotPackets(client, out _)[0].Flags,
                        Is.EqualTo(PacketFlags.UnreliableSequenced));

                    // Acknowledging tick 2 frees one slot: the window holds ticks 3, 4 and 5.
                    SendPeerPacket(client, schema.Fingerprint, PacketKind.Ack, 1, 3, 2);
                    server.Receive();
                    server.Tick(_ => { });
                    ReceiveSnapshotPackets(client, out var fifth);
                    Assert.That(fifth.SnapshotTick, Is.EqualTo(5));

                    // Tick 6: nothing is three ticks old except the reliable tick 3, which stays.
                    server.Tick(_ => { });
                    AssertNoPacket(client, "the reliable delta of tick 3 does not time out");
                    Assert.That(server.CaptureReplicationDiagnostics()
                        .UnreliableDeltaWindowExpirations, Is.Zero);

                    // Tick 7: the unreliable tick 4 timed out and gives its slot away.
                    server.Tick(_ => { });
                    ReceiveSnapshotPackets(client, out var seventh);
                    Assert.That(seventh.SnapshotTick, Is.EqualTo(7));
                    Assert.That(server.CaptureReplicationDiagnostics()
                        .UnreliableDeltaWindowExpirations, Is.EqualTo(1));
                }
            }
            finally { World<AuthorityWorld>.Destroy(); }
        }

        [Test]
        public void UnreliableDeltasRejectApplicationAckAdmission()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                Assert.Throws<ArgumentException>(() =>
                    new NetworkServer<AuthorityWorld>(schema, static (_, _) => true,
                        snapshotAdmissionMode: NetworkSnapshotAdmissionMode.ApplicationAck,
                        settings: new NetworkServerSettings { UnreliableDeltaChunks = 1 })
                        .Dispose());
            }
            finally { World<AuthorityWorld>.Destroy(); }
        }

        [Test]
        public void UnreliableDeltaIsAcknowledgedUnreliablyAndALostAcknowledgementIsCoveredByTheNext()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            try
            {
                MemoryNetworkTransport.CreatePair(new ConnectionId(927),
                    out var clientTransport, out var serverTransport);
                using (clientTransport)
                using (serverTransport)
                using (var server = new NetworkServer<AuthorityWorld>(
                           Schema<AuthorityWorld>(true), static (_, _) => true,
                           settings: new NetworkServerSettings { UnreliableDeltaChunks = 2 }))
                using (var client = new NetworkClient<ClientAWorld>(clientTransport,
                           Schema<ClientAWorld>(false), new ScopeId(1)))
                {
                    var entity = World<AuthorityWorld>.NewEntity<TestEntity>();
                    entity.Set(new TestComponent { Value = 1 });
                    server.AddConnection(serverTransport, 1, 1, new ScopeId(1));
                    Assert.That(client.BeginHandshake(), Is.True);
                    server.Receive();
                    server.Tick(_ => { });
                    client.Process();
                    var keyframeAck = TakeAcknowledgement(serverTransport, out var keyframeHeader);
                    Assert.That(keyframeHeader.Flags, Is.EqualTo(PacketFlags.ReliableOrdered));
                    Assert.That(clientTransport.TrySend(keyframeAck), Is.True);
                    server.Receive();

                    // The acknowledgement of tick 2 is held back to arrive late below.
                    entity.Set(new TestComponent { Value = 2 });
                    server.Tick(_ => { });
                    client.Process();
                    var late = TakeAcknowledgement(serverTransport, out var lateHeader);
                    Assert.That(lateHeader.Flags, Is.EqualTo(PacketFlags.UnreliableSequenced));
                    Assert.That(lateHeader.AcknowledgedSnapshotTick, Is.EqualTo(2));

                    entity.Set(new TestComponent { Value = 3 });
                    server.Tick(_ => { });
                    client.Process();
                    Assert.That(ReadReplicaValue(), Is.EqualTo(3));
                    var next = TakeAcknowledgement(serverTransport, out var nextHeader);
                    Assert.That(nextHeader.Flags, Is.EqualTo(PacketFlags.UnreliableSequenced));
                    Assert.That(nextHeader.AcknowledgedSnapshotTick, Is.EqualTo(3));
                    Assert.That(clientTransport.TrySend(next), Is.True);
                    Assert.That(clientTransport.TrySend(late), Is.True);
                    server.Receive();
                    Assert.That(server.CaptureReplicationDiagnostics().UnreliableAcknowledgements,
                        Is.EqualTo(2));

                    entity.Set(new TestComponent { Value = 4 });
                    server.Tick(_ => { });
                    ReceiveSnapshotPackets(clientTransport, out var delta);
                    Assert.That(delta.PayloadKind, Is.EqualTo(SnapshotPayloadKind.Delta));
                    Assert.That(delta.BaselineTick, Is.EqualTo(3),
                        "The late acknowledgement of tick 2 does not move the baseline back.");

                    // Unreliable acknowledgements stay out of the reliable packet numbering.
                    Assert.That(client.SynchronizeClock(), Is.True);
                    server.Receive();
                    Assert.That(clientTransport.TryReceive(out var pong), Is.True);
                    try
                    {
                        Assert.That(NetworkPacket.TryDecode(pong, out var pongHeader, out _),
                            Is.True);
                        Assert.That(pongHeader.Kind, Is.EqualTo(PacketKind.Pong));
                    }
                    finally { pong.Dispose(); }
                    Assert.That(server.CaptureReplicationDiagnostics().KeyframeSnapshots,
                        Is.EqualTo(1));
                }
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
                World<ClientAWorld>.Destroy();
            }
        }

        [Test]
        public void ReliableDeltaIsAcknowledgedReliably()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            try
            {
                MemoryNetworkTransport.CreatePair(new ConnectionId(928),
                    out var clientTransport, out var serverTransport);
                using (clientTransport)
                using (serverTransport)
                using (var server = new NetworkServer<AuthorityWorld>(
                           Schema<AuthorityWorld>(true), static (_, _) => true))
                using (var client = new NetworkClient<ClientAWorld>(clientTransport,
                           Schema<ClientAWorld>(false), new ScopeId(1)))
                {
                    var entity = World<AuthorityWorld>.NewEntity<TestEntity>();
                    entity.Set(new TestComponent { Value = 1 });
                    server.AddConnection(serverTransport, 1, 1, new ScopeId(1));
                    Assert.That(client.BeginHandshake(), Is.True);
                    server.Receive();
                    server.Tick(_ => { });
                    client.Process();
                    server.Receive();

                    entity.Set(new TestComponent { Value = 2 });
                    server.Tick(_ => { });
                    client.Process();
                    Assert.That(ReadReplicaValue(), Is.EqualTo(2));
                    TakeAcknowledgement(serverTransport, out var header).Dispose();
                    Assert.That(header.Flags, Is.EqualTo(PacketFlags.ReliableOrdered));
                    Assert.That(server.CaptureReplicationDiagnostics().UnreliableAcknowledgements,
                        Is.Zero);
                }
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
                World<ClientAWorld>.Destroy();
            }
        }

        // Takes the one acknowledgement the client left on the wire; the caller decides whether it arrives.
        private static NetworkBufferLease TakeAcknowledgement(INetworkTransport serverSide,
            out PacketHeader header)
        {
            Assert.That(serverSide.TryReceive(out var packet), Is.True);
            Assert.That(NetworkPacket.TryDecode(packet, out header, out _), Is.True);
            Assert.That(header.Kind, Is.EqualTo(PacketKind.Ack));
            return packet;
        }

        [Test]
        public void StaleAndRepeatedUnreliableAcknowledgementsDoNotMoveTheBaseline()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                MemoryNetworkTransport.CreatePair(new ConnectionId(931),
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
                    for (var tick = 2; tick <= 3; tick++)
                    {
                        server.Tick(_ => { });
                        ReceiveSnapshotPackets(client, out _);
                    }

                    // The acknowledgement of tick 3 overtakes the one of tick 2, which then
                    // arrives twice.
                    SendUnreliableAcknowledgement(client, schema.Fingerprint, 1, 2, 3);
                    SendUnreliableAcknowledgement(client, schema.Fingerprint, 1, 1, 2);
                    SendUnreliableAcknowledgement(client, schema.Fingerprint, 1, 1, 2);
                    server.Receive();

                    server.Tick(_ => { });
                    ReceiveSnapshotPackets(client, out var delta);
                    Assert.That(delta.PayloadKind, Is.EqualTo(SnapshotPayloadKind.Delta));
                    Assert.That(delta.BaselineTick, Is.EqualTo(3));
                    var diagnostics = server.CaptureReplicationDiagnostics();
                    Assert.That(diagnostics.KeyframeSnapshots, Is.EqualTo(1));
                    Assert.That(diagnostics.UnreliableAcknowledgements, Is.EqualTo(3));
                    Assert.That(server.ConnectionCount, Is.EqualTo(1));
                }
            }
            finally { World<AuthorityWorld>.Destroy(); }
        }

        [Test]
        public void CommandBatchIsAcceptedAfterAnUnreliableAcknowledgementNumberedAheadOfIt()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            var receiver = World<AuthorityWorld>
                .RegisterEventReceiver<NetworkCommandAcceptedEvent<TestCommand>>();
            try
            {
                using var pool = new NetworkBufferPool(1 << 20);
                MemoryNetworkTransport.CreatePair(new ConnectionId(932),
                    out var clientTransport, out var serverTransport);
                using (clientTransport)
                using (serverTransport)
                {
                    var serverSchema = Schema<AuthorityWorld>(true);
                    var clientSchema = Schema<ClientAWorld>(false);
                    using var server = new NetworkServer<AuthorityWorld>(
                        serverSchema, static (_, _) => true, bufferPool: pool,
                        settings: new NetworkServerSettings { UnreliableDeltaChunks = 1 });
                    server.AddConnection(serverTransport, 7, 1, new ScopeId(1));
                    SendHello(clientTransport, pool, clientSchema.Fingerprint);
                    server.Receive();
                    server.Tick(_ => { });

                    var clientSession = new NetworkSession<ClientAWorld>(
                        clientTransport.Connection, NetworkRole.Client, clientSchema, pool);
                    Assert.That(clientSession.Admit(serverSchema.Fingerprint, 7, 1,
                        new ScopeId(1)), Is.EqualTo(NetworkAdmissionResult.Accepted));
                    Assert.That(clientSession.CreateCommand(
                        new TestCommand { Value = 1 }, 2, out var command),
                        Is.EqualTo(NetworkCommandResult.Queued));
                    var payload = EncodeCommandBatch(command);
                    command.Dispose();

                    // The client numbers its unreliable packets with one counter: the batch got
                    // 1 and the acknowledgement 2, and the acknowledgement arrives first.
                    SendUnreliableAcknowledgement(clientTransport, serverSchema.Fingerprint, 1, 2, 1);
                    SendCommandBatch(clientTransport, pool, serverSchema.Fingerprint, 1, 1, payload);
                    server.Receive();
                    server.Tick(_ => { });

                    var accepted = new List<int>();
                    foreach (World<AuthorityWorld>
                                 .Event<NetworkCommandAcceptedEvent<TestCommand>> item in receiver)
                        accepted.Add(item.Value.Command.Value);
                    CollectionAssert.AreEqual(new[] { 1 }, accepted);
                    Assert.That(server.ConnectionCount, Is.EqualTo(1));
                    Assert.That(server.CaptureReplicationDiagnostics().UnreliableAcknowledgements,
                        Is.EqualTo(1));
                }
            }
            finally
            {
                World<AuthorityWorld>.DeleteEventReceiver(ref receiver);
                World<AuthorityWorld>.Destroy();
                World<ClientAWorld>.Destroy();
            }
        }

        private static void SendUnreliableAcknowledgement(INetworkTransport transport,
            SchemaFingerprint schema, uint epoch, uint sequence, uint acknowledgedTick)
        {
            var header = Packet(PacketKind.Ack, epoch, sequence);
            header.Flags = PacketFlags.UnreliableSequenced;
            header.CompatibilityTag = PacketHeader.CompatibilityTagOf(schema, 0, 0);
            header.AcknowledgedSnapshotTick = acknowledgedTick;
            Assert.That(NetworkPacket.TryEncode(Buffers, header, ReadOnlySpan<byte>.Empty,
                out var packet), Is.True);
            Assert.That(transport.TrySend(packet), Is.True);
        }

        // Admits peer 1 and sends the first keyframe (tick 1); returns the entity it carries.
        private static World<AuthorityWorld>.Entity EstablishUnreliablePeer(
            NetworkServer<AuthorityWorld> server, INetworkTransport endpoint,
            INetworkTransport client, NetworkSchema<AuthorityWorld> schema)
        {
            server.AddConnection(endpoint, 1, 1, new ScopeId(71));
            SendPeerPacket(client, schema.Fingerprint, PacketKind.Hello, 0, 1, 0);
            server.Receive();
            Assert.That(client.TryReceive(out var ready), Is.True);
            ready.Dispose();
            var entity = World<AuthorityWorld>.NewEntity<TestEntity>();
            entity.Set(new TestComponent { Value = 1 });
            server.Tick(_ => { });
            return entity;
        }

        // Reads every chunk packet of the one snapshot waiting on the transport.
        private static List<PacketHeader> ReceiveSnapshotPackets(INetworkTransport transport,
            out SnapshotChunkHeader chunk)
        {
            var headers = new List<PacketHeader>();
            chunk = default;
            do
            {
                Assert.That(transport.TryReceive(out var packet), Is.True);
                try
                {
                    Assert.That(NetworkPacket.TryDecode(packet, out var header,
                        out var payload), Is.True);
                    Assert.That(header.Kind, Is.EqualTo(PacketKind.SnapshotChunk));
                    Assert.That(SnapshotChunkHeader.TryRead(payload.Span, out chunk), Is.True);
                    Assert.That(chunk.ChunkIndex, Is.EqualTo((uint)headers.Count));
                    headers.Add(header);
                }
                finally
                {
                    packet.Dispose();
                }
            } while (headers.Count < chunk.ChunkCount);
            return headers;
        }
    }
}
