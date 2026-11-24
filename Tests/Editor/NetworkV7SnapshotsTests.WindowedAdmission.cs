namespace UniGame.StaticEcs.Network.Tests
{
    using System;
    using FFS.Libraries.StaticEcs;
    using NUnit.Framework;

    public sealed partial class NetworkV7Tests
    {
        [Test]
        public void WindowedAdmissionRejectsOutOfRangeInFlightLimit()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                Assert.Throws<ArgumentOutOfRangeException>(() =>
                    new NetworkServer<AuthorityWorld>(schema, static (_, _) => true,
                        snapshotAdmissionMode: NetworkSnapshotAdmissionMode.Windowed,
                        maxSnapshotsInFlight: 0).Dispose());
                Assert.Throws<ArgumentOutOfRangeException>(() =>
                    new NetworkServer<AuthorityWorld>(schema, static (_, _) => true,
                        snapshotAdmissionMode: NetworkSnapshotAdmissionMode.Windowed,
                        maxSnapshotsInFlight:
                            NetworkServer<AuthorityWorld>.MaxSnapshotsInFlightLimit + 1)
                        .Dispose());
            }
            finally { World<AuthorityWorld>.Destroy(); }
        }

        [Test]
        public void WindowedAdmissionSendsWhileDeliveryCallbackIsStillPending()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                MemoryNetworkTransport.CreatePair(new ConnectionId(911),
                    out var client, out var endpoint);
                using (client)
                using (var transport = new WindowedNetworkTransport(endpoint))
                using (var server = new NetworkServer<AuthorityWorld>(schema,
                           static (_, _) => true,
                           snapshotAdmissionMode: NetworkSnapshotAdmissionMode.Windowed,
                           maxSnapshotsInFlight: 2))
                {
                    server.AddConnection(transport, 1, 1, new ScopeId(71));
                    SendPeerPacket(client, schema.Fingerprint, PacketKind.Hello, 0, 1, 0);
                    server.Receive();
                    Assert.That(client.TryReceive(out var ready), Is.True);
                    ready.Dispose();

                    var entity = World<AuthorityWorld>.NewEntity<TestEntity>();
                    entity.Set(new TestComponent { Value = 1 });
                    server.Tick(_ => { });
                    Assert.That(ReceiveChunk(client).SnapshotTick, Is.EqualTo(1));

                    // The first keyframe is still unacknowledged: a second one would only
                    // repeat it, so the window is one until a baseline exists.
                    transport.HasPendingReliablePackets = true;
                    entity.Set(new TestComponent { Value = 2 });
                    server.Tick(_ => { });
                    AssertNoPacket(client, "an unacknowledged keyframe is not repeated");
                    Assert.That(server.CaptureSnapshotAdmissionDiagnostics()
                        .SnapshotWindowDeferrals, Is.EqualTo(1));

                    // With a baseline acknowledged, the native delivery callback of a delta
                    // may still be pending: Current mode would defer here, windowed mode
                    // still has a free slot.
                    SendPeerPacket(client, schema.Fingerprint, PacketKind.Ack, 1, 2, 1);
                    server.Receive();
                    server.Tick(_ => { });
                    var third = ReceiveChunk(client);
                    Assert.That(third.SnapshotTick, Is.EqualTo(3));
                    Assert.That(third.PayloadKind, Is.EqualTo(SnapshotPayloadKind.Delta));
                    Assert.That(third.BaselineTick, Is.EqualTo(1));
                    entity.Set(new TestComponent { Value = 4 });
                    server.Tick(_ => { });
                    Assert.That(ReceiveChunk(client).SnapshotTick, Is.EqualTo(4));

                    entity.Set(new TestComponent { Value = 5 });
                    server.Tick(_ => { });
                    AssertNoPacket(client, "two unacknowledged ticks fill the window");
                    var diagnostics = server.CaptureSnapshotAdmissionDiagnostics();
                    Assert.That(diagnostics.Mode,
                        Is.EqualTo(NetworkSnapshotAdmissionMode.Windowed));
                    Assert.That(diagnostics.SnapshotWindowDeferrals, Is.EqualTo(2));
                    Assert.That(diagnostics.PendingReliableDeferrals, Is.Zero);
                    Assert.That(diagnostics.SuppressedSnapshots, Is.Zero);

                    // Acknowledging tick 3 frees one slot even though the transport still
                    // reports native bytes in flight for tick 4.
                    SendPeerPacket(client, schema.Fingerprint, PacketKind.Ack, 1, 3, 3);
                    server.Receive();
                    entity.Set(new TestComponent { Value = 6 });
                    server.Tick(_ => { });
                    var delta = ReceiveChunk(client);
                    Assert.That(delta.SnapshotTick, Is.EqualTo(6));
                    Assert.That(delta.PayloadKind, Is.EqualTo(SnapshotPayloadKind.Delta));
                    Assert.That(delta.BaselineTick, Is.EqualTo(3));

                    // A cumulative acknowledgement releases every older in-flight tick.
                    SendPeerPacket(client, schema.Fingerprint, PacketKind.Ack, 1, 4, 6);
                    server.Receive();
                    server.Tick(_ => { });
                    Assert.That(ReceiveChunk(client).SnapshotTick, Is.EqualTo(7));
                    server.Tick(_ => { });
                    Assert.That(ReceiveChunk(client).SnapshotTick, Is.EqualTo(8));
                    Assert.That(server.CaptureSnapshotAdmissionDiagnostics()
                        .SnapshotBatchesAccepted, Is.EqualTo(6));
                }
            }
            finally { World<AuthorityWorld>.Destroy(); }
        }

        [Test]
        public void WindowedAdmissionDefersOnlyForUnsubmittedReliableBacklog()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                MemoryNetworkTransport.CreatePair(new ConnectionId(912),
                    out var client, out var endpoint);
                using (client)
                using (var transport = new WindowedNetworkTransport(endpoint))
                using (var server = new NetworkServer<AuthorityWorld>(schema,
                           static (_, _) => true,
                           snapshotAdmissionMode: NetworkSnapshotAdmissionMode.Windowed,
                           maxSnapshotsInFlight: 4))
                {
                    server.AddConnection(transport, 1, 1, new ScopeId(71));
                    SendPeerPacket(client, schema.Fingerprint, PacketKind.Hello, 0, 1, 0);
                    server.Receive();
                    Assert.That(client.TryReceive(out var ready), Is.True);
                    ready.Dispose();

                    var entity = World<AuthorityWorld>.NewEntity<TestEntity>();
                    entity.Set(new TestComponent { Value = 1 });
                    server.Tick(_ => { });
                    Assert.That(ReceiveChunk(client).SnapshotTick, Is.EqualTo(1));
                    SendPeerPacket(client, schema.Fingerprint, PacketKind.Ack, 1, 2, 1);
                    server.Receive();

                    transport.HasPendingReliablePackets = true;
                    transport.HasQueuedReliablePackets = true;
                    server.Tick(_ => { });
                    AssertNoPacket(client, "a queued reliable packet is real backpressure");
                    Assert.That(server.CaptureSnapshotAdmissionDiagnostics()
                        .PendingReliableDeferrals, Is.EqualTo(1));

                    transport.HasQueuedReliablePackets = false;
                    server.Tick(_ => { });
                    Assert.That(ReceiveChunk(client).SnapshotTick, Is.EqualTo(3));
                    Assert.That(server.CaptureSnapshotAdmissionDiagnostics()
                        .SnapshotWindowDeferrals, Is.Zero);
                }
            }
            finally { World<AuthorityWorld>.Destroy(); }
        }

        [Test]
        public void WindowedAdmissionLetsRecoveryKeyframeBypassFullWindow()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                MemoryNetworkTransport.CreatePair(new ConnectionId(913),
                    out var client, out var endpoint);
                using (client)
                using (var transport = new WindowedNetworkTransport(endpoint))
                using (var server = new NetworkServer<AuthorityWorld>(schema,
                           static (_, _) => true,
                           snapshotAdmissionMode: NetworkSnapshotAdmissionMode.Windowed,
                           maxSnapshotsInFlight: 1))
                {
                    server.AddConnection(transport, 1, 1, new ScopeId(71));
                    SendPeerPacket(client, schema.Fingerprint, PacketKind.Hello, 0, 1, 0);
                    server.Receive();
                    Assert.That(client.TryReceive(out var ready), Is.True);
                    ready.Dispose();

                    var entity = World<AuthorityWorld>.NewEntity<TestEntity>();
                    entity.Set(new TestComponent { Value = 1 });
                    server.Tick(_ => { });
                    Assert.That(ReceiveChunk(client).SnapshotTick, Is.EqualTo(1));
                    server.Tick(_ => { });
                    AssertNoPacket(client, "window of one is full until the ACK");

                    SendResyncRequest(client, schema.Fingerprint, 1, 2, 17);
                    server.Receive();
                    server.Tick(_ => { });
                    var keyframe = ReceiveChunk(client);
                    Assert.That(keyframe.SnapshotTick, Is.EqualTo(3));
                    Assert.That(keyframe.PayloadKind,
                        Is.EqualTo(SnapshotPayloadKind.Keyframe));
                    Assert.That(keyframe.ResyncCorrelationId, Is.EqualTo(17));
                }
            }
            finally { World<AuthorityWorld>.Destroy(); }
        }

        /// <summary>Reports native in-flight bytes and the unsent queue independently, like the LiteNetLib endpoint.</summary>
        private sealed class WindowedNetworkTransport : INetworkTransport,
            INetworkReliableSendState, INetworkReliableBacklogState
        {
            private readonly INetworkTransport _inner;

            internal WindowedNetworkTransport(INetworkTransport inner)
            {
                _inner = inner;
                MaxUnreliablePayloadBytes = inner.MaxUnreliablePayloadBytes;
            }

            public bool HasPendingReliablePackets { get; set; }
            public bool HasQueuedReliablePackets { get; set; }
            internal bool RefuseSends { get; set; }
            public ConnectionId Connection => _inner.Connection;
            public int MaxReliablePayloadBytes => _inner.MaxReliablePayloadBytes;
            public int MaxUnreliablePayloadBytes { get; set; }

            public bool TrySend(NetworkBufferLease packet)
            {
                if (!RefuseSends)
                    return _inner.TrySend(packet);
                packet?.Dispose();
                return false;
            }

            public bool TryReceive(out NetworkBufferLease packet) =>
                _inner.TryReceive(out packet);
            public void Dispose() => _inner.Dispose();
        }
    }
}
