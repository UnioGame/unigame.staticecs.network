namespace UniGame.StaticEcs.Network.Tests
{
    using System;
    using System.Collections.Generic;
    using FFS.Libraries.StaticEcs;
    using NUnit.Framework;

    /// <summary>
    /// Covers what a client does with the chunks of an unreliable delta the network reorders or
    /// repeats: a delta of two datagrams assembles in any order, and anything older than the
    /// snapshot in progress or applied is dropped without a recovery.
    /// </summary>
    public sealed partial class NetworkV7Tests
    {
        [Test]
        public void TwoChunkUnreliableDeltaArrivingLastChunkFirstIsAppliedOnce()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            try
            {
                using var link = new TwoChunkDeltaLink();
                var chunks = link.TakeDelta(3);

                link.Deliver(chunks[1]);
                link.Client.Process();
                Assert.That(ReadReplicaValue(), Is.EqualTo(1), "Half a delta applies nothing.");
                Assert.That(link.Client.AcknowledgedSnapshotTick, Is.EqualTo(1));

                link.Deliver(chunks[0]);
                link.Client.Process();
                Assert.That(ReadReplicaValue(), Is.EqualTo(3));
                Assert.That(link.Client.AcknowledgedSnapshotTick, Is.EqualTo(link.Server.ServerTick));

                TakeAcknowledgement(link.ServerMemory, out var acknowledgement).Dispose();
                Assert.That(acknowledgement.Flags, Is.EqualTo(PacketFlags.UnreliableSequenced));
                Assert.That(acknowledgement.AcknowledgedSnapshotTick, Is.EqualTo(link.Server.ServerTick));
                AssertNoPacket(link.ServerMemory, "one applied snapshot is acknowledged once");
                link.AssertNoRecovery();
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
                World<ClientAWorld>.Destroy();
            }
        }

        // The state of the peer's own entity rides behind the last chunk. With that chunk first it
        // reaches the client before the snapshot it belongs to and has to wait for it.
        [Test]
        public void OwnEntityStateBehindTheLastChunkSurvivesThatChunkArrivingFirst()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            try
            {
                const int Others = 6;
                var mine = SpawnBudgetEntity(1);
                mine.Set(new NetworkOwnerComponent { PeerId = 7 });
                var cellEntities = new List<World<AuthorityWorld>.Entity> { mine };
                // The other entities always win the allowance, so the peer's own entity waits in
                // the shared snapshot and its current state travels in the trailer.
                var policy = new StubCaptureBudgetPolicy
                {
                    BudgetBytes = OneByteChangeCost * Others,
                    RefreshesObserversPersonally = true,
                };
                var others = new World<AuthorityWorld>.Entity[Others];
                for (var i = 0; i < Others; i++)
                {
                    others[i] = SpawnBudgetEntity(100 + i);
                    policy.Weights[others[i].GID] = 1000;
                    cellEntities.Add(others[i]);
                }
                var cell = new ScopeId(711);
                var scope = new ScopeId(712);
                var provider = new StubCellularScopeProvider();
                provider.CellEntities[cell] = cellEntities;
                provider.ScopeCells[scope] = new[] { cell };

                MemoryNetworkTransport.CreatePair(new ConnectionId(941),
                    out var clientTransport, out var serverMemory);
                using (clientTransport)
                using (var serverTransport = new LimitedNetworkTransport(serverMemory,
                           serverMemory.MaxUnreliablePayloadBytes))
                using (var server = new NetworkServer<AuthorityWorld>(
                           Schema<AuthorityWorld>(true), static (_, _) => true,
                           scopeProvider: provider, captureBudgetPolicy: policy,
                           settings: new NetworkServerSettings { UnreliableDeltaChunks = 2 }))
                using (var client = new NetworkClient<ClientAWorld>(clientTransport,
                           Schema<ClientAWorld>(false), scope))
                {
                    server.AddConnection(serverTransport, 7, 21, scope);
                    Assert.That(client.BeginHandshake(), Is.True);
                    server.Receive();
                    server.Tick(_ => { });
                    client.Process();
                    server.Receive();

                    // This delta is lost; it only shows how long the body and the trailer are.
                    ChangeAll(mine, others, 1000);
                    server.Tick(_ => { });
                    var whole = ReceiveSnapshotPackets(clientTransport, out _);
                    Assert.That(whole.Count, Is.EqualTo(1));
                    var trailer = whole[0].TrailerLength;
                    Assert.That(trailer, Is.GreaterThan(0), "The own entity waits in the snapshot.");
                    var body = (int)whole[0].PayloadLength - SnapshotChunkHeader.Size - trailer;
                    // Two datagrams whose second one still has room for the trailer.
                    var datagramBody = (body + trailer + 1) / 2;
                    Assert.That(datagramBody, Is.LessThan(body), "The arrangement needs two chunks.");
                    serverTransport.MaxUnreliablePayloadBytes =
                        PacketHeader.Size + SnapshotChunkHeader.Size + datagramBody;

                    ChangeAll(mine, others, 2000);
                    server.Tick(_ => { });
                    var chunks = TakeUnreliableChunks(clientTransport, 2);
                    Assert.That(NetworkPacket.TryDecode(chunks[1], out var last, out _), Is.True);
                    Assert.That(last.TrailerLength, Is.EqualTo(trailer),
                        "The own-entity state rides behind the last chunk.");

                    Assert.That(serverMemory.TrySend(chunks[1]), Is.True);
                    client.Process();
                    Assert.That(CountReplicas<ClientAWorld>(1), Is.EqualTo(1),
                        "The state waits for the snapshot of its tick.");

                    Assert.That(serverMemory.TrySend(chunks[0]), Is.True);
                    client.Process();
                    Assert.That(client.AcknowledgedSnapshotTick, Is.EqualTo(server.ServerTick));
                    Assert.That(CountReplicas<ClientAWorld>(2000), Is.EqualTo(1),
                        "The owner sees its entity as of the snapshot tick.");
                    for (var i = 0; i < Others; i++)
                        Assert.That(CountReplicas<ClientAWorld>(2001 + i), Is.EqualTo(1));
                    Assert.That(client.Session.State, Is.EqualTo(NetworkSessionState.Established));
                    Assert.That(server.OwnedEntityRefreshFailures, Is.Zero);
                }
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
                World<ClientAWorld>.Destroy();
            }
        }

        [Test]
        public void ChunkOfAnOlderDeltaArrivingWhileANewerOneAssemblesIsDroppedWithoutMixing()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            try
            {
                using var link = new TwoChunkDeltaLink();
                var older = link.TakeDelta(3);
                var newer = link.TakeDelta(4);

                // Each delta has one half on each side of the other's first half.
                link.Deliver(older[1]);
                link.Client.Process();
                link.Deliver(newer[0]);
                link.Client.Process();
                link.Deliver(older[0]);
                link.Client.Process();
                Assert.That(ReadReplicaValue(), Is.EqualTo(1),
                    "The halves of the older delta never meet: the newer one replaced it.");
                Assert.That(link.Client.AcknowledgedSnapshotTick, Is.EqualTo(1));

                link.Deliver(newer[1]);
                link.Client.Process();
                Assert.That(ReadReplicaValue(), Is.EqualTo(4));
                Assert.That(link.Client.AcknowledgedSnapshotTick, Is.EqualTo(link.Server.ServerTick));

                TakeAcknowledgement(link.ServerMemory, out var acknowledgement).Dispose();
                Assert.That(acknowledgement.AcknowledgedSnapshotTick, Is.EqualTo(link.Server.ServerTick));
                AssertNoPacket(link.ServerMemory, "only the newer delta is acknowledged");
                link.AssertNoRecovery();
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
                World<ClientAWorld>.Destroy();
            }
        }

        [Test]
        public void ChunksOfAnOlderTwoChunkDeltaArrivingAfterANewerOneWasAppliedAreIgnored()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            try
            {
                using var link = new TwoChunkDeltaLink();
                var older = link.TakeDelta(3);
                var newer = link.TakeDelta(4);

                link.Deliver(newer[0]);
                link.Deliver(newer[1]);
                link.Client.Process();
                Assert.That(ReadReplicaValue(), Is.EqualTo(4));
                link.ForwardAcknowledgement();

                link.Deliver(older[0]);
                link.Deliver(older[1]);
                link.Client.Process();
                Assert.That(ReadReplicaValue(), Is.EqualTo(4), "The world does not go back a tick.");
                Assert.That(link.Client.AcknowledgedSnapshotTick, Is.EqualTo(4));
                AssertNoPacket(link.ServerMemory, "a late delta is not acknowledged");
                link.AssertNoRecovery();

                // The late chunks left nothing half-assembled behind.
                link.DeliverInOrder(link.TakeDelta(5));
                Assert.That(ReadReplicaValue(), Is.EqualTo(5));
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
                World<ClientAWorld>.Destroy();
            }
        }

        [Test]
        public void RepeatedChunksOfAnUnreliableDeltaAreIgnoredWhileItAssemblesAndAfterItWasApplied()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            try
            {
                using var link = new TwoChunkDeltaLink();
                var chunks = link.TakeDelta(3);
                var firstAgain = Buffers.Copy(chunks[0].Span);
                var firstLate = Buffers.Copy(chunks[0].Span);
                var lastLate = Buffers.Copy(chunks[1].Span);

                link.Deliver(chunks[0]);
                link.Deliver(firstAgain);
                link.Client.Process();
                Assert.That(ReadReplicaValue(), Is.EqualTo(1),
                    "The same half twice is still half a delta.");

                link.Deliver(chunks[1]);
                link.Client.Process();
                Assert.That(ReadReplicaValue(), Is.EqualTo(3));
                link.ForwardAcknowledgement();

                link.Deliver(lastLate);
                link.Deliver(firstLate);
                link.Client.Process();
                Assert.That(ReadReplicaValue(), Is.EqualTo(3));
                Assert.That(link.Client.AcknowledgedSnapshotTick, Is.EqualTo(3));
                AssertNoPacket(link.ServerMemory, "an applied snapshot is not acknowledged again");
                link.AssertNoRecovery();

                // The repeats did not start an assembly that would swallow the next delta.
                link.DeliverInOrder(link.TakeDelta(5));
                Assert.That(ReadReplicaValue(), Is.EqualTo(5));
                Assert.That(link.Server.CaptureReplicationDiagnostics().KeyframeSnapshots,
                    Is.EqualTo(1));
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
                World<ClientAWorld>.Destroy();
            }
        }

        private static void ChangeAll(World<AuthorityWorld>.Entity mine,
            World<AuthorityWorld>.Entity[] others, int value)
        {
            mine.Set(new TestComponent { Value = value });
            for (var i = 0; i < others.Length; i++)
                others[i].Set(new TestComponent { Value = value + 1 + i });
        }

        // Takes the chunk packets of the one unreliable delta waiting on the transport without
        // delivering them; the caller decides the order they arrive in.
        private static NetworkBufferLease[] TakeUnreliableChunks(INetworkTransport transport,
            int expected)
        {
            var packets = new NetworkBufferLease[expected];
            for (var i = 0; i < expected; i++)
            {
                Assert.That(transport.TryReceive(out packets[i]), Is.True);
                Assert.That(NetworkPacket.TryDecode(packets[i], out var header, out var payload),
                    Is.True);
                Assert.That(header.Kind, Is.EqualTo(PacketKind.SnapshotChunk));
                Assert.That(header.Flags, Is.EqualTo(PacketFlags.UnreliableSequenced));
                Assert.That(SnapshotChunkHeader.TryRead(payload.Span, out var chunk), Is.True);
                Assert.That(chunk.PayloadKind, Is.EqualTo(SnapshotPayloadKind.Delta));
                Assert.That(chunk.ChunkIndex, Is.EqualTo((uint)i));
                Assert.That(chunk.ChunkCount, Is.EqualTo((uint)expected));
            }
            AssertNoPacket(transport, "the delta has no further chunk");
            return packets;
        }

        /// <summary>
        /// A server and a client that applied the first keyframe, joined by a memory link whose
        /// server side cuts every later delta of the one replicated entity into two datagrams.
        /// </summary>
        private sealed class TwoChunkDeltaLink : IDisposable
        {
            private readonly MemoryNetworkTransport _clientTransport;
            private readonly LimitedNetworkTransport _serverTransport;
            private readonly World<AuthorityWorld>.Entity _entity;

            internal TwoChunkDeltaLink()
            {
                MemoryNetworkTransport.CreatePair(new ConnectionId(940),
                    out _clientTransport, out var serverMemory);
                ServerMemory = serverMemory;
                _serverTransport = new LimitedNetworkTransport(serverMemory,
                    serverMemory.MaxUnreliablePayloadBytes);
                Server = new NetworkServer<AuthorityWorld>(Schema<AuthorityWorld>(true),
                    static (_, _) => true,
                    settings: new NetworkServerSettings { UnreliableDeltaChunks = 2 });
                Client = new NetworkClient<ClientAWorld>(_clientTransport,
                    Schema<ClientAWorld>(false), new ScopeId(1));

                _entity = World<AuthorityWorld>.NewEntity<TestEntity>();
                _entity.Set(new TestComponent { Value = 1 });
                Server.AddConnection(_serverTransport, 1, 1, new ScopeId(1));
                Assert.That(Client.BeginHandshake(), Is.True);
                Server.Receive();
                Server.Tick(_ => { });
                Client.Process();
                Server.Receive();

                // The delta of tick 2 is lost; its body tells how to cut the next ones in two.
                _entity.Set(new TestComponent { Value = 2 });
                Server.Tick(_ => { });
                var whole = ReceiveSnapshotPackets(_clientTransport, out _);
                Assert.That(whole.Count, Is.EqualTo(1));
                var body = (int)whole[0].PayloadLength - SnapshotChunkHeader.Size;
                _serverTransport.MaxUnreliablePayloadBytes = PacketHeader.Size +
                    SnapshotChunkHeader.Size + (body + 1) / 2;
            }

            internal NetworkServer<AuthorityWorld> Server { get; }
            internal NetworkClient<ClientAWorld> Client { get; }

            /// <summary>The server end of the link: what the client sent and what reaches the client.</summary>
            internal MemoryNetworkTransport ServerMemory { get; }

            /// <summary>Ticks the server with a new value and returns the two chunks of its delta, undelivered.</summary>
            internal NetworkBufferLease[] TakeDelta(int value)
            {
                _entity.Set(new TestComponent { Value = value });
                Server.Tick(_ => { });
                return TakeUnreliableChunks(_clientTransport, 2);
            }

            internal void Deliver(NetworkBufferLease packet) =>
                Assert.That(ServerMemory.TrySend(packet), Is.True);

            internal void DeliverInOrder(NetworkBufferLease[] chunks)
            {
                for (var i = 0; i < chunks.Length; i++)
                    Deliver(chunks[i]);
                Client.Process();
            }

            /// <summary>Hands the one acknowledgement on the wire to the server, which moves the delta baseline.</summary>
            internal void ForwardAcknowledgement()
            {
                var acknowledgement = TakeAcknowledgement(ServerMemory, out _);
                Assert.That(_clientTransport.TrySend(acknowledgement), Is.True);
                Server.Receive();
            }

            /// <summary>The first keyframe leaves one transition behind; nothing after it may ask for a keyframe.</summary>
            internal void AssertNoRecovery()
            {
                while (Client.TryConsumeRecoveryTransition(out var transition))
                    Assert.That(transition.Phase, Is.EqualTo(NetworkRecoveryPhase.None));
                Assert.That(Client.Session.State, Is.EqualTo(NetworkSessionState.Established));
            }

            public void Dispose()
            {
                Client.Dispose();
                Server.Dispose();
                _clientTransport.Dispose();
                _serverTransport.Dispose();
            }
        }
    }
}
