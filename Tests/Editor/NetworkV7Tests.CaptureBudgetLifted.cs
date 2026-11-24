using System;
using System.Collections.Generic;
using FFS.Libraries.StaticEcs;
using NUnit.Framework;

namespace UniGame.StaticEcs.Network.Tests
{
    /// <summary>
    /// Acceptance of the switched-off byte allowance: a policy that lifts the limit
    /// (<see cref="INetworkScopeCaptureBudgetPolicy{TWorld}.BudgetBytes"/> of zero) must leave every
    /// captured byte and every sent packet as they are without any policy.
    /// </summary>
    public sealed partial class NetworkV7Tests
    {
        [Test]
        public void CaptureWithTheAllowanceLiftedMatchesACaptureWithoutAPolicyOverSeveralTicks()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                var near = SpawnBudgetEntity(1);
                var crowded = SpawnBudgetEntity(2);
                var owned = SpawnBudgetEntity(3);
                owned.Set(new NetworkOwnerComponent { PeerId = 7 });
                var leaving = SpawnBudgetEntity(4);
                var firstCell = new ScopeId(721);
                var secondCell = new ScopeId(722);
                // The scopes overlap in the second cell, like two neighbouring windows.
                var wide = new ScopeId(723);
                var narrow = new ScopeId(724);
                var provider = new StubCellularScopeProvider();
                provider.CellEntities[firstCell] =
                    new List<World<AuthorityWorld>.Entity> { near, crowded };
                provider.CellEntities[secondCell] =
                    new List<World<AuthorityWorld>.Entity> { owned, leaving };
                provider.ScopeCells[wide] = new[] { firstCell, secondCell };
                provider.ScopeCells[narrow] = new[] { secondCell };

                // Weights and the personal refresh would reorder and hold entities under a real
                // allowance; lifted, none of them may show in the bytes.
                var policy = new StubCaptureBudgetPolicy
                {
                    BudgetBytes = 0,
                    MaximumWaitTicks = 1,
                    MaximumMisses = 1,
                    RefreshesObserversPersonally = true,
                };
                policy.Weights[near.GID] = 1000;
                policy.Weights[crowded.GID] = 1;
                var observers = new Dictionary<uint, ScopeId> { [7] = wide };

                using var pool = new NetworkBufferPool(1L << 20);
                using var lifted = new NetworkReplicator<AuthorityWorld>(schema,
                    static (_, _) => true, scopeProvider: provider, bufferPool: pool,
                    captureBudgetPolicy: policy);
                using var plain = new NetworkReplicator<AuthorityWorld>(schema,
                    static (_, _) => true, scopeProvider: provider, bufferPool: pool);

                var arrived = default(World<AuthorityWorld>.Entity);
                var changes = new Action[]
                {
                    () => { },
                    () =>
                    {
                        near.Set(new TestComponent { Value = 11 });
                        crowded.Set(new TestComponent { Value = 12 });
                        owned.Set(new TestComponent { Value = 13 });
                        leaving.Set(new TestComponent { Value = 14 });
                    },
                    () => crowded.Set(new TestComponent { Value = 22 }),
                    // A record joins an entity: its whole block changes shape.
                    () => crowded.Set(new NetworkOwnerComponent { PeerId = 9 }),
                    () =>
                    {
                        provider.CellEntities[firstCell].Remove(near);
                        provider.CellEntities[secondCell].Add(near);
                        near.Set(new TestComponent { Value = 31 });
                    },
                    () =>
                    {
                        provider.CellEntities[secondCell].Remove(leaving);
                        arrived = SpawnBudgetEntity(41);
                        provider.CellEntities[firstCell].Add(arrived);
                    },
                    () => { },
                    () =>
                    {
                        owned.Set(new TestComponent { Value = 53 });
                        arrived.Set(new TestComponent { Value = 51 });
                    },
                };

                for (var index = 0; index < changes.Length; index++)
                {
                    var tick = (uint)index + 1;
                    changes[index]();
                    lifted.BeginCaptureBudgetTick(tick, observers);
                    AssertSameCapture(lifted, plain, tick, wide);
                    AssertSameCapture(lifted, plain, tick, narrow);
                    Assert.That(lifted.TryGetHeldOwnerBlock(wide, tick, 7, out _, out _, out _),
                        Is.False, "tick " + tick + ": nothing is held, so the owner needs no state");
                }

                Assert.That(lifted.CaptureBudgetRefreshedEntities, Is.Zero);
                Assert.That(lifted.CaptureBudgetDeferredEntities, Is.Zero);
                Assert.That(lifted.CaptureBudgetOverflowCaptures, Is.Zero);
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
            }
        }

        [Test]
        public void ServerWithTheAllowanceLiftedSendsTheSamePacketsAsAServerWithoutAPolicy()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                var mine = SpawnBudgetEntity(1);
                mine.Set(new NetworkOwnerComponent { PeerId = 7 });
                var other = SpawnBudgetEntity(2);
                var cell = new ScopeId(731);
                var scope = new ScopeId(732);
                var provider = new StubCellularScopeProvider();
                provider.CellEntities[cell] = new List<World<AuthorityWorld>.Entity> { mine, other };
                provider.ScopeCells[scope] = new[] { cell };
                var policy = new StubCaptureBudgetPolicy
                {
                    BudgetBytes = 0,
                    RefreshesObserversPersonally = true,
                };
                policy.Weights[other.GID] = 1000;

                MemoryNetworkTransport.CreatePair(new ConnectionId(961), out var liftedClient,
                    out var liftedEndpoint);
                MemoryNetworkTransport.CreatePair(new ConnectionId(962), out var plainClient,
                    out var plainEndpoint);
                using (liftedClient)
                using (liftedEndpoint)
                using (plainClient)
                using (plainEndpoint)
                using (var lifted = new NetworkServer<AuthorityWorld>(schema,
                           static (_, _) => true, scopeProvider: provider,
                           captureBudgetPolicy: policy,
                           settings: new NetworkServerSettings { UnreliableDeltaChunks = 2 }))
                using (var plain = new NetworkServer<AuthorityWorld>(schema,
                           static (_, _) => true, scopeProvider: provider,
                           settings: new NetworkServerSettings { UnreliableDeltaChunks = 2 }))
                {
                    const uint Epoch = 21;
                    lifted.AddConnection(liftedEndpoint, 7, Epoch, scope);
                    plain.AddConnection(plainEndpoint, 7, Epoch, scope);
                    SendPeerPacket(liftedClient, schema.Fingerprint, PacketKind.Hello, 0, 1, 0);
                    SendPeerPacket(plainClient, schema.Fingerprint, PacketKind.Hello, 0, 1, 0);
                    lifted.Receive();
                    plain.Receive();
                    AssertSamePackets(liftedClient, plainClient, "admission");

                    var arrived = default(World<AuthorityWorld>.Entity);
                    var changes = new Action[]
                    {
                        () => { },
                        () =>
                        {
                            mine.Set(new TestComponent { Value = 11 });
                            other.Set(new TestComponent { Value = 12 });
                        },
                        () => mine.Set(new TestComponent { Value = 21 }),
                        () => other.Set(new NetworkOwnerComponent { PeerId = 9 }),
                        () =>
                        {
                            arrived = SpawnBudgetEntity(41);
                            provider.CellEntities[cell].Add(arrived);
                        },
                        () => provider.CellEntities[cell].Remove(other),
                    };
                    var deltas = 0;
                    for (var index = 0; index < changes.Length; index++)
                    {
                        changes[index]();
                        lifted.Tick(_ => { });
                        plain.Tick(_ => { });
                        var tick = lifted.ServerTick;
                        Assert.That(plain.ServerTick, Is.EqualTo(tick));
                        var packets = AssertSamePackets(liftedClient, plainClient, "tick " + tick);
                        Assert.That(packets, Is.GreaterThan(0), "tick " + tick);

                        // Both peers acknowledge alike; every second delta stays unacknowledged
                        // so some deltas span more than one tick.
                        if (index == 0)
                        {
                            SendPeerPacket(liftedClient, schema.Fingerprint, PacketKind.Ack, Epoch, 2, tick);
                            SendPeerPacket(plainClient, schema.Fingerprint, PacketKind.Ack, Epoch, 2, tick);
                        }
                        else if (index % 2 == 1)
                        {
                            SendUnreliableAcknowledgement(liftedClient, schema.Fingerprint, Epoch,
                                (uint)index, tick);
                            SendUnreliableAcknowledgement(plainClient, schema.Fingerprint, Epoch,
                                (uint)index, tick);
                        }
                        lifted.Receive();
                        plain.Receive();
                        if (index > 0)
                            deltas++;
                    }

                    var liftedCounters = lifted.CaptureReplicationDiagnostics();
                    var plainCounters = plain.CaptureReplicationDiagnostics();
                    Assert.That(liftedCounters.DeltaSnapshots, Is.EqualTo(deltas),
                        "The comparison covered deltas, not a keyframe per tick.");
                    Assert.That(liftedCounters.DeltaSnapshotsUnreliable,
                        Is.EqualTo(plainCounters.DeltaSnapshotsUnreliable));
                    Assert.That(liftedCounters.CaptureBudgetRefreshedEntities, Is.Zero);
                    Assert.That(liftedCounters.CaptureBudgetDeferredEntities, Is.Zero);
                    Assert.That(lifted.OwnedEntityRefreshPackets, Is.Zero);
                }
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
            }
        }

        private static void AssertSameCapture(NetworkReplicator<AuthorityWorld> lifted,
            NetworkReplicator<AuthorityWorld> plain, uint tick, ScopeId scope)
        {
            Assert.That(lifted.Capture(tick, scope, out var actual),
                Is.EqualTo(SnapshotCaptureResult.Success));
            using (actual)
            {
                Assert.That(plain.Capture(tick, scope, out var expected),
                    Is.EqualTo(SnapshotCaptureResult.Success));
                using (expected)
                {
                    var where = "tick " + tick + ", scope " + scope.Value;
                    Assert.That(actual.Bytes.ToArray(), Is.EqualTo(expected.Bytes.ToArray()), where);
                    Assert.That(actual.EntityCount, Is.EqualTo(expected.EntityCount), where);
                    Assert.That(actual.RecordCount, Is.EqualTo(expected.RecordCount), where);
                    Assert.That(actual.PayloadHash, Is.EqualTo(expected.PayloadHash), where);
                }
            }
        }

        // Drains both transports and compares what each server sent, packet by packet. Returns the
        // number of packets one of them sent.
        private static int AssertSamePackets(INetworkTransport lifted, INetworkTransport plain,
            string where)
        {
            var count = 0;
            while (lifted.TryReceive(out var actual))
            {
                using (actual)
                {
                    Assert.That(plain.TryReceive(out var expected), Is.True,
                        where + ": the server without a policy sent fewer packets");
                    using (expected)
                        Assert.That(actual.Span.ToArray(), Is.EqualTo(expected.Span.ToArray()),
                            where + ": packet " + count);
                }
                count++;
            }
            AssertNoPacket(plain, where + ": the server without a policy sent more packets");
            return count;
        }
    }
}
