using System;
using System.Collections.Generic;
using FFS.Libraries.StaticEcs;
using NUnit.Framework;

namespace UniGame.StaticEcs.Network.Tests
{
    /// <summary>
    /// Covers <see cref="INetworkScopeCaptureBudgetPolicy{TWorld}"/>: a cellular scope snapshot
    /// refreshes changed entities by priority within the tick's byte allowance and keeps the bytes
    /// of its previous snapshot for the rest.
    /// </summary>
    public sealed partial class NetworkV7Tests
    {
        // What a delta spends on an entity whose TestComponent changed: skip count, opcode, one
        // mask byte, the record length and its four payload bytes.
        private const int OneByteChangeCost = 8;

        // Format version, then the entity, record and operation counts.
        private const int DeltaHeaderBytes = 13;

        [Test]
        public void CaptureBudgetKeepsThePreviousBytesOfTheLowestWeightEntity()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var a = SpawnBudgetEntity(1);
                var b = SpawnBudgetEntity(2);
                var c = SpawnBudgetEntity(3);
                var policy = new StubCaptureBudgetPolicy { BudgetBytes = OneByteChangeCost * 2 };
                policy.Weights[a.GID] = 1;
                policy.Weights[b.GID] = 5;
                policy.Weights[c.GID] = 9;
                using var fixture = new CaptureBudgetFixture(policy, a, b, c);

                Assert.That(fixture.Budgeted(1), Is.EqualTo(fixture.Plain()),
                    "An entity new to the scope is never held.");

                a.Set(new TestComponent { Value = 9 });
                b.Set(new TestComponent { Value = 10 });
                c.Set(new TestComponent { Value = 11 });
                var second = fixture.Budgeted(2);
                a.Set(new TestComponent { Value = 1 });
                Assert.That(second, Is.EqualTo(fixture.Plain()),
                    "The lowest weight keeps the bytes of the previous snapshot.");
                a.Set(new TestComponent { Value = 9 });
                Assert.That(fixture.Replicator.CaptureBudgetDeferredEntities, Is.EqualTo(1));
                Assert.That(fixture.Replicator.CaptureBudgetRefreshedEntities, Is.EqualTo(2));

                Assert.That(fixture.Budgeted(3), Is.EqualTo(fixture.Plain()),
                    "With nothing else changed the held entity catches up.");
                Assert.That(fixture.Replicator.CaptureBudgetDeferredEntities, Is.EqualTo(1));
                Assert.That(fixture.Replicator.CaptureBudgetRefreshedEntities, Is.EqualTo(3));
                Assert.That(fixture.Replicator.CaptureBudgetOverflowCaptures, Is.EqualTo(1),
                    "Only the first capture, whose three new entities alone exceed the allowance.");
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
            }
        }

        // The allowance fits one small change. Entities whose record sequence changed cost their
        // whole block, never fit and rank first; the small change ranks last.
        [TestCase(4, 3, true, Description = "Three misses of four: the small change takes the room.")]
        [TestCase(4, 4, false, Description = "The fourth miss ends the capture's spending.")]
        [TestCase(1, 1, false, Description = "A limit of one stops at the first miss.")]
        [TestCase(6, 5, true, Description = "A larger limit keeps pricing past the fourth miss.")]
        public void CaptureBudgetStopsPricingAfterTheCandidatesThatDidNotFit(int maximumMisses,
            int oversized, bool smallRefreshed)
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var policy = new StubCaptureBudgetPolicy
                {
                    BudgetBytes = OneByteChangeCost,
                    MaximumMisses = maximumMisses,
                };
                var entities = new List<World<AuthorityWorld>.Entity>();
                for (var i = 0; i < oversized; i++)
                {
                    var large = SpawnBudgetEntity(10 + i);
                    policy.Weights[large.GID] = 1000;
                    entities.Add(large);
                }
                var small = SpawnBudgetEntity(1);
                policy.Weights[small.GID] = 1;
                entities.Add(small);
                using var fixture = new CaptureBudgetFixture(policy, entities.ToArray());
                fixture.Budgeted(1);

                for (var i = 0; i < oversized; i++)
                    entities[i].Set(new NetworkOwnerComponent { PeerId = (uint)(50 + i) });
                small.Set(new TestComponent { Value = 2 });
                fixture.Budgeted(2);

                Assert.That(fixture.Replicator.CaptureBudgetRefreshedEntities,
                    Is.EqualTo(smallRefreshed ? 1 : 0));
                Assert.That(fixture.Replicator.CaptureBudgetDeferredEntities,
                    Is.EqualTo(smallRefreshed ? oversized : oversized + 1));
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
            }
        }

        [Test]
        public void CaptureBudgetRefreshesAStarvedEntityAfterItsMaximumWait()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var starved = SpawnBudgetEntity(1);
                var favoured = SpawnBudgetEntity(2);
                var policy = new StubCaptureBudgetPolicy
                {
                    BudgetBytes = OneByteChangeCost,
                    MaximumWaitTicks = 2,
                };
                policy.Weights[starved.GID] = 1;
                policy.Weights[favoured.GID] = 1000;
                using var fixture = new CaptureBudgetFixture(policy, starved, favoured);
                fixture.Budgeted(1);

                // Captures 2 and 3: the favoured entity takes the whole allowance.
                for (var capture = 2; capture <= 3; capture++)
                {
                    starved.Set(new TestComponent { Value = 8 + capture });
                    favoured.Set(new TestComponent { Value = 16 + capture });
                    var bytes = fixture.Budgeted((uint)capture);
                    starved.Set(new TestComponent { Value = 1 });
                    Assert.That(bytes, Is.EqualTo(fixture.Plain()), "capture " + capture);
                }

                // Capture 4: two waits reached the limit, so the starved entity goes first and the
                // favoured one is the entity that keeps its previous bytes.
                starved.Set(new TestComponent { Value = 12 });
                favoured.Set(new TestComponent { Value = 20 });
                var fourth = fixture.Budgeted(4);
                favoured.Set(new TestComponent { Value = 19 });
                Assert.That(fourth, Is.EqualTo(fixture.Plain()));
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
            }
        }

        [Test]
        public void CaptureBudgetNeverHoldsTheEntityOfAPeerThatObservesTheScope()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var owned = SpawnBudgetEntity(1);
                owned.Set(new NetworkOwnerComponent { PeerId = 7 });
                var other = SpawnBudgetEntity(2);
                var policy = new StubCaptureBudgetPolicy { BudgetBytes = OneByteChangeCost };
                policy.Weights[other.GID] = 1000;
                using var fixture = new CaptureBudgetFixture(policy, owned, other);
                var observers = new Dictionary<uint, ScopeId> { [7] = fixture.Scope };
                fixture.Replicator.BeginCaptureBudgetTick(1, observers);
                fixture.Budgeted(1);
                Assert.That(policy.BeginTickCalls, Is.EqualTo(1));

                owned.Set(new TestComponent { Value = 9 });
                other.Set(new TestComponent { Value = 10 });
                fixture.Replicator.BeginCaptureBudgetTick(2, observers);
                var observed = fixture.Budgeted(2);
                other.Set(new TestComponent { Value = 2 });
                Assert.That(observed, Is.EqualTo(fixture.Plain()),
                    "The owner predicts its entity, so it is refreshed before any weight.");
                other.Set(new TestComponent { Value = 10 });

                // The owner now observes another scope: here its entity is ranked like any other.
                observers[7] = new ScopeId(fixture.Scope.Value + 1000);
                fixture.Replicator.BeginCaptureBudgetTick(3, observers);
                fixture.Budgeted(3);
                owned.Set(new TestComponent { Value = 12 });
                other.Set(new TestComponent { Value = 14 });
                fixture.Replicator.BeginCaptureBudgetTick(4, observers);
                var unobserved = fixture.Budgeted(4);
                owned.Set(new TestComponent { Value = 9 });
                Assert.That(unobserved, Is.EqualTo(fixture.Plain()));
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
            }
        }

        [Test]
        public void CaptureBudgetSpendsNothingOnAnUnchangedEntity()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var idle = SpawnBudgetEntity(1);
                var moving = SpawnBudgetEntity(2);
                var policy = new StubCaptureBudgetPolicy { BudgetBytes = OneByteChangeCost };
                policy.Weights[idle.GID] = 1000;
                using var fixture = new CaptureBudgetFixture(policy, idle, moving);
                fixture.Budgeted(1);

                moving.Set(new TestComponent { Value = 10 });
                Assert.That(fixture.Budgeted(2), Is.EqualTo(fixture.Plain()));
                Assert.That(fixture.Replicator.CaptureBudgetDeferredEntities, Is.Zero);
                Assert.That(fixture.Replicator.CaptureBudgetRefreshedEntities, Is.EqualTo(1));
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
            }
        }

        [Test]
        public void CaptureBudgetOfZeroLeavesTheSnapshotUntouched()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var a = SpawnBudgetEntity(1);
                var b = SpawnBudgetEntity(2);
                using var fixture = new CaptureBudgetFixture(
                    new StubCaptureBudgetPolicy { BudgetBytes = 0 }, a, b);
                fixture.Budgeted(1);

                a.Set(new TestComponent { Value = 9 });
                b.Set(new TestComponent { Value = 10 });
                Assert.That(fixture.Budgeted(2), Is.EqualTo(fixture.Plain()));
                Assert.That(fixture.Replicator.CaptureBudgetRefreshedEntities, Is.Zero);
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
            }
        }

        [Test]
        public void CaptureBudgetKeepsTheCurrentBlockOfAHeldObserverEntityForItsOwner()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var owned = SpawnBudgetEntity(1);
                owned.Set(new NetworkOwnerComponent { PeerId = 7 });
                var other = SpawnBudgetEntity(2);
                var policy = new StubCaptureBudgetPolicy
                {
                    BudgetBytes = OneByteChangeCost,
                    RefreshesObserversPersonally = true,
                };
                policy.Weights[other.GID] = 1000;
                using var fixture = new CaptureBudgetFixture(policy, owned, other);
                var observers = new Dictionary<uint, ScopeId> { [7] = fixture.Scope };
                fixture.Replicator.BeginCaptureBudgetTick(1, observers);
                fixture.Budgeted(1);
                Assert.That(fixture.Replicator.TryGetHeldOwnerBlock(fixture.Scope, 1, 7, out _,
                    out _, out _), Is.False, "The first capture holds nothing.");

                owned.Set(new TestComponent { Value = 9 });
                other.Set(new TestComponent { Value = 10 });
                fixture.Replicator.BeginCaptureBudgetTick(2, observers);
                var shared = fixture.Budgeted(2);
                var current = fixture.Plain();
                owned.Set(new TestComponent { Value = 1 });
                Assert.That(shared, Is.EqualTo(fixture.Plain()),
                    "The shared snapshot keeps the older state of the observer's entity.");

                Assert.That(fixture.Replicator.TryGetHeldOwnerBlock(fixture.Scope, 2, 7,
                    out var block, out var held, out var records), Is.True);
                Assert.That(records, Is.GreaterThan(0));
                Assert.That(current.AsSpan().IndexOf(block), Is.GreaterThan(0),
                    "The owner's block is the state of the capture tick.");
                Assert.That(shared.AsSpan().IndexOf(block), Is.LessThan(0));
                Assert.That(shared.AsSpan().IndexOf(held), Is.GreaterThan(0),
                    "The held block is the one the shared snapshot carries.");
                Assert.That(held.Length, Is.EqualTo(block.Length));
                Assert.That(fixture.Replicator.TryGetHeldOwnerBlock(fixture.Scope, 2, 8, out _,
                    out _, out _), Is.False, "Only the owner has a block.");
                Assert.That(fixture.Replicator.TryGetHeldOwnerBlock(fixture.Scope, 3, 7, out _,
                    out _, out _), Is.False, "A block belongs to the tick of its capture.");
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
            }
        }

        [Test]
        public void OwnerSeesItsHeldEntityCurrentWhileAnotherObserverSeesTheOlderState()
        {
            var refreshes = RunHeldOwnerScenario(0);

            Assert.That(refreshes.OwnedEntityRefreshTrailers,
                Is.EqualTo(refreshes.OwnedEntityRefreshPackets),
                "Every state rides at the end of the snapshot packet.");
            Assert.That(refreshes.OwnedEntityRefreshBytes,
                Is.LessThan(refreshes.OwnedEntityRefreshPackets * 40),
                "One changed record travels, not the whole entity block, and no packet header.");
        }

        [Test]
        public void OwnerSeesItsHeldEntityCurrentOnUnreliableDeltas()
        {
            var refreshes = RunHeldOwnerScenario(0,
                new NetworkServerSettings { UnreliableDeltaChunks = 2 });

            Assert.That(refreshes.DeltaSnapshotsUnreliable, Is.GreaterThan(0));
            Assert.That(refreshes.OwnedEntityRefreshTrailers,
                Is.EqualTo(refreshes.OwnedEntityRefreshPackets),
                "The state rides in the datagram of the delta it belongs to.");
        }

        [Test]
        public void DeltaWhoseOwnEntityStateDoesNotFitItsLastChunkStaysReliable()
        {
            // Eight body bytes per datagram: the delta fits a few chunks, the state behind the
            // last one does not, so that snapshot keeps the ordered channel.
            var refreshes = RunHeldOwnerScenario(0,
                new NetworkServerSettings
                {
                    UnreliableDeltaChunks = NetworkServerSettings.MaximumUnreliableDeltaChunks,
                },
                PacketHeader.Size + SnapshotChunkHeader.Size + 8);

            Assert.That(refreshes.OwnedEntityRefreshPackets, Is.GreaterThan(0));
            Assert.That(refreshes.OwnedEntityRefreshTrailers,
                Is.EqualTo(refreshes.OwnedEntityRefreshPackets),
                "On the reliable channel the state fits behind the whole delta.");
            Assert.That(refreshes.DeltaSnapshotsUnreliable,
                Is.LessThan(refreshes.DeltaSnapshots));
        }

        [Test]
        public void OwnEntityStateThatDoesNotFitTheSnapshotPacketTravelsInAPacketOfItsOwn()
        {
            // Chunk bodies of eight bytes leave no room for the state behind the last one.
            var refreshes = RunHeldOwnerScenario(
                PacketHeader.Size + SnapshotChunkHeader.Size + 8);

            Assert.That(refreshes.OwnedEntityRefreshTrailers, Is.Zero);
            Assert.That(refreshes.OwnedEntityRefreshBytes,
                Is.GreaterThan(refreshes.OwnedEntityRefreshPackets * PacketHeader.Size));
        }

        // Two players share a cell; the allowance refreshes only peer 8's entity each tick, so the
        // snapshot holds an older state of peer 7's. Returns the server counters after peer 7 was
        // checked to see its own entity current on every snapshot it applied.
        private NetworkReplicationDiagnostics RunHeldOwnerScenario(int reliableLimit,
            NetworkServerSettings settings = null, int unreliableLimit = 0)
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            CreateReplicationWorld<ClientBWorld>(false);
            try
            {
                const int InitialMine = 1;
                var mine = SpawnBudgetEntity(InitialMine);
                mine.Set(new NetworkOwnerComponent { PeerId = 7 });
                var theirs = SpawnBudgetEntity(2);
                theirs.Set(new NetworkOwnerComponent { PeerId = 8 });
                var cell = new ScopeId(701);
                var scope = new ScopeId(702);
                var provider = new StubCellularScopeProvider();
                provider.CellEntities[cell] = new List<World<AuthorityWorld>.Entity> { mine, theirs };
                provider.ScopeCells[scope] = new[] { cell };
                // Peer 8's entity always wins the single refresh of a tick, so peer 7's waits.
                var policy = new StubCaptureBudgetPolicy
                {
                    BudgetBytes = OneByteChangeCost,
                    RefreshesObserversPersonally = true,
                };
                policy.Weights[theirs.GID] = 1000;

                var server = new NetworkServer<AuthorityWorld>(Schema<AuthorityWorld>(true),
                    static (_, _) => true, scopeProvider: provider, captureBudgetPolicy: policy,
                    settings: settings);
                MemoryNetworkTransport.CreatePair(new ConnectionId(801), out var memoryClientA,
                    out var memoryServerA);
                MemoryNetworkTransport.CreatePair(new ConnectionId(802), out var memoryClientB,
                    out var memoryServerB);
                var unlimited = memoryClientA.MaxReliablePayloadBytes;
                var clientA = new LimitedNetworkTransport(memoryClientA, unlimited);
                var serverA = new LimitedNetworkTransport(memoryServerA, unlimited);
                var clientB = new LimitedNetworkTransport(memoryClientB, unlimited);
                var serverB = new LimitedNetworkTransport(memoryServerB, unlimited);
                using (clientA)
                using (serverA)
                using (clientB)
                using (serverB)
                {
                    server.AddConnection(serverA, 7, 21, scope);
                    server.AddConnection(serverB, 8, 22, scope);
                    var owner = new NetworkClient<ClientAWorld>(clientA, Schema<ClientAWorld>(false), scope);
                    var other = new NetworkClient<ClientBWorld>(clientB, Schema<ClientBWorld>(false), scope);
                    owner.BeginHandshake();
                    other.BeginHandshake();
                    for (var i = 0; i < 6; i++)
                    {
                        server.Receive();
                        server.Tick(_ => { });
                        owner.Process();
                        other.Process();
                    }
                    Assert.That(CountReplicas<ClientAWorld>(InitialMine), Is.EqualTo(1));
                    Assert.That(CountReplicas<ClientBWorld>(InitialMine), Is.EqualTo(1));
                    if (reliableLimit > 0)
                    {
                        clientA.MaxReliablePayloadBytes = reliableLimit;
                        serverA.MaxReliablePayloadBytes = reliableLimit;
                        clientB.MaxReliablePayloadBytes = reliableLimit;
                        serverB.MaxReliablePayloadBytes = reliableLimit;
                    }
                    if (unreliableLimit > 0)
                    {
                        serverA.MaxUnreliablePayloadBytes = unreliableLimit;
                        serverB.MaxUnreliablePayloadBytes = unreliableLimit;
                    }

                    var appliedByOwner = 0;
                    for (var i = 0; i < 12; i++)
                    {
                        var mineNow = 100 + i;
                        var theirsNow = 200 + i;
                        mine.Set(new TestComponent { Value = mineNow });
                        theirs.Set(new TestComponent { Value = theirsNow });
                        server.Receive();
                        server.Tick(_ => { });
                        owner.Process();
                        other.Process();
                        if (owner.AcknowledgedSnapshotTick != server.ServerTick)
                            continue;
                        appliedByOwner++;
                        Assert.That(CountReplicas<ClientAWorld>(mineNow), Is.EqualTo(1),
                            "tick " + i + ": the owner reconciles against the current state");
                        Assert.That(CountReplicas<ClientAWorld>(theirsNow), Is.EqualTo(1),
                            "tick " + i + ": the refreshed entity reaches the owner too");
                    }

                    Assert.That(appliedByOwner, Is.GreaterThanOrEqualTo(3));
                    Assert.That(CountReplicas<ClientBWorld>(InitialMine), Is.EqualTo(1),
                        "The other observer still has the state the shared snapshot holds.");
                    Assert.That(server.OwnedEntityRefreshPackets, Is.GreaterThanOrEqualTo(appliedByOwner));
                    Assert.That(server.OwnedEntityRefreshFailures, Is.Zero);
                    Assert.That(owner.Session.State, Is.EqualTo(NetworkSessionState.Established));
                    Assert.That(other.Session.State, Is.EqualTo(NetworkSessionState.Established));
                    return server.CaptureReplicationDiagnostics();
                }
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
                World<ClientAWorld>.Destroy();
                World<ClientBWorld>.Destroy();
            }
        }

        [TestCase(105, Description = "The hook shrinks the record.")]
        [TestCase(100_000, Description = "The hook declines, so the payload travels whole.")]
        public void EntityPatchMeasureEqualsTheDeltaBytes(int changedValue)
        {
            CreateDeltaHookWorld<DeltaHookWorld>();
            try
            {
                var entity = World<DeltaHookWorld>.NewEntity<TestEntity>();
                entity.Set(new DeltaTestComponent { Value = 100 });
                var schema = DeltaHookSchema<DeltaHookWorld>();
                using var pair = new SnapshotPair<DeltaHookWorld>(schema,
                    () => entity.Set(new DeltaTestComponent { Value = changedValue }));

                Assert.That(SnapshotDeltaCodec.MeasureEntityPatch(pair.BaselineBlock,
                    pair.TargetBlock, schema.DeltaHooks), Is.EqualTo(pair.DeltaOperationBytes));
            }
            finally
            {
                World<DeltaHookWorld>.Destroy();
            }
        }

        [Test]
        public void EntityPatchMeasureOfAPlainRecordIsItsWholePayload()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var entity = SpawnBudgetEntity(1);
                var schema = Schema<AuthorityWorld>(true);
                using var pair = new SnapshotPair<AuthorityWorld>(schema,
                    () => entity.Set(new TestComponent { Value = 2 }));

                Assert.That(pair.DeltaOperationBytes, Is.EqualTo(OneByteChangeCost));
                Assert.That(SnapshotDeltaCodec.MeasureEntityPatch(pair.BaselineBlock,
                    pair.TargetBlock, schema.DeltaHooks), Is.EqualTo(OneByteChangeCost));
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
            }
        }

        [Test]
        public void EntityPatchMeasureOfAChangedRecordSequenceIsTheWholeBlock()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var entity = SpawnBudgetEntity(1);
                var schema = Schema<AuthorityWorld>(true);
                using var pair = new SnapshotPair<AuthorityWorld>(schema,
                    () => entity.Set(new NetworkOwnerComponent { PeerId = 7 }));

                var measured = SnapshotDeltaCodec.MeasureEntityPatch(pair.BaselineBlock,
                    pair.TargetBlock, schema.DeltaHooks);
                Assert.That(measured, Is.EqualTo(pair.TargetBlock.Length + 2));
                Assert.That(measured, Is.GreaterThanOrEqualTo(pair.DeltaOperationBytes),
                    "The whole block is an upper bound of the record-by-record patch.");
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
            }
        }

        private static World<AuthorityWorld>.Entity SpawnBudgetEntity(int value)
        {
            var entity = World<AuthorityWorld>.NewEntity<TestEntity>();
            entity.Set(new TestComponent { Value = value });
            return entity;
        }

        /// <summary>
        /// Two snapshots of a world with one replicated entity, before and after a change, and the
        /// delta between them.
        /// </summary>
        private sealed class SnapshotPair<TWorld> : IDisposable where TWorld : struct, IWorldType
        {
            private readonly NetworkBufferPool _pool = new NetworkBufferPool(1L << 20);
            private readonly NetworkReplicator<TWorld> _replicator;
            private readonly NetworkSnapshot _baseline;
            private readonly NetworkSnapshot _target;
            private readonly NetworkBufferLease _delta;

            internal SnapshotPair(NetworkSchema<TWorld> schema, Action change)
            {
                _replicator = new NetworkReplicator<TWorld>(schema, static (_, _) => true,
                    new ScopeId(1), bufferPool: _pool);
                Assert.That(_replicator.Capture(1, out _baseline),
                    Is.EqualTo(SnapshotCaptureResult.Success));
                change();
                Assert.That(_replicator.Capture(2, out _target),
                    Is.EqualTo(SnapshotCaptureResult.Success));
                Assert.That(SnapshotDeltaCodec.TryEncode(_pool, _baseline, _target, out _delta,
                    schema.DeltaHooks), Is.True);
            }

            // A snapshot is its entity count, then the entity blocks.
            internal ReadOnlySpan<byte> BaselineBlock => _baseline.Bytes.Span.Slice(sizeof(int));
            internal ReadOnlySpan<byte> TargetBlock => _target.Bytes.Span.Slice(sizeof(int));
            internal int DeltaOperationBytes => _delta.Length - DeltaHeaderBytes;

            public void Dispose()
            {
                _delta?.Dispose();
                _baseline?.Dispose();
                _target?.Dispose();
                _replicator.Dispose();
                _pool.Dispose();
            }
        }

        private sealed class StubCaptureBudgetPolicy :
            INetworkScopeCaptureBudgetPolicy<AuthorityWorld>
        {
            internal readonly Dictionary<EntityGID, int> Weights =
                new Dictionary<EntityGID, int>();
            internal int BeginTickCalls;

            public int BudgetBytes { get; set; }
            public int MaximumWaitTicks { get; set; } = 100;
            public int MaximumMisses { get; set; } = 4;
            public bool RefreshesObserversPersonally { get; set; }

            public void BeginTick(uint serverTick) => BeginTickCalls++;

            public int Weight(ScopeId scope, in World<AuthorityWorld>.Entity entity,
                EntityGID gid) => Weights.TryGetValue(gid, out var weight) ? weight : 1;
        }

        /// <summary>
        /// One cell and one scope captured by a replicator with the allowance and by a plain one;
        /// the plain capture of the current world state is the expected snapshot.
        /// </summary>
        private sealed class CaptureBudgetFixture : System.IDisposable
        {
            private readonly NetworkBufferPool _pool = new NetworkBufferPool(1L << 20);
            private readonly NetworkReplicator<AuthorityWorld> _plain;
            private uint _plainTick = 1000;

            internal CaptureBudgetFixture(StubCaptureBudgetPolicy policy,
                params World<AuthorityWorld>.Entity[] entities)
            {
                var schema = Schema<AuthorityWorld>(true);
                var cell = new ScopeId(701);
                Scope = new ScopeId(702);
                var provider = new StubCellularScopeProvider();
                provider.CellEntities[cell] = new List<World<AuthorityWorld>.Entity>(entities);
                provider.ScopeCells[Scope] = new[] { cell };
                Replicator = new NetworkReplicator<AuthorityWorld>(schema,
                    static (_, _) => true, scopeProvider: provider, bufferPool: _pool,
                    captureBudgetPolicy: policy);
                _plain = new NetworkReplicator<AuthorityWorld>(schema,
                    static (_, _) => true, scopeProvider: provider, bufferPool: _pool);
            }

            internal ScopeId Scope { get; }
            internal NetworkReplicator<AuthorityWorld> Replicator { get; }

            internal byte[] Budgeted(uint tick) => Capture(Replicator, tick);

            // A cell is captured once per tick, so every expected snapshot takes its own tick.
            internal byte[] Plain() => Capture(_plain, ++_plainTick);

            private byte[] Capture(NetworkReplicator<AuthorityWorld> replicator, uint tick)
            {
                Assert.That(replicator.Capture(tick, Scope, out var snapshot),
                    Is.EqualTo(SnapshotCaptureResult.Success));
                try
                {
                    return snapshot.Bytes.ToArray();
                }
                finally
                {
                    snapshot.Dispose();
                }
            }

            public void Dispose()
            {
                Replicator.Dispose();
                _plain.Dispose();
                _pool.Dispose();
            }
        }
    }
}
