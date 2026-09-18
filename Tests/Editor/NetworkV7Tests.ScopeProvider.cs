using System.Collections.Generic;
using FFS.Libraries.StaticEcs;
using NUnit.Framework;

namespace UniGame.StaticEcs.Network.Tests
{
    /// <summary>
    /// NCORE-15: spatial cells as dynamic snapshot scopes. Covers the package-level hook contract
    /// (<see cref="INetworkScopeProvider{TWorld}"/>), server-side hysteresis and forced keyframes,
    /// client scope adoption restricted to keyframes, per-scope capture sharing, and the shared
    /// per-scope history byte budget. The actual spatial grid (hash cells over a position
    /// component) is game/sandbox-owned and tested there; this file only exercises the generic
    /// hook the package exposes to it, using a small deterministic stub provider.
    /// </summary>
    public sealed partial class NetworkV7Tests
    {
        [Test]
        public void SnapshotChunkHeaderRoundTripsScopeValue()
        {
            var header = new SnapshotChunkHeader
            {
                PayloadKind = SnapshotPayloadKind.Keyframe,
                SnapshotTick = 4,
                BaselineTick = 0,
                ScopeValue = 0x0102030405060708UL,
                TotalLength = 1,
                TotalHash = 1,
                ChunkIndex = 0,
                ChunkCount = 1,
            };
            var bytes = new byte[SnapshotChunkHeader.Size];
            Assert.That(header.TryWrite(bytes), Is.True);
            Assert.That(SnapshotChunkHeader.Size, Is.EqualTo(41),
                "wire size grew by 8 bytes (NCORE-15, protocol v10) to carry the scope");
            Assert.That(SnapshotChunkHeader.TryRead(bytes, out var decoded), Is.True);
            Assert.That(decoded.ScopeValue, Is.EqualTo(header.ScopeValue));

            header.ScopeValue = 0;
            Assert.That(header.TryWrite(bytes), Is.True);
            Assert.That(SnapshotChunkHeader.TryRead(bytes, out decoded), Is.True);
            Assert.That(decoded.ScopeValue, Is.Zero,
                "scope 0 (interest cells disabled) round-trips exactly like every other value");
        }

        [Test]
        public void CaptureUsesScopeProviderInsteadOfSelectorWhenSupplied()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                var included = World<AuthorityWorld>.NewEntity<TestEntity>();
                included.Set(new TestComponent { Value = 11 });
                var excluded = World<AuthorityWorld>.NewEntity<TestEntity>();
                excluded.Set(new TestComponent { Value = 22 });

                var provider = new StubScopeProvider();
                provider.ScopeEntities[new ScopeId(1)] = new List<World<AuthorityWorld>.Entity> { included };

                // The selector always returns false: if Capture still consulted it, nothing would
                // be captured. Only the provider's explicit entity list should end up in the
                // snapshot.
                var replicator = new NetworkReplicator<AuthorityWorld>(schema,
                    static (_, _) => false, scopeProvider: provider);
                try
                {
                    Assert.That(replicator.Capture(1, new ScopeId(1), out var snapshot),
                        Is.EqualTo(SnapshotCaptureResult.Success));
                    Assert.That(snapshot.EntityCount, Is.EqualTo(1));
                    Assert.That(provider.CollectCalls, Is.EqualTo(1));
                    snapshot.Dispose();
                }
                finally
                {
                    replicator.Dispose();
                }
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
            }
        }

        [Test]
        public void ServerSharesOneCapturePerOccupiedScopeNotPerPeer()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            CreateReplicationWorld<ClientBWorld>(false);
            try
            {
                var authoritySchema = Schema<AuthorityWorld>(true);
                var owner = World<AuthorityWorld>.NewEntity<TestEntity>();
                owner.Set(new TestComponent { Value = 1 });

                var provider = new StubScopeProvider();
                provider.ScopeEntities[new ScopeId(1)] =
                    new List<World<AuthorityWorld>.Entity> { owner };
                var observer = new TraceCollector();
                var server = new NetworkServer<AuthorityWorld>(authoritySchema,
                    static (_, _) => true, observer: observer, scopeProvider: provider);

                MemoryNetworkTransport.CreatePair(new ConnectionId(101),
                    out var clientTransportA, out var serverTransportA);
                MemoryNetworkTransport.CreatePair(new ConnectionId(102),
                    out var clientTransportB, out var serverTransportB);
                using (clientTransportA)
                using (serverTransportA)
                using (clientTransportB)
                using (serverTransportB)
                {
                    // Two peers, both left on their admission-time scope (the stub never
                    // reassigns them here): a dense hub, one occupied scope.
                    server.AddConnection(serverTransportA, 1, 11, new ScopeId(1));
                    server.AddConnection(serverTransportB, 2, 12, new ScopeId(1));

                    var clientA = new NetworkClient<ClientAWorld>(clientTransportA,
                        Schema<ClientAWorld>(false), new ScopeId(1));
                    var clientB = new NetworkClient<ClientBWorld>(clientTransportB,
                        Schema<ClientBWorld>(false), new ScopeId(1));
                    clientA.BeginHandshake();
                    clientB.BeginHandshake();
                    server.Receive();

                    observer.Events.Clear();
                    provider.CollectCalls = 0;
                    server.Tick(_ => { });

                    Assert.That(provider.CollectCalls, Is.EqualTo(1),
                        "one occupied scope must capture exactly once regardless of peer count");
                    var captureSuccesses = 0;
                    foreach (var trace in observer.Events)
                        if (trace.Phase == NetworkPhase.SnapshotCapture &&
                            trace.Result == NetworkResultCategory.Success)
                            captureSuccesses++;
                    Assert.That(captureSuccesses, Is.EqualTo(1));
                }
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
                World<ClientAWorld>.Destroy();
                World<ClientBWorld>.Destroy();
            }
        }

        [Test]
        public void HysteresisScopeReassignmentForcesKeyframeCarryingTheNewScope()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            try
            {
                var authoritySchema = Schema<AuthorityWorld>(true);
                var owner = World<AuthorityWorld>.NewEntity<TestEntity>();
                owner.Set(new TestComponent { Value = 1 });

                var provider = new StubScopeProvider();
                provider.ScopeEntities[new ScopeId(1)] =
                    new List<World<AuthorityWorld>.Entity> { owner };
                provider.ScopeEntities[new ScopeId(2)] =
                    new List<World<AuthorityWorld>.Entity> { owner };
                var server = new NetworkServer<AuthorityWorld>(authoritySchema,
                    static (_, _) => true, scopeProvider: provider);

                MemoryNetworkTransport.CreatePair(new ConnectionId(201),
                    out var clientTransport, out var serverTransport);
                using (clientTransport)
                using (serverTransport)
                {
                    server.AddConnection(serverTransport, 7, 21, new ScopeId(1));
                    var client = new NetworkClient<ClientAWorld>(clientTransport,
                        Schema<ClientAWorld>(false), new ScopeId(1));
                    client.BeginHandshake();
                    server.Receive();
                    server.Tick(_ => { });
                    // Applies the forced first keyframe (baselineTick == 0) and sends the client's
                    // own ACK for it -- no need to fabricate one.
                    client.Process();

                    Assert.That(client.Session.Scope.Value, Is.EqualTo(1u));
                    Assert.That(client.AcknowledgedSnapshotTick, Is.EqualTo(server.ServerTick));

                    server.Receive();
                    // Hysteresis moves the peer to a new cell: force it via the stub.
                    provider.Assignments[7] = new ScopeId(2);
                    server.Tick(_ => { });
                    client.Process();

                    Assert.That(server.TryGetConnection(0, out var connection), Is.True);
                    Assert.That(connection.Connection.Scope.Value, Is.EqualTo(2u));
                    Assert.That(client.Session.Scope.Value, Is.EqualTo(2u),
                        "the client must adopt the new scope from the forced keyframe");
                    Assert.That(client.AcknowledgedSnapshotTick, Is.EqualTo(server.ServerTick),
                        "a scope reassignment must always land as a keyframe the client can apply " +
                        "and ACK immediately, never a delta it would reject for an unfamiliar scope");
                }
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
                World<ClientAWorld>.Destroy();
            }
        }

        [Test]
        public void ClientRejectsADeltaWhoseScopeDoesNotMatchItsSessionScope()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            try
            {
                var authoritySchema = Schema<AuthorityWorld>(true);
                var clientSchema = Schema<ClientAWorld>(false);
                MemoryNetworkTransport.CreatePair(new ConnectionId(301),
                    out var clientTransport, out var serverTransport);
                using (clientTransport)
                using (serverTransport)
                {
                    // A real handshake against scope 1, so the client's session scope is genuinely
                    // established (not just a constructor default) before the attack packet.
                    var server = new NetworkServer<AuthorityWorld>(authoritySchema,
                        static (_, _) => true);
                    // Epoch 1 matches the SendSnapshotChunk test helper's hardcoded packet epoch
                    // below, so the fabricated attack packet passes epoch validation and is
                    // rejected for the reason this test actually exercises (scope mismatch), not
                    // an unrelated epoch mismatch.
                    server.AddConnection(serverTransport, 3, 1, new ScopeId(1));
                    var client = new NetworkClient<ClientAWorld>(clientTransport, clientSchema,
                        new ScopeId(1));
                    client.BeginHandshake();
                    server.Receive();
                    server.Tick(_ => { });
                    client.Process();
                    Assert.That(client.Session.Scope.Value, Is.EqualTo(1u));
                    var acknowledgedBefore = client.AcknowledgedSnapshotTick;

                    // A well-formed chunk header, but for a scope the client never adopted (no
                    // keyframe ever carried scope 2): Delta must be rejected outright rather than
                    // reconstructed against the wrong baseline history.
                    var chunk = new SnapshotChunkHeader
                    {
                        PayloadKind = SnapshotPayloadKind.Delta,
                        SnapshotTick = acknowledgedBefore + 1,
                        BaselineTick = acknowledgedBefore,
                        ScopeValue = 2,
                        TotalLength = 1,
                        TotalHash = 123,
                        ChunkIndex = 0,
                        ChunkCount = 1,
                    };
                    SendSnapshotChunk(serverTransport, clientSchema.Fingerprint, 2, chunk,
                        new byte[] { 0 });
                    client.Process();

                    Assert.That(client.Session.Scope.Value, Is.EqualTo(1u),
                        "an unfamiliar-scope delta must never change the session scope");
                    Assert.That(client.AcknowledgedSnapshotTick, Is.EqualTo(acknowledgedBefore),
                        "the malformed delta must not be applied");
                    Assert.That(client.TryConsumeRecoveryTransition(out var transition),
                        Is.True);
                    Assert.That(transition.Phase,
                        Is.EqualTo(NetworkRecoveryPhase.AwaitingKeyframe));
                }
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
                World<ClientAWorld>.Destroy();
            }
        }

        [Test]
        public void OwnedEntityProvidedByTheScopeProviderIsAlwaysCaptured()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                var ownedByPeer = World<AuthorityWorld>.NewEntity<TestEntity>();
                ownedByPeer.Set(new TestComponent { Value = 42 });
                var neighbour = World<AuthorityWorld>.NewEntity<SecondEntity>();

                var provider = new StubScopeProvider();
                // The provider's contract (implemented for real by a spatial grid at the
                // game/sandbox layer) is that a peer's own entity is part of the cell it is used
                // to place the peer in, so it is always included alongside whatever else that
                // scope collects.
                provider.ScopeEntities[new ScopeId(3)] =
                    new List<World<AuthorityWorld>.Entity> { ownedByPeer, neighbour };

                var replicator = new NetworkReplicator<AuthorityWorld>(schema,
                    static (_, _) => true, scopeProvider: provider);
                try
                {
                    Assert.That(replicator.Capture(1, new ScopeId(3), out var snapshot),
                        Is.EqualTo(SnapshotCaptureResult.Success));
                    Assert.That(snapshot.EntityCount, Is.EqualTo(2));
                    Assert.That(SnapshotDeltaCodec.TryInspectCanonical(snapshot.Bytes.Span,
                        out var entities, out _), Is.True);
                    Assert.That(entities, Is.EqualTo(2));
                    snapshot.Dispose();
                }
                finally
                {
                    replicator.Dispose();
                }
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
            }
        }

        [Test]
        public void SharedHistoryByteBudgetScalesDownWithActiveScopesAndRestoresWhenOneDrops()
        {
            var schema = Schema<TestWorld>(true);
            var coordinator = new NetworkServerCoordinator<TestWorld>(64, 1_000_000);
            var scopeA = new ScopeId(1);
            var scopeB = new ScopeId(2);

            var captureA = new NetworkSnapshot(1, schema.Fingerprint, scopeA,
                Lease(new byte[] { 1 }), 0, 0);
            coordinator.StoreCapture(scopeA, captureA);
            Assert.That(coordinator.History(scopeA).MaxBytes, Is.EqualTo(1_000_000),
                "with exactly one active scope the divided budget must equal the full configured cap");

            var sessionB = new NetworkSession<TestWorld>(new ConnectionId(2), NetworkRole.Server,
                schema);
            sessionB.Admit(schema.Fingerprint, 2, 20, scopeB);
            coordinator.Add(sessionB);
            var captureB = new NetworkSnapshot(1, schema.Fingerprint, scopeB,
                Lease(new byte[] { 2 }), 0, 0);
            coordinator.StoreCapture(scopeB, captureB);

            Assert.That(coordinator.History(scopeA).MaxBytes, Is.EqualTo(500_000));
            Assert.That(coordinator.History(scopeB).MaxBytes, Is.EqualTo(500_000));

            coordinator.Remove(new ConnectionId(2));
            Assert.That(coordinator.History(scopeB), Is.Null,
                "a scope with no remaining sessions must drop its history");
            Assert.That(coordinator.History(scopeA).MaxBytes, Is.EqualTo(1_000_000),
                "dropping the second scope must restore the first scope's full budget");
        }

        /// <summary>Deterministic stand-in for a real spatial provider (NCORE-15 test double).</summary>
        private sealed class StubScopeProvider : INetworkScopeProvider<AuthorityWorld>
        {
            internal readonly Dictionary<uint, ScopeId> Assignments = new Dictionary<uint, ScopeId>();
            internal readonly Dictionary<ScopeId, List<World<AuthorityWorld>.Entity>> ScopeEntities =
                new Dictionary<ScopeId, List<World<AuthorityWorld>.Entity>>();
            internal int CollectCalls;
            internal int RefreshCalls;

            public void RefreshTick(uint serverTick) => RefreshCalls++;

            public bool TryUpdateScope(uint peerId, ref ScopeId scope)
            {
                if (!Assignments.TryGetValue(peerId, out var target) || target == scope)
                    return false;
                scope = target;
                return true;
            }

            public void CollectEntities(ScopeId scope, List<World<AuthorityWorld>.Entity> buffer,
                HashSet<EntityGID> seen)
            {
                CollectCalls++;
                if (!ScopeEntities.TryGetValue(scope, out var entities))
                    return;
                for (var i = 0; i < entities.Count; i++)
                    if (seen.Add(entities[i].GID))
                        buffer.Add(entities[i]);
            }
        }
    }
}
