using System;
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

        // NCORE-15b: a scope-mismatched delta is silently ignored instead of treated as
        // malformed (see NetworkClient.TryStageSnapshot's delta branch). The three tests below
        // exercise that contract end to end: an in-flight old-scope delta is dropped without
        // recovery or a counted protocol error, the client still accepts whatever legitimate
        // packet arrives next, and a delta that is malformed for an unrelated reason (not scope)
        // is still rejected exactly as before.
        [Test]
        public void ClientIgnoresAStaleScopeDeltaWithoutRequestingRecovery()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            try
            {
                var authoritySchema = Schema<AuthorityWorld>(true);
                var clientSchema = Schema<ClientAWorld>(false);
                var owner = World<AuthorityWorld>.NewEntity<TestEntity>();
                owner.Set(new TestComponent { Value = 1 });

                var provider = new StubScopeProvider();
                provider.ScopeEntities[new ScopeId(1)] =
                    new List<World<AuthorityWorld>.Entity> { owner };
                provider.ScopeEntities[new ScopeId(2)] =
                    new List<World<AuthorityWorld>.Entity> { owner };
                var server = new NetworkServer<AuthorityWorld>(authoritySchema,
                    static (_, _) => true, scopeProvider: provider);

                MemoryNetworkTransport.CreatePair(new ConnectionId(301),
                    out var clientTransport, out var serverTransport);
                using (clientTransport)
                using (serverTransport)
                {
                    server.AddConnection(serverTransport, 3, 1, new ScopeId(1));
                    var observer = new TraceCollector();
                    var client = new NetworkClient<ClientAWorld>(clientTransport, clientSchema,
                        new ScopeId(1), observer);
                    client.BeginHandshake();
                    server.Receive();
                    server.Tick(_ => { });
                    client.Process();
                    Assert.That(client.Session.Scope.Value, Is.EqualTo(1u));

                    // A genuine hysteresis reassignment: the client legitimately adopts scope 2
                    // from a real forced keyframe, exactly like
                    // HysteresisScopeReassignmentForcesKeyframeCarryingTheNewScope.
                    server.Receive();
                    provider.Assignments[3] = new ScopeId(2);
                    server.Tick(_ => { });
                    client.Process();
                    Assert.That(client.Session.Scope.Value, Is.EqualTo(2u));
                    var acknowledgedBefore = client.AcknowledgedSnapshotTick;
                    // Drain the harmless "recovery complete" transition every successful keyframe
                    // apply latches (see NetworkClient.ApplySnapshot's completesRecovery branch) --
                    // both keyframes above legitimately queued one -- so the assertion below
                    // observes only what the stale-scope packet itself causes.
                    client.TryConsumeRecoveryTransition(out _);

                    // A well-formed chunk header for a fresh (never-acknowledged) tick, but
                    // carrying the peer's *previous* scope -- a delta the server queued before
                    // the reassignment above and that is still, physically, in flight.
                    var chunk = new SnapshotChunkHeader
                    {
                        PayloadKind = SnapshotPayloadKind.Delta,
                        SnapshotTick = acknowledgedBefore + 1,
                        BaselineTick = acknowledgedBefore,
                        ScopeValue = 1,
                        TotalLength = 1,
                        TotalHash = 123,
                        ChunkIndex = 0,
                        ChunkCount = 1,
                    };
                    observer.Events.Clear();
                    SendSnapshotChunk(serverTransport, clientSchema.Fingerprint, 1, chunk,
                        new byte[] { 0 });
                    client.Process();

                    Assert.That(client.Session.Scope.Value, Is.EqualTo(2u),
                        "an in-flight old-scope delta must never revert the session scope");
                    Assert.That(client.AcknowledgedSnapshotTick, Is.EqualTo(acknowledgedBefore),
                        "the stale-scope delta must not be applied");
                    Assert.That(client.TryConsumeRecoveryTransition(out _), Is.False,
                        "an in-flight old-scope delta must not trigger a resync/recovery");
                    Assert.That(
                        observer.Single(NetworkPhase.Decode, NetworkPacketKind.SnapshotChunk)
                            .Result,
                        Is.EqualTo(NetworkResultCategory.Rejected),
                        "must be classified outside {Protocol, Malformed, Schema, Limits} so " +
                        "load-harness/game protocol-error counters do not count it as an error");
                }
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
                World<ClientAWorld>.Destroy();
            }
        }

        [Test]
        public void ClientAcceptsTheNextLegitimateDeltaAfterIgnoringAStaleScopeDelta()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            try
            {
                var authoritySchema = Schema<AuthorityWorld>(true);
                var clientSchema = Schema<ClientAWorld>(false);
                var owner = World<AuthorityWorld>.NewEntity<TestEntity>();
                owner.Set(new TestComponent { Value = 1 });

                var provider = new StubScopeProvider();
                provider.ScopeEntities[new ScopeId(1)] =
                    new List<World<AuthorityWorld>.Entity> { owner };
                provider.ScopeEntities[new ScopeId(2)] =
                    new List<World<AuthorityWorld>.Entity> { owner };
                var server = new NetworkServer<AuthorityWorld>(authoritySchema,
                    static (_, _) => true, scopeProvider: provider);

                MemoryNetworkTransport.CreatePair(new ConnectionId(302),
                    out var clientTransport, out var serverTransport);
                using (clientTransport)
                using (serverTransport)
                {
                    server.AddConnection(serverTransport, 4, 1, new ScopeId(1));
                    var client = new NetworkClient<ClientAWorld>(clientTransport, clientSchema,
                        new ScopeId(1));
                    client.BeginHandshake();
                    server.Receive();
                    server.Tick(_ => { });
                    client.Process();

                    server.Receive();
                    provider.Assignments[4] = new ScopeId(2);
                    server.Tick(_ => { });
                    client.Process();
                    var acknowledgedBefore = client.AcknowledgedSnapshotTick;

                    var staleChunk = new SnapshotChunkHeader
                    {
                        PayloadKind = SnapshotPayloadKind.Delta,
                        SnapshotTick = acknowledgedBefore + 1,
                        BaselineTick = acknowledgedBefore,
                        ScopeValue = 1,
                        TotalLength = 1,
                        TotalHash = 123,
                        ChunkIndex = 0,
                        ChunkCount = 1,
                    };
                    SendSnapshotChunk(serverTransport, clientSchema.Fingerprint, 1, staleChunk,
                        new byte[] { 0 });
                    client.Process();

                    // The real server, still targeting scope 2, keeps ticking normally: its next
                    // (legitimate) send for the peer's current scope must still be accepted --
                    // ignoring the stale packet above must not wedge assembly or ACK progression.
                    server.Receive();
                    server.Tick(_ => { });
                    client.Process();

                    Assert.That(client.Session.Scope.Value, Is.EqualTo(2u));
                    Assert.That(client.AcknowledgedSnapshotTick, Is.EqualTo(server.ServerTick),
                        "a legitimate same-scope packet after a stale-scope drop must still " +
                        "advance the client's acknowledged tick");
                }
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
                World<ClientAWorld>.Destroy();
            }
        }

        [Test]
        public void ClientStillRejectsADeltaWithAnUnknownBaselineAtItsOwnCurrentScope()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            try
            {
                var authoritySchema = Schema<AuthorityWorld>(true);
                var clientSchema = Schema<ClientAWorld>(false);
                MemoryNetworkTransport.CreatePair(new ConnectionId(303),
                    out var clientTransport, out var serverTransport);
                using (clientTransport)
                using (serverTransport)
                {
                    var server = new NetworkServer<AuthorityWorld>(authoritySchema,
                        static (_, _) => true);
                    server.AddConnection(serverTransport, 5, 1, new ScopeId(1));
                    var client = new NetworkClient<ClientAWorld>(clientTransport, clientSchema,
                        new ScopeId(1));
                    client.BeginHandshake();
                    server.Receive();
                    server.Tick(_ => { });
                    client.Process();
                    Assert.That(client.Session.Scope.Value, Is.EqualTo(1u));
                    var acknowledgedBefore = client.AcknowledgedSnapshotTick;
                    // Drain the harmless "recovery complete" transition every successful keyframe
                    // apply latches (see NetworkClient.ApplySnapshot's completesRecovery branch),
                    // so the assertion below observes only what this test's packet causes.
                    client.TryConsumeRecoveryTransition(out _);

                    // The scope matches the client's own current scope, so this is not the
                    // NCORE-15b stale-scope case: the baseline tick is a real, known history
                    // entry, but the delta body is corrupt. This must still be treated as
                    // malformed and trigger recovery exactly as before this change.
                    var chunk = new SnapshotChunkHeader
                    {
                        PayloadKind = SnapshotPayloadKind.Delta,
                        SnapshotTick = acknowledgedBefore + 1,
                        BaselineTick = acknowledgedBefore,
                        ScopeValue = 1,
                        TotalLength = 1,
                        TotalHash = 123,
                        ChunkIndex = 0,
                        ChunkCount = 1,
                    };
                    SendSnapshotChunk(serverTransport, clientSchema.Fingerprint, 1, chunk,
                        new byte[] { 0 });
                    client.Process();

                    Assert.That(client.AcknowledgedSnapshotTick, Is.EqualTo(acknowledgedBefore),
                        "the malformed delta must not be applied");
                    Assert.That(client.TryConsumeRecoveryTransition(out var transition),
                        Is.True, "a genuinely malformed delta must still trigger recovery");
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

        // NCORE-15b: merge-capture -- a scope provider that additionally implements
        // INetworkCellularScopeProvider makes NetworkReplicator.Capture serialize each cell once
        // and build a scope by copying its member cells' already-encoded bytes (CaptureViaCells)
        // instead of re-collecting and re-serializing the whole scope every time.
        [Test]
        public void MergeCaptureProducesByteIdenticalSnapshotsToThePlainPerScopeCapturePath()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                var a = World<AuthorityWorld>.NewEntity<TestEntity>();
                a.Set(new TestComponent { Value = 1 });
                var b = World<AuthorityWorld>.NewEntity<TestEntity>();
                b.Set(new TestComponent { Value = 2 });
                var c = World<AuthorityWorld>.NewEntity<TestEntity>();
                c.Set(new TestComponent { Value = 3 });

                var cellA = new ScopeId(101);
                var cellB = new ScopeId(102);
                var cellC = new ScopeId(103);
                var scope = new ScopeId(200);

                var plainProvider = new StubScopeProvider();
                plainProvider.ScopeEntities[scope] =
                    new List<World<AuthorityWorld>.Entity> { a, b, c };
                var plainReplicator = new NetworkReplicator<AuthorityWorld>(schema,
                    static (_, _) => true, scopeProvider: plainProvider);

                var cellularProvider = new StubCellularScopeProvider();
                cellularProvider.CellEntities[cellA] =
                    new List<World<AuthorityWorld>.Entity> { a };
                cellularProvider.CellEntities[cellB] =
                    new List<World<AuthorityWorld>.Entity> { b };
                cellularProvider.CellEntities[cellC] =
                    new List<World<AuthorityWorld>.Entity> { c };
                cellularProvider.ScopeCells[scope] = new[] { cellA, cellB, cellC };
                var cellularReplicator = new NetworkReplicator<AuthorityWorld>(schema,
                    static (_, _) => true, scopeProvider: cellularProvider);
                try
                {
                    Assert.That(plainReplicator.Capture(1, scope, out var plainSnapshot),
                        Is.EqualTo(SnapshotCaptureResult.Success));
                    Assert.That(cellularReplicator.Capture(1, scope, out var cellularSnapshot),
                        Is.EqualTo(SnapshotCaptureResult.Success));

                    Assert.That(
                        cellularSnapshot.Bytes.Span.SequenceEqual(plainSnapshot.Bytes.Span),
                        Is.True,
                        "merge-capture must be byte-for-byte identical to the plain " +
                        "per-scope capture path");
                    Assert.That(cellularSnapshot.EntityCount,
                        Is.EqualTo(plainSnapshot.EntityCount));
                    Assert.That(cellularSnapshot.RecordCount,
                        Is.EqualTo(plainSnapshot.RecordCount));
                    Assert.That(cellularSnapshot.PayloadHash,
                        Is.EqualTo(plainSnapshot.PayloadHash));

                    plainSnapshot.Dispose();
                    cellularSnapshot.Dispose();
                }
                finally
                {
                    plainReplicator.Dispose();
                    cellularReplicator.Dispose();
                }
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
            }
        }

        [Test]
        public void CaptureViaCellsSharesCellCaptureAcrossOverlappingScopesWithinOneTick()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                var shared = World<AuthorityWorld>.NewEntity<TestEntity>();
                shared.Set(new TestComponent { Value = 1 });
                var onlyInScopeOne = World<AuthorityWorld>.NewEntity<TestEntity>();
                onlyInScopeOne.Set(new TestComponent { Value = 2 });
                var onlyInScopeTwo = World<AuthorityWorld>.NewEntity<TestEntity>();
                onlyInScopeTwo.Set(new TestComponent { Value = 3 });

                var sharedCell = new ScopeId(301);
                var cellOne = new ScopeId(302);
                var cellTwo = new ScopeId(303);
                var scopeOne = new ScopeId(400);
                var scopeTwo = new ScopeId(401);

                var provider = new StubCellularScopeProvider();
                provider.CellEntities[sharedCell] =
                    new List<World<AuthorityWorld>.Entity> { shared };
                provider.CellEntities[cellOne] =
                    new List<World<AuthorityWorld>.Entity> { onlyInScopeOne };
                provider.CellEntities[cellTwo] =
                    new List<World<AuthorityWorld>.Entity> { onlyInScopeTwo };
                provider.ScopeCells[scopeOne] = new[] { sharedCell, cellOne };
                provider.ScopeCells[scopeTwo] = new[] { sharedCell, cellTwo };

                var replicator = new NetworkReplicator<AuthorityWorld>(schema,
                    static (_, _) => true, scopeProvider: provider);
                try
                {
                    Assert.That(replicator.Capture(1, scopeOne, out var snapshotOne),
                        Is.EqualTo(SnapshotCaptureResult.Success));
                    Assert.That(replicator.Capture(1, scopeTwo, out var snapshotTwo),
                        Is.EqualTo(SnapshotCaptureResult.Success));

                    Assert.That(GetOrZero(provider.CollectCellCallCounts, sharedCell),
                        Is.EqualTo(1),
                        "a cell shared by two scopes captured in the same tick must be " +
                        "serialized exactly once");
                    Assert.That(GetOrZero(provider.CollectCellCallCounts, cellOne),
                        Is.EqualTo(1));
                    Assert.That(GetOrZero(provider.CollectCellCallCounts, cellTwo),
                        Is.EqualTo(1));
                    Assert.That(snapshotOne.EntityCount, Is.EqualTo(2));
                    Assert.That(snapshotTwo.EntityCount, Is.EqualTo(2));

                    snapshotOne.Dispose();
                    snapshotTwo.Dispose();

                    // Freshness is per tick, not sticky forever: a later tick must re-serialize.
                    Assert.That(replicator.Capture(2, scopeOne, out var snapshotOneAgain),
                        Is.EqualTo(SnapshotCaptureResult.Success));
                    Assert.That(GetOrZero(provider.CollectCellCallCounts, sharedCell),
                        Is.EqualTo(2));
                    snapshotOneAgain.Dispose();
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
        public void CaptureViaCellsHonoursTheUpdateHoldPolicyPerCell()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                var entity = World<AuthorityWorld>.NewEntity<TestEntity>();
                entity.Set(new TestComponent { Value = 1 });

                var cell = new ScopeId(501);
                var scope = new ScopeId(502);
                var provider = new StubCellularScopeProvider();
                provider.CellEntities[cell] = new List<World<AuthorityWorld>.Entity> { entity };
                provider.ScopeCells[scope] = new[] { cell };

                var policy = new StubHoldEverythingPolicy();
                var replicator = new NetworkReplicator<AuthorityWorld>(schema,
                    static (_, _) => true, scopeProvider: provider, updatePolicy: policy);
                try
                {
                    Assert.That(replicator.Capture(1, scope, out var first),
                        Is.EqualTo(SnapshotCaptureResult.Success));
                    entity.Set(new TestComponent { Value = 999 });
                    Assert.That(replicator.Capture(2, scope, out var second),
                        Is.EqualTo(SnapshotCaptureResult.Success));

                    Assert.That(second.Bytes.Span.SequenceEqual(first.Bytes.Span), Is.True,
                        "the NCORE-16 hold policy must still apply per cell under merge-capture: " +
                        "a held entity's captured bytes must not move even though the live ECS " +
                        "value did");

                    first.Dispose();
                    second.Dispose();
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

        private static int GetOrZero(Dictionary<ScopeId, int> counts, ScopeId key) =>
            counts.TryGetValue(key, out var value) ? value : 0;

        /// <summary>Deterministic stand-in for a real spatial provider that also decomposes a
        /// scope into cells (NCORE-15b test double).</summary>
        private sealed class StubCellularScopeProvider : INetworkScopeProvider<AuthorityWorld>,
            INetworkCellularScopeProvider<AuthorityWorld>
        {
            internal readonly Dictionary<ScopeId, List<World<AuthorityWorld>.Entity>>
                CellEntities = new Dictionary<ScopeId, List<World<AuthorityWorld>.Entity>>();
            internal readonly Dictionary<ScopeId, ScopeId[]> ScopeCells =
                new Dictionary<ScopeId, ScopeId[]>();
            internal readonly Dictionary<ScopeId, int> CollectCellCallCounts =
                new Dictionary<ScopeId, int>();

            public void RefreshTick(uint serverTick) { }

            public bool TryUpdateScope(uint peerId, ref ScopeId scope) => false;

            public void CollectEntities(ScopeId scope, List<World<AuthorityWorld>.Entity> buffer,
                HashSet<EntityGID> seen) =>
                throw new System.InvalidOperationException(
                    "NetworkReplicator must prefer the cellular path once it is available, " +
                    "never fall back to the non-cellular one.");

            public int CollectScopeCells(ScopeId scope, Span<ScopeId> cells)
            {
                var members = ScopeCells[scope];
                members.AsSpan().CopyTo(cells);
                return members.Length;
            }

            public void CollectCellEntities(ScopeId cellId,
                List<World<AuthorityWorld>.Entity> buffer, HashSet<EntityGID> seen)
            {
                CollectCellCallCounts[cellId] = GetOrZero(CollectCellCallCounts, cellId) + 1;
                if (!CellEntities.TryGetValue(cellId, out var entities))
                    return;
                for (var i = 0; i < entities.Count; i++)
                    if (seen.Add(entities[i].GID))
                        buffer.Add(entities[i]);
            }
        }

        private sealed class StubHoldEverythingPolicy : INetworkUpdatePolicy<AuthorityWorld>
        {
            public bool ShouldHold(uint serverTick, ScopeId scope,
                in World<AuthorityWorld>.Entity entity, EntityGID gid) => true;
        }
    }
}
