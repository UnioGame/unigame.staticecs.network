namespace UniGame.StaticEcs.Network.Tests
{
    using System;
    using FFS.Libraries.StaticEcs;
    using NUnit.Framework;

    // NCORE-26b: correctness coverage for NetworkReplicator.AcceptCanonical, the opt-in
    // Stage()+Apply() substitute a "light" client (e.g. the thin load generator's light client
    // mode) uses to accept an already schema/hash-verified canonical snapshot as its new baseline
    // without ever creating, initializing, or touching World<TWorld>. Fixture-building mirrors
    // NetworkReconstructionCacheTests: canonical snapshot bytes/objects are built directly, no ECS
    // world involved, since AcceptCanonical's whole point is never needing one.
    public sealed class NetworkReplicatorAcceptCanonicalTests
    {
        public struct AcceptCanonicalWorld : IWorldType { }
        public struct AcceptCanonicalEntity : IEntityType, INetworkType { public byte Id() => 1; }

        private static readonly ScopeId Scope = new ScopeId(1);
        private static readonly ScopeId OtherScope = new ScopeId(2);

        [Test]
        public void AcceptCanonicalStoresSnapshotAsFutureBaselineWithoutTouchingEcs()
        {
            // The whole point of AcceptCanonical is that it never needs World<TWorld> to exist;
            // asserting NotCreated for the entire test (nobody ever calls World<>.Create here) is
            // the direct proof, not an incidental side observation.
            Assert.That(World<AcceptCanonicalWorld>.Status, Is.EqualTo(WorldStatus.NotCreated));

            var pool = new NetworkBufferPool(1L << 16);
            try
            {
                var schema = Schema();
                var replicator = new NetworkReplicator<AcceptCanonicalWorld>(schema, Scope);
                var snapshot = new NetworkSnapshot(5, schema.Fingerprint, Scope,
                    pool.Copy(new byte[] { 1, 2, 3 }), 0, 0);

                Assert.That(replicator.AcceptCanonical(snapshot),
                    Is.EqualTo(SnapshotApplyResult.Success));

                Assert.That(replicator.History.Count, Is.EqualTo(1));
                Assert.That(replicator.History.TryGet(5, out var stored), Is.True);
                Assert.That(stored, Is.SameAs(snapshot));

                var next = new NetworkSnapshot(6, schema.Fingerprint, Scope,
                    pool.Copy(new byte[] { 4, 5, 6 }), 0, 0);
                Assert.That(replicator.AcceptCanonical(next),
                    Is.EqualTo(SnapshotApplyResult.Success));
                Assert.That(replicator.History.Count, Is.EqualTo(2));
                Assert.That(replicator.History.TryGet(6, out var storedNext), Is.True);
                Assert.That(storedNext, Is.SameAs(next));

                Assert.That(World<AcceptCanonicalWorld>.Status, Is.EqualTo(WorldStatus.NotCreated));
                replicator.Dispose();
            }
            finally
            {
                pool.Dispose();
            }
        }

        [Test]
        public void AcceptCanonicalRejectsSchemaMismatchWithoutStoringOrDisposing()
        {
            var pool = new NetworkBufferPool(1L << 16);
            try
            {
                var schema = Schema();
                // A real generated schema fingerprint is a hash of its registered type set and is
                // never the empty fingerprint in practice; asserting that first makes Empty a
                // reliably *different* value below rather than a coincidence.
                Assert.That(schema.Fingerprint, Is.Not.EqualTo(SchemaFingerprint.Empty));
                var replicator = new NetworkReplicator<AcceptCanonicalWorld>(schema, Scope);
                var snapshot = new NetworkSnapshot(1, SchemaFingerprint.Empty, Scope,
                    pool.Copy(new byte[] { 1 }), 0, 0);

                Assert.That(replicator.AcceptCanonical(snapshot),
                    Is.EqualTo(SnapshotApplyResult.SchemaMismatch));
                Assert.That(replicator.History.Count, Is.Zero);

                // Rejected on schema mismatch: AcceptCanonical never took ownership, so the
                // caller must still dispose it itself — exactly like Stage()'s SchemaMismatch
                // path leaves the snapshot for its caller to dispose.
                snapshot.Dispose();
                replicator.Dispose();
            }
            finally
            {
                pool.Dispose();
            }
        }

        [Test]
        public void AcceptCanonicalRejectsScopeMismatchWithoutStoringOrDisposing()
        {
            var pool = new NetworkBufferPool(1L << 16);
            try
            {
                var schema = Schema();
                var replicator = new NetworkReplicator<AcceptCanonicalWorld>(schema, Scope);
                var snapshot = new NetworkSnapshot(1, schema.Fingerprint, OtherScope,
                    pool.Copy(new byte[] { 1 }), 0, 0);

                Assert.That(replicator.AcceptCanonical(snapshot),
                    Is.EqualTo(SnapshotApplyResult.SchemaMismatch));
                Assert.That(replicator.History.Count, Is.Zero);

                snapshot.Dispose();
                replicator.Dispose();
            }
            finally
            {
                pool.Dispose();
            }
        }

        [Test]
        public void AcceptCanonicalRejectsNullSnapshot()
        {
            var replicator = new NetworkReplicator<AcceptCanonicalWorld>(Schema(), Scope);
            Assert.That(replicator.AcceptCanonical(null),
                Is.EqualTo(SnapshotApplyResult.LimitExceeded));
            Assert.That(replicator.History.Count, Is.Zero);
            replicator.Dispose();
        }

        private static NetworkSchema<AcceptCanonicalWorld> Schema()
        {
            var factory = NetworkCompilerSupport.Create<AcceptCanonicalWorld>();
            factory.Entity<AcceptCanonicalEntity>(new NetworkTypeId(1));
            return factory.Freeze();
        }
    }
}
