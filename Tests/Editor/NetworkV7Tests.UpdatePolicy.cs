using System;
using System.Collections.Generic;
using FFS.Libraries.StaticEcs;
using NUnit.Framework;

namespace UniGame.StaticEcs.Network.Tests
{
    /// <summary>
    /// NCORE-16: per-entity update-frequency tiers. Covers the package-level hook contract
    /// (<see cref="INetworkUpdatePolicy{TWorld}"/>) inside <see cref="NetworkReplicator{TWorld}.Capture"/>:
    /// a held entity costs the delta codec nothing beyond a truly unchanged entity, an entity new to
    /// a scope (or a scope forgotten after going inactive) is always fresh regardless of the policy's
    /// answer, a "refresh" answer never holds, an entity can be held for several ticks and then
    /// refresh on its own phase tick, and a structural change (a component gained while held) only
    /// forces that one record fresh instead of un-holding the whole entity. The concrete tiered
    /// policy (activity radius, ownership, GID-hash phase) is game/sandbox-owned and tested there;
    /// this file only exercises the generic hook the package exposes to it, using a small
    /// deterministic stub policy.
    /// </summary>
    public sealed partial class NetworkV7Tests
    {
        [Test]
        public void HeldEntityDeltaCostsExactlyWhatATrulyUnchangedEntityCosts()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                var entity = World<AuthorityWorld>.NewEntity<TestEntity>();
                entity.Set(new TestComponent { Value = 1 });
                var pool = new NetworkBufferPool(NetworkBufferPool.DefaultServerRetainedBytes);
                try
                {
                    // Reference: two captures with nothing ever touched and no policy at all --
                    // today's existing "unchanged costs 0 bytes" behavior this feature must match,
                    // not merely approach.
                    var plainReplicator = new NetworkReplicator<AuthorityWorld>(schema,
                        static (_, _) => true);
                    NetworkSnapshot referenceBaseline = null, referenceTarget = null;
                    NetworkBufferLease referenceDelta = null;
                    try
                    {
                        Assert.That(plainReplicator.Capture(1, new ScopeId(1), out referenceBaseline),
                            Is.EqualTo(SnapshotCaptureResult.Success));
                        Assert.That(plainReplicator.Capture(2, new ScopeId(1), out referenceTarget),
                            Is.EqualTo(SnapshotCaptureResult.Success));
                        Assert.That(SnapshotDeltaCodec.TryEncode(pool, referenceBaseline,
                            referenceTarget, out referenceDelta, schema.DeltaHooks), Is.True);
                    }
                    finally
                    {
                        plainReplicator.Dispose();
                    }

                    // Held: the live value DOES change between captures, but the policy holds this
                    // entity, so the captured bytes must not move and the delta must cost exactly
                    // what the reference (truly unchanged) delta above cost.
                    var policy = new StubUpdatePolicy();
                    policy.HoldGids.Add(entity.GID.Raw);
                    var heldReplicator = new NetworkReplicator<AuthorityWorld>(schema,
                        static (_, _) => true, updatePolicy: policy);
                    try
                    {
                        Assert.That(heldReplicator.Capture(1, new ScopeId(2), out var heldBaseline),
                            Is.EqualTo(SnapshotCaptureResult.Success));
                        entity.Set(new TestComponent { Value = 999 });
                        Assert.That(heldReplicator.Capture(2, new ScopeId(2), out var heldTarget),
                            Is.EqualTo(SnapshotCaptureResult.Success));

                        Assert.That(heldTarget.Bytes.Span.SequenceEqual(heldBaseline.Bytes.Span),
                            Is.True,
                            "a held entity's captured bytes must not move even though the live " +
                            "ECS value did");

                        Assert.That(SnapshotDeltaCodec.TryEncode(pool, heldBaseline, heldTarget,
                            out var heldDelta, schema.DeltaHooks), Is.True);
                        Assert.That(heldDelta.Length, Is.EqualTo(referenceDelta.Length),
                            "holding must cost the delta codec exactly what a truly unchanged " +
                            "entity costs -- no partial credit for 'mostly' unchanged");

                        heldDelta.Dispose();
                        heldBaseline.Dispose();
                        heldTarget.Dispose();
                    }
                    finally
                    {
                        heldReplicator.Dispose();
                    }

                    referenceDelta?.Dispose();
                    referenceBaseline?.Dispose();
                    referenceTarget?.Dispose();
                }
                finally
                {
                    pool.Dispose();
                }
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
            }
        }

        [Test]
        public void NewEntityToScopeIsAlwaysFreshEvenWhenPolicySaysHold()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                var entity = World<AuthorityWorld>.NewEntity<TestEntity>();
                entity.Set(new TestComponent { Value = 7 });

                // Holds every entity from the very first tick: with no previous capture of this
                // scope to hold against, the very first sighting must still be fresh.
                var policy = new StubUpdatePolicy { HoldEverything = true };
                var replicator = new NetworkReplicator<AuthorityWorld>(schema,
                    static (_, _) => true, updatePolicy: policy);
                try
                {
                    Assert.That(replicator.Capture(1, new ScopeId(1), out var first),
                        Is.EqualTo(SnapshotCaptureResult.Success));
                    Assert.That(SnapshotDeltaCodec.TryInspectCanonical(first.Bytes.Span,
                        out var entities, out _), Is.True);
                    Assert.That(entities, Is.EqualTo(1));
                    // The captured Value must be the live one (7), proving this was a real write,
                    // not a copy of nonexistent previous bytes.
                    Assert.That(ContainsIntValue(first.Bytes.Span, 7), Is.True,
                        "an entity new to this scope must be freshly captured even though the " +
                        "policy answered 'hold'");
                    first.Dispose();
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
        public void PolicyAnswerFalseAlwaysRefreshesEveryTick()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                var entity = World<AuthorityWorld>.NewEntity<TestEntity>();
                entity.Set(new TestComponent { Value = 1 });

                // Simulates an owned player entity: the sandbox's tiered policy always answers
                // "refresh" for it, regardless of activity radius or phase.
                var policy = new StubUpdatePolicy();
                var replicator = new NetworkReplicator<AuthorityWorld>(schema,
                    static (_, _) => true, updatePolicy: policy);
                try
                {
                    Assert.That(replicator.Capture(1, new ScopeId(1), out var first),
                        Is.EqualTo(SnapshotCaptureResult.Success));
                    entity.Set(new TestComponent { Value = 2 });
                    Assert.That(replicator.Capture(2, new ScopeId(1), out var second),
                        Is.EqualTo(SnapshotCaptureResult.Success));

                    Assert.That(second.Bytes.Span.SequenceEqual(first.Bytes.Span), Is.False,
                        "an entity the policy never holds must reflect every live change");
                    Assert.That(ContainsIntValue(second.Bytes.Span, 2), Is.True);

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

        [Test]
        public void PeriodicHoldPatternRefreshesOnlyOnItsOwnPhaseTick()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                var entity = World<AuthorityWorld>.NewEntity<TestEntity>();
                entity.Set(new TestComponent { Value = 0 });

                // A period-4 tier, the sandbox's own example for far NPCs: held on 3 of every 4
                // ticks, refreshed on the 4th (phase offsetting by GID hash is the sandbox's own
                // concern; this only proves the mechanism supports an arbitrary period correctly).
                var policy = new StubUpdatePolicy { RefreshEveryNthTick = 4 };
                var replicator = new NetworkReplicator<AuthorityWorld>(schema,
                    static (_, _) => true, updatePolicy: policy);
                try
                {
                    // Genesis capture at tick 0: unconditionally fresh (no previous capture of
                    // this scope exists yet), independently of what the periodic policy would
                    // otherwise answer. The periodic pattern itself is only exercised from here.
                    Assert.That(replicator.Capture(0, new ScopeId(1), out var previous),
                        Is.EqualTo(SnapshotCaptureResult.Success));
                    for (uint tick = 1; tick <= 4; tick++)
                    {
                        entity.Set(new TestComponent { Value = (int)tick });
                        Assert.That(replicator.Capture(tick, new ScopeId(1), out var capture),
                            Is.EqualTo(SnapshotCaptureResult.Success));
                        if (tick % 4 != 0)
                            Assert.That(capture.Bytes.Span.SequenceEqual(previous.Bytes.Span),
                                Is.True, $"tick {tick} is held and must repeat the last " +
                                "refreshed bytes (value 0, from the genesis capture)");
                        else
                            Assert.That(ContainsIntValue(capture.Bytes.Span, (int)tick), Is.True,
                                $"tick {tick} is this entity's own refresh tick");
                        previous.Dispose();
                        previous = capture;
                    }
                    previous.Dispose();
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
        public void ForgetScopeClearsHeldCacheSoNextCaptureIsFresh()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                var entity = World<AuthorityWorld>.NewEntity<TestEntity>();
                entity.Set(new TestComponent { Value = 1 });

                var policy = new StubUpdatePolicy { HoldEverything = true };
                var replicator = new NetworkReplicator<AuthorityWorld>(schema,
                    static (_, _) => true, updatePolicy: policy);
                try
                {
                    var scope = new ScopeId(5);
                    Assert.That(replicator.Capture(1, scope, out var first),
                        Is.EqualTo(SnapshotCaptureResult.Success));
                    first.Dispose();

                    entity.Set(new TestComponent { Value = 2 });
                    // Mirrors NCORE-15: once a scope has no established peer left, its shared
                    // capture history is dropped -- the held-entity cache must follow it, so a
                    // scope that becomes active again later starts from a clean slate instead of
                    // silently resurrecting a stale generation's bytes.
                    replicator.ForgetScope(scope);

                    Assert.That(replicator.Capture(2, scope, out var second),
                        Is.EqualTo(SnapshotCaptureResult.Success));
                    Assert.That(ContainsIntValue(second.Bytes.Span, 2), Is.True,
                        "after ForgetScope, the next capture must be fresh even though the " +
                        "policy still answers 'hold'");
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

        [Test]
        public void ComponentAddedWhileHeldIsWrittenFreshWithoutUnholdingExistingRecords()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                var entity = World<AuthorityWorld>.NewEntity<TestEntity>();
                entity.Set(new TestComponent { Value = 11 });

                var policy = new StubUpdatePolicy { HoldEverything = true };
                var replicator = new NetworkReplicator<AuthorityWorld>(schema,
                    static (_, _) => true, updatePolicy: policy);
                try
                {
                    Assert.That(replicator.Capture(1, new ScopeId(1), out var first),
                        Is.EqualTo(SnapshotCaptureResult.Success));
                    Assert.That(first.RecordCount, Is.EqualTo(1));

                    // The value changes AND a new component appears while the entity is held.
                    // The held TestComponent record must still freeze at 11; only the newly
                    // present NetworkOwnerComponent record may reflect live data.
                    entity.Set(new TestComponent { Value = 999 });
                    entity.Set(new NetworkOwnerComponent { PeerId = 42 });
                    Assert.That(replicator.Capture(2, new ScopeId(1), out var second),
                        Is.EqualTo(SnapshotCaptureResult.Success));

                    Assert.That(second.RecordCount, Is.EqualTo(2),
                        "the newly present component must still be captured");
                    Assert.That(ContainsIntValue(second.Bytes.Span, 999), Is.False,
                        "the held record's value must not have moved to the live one");
                    Assert.That(ContainsIntValue(second.Bytes.Span, 11), Is.True,
                        "the held record must still carry its frozen value");

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

        [Test]
        public void ScopeCaptureVariantsIsolateHeldBytesAndBoundTheirCacheLifetime()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                var entity = World<AuthorityWorld>.NewEntity<TestEntity>();
                entity.Set(new TestComponent { Value = 1 });
                var cell = new ScopeId(701);
                var heldScope = new ScopeId(702);
                var freshScope = new ScopeId(703);
                var provider = new StubCellularScopeProvider();
                provider.CellEntities[cell] = new List<World<AuthorityWorld>.Entity> { entity };
                provider.ScopeCells[heldScope] = new[] { cell };
                provider.ScopeCells[freshScope] = new[] { cell };
                var policy = new ScopeVariantStubPolicy(heldScope);
                using var pool = new NetworkBufferPool(1L << 20);
                var replicator = new NetworkReplicator<AuthorityWorld>(schema,
                    static (_, _) => true, scopeProvider: provider, updatePolicy: policy,
                    bufferPool: pool);
                try
                {
                    Assert.That(replicator.Capture(1, heldScope, out var heldFirst),
                        Is.EqualTo(SnapshotCaptureResult.Success));
                    Assert.That(replicator.Capture(1, freshScope, out var freshFirst),
                        Is.EqualTo(SnapshotCaptureResult.Success));
                    Assert.That(replicator.CellCaptureVariantCount, Is.EqualTo(2),
                        "the same cell needs distinct capture state for distinct observing scopes");
                    Assert.That(GetOrZero(provider.CollectCellCallCounts, cell), Is.EqualTo(2),
                        "a scope-relative policy must not reuse one scope's held bytes for another");
                    heldFirst.Dispose();
                    freshFirst.Dispose();

                    entity.Set(new TestComponent { Value = 2 });
                    Assert.That(replicator.Capture(2, heldScope, out var heldSecond),
                        Is.EqualTo(SnapshotCaptureResult.Success));
                    Assert.That(replicator.Capture(2, freshScope, out var freshSecond),
                        Is.EqualTo(SnapshotCaptureResult.Success));
                    Assert.That(ContainsIntValue(heldSecond.Bytes.Span, 1), Is.True,
                        "the held scope keeps its own cached entity bytes");
                    Assert.That(ContainsIntValue(freshSecond.Bytes.Span, 2), Is.True,
                        "another scope must capture the live entity when its policy refreshes it");
                    heldSecond.Dispose();
                    freshSecond.Dispose();

                    replicator.ForgetScope(heldScope);
                    Assert.That(replicator.CellCaptureVariantCount, Is.EqualTo(1),
                        "last-consumer cleanup removes only the departed scope variant");

                    entity.Set(new TestComponent { Value = 3 });
                    Assert.That(replicator.Capture(3, heldScope, out var rejoined),
                        Is.EqualTo(SnapshotCaptureResult.Success));
                    Assert.That(ContainsIntValue(rejoined.Bytes.Span, 3), Is.True,
                        "a rejoined scope starts from a fresh capture, even when it would hold");
                    rejoined.Dispose();

                    Assert.That(replicator.Capture(68, freshScope, out var afterStaleBound),
                        Is.EqualTo(SnapshotCaptureResult.Success));
                    afterStaleBound.Dispose();
                    Assert.That(replicator.CellCaptureVariantCount, Is.EqualTo(1),
                        "the 64-tick stale bound releases inactive capture variants");
                    Assert.That(replicator.CellCaptureVariantEvictions, Is.GreaterThanOrEqualTo(3));
                }
                finally
                {
                    replicator.Dispose();
                    Assert.That(pool.CaptureDiagnostics().OutstandingLeases, Is.Zero);
                }
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
            }
        }

        [Test]
        public void SnapshotAttributionLeavesDeltaRecoveryWireIdentical()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            try
            {
                var schema = Schema<AuthorityWorld>(true);
                var marked = World<AuthorityWorld>.NewEntity<TestEntity>();
                marked.Set(new TestComponent { Value = 1 });
                var unmarked = World<AuthorityWorld>.NewEntity<TestEntity>();
                unmarked.Set(new TestComponent { Value = 10 });
                var scope = new ScopeId(801);
                var policy = new AttributionStubPolicy(marked.GID.Raw);
                using var pool = new NetworkBufferPool(1L << 20);
                var plain = new NetworkReplicator<AuthorityWorld>(schema,
                    static (_, _) => true);
                var attributed = new NetworkReplicator<AuthorityWorld>(schema,
                    static (_, _) => true, updatePolicy: policy);
                NetworkSnapshot plainBaseline = null;
                NetworkSnapshot plainTarget = null;
                NetworkSnapshot attributedBaseline = null;
                NetworkSnapshot attributedTarget = null;
                NetworkBufferLease plainDelta = null;
                NetworkBufferLease attributedDelta = null;
                NetworkBufferLease reconstructed = null;
                try
                {
                    Assert.That(plain.Capture(1, scope, out plainBaseline),
                        Is.EqualTo(SnapshotCaptureResult.Success));
                    Assert.That(attributed.Capture(1, scope, out attributedBaseline),
                        Is.EqualTo(SnapshotCaptureResult.Success));
                    Assert.That(plainBaseline.Bytes.Span.SequenceEqual(
                        attributedBaseline.Bytes.Span), Is.True);

                    marked.Set(new TestComponent { Value = 2 });
                    Assert.That(plain.Capture(2, scope, out plainTarget),
                        Is.EqualTo(SnapshotCaptureResult.Success));
                    Assert.That(attributed.Capture(2, scope, out attributedTarget),
                        Is.EqualTo(SnapshotCaptureResult.Success));
                    Assert.That(plainTarget.Bytes.Span.SequenceEqual(
                        attributedTarget.Bytes.Span), Is.True);

                    Assert.That(SnapshotDeltaCodec.TryEncode(pool, plainBaseline,
                        plainTarget, out plainDelta, schema.DeltaHooks), Is.True);
                    Assert.That(SnapshotDeltaCodec.TryEncode(pool, attributedBaseline,
                        attributedTarget, out attributedDelta, schema.DeltaHooks,
                        out var attribution), Is.True);
                    Assert.That(attributedDelta.Span.SequenceEqual(plainDelta.Span), Is.True,
                        "attribution is observational and must not change the ordinary delta wire");
                    // This fixture has one changed TestComponent. The delta body is its
                    // one-byte version plus three uint32 header fields, followed by the
                    // zero skip, PatchFast opcode, one-byte mask, four-byte payload length,
                    // and the four-byte TestComponent value.
                    const int deltaHeaderBytes = 1 + sizeof(uint) * 3;
                    const int markedPatchBytes = 1 + 1 + 1 + 1 + sizeof(int);
                    var expectedEncodedDeltaBytes = deltaHeaderBytes + markedPatchBytes;
                    Assert.That(attribution.AttributedRecordBytes,
                        Is.EqualTo(markedPatchBytes),
                        "attribution covers only the marked PatchFast operation");
                    Assert.That(attribution.EncodedSnapshotBytes,
                        Is.EqualTo(expectedEncodedDeltaBytes),
                        "encoded attribution total is the complete delta body");
                    Assert.That(attributedDelta.Length,
                        Is.EqualTo(expectedEncodedDeltaBytes));
                    Assert.That(attribution.IsReconciled, Is.True);

                    var header = DeltaHeader(attributedBaseline, attributedTarget);
                    Assert.That(SnapshotDeltaCodec.TryReconstruct(pool, attributedBaseline,
                        attributedDelta.Span, in header, schema.Fingerprint, scope,
                        out reconstructed, out _, out _, schema.DeltaHooks), Is.True);
                    Assert.That(reconstructed.Span.SequenceEqual(attributedTarget.Bytes.Span),
                        Is.True, "attribution must preserve ordinary baseline recovery");
                }
                finally
                {
                    reconstructed?.Dispose();
                    attributedDelta?.Dispose();
                    plainDelta?.Dispose();
                    attributedTarget?.Dispose();
                    attributedBaseline?.Dispose();
                    plainTarget?.Dispose();
                    plainBaseline?.Dispose();
                    attributed.Dispose();
                    plain.Dispose();
                    Assert.That(pool.CaptureDiagnostics().OutstandingLeases, Is.Zero);
                }
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
            }
        }

        // Looks for a little-endian int32 anywhere in a canonical snapshot's bytes. Good enough to
        // distinguish "the held/frozen value" from "the live value" in these tests without decoding
        // the whole wire format by hand.
        private static bool ContainsIntValue(System.ReadOnlySpan<byte> bytes, int value)
        {
            if (bytes.Length < sizeof(int))
                return false;
            for (var i = 0; i <= bytes.Length - sizeof(int); i++)
            {
                var candidate = bytes[i] | bytes[i + 1] << 8 | bytes[i + 2] << 16 |
                                bytes[i + 3] << 24;
                if (candidate == value)
                    return true;
            }
            return false;
        }

        /// <summary>Deterministic stand-in for a real tiered policy (NCORE-16 test double).</summary>
        private sealed class StubUpdatePolicy : INetworkUpdatePolicy<AuthorityWorld>
        {
            internal readonly HashSet<ulong> HoldGids = new HashSet<ulong>();
            internal bool HoldEverything;
            /// <summary>0 disables periodic behavior; otherwise holds every tick except one where
            /// <c>serverTick % RefreshEveryNthTick == 0</c>.</summary>
            internal uint RefreshEveryNthTick;
            internal int Calls;

            public bool ShouldHold(uint serverTick, ScopeId scope,
                in World<AuthorityWorld>.Entity entity, EntityGID gid)
            {
                Calls++;
                if (RefreshEveryNthTick > 0)
                    return serverTick % RefreshEveryNthTick != 0;
                return HoldEverything || HoldGids.Contains(gid.Raw);
            }
        }

        private sealed class ScopeVariantStubPolicy :
            INetworkScopeCaptureVariantPolicy<AuthorityWorld>
        {
            private readonly ScopeId _heldScope;

            internal ScopeVariantStubPolicy(ScopeId heldScope)
            {
                _heldScope = heldScope;
            }

            public ulong CapturePolicyEpoch => 1;

            public bool ShouldHold(uint serverTick, ScopeId scope,
                in World<AuthorityWorld>.Entity entity, EntityGID gid) => scope == _heldScope;
        }

        private sealed class AttributionStubPolicy : INetworkUpdatePolicy<AuthorityWorld>,
            INetworkSnapshotAttributionPolicy<AuthorityWorld>
        {
            private readonly ulong _markedGid;

            internal AttributionStubPolicy(ulong markedGid)
            {
                _markedGid = markedGid;
            }

            public bool ShouldHold(uint serverTick, ScopeId scope,
                in World<AuthorityWorld>.Entity entity, EntityGID gid) => false;

            public bool ShouldAttribute(in World<AuthorityWorld>.Entity entity,
                EntityGID gid) => gid.Raw == _markedGid;
        }
    }
}
