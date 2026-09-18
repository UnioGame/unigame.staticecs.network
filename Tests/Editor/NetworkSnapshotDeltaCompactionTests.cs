namespace UniGame.StaticEcs.Network.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using FFS.Libraries.StaticEcs;
    using NUnit.Framework;

    // NCORE-13: golden/property coverage for the compact snapshot delta wire
    // format (SnapshotDeltaCodec). Unlike NetworkV7SnapshotsTests, these tests
    // build canonical snapshot bytes directly from primitive descriptors instead
    // of driving a Static ECS world through NetworkReplicator.Capture: the codec
    // only ever sees already-serialized canonical bytes, so exercising it this
    // way lets a single test sweep many entity/record shapes (counts, adds,
    // removes, disabled toggles, payload sizes) without paying for a world per
    // case, while still round-tripping through the exact same internal API
    // (SnapshotDeltaCodec.TryEncode / TryReconstruct) production code calls.
    //
    // "Old vs new identical reconstruction" here means: the reconstructed
    // canonical bytes and xxHash64 must exactly equal the target snapshot's own
    // canonical bytes/hash, which is the invariant NetworkReplicator.Stage/Apply
    // and NetworkClient actually depend on (canonical semantics are unchanged by
    // NCORE-13; only the delta wire encoding is). There is no separate "old"
    // codec left in this package to diff against — the encoder and decoder were
    // both replaced together, and the golden test below pins the pre-NCORE-13
    // byte layout for the still-supported plain/no-op case.
    public sealed class NetworkSnapshotDeltaCompactionTests
    {
        private readonly struct RecordDesc
        {
            internal RecordDesc(uint typeId, byte kind, byte version,
                bool disabled, byte[] payload)
            {
                TypeId = typeId;
                Kind = kind;
                Version = version;
                Disabled = disabled;
                Payload = payload;
            }

            internal uint TypeId { get; }
            internal byte Kind { get; }
            internal byte Version { get; }
            internal bool Disabled { get; }
            internal byte[] Payload { get; }
        }

        private readonly struct EntityDesc
        {
            internal EntityDesc(ulong gid, uint kind, bool disabled,
                RecordDesc[] records)
            {
                Gid = gid;
                Kind = kind;
                Disabled = disabled;
                Records = records;
            }

            internal ulong Gid { get; }
            internal uint Kind { get; }
            internal bool Disabled { get; }
            internal RecordDesc[] Records { get; }
        }

        private static readonly SchemaFingerprint Fingerprint =
            new SchemaFingerprint(0x1111_2222_3333_4444, 0x5555_6666_7777_8888);
        private static readonly ScopeId Scope = new ScopeId(1);

        [Test]
        public void MovingEntitySingleFixedRecordPatchIsAtMostSixteenBytes()
        {
            var bufferPool = new NetworkBufferPool(1L << 20);
            try
            {
                // The acceptance target ("<=16 B per moving entity") is a
                // marginal, per-entity cost in a crowd: a single-entity
                // snapshot's delta is dominated by the fixed ~13 B header, so
                // measure the cost a moving entity ADDS on top of a same-sized
                // ballast crowd that does not move (that ballast crowd costs
                // nothing beyond the header: unchanged entities are pure
                // skip-count arithmetic, see EmptyDeltaIsHeaderOnly).
                const int ballastCount = 200;
                var ballast = RandomEntities(new Random(5), ballastCount);
                var movingGid = new EntityGID(5_000_001, 1, 0).Raw;
                var baselineEntities = InsertSorted(ballast, new EntityDesc(
                    movingGid, 1, false, new[]
                    {
                        // A 12-byte fixed payload, standing in for a
                        // PositionComponent's serialized float3.
                        PositionRecord(1, 0f, 1f, 2f),
                    }));
                var targetEntities = InsertSorted(ballast, new EntityDesc(
                    movingGid, 1, false, new[]
                    {
                        PositionRecord(1, 3.5f, -1f, 42f),
                    }));

                var snapshotPool = new NetworkSnapshotPool(4);
                var baseline = MakeSnapshot(snapshotPool, bufferPool, 1,
                    baselineEntities);
                var target = MakeSnapshot(snapshotPool, bufferPool, 2,
                    targetEntities);
                var noOpTarget = MakeSnapshot(snapshotPool, bufferPool, 3,
                    baselineEntities);
                try
                {
                    Assert.That(SnapshotDeltaCodec.TryEncode(bufferPool,
                        baseline, target, out var delta), Is.True);
                    Assert.That(SnapshotDeltaCodec.TryEncode(bufferPool,
                        baseline, noOpTarget, out var noOpDelta), Is.True);
                    using (delta)
                    using (noOpDelta)
                    {
                        var marginalBytes = delta.Length - noOpDelta.Length;
                        TestContext.Progress.WriteLine(
                            "NCORE-13 moving-entity single 12B record patch = " +
                            marginalBytes + " marginal bytes (" + delta.Length +
                            " total among " + (ballastCount + 1) +
                            " entities; was ~44 B pre-NCORE-13)");
                        Assert.That(marginalBytes, Is.LessThanOrEqualTo(16));
                        AssertRoundTrips(bufferPool, baseline, target, delta);
                    }
                }
                finally
                {
                    baseline.Dispose();
                    target.Dispose();
                    noOpTarget.Dispose();
                }
            }
            finally
            {
                Assert.That(bufferPool.CaptureDiagnostics().OutstandingLeases,
                    Is.Zero);
                bufferPool.Dispose();
            }
        }

        [Test]
        public void PositionAndAnimationSlotPatchSizeIsReported()
        {
            var bufferPool = new NetworkBufferPool(1L << 20);
            try
            {
                const int ballastCount = 200;
                var ballast = RandomEntities(new Random(6), ballastCount);
                var movingGid = new EntityGID(5_000_002, 1, 0).Raw;
                var animationBaseline = AnimationRecord(2, slot: 3, tick: 100);
                var baselineEntities = InsertSorted(ballast, new EntityDesc(
                    movingGid, 1, false, new[]
                    {
                        PositionRecord(1, 0f, 0f, 0f), animationBaseline,
                    }));
                var targetEntities = InsertSorted(ballast, new EntityDesc(
                    movingGid, 1, false, new[]
                    {
                        // Only the position moved; the animation slot did not
                        // change, so it must cost zero wire bytes.
                        PositionRecord(1, 1.5f, 0f, -0.25f), animationBaseline,
                    }));

                var snapshotPool = new NetworkSnapshotPool(4);
                var baseline = MakeSnapshot(snapshotPool, bufferPool, 1,
                    baselineEntities);
                var target = MakeSnapshot(snapshotPool, bufferPool, 2,
                    targetEntities);
                var noOpTarget = MakeSnapshot(snapshotPool, bufferPool, 3,
                    baselineEntities);
                try
                {
                    Assert.That(SnapshotDeltaCodec.TryEncode(bufferPool,
                        baseline, target, out var delta), Is.True);
                    Assert.That(SnapshotDeltaCodec.TryEncode(bufferPool,
                        baseline, noOpTarget, out var noOpDelta), Is.True);
                    using (delta)
                    using (noOpDelta)
                    {
                        var marginalBytes = delta.Length - noOpDelta.Length;
                        TestContext.Progress.WriteLine(
                            "NCORE-13 position-only patch on a position+" +
                            "animation entity = " + marginalBytes +
                            " marginal bytes (" + delta.Length + " total)");
                        // Same marginal cost as the position-only case: the
                        // unchanged animation record costs one clear mask bit
                        // and nothing else.
                        Assert.That(marginalBytes, Is.LessThanOrEqualTo(17));
                        AssertRoundTrips(bufferPool, baseline, target, delta);
                    }
                }
                finally
                {
                    baseline.Dispose();
                    target.Dispose();
                    noOpTarget.Dispose();
                }
            }
            finally
            {
                Assert.That(bufferPool.CaptureDiagnostics().OutstandingLeases,
                    Is.Zero);
                bufferPool.Dispose();
            }
        }

        [Test]
        public void EmptyDeltaIsHeaderOnly()
        {
            var bufferPool = new NetworkBufferPool(1L << 20);
            try
            {
                var entities = RandomEntities(new Random(1), 12);
                var snapshotPool = new NetworkSnapshotPool(4);
                var baseline = MakeSnapshot(snapshotPool, bufferPool, 1,
                    entities);
                var target = MakeSnapshot(snapshotPool, bufferPool, 2,
                    entities);
                try
                {
                    Assert.That(SnapshotDeltaCodec.TryEncode(bufferPool,
                        baseline, target, out var delta), Is.True);
                    using (delta)
                    {
                        // format version (1) + entityCount + recordCount +
                        // operationCount(=0); no per-entity bytes at all.
                        Assert.That(delta.Length, Is.EqualTo(13));
                        AssertRoundTrips(bufferPool, baseline, target, delta);
                    }
                }
                finally
                {
                    baseline.Dispose();
                    target.Dispose();
                }
            }
            finally
            {
                Assert.That(bufferPool.CaptureDiagnostics().OutstandingLeases,
                    Is.Zero);
                bufferPool.Dispose();
            }
        }

        [Test]
        public void KeyframeFallbackTriggersWhenDeltaIsNotSmaller()
        {
            var bufferPool = new NetworkBufferPool(1L << 20);
            try
            {
                var baselineEntities = Array.Empty<EntityDesc>();
                var targetEntities = RandomEntities(new Random(2), 5);
                var snapshotPool = new NetworkSnapshotPool(4);
                var baseline = MakeSnapshot(snapshotPool, bufferPool, 1,
                    baselineEntities);
                var target = MakeSnapshot(snapshotPool, bufferPool, 2,
                    targetEntities);
                try
                {
                    // Every target entity is a bare Add against an empty
                    // baseline: the delta carries the same entity bytes as a
                    // keyframe plus per-operation framing, so it can never be
                    // smaller. TryEncode must refuse it so the caller sends a
                    // keyframe instead.
                    Assert.That(SnapshotDeltaCodec.TryEncode(bufferPool,
                        baseline, target, out var delta), Is.False);
                    Assert.That(delta, Is.Null);
                }
                finally
                {
                    baseline.Dispose();
                    target.Dispose();
                }
            }
            finally
            {
                Assert.That(bufferPool.CaptureDiagnostics().OutstandingLeases,
                    Is.Zero);
                bufferPool.Dispose();
            }
        }

        [Test]
        public void LargeEntityCountRoundTrips()
        {
            var bufferPool = new NetworkBufferPool(16L << 20);
            try
            {
                var rng = new Random(20260918);
                var baselineEntities = RandomEntities(rng, 3000);
                var targetEntities = Mutate(rng, baselineEntities,
                    addCount: 50, removeCount: 50, patchCount: 400);

                var snapshotPool = new NetworkSnapshotPool(4);
                var baseline = MakeSnapshot(snapshotPool, bufferPool, 1,
                    baselineEntities);
                var target = MakeSnapshot(snapshotPool, bufferPool, 2,
                    targetEntities);
                try
                {
                    Assert.That(SnapshotDeltaCodec.TryEncode(bufferPool,
                        baseline, target, out var delta), Is.True);
                    using (delta)
                    {
                        TestContext.Progress.WriteLine(
                            "NCORE-13 3000-entity baseline, 50 add/50 remove/" +
                            "400 patch delta = " + delta.Length + " bytes");
                        AssertRoundTrips(bufferPool, baseline, target, delta);
                    }
                }
                finally
                {
                    baseline.Dispose();
                    target.Dispose();
                }
            }
            finally
            {
                Assert.That(bufferPool.CaptureDiagnostics().OutstandingLeases,
                    Is.Zero);
                bufferPool.Dispose();
            }
        }

        [Test]
        public void RandomizedBaselineTargetPairsRoundTripAcrossManyShapes()
        {
            var bufferPool = new NetworkBufferPool(16L << 20);
            var snapshotPool = new NetworkSnapshotPool(8);
            var reconstructed = 0;
            var fellBackToKeyframe = 0;
            try
            {
                var rng = new Random(424242);
                for (var iteration = 0; iteration < 400; iteration++)
                {
                    var baselineEntities = RandomEntities(rng, rng.Next(0, 40));
                    var targetEntities = Mutate(rng, baselineEntities,
                        addCount: rng.Next(0, 6), removeCount: rng.Next(0, 6),
                        patchCount: rng.Next(0, 12));

                    var baseline = MakeSnapshot(snapshotPool, bufferPool, 1,
                        baselineEntities);
                    var target = MakeSnapshot(snapshotPool, bufferPool, 2,
                        targetEntities);
                    try
                    {
                        if (!SnapshotDeltaCodec.TryEncode(bufferPool, baseline,
                                target, out var delta))
                        {
                            fellBackToKeyframe++;
                            continue;
                        }
                        using (delta)
                        {
                            reconstructed++;
                            AssertRoundTrips(bufferPool, baseline, target,
                                delta, $"iteration {iteration}");
                        }
                    }
                    finally
                    {
                        baseline.Dispose();
                        target.Dispose();
                    }
                }
            }
            finally
            {
                Assert.That(bufferPool.CaptureDiagnostics().OutstandingLeases,
                    Is.Zero);
                bufferPool.Dispose();
            }

            TestContext.Progress.WriteLine(
                $"NCORE-13 property sweep: {reconstructed} delta round-trips, " +
                $"{fellBackToKeyframe} keyframe fallbacks");
            Assert.That(reconstructed, Is.GreaterThan(300),
                "the sweep should exercise the delta path, not only fall back");
        }

        [Test]
        public void DisabledFlagTogglesRoundTripWithoutRecordChanges()
        {
            var bufferPool = new NetworkBufferPool(1L << 20);
            try
            {
                var gid = new EntityGID(3, 1, 0).Raw;
                var record = PositionRecord(1, 1f, 2f, 3f);
                var baselineEntities = new[]
                {
                    new EntityDesc(gid, 1, false, new[] { record }),
                };
                var targetEntities = new[]
                {
                    // Only the entity's own disabled flag changes; the record
                    // bytes are byte-identical.
                    new EntityDesc(gid, 1, true, new[] { record }),
                };

                var snapshotPool = new NetworkSnapshotPool(4);
                var baseline = MakeSnapshot(snapshotPool, bufferPool, 1,
                    baselineEntities);
                var target = MakeSnapshot(snapshotPool, bufferPool, 2,
                    targetEntities);
                try
                {
                    Assert.That(SnapshotDeltaCodec.TryEncode(bufferPool,
                        baseline, target, out var delta), Is.True);
                    using (delta)
                        AssertRoundTrips(bufferPool, baseline, target, delta);
                }
                finally
                {
                    baseline.Dispose();
                    target.Dispose();
                }
            }
            finally
            {
                Assert.That(bufferPool.CaptureDiagnostics().OutstandingLeases,
                    Is.Zero);
                bufferPool.Dispose();
            }
        }

        [Test]
        public void RecordDisabledToggleWithoutPayloadChangeRoundTrips()
        {
            var bufferPool = new NetworkBufferPool(1L << 20);
            try
            {
                var gid = new EntityGID(4, 1, 0).Raw;
                var baselineEntities = new[]
                {
                    new EntityDesc(gid, 1, false, new[]
                    {
                        new RecordDesc(2, (byte)NetworkSchemaKind.Component, 0,
                            false, new byte[] { 9, 9 }),
                    }),
                };
                var targetEntities = new[]
                {
                    new EntityDesc(gid, 1, false, new[]
                    {
                        new RecordDesc(2, (byte)NetworkSchemaKind.Component, 0,
                            true, new byte[] { 9, 9 }),
                    }),
                };

                var snapshotPool = new NetworkSnapshotPool(4);
                var baseline = MakeSnapshot(snapshotPool, bufferPool, 1,
                    baselineEntities);
                var target = MakeSnapshot(snapshotPool, bufferPool, 2,
                    targetEntities);
                try
                {
                    Assert.That(SnapshotDeltaCodec.TryEncode(bufferPool,
                        baseline, target, out var delta), Is.True);
                    using (delta)
                        AssertRoundTrips(bufferPool, baseline, target, delta);
                }
                finally
                {
                    baseline.Dispose();
                    target.Dispose();
                }
            }
            finally
            {
                Assert.That(bufferPool.CaptureDiagnostics().OutstandingLeases,
                    Is.Zero);
                bufferPool.Dispose();
            }
        }

#if UNITY_2022_2_OR_NEWER
        // NCORE-13b: differential correctness test for the Burst port of the indexed
        // structural diff (SnapshotDeltaBurstBackend.TryPlan). Burst only classifies
        // entities (skip/Remove/Add/PatchFast/PatchFull) and computes PatchFast masks;
        // every wire byte -- including every value-delta hook payload -- is still
        // written by the exact managed code the portable path uses (see
        // SnapshotDeltaCodec.TryEncodeCoreIndexedBurst / TryWritePatchFastFromMask), so a
        // Burst bug could only show up as a *structural* mismatch (wrong skip/opcode/mask),
        // which this test targets directly: it sweeps randomized adds/removes/record-set
        // changes/disabled toggles (via the existing RandomEntities/Mutate fixture) AND a
        // set of "mover" entities carrying a hook-tagged record (reusing
        // NetworkV7Tests.DeltaTestComponent) whose payload changes almost every tick -- the
        // realistic case a moving player/NPC's Position record exercises, and the one most
        // likely to erode any Burst win since the hook call itself always stays managed.
        [Test]
        public void SnapshotDeltaCodec_BurstMatchesPortableWithHooksAddsRemovesAndDisabledToggles()
        {
            const uint hookTypeId = 900;
            var hooks = new NetworkComponentDeltaHooks(new[] { hookTypeId },
                new INetworkComponentDelta[] { new NetworkV7Tests.DeltaTestComponent() });

            var bufferPool = new NetworkBufferPool(16L << 20);
            var snapshotPool = new NetworkSnapshotPool(8);
            var compared = 0;
            try
            {
                var rng = new Random(130926);
                var baselineEntities = RandomEntities(rng, 40);
                var moverIds = new ulong[5];
                for (var i = 0; i < moverIds.Length; i++)
                {
                    moverIds[i] = new EntityGID(9_000_000u + (uint)i, 1, 0).Raw;
                    baselineEntities = InsertSorted(baselineEntities,
                        new EntityDesc(moverIds[i], 1, false,
                            new[] { HookRecord(hookTypeId, i * 17, false) }));
                }

                for (var iteration = 0; iteration < 60; iteration++)
                {
                    var targetEntities = Mutate(rng, baselineEntities,
                        addCount: rng.Next(0, 4), removeCount: rng.Next(0, 4),
                        patchCount: rng.Next(0, 8));
                    foreach (var moverId in moverIds)
                        targetEntities = MutateMover(rng, targetEntities, moverId,
                            hookTypeId);

                    var baseline = MakeSnapshot(snapshotPool, bufferPool, 1,
                        baselineEntities);
                    var target = MakeSnapshot(snapshotPool, bufferPool, 2,
                        targetEntities);
                    try
                    {
                        SnapshotDeltaBurstBackend.ForcePortableForTests = true;
                        var portableOk = SnapshotDeltaCodec.TryEncode(bufferPool,
                            baseline, target, out var portableDelta, hooks);

                        SnapshotDeltaBurstBackend.ForcePortableForTests = false;
                        var burstOk = SnapshotDeltaCodec.TryEncode(bufferPool,
                            baseline, target, out var burstDelta, hooks);

                        Assert.That(burstOk, Is.EqualTo(portableOk),
                            $"iteration {iteration}: portable/Burst disagreed on " +
                            "whether a delta was worthwhile");
                        if (portableOk)
                        {
                            using (portableDelta)
                            using (burstDelta)
                            {
                                Assert.That(
                                    burstDelta.Span.SequenceEqual(portableDelta.Span),
                                    Is.True,
                                    $"iteration {iteration}: Burst delta differs " +
                                    "from portable");
                                AssertRoundTripsWithHooks(bufferPool, baseline,
                                    target, burstDelta, hooks,
                                    $"iteration {iteration}");
                            }
                            compared++;
                        }
                    }
                    finally
                    {
                        baseline.Dispose();
                        target.Dispose();
                    }
                    baselineEntities = targetEntities;
                }
            }
            finally
            {
                SnapshotDeltaBurstBackend.ForcePortableForTests = false;
                Assert.That(bufferPool.CaptureDiagnostics().OutstandingLeases,
                    Is.Zero);
                bufferPool.Dispose();
            }

            TestContext.Progress.WriteLine(
                $"NCORE-13b Burst/portable differential: {compared} pairs " +
                "compared byte-identical");
            Assert.That(compared, Is.GreaterThan(30),
                "the sweep should exercise the delta path most iterations, not " +
                "only fall back to keyframe");
        }

        // NCORE-13b: prepared for the manager to run inside Unity. This dotnet-test
        // environment cannot compile or execute real Burst code -- SnapshotDeltaBurstBackend's
        // Unity-only implementation (and the FunctionPointer/BurstCompiler types it uses) is
        // entirely `#if UNITY_2022_2_OR_NEWER`, so outside Unity SnapshotDeltaBurstBackend.TryPlan
        // always returns Unavailable and both arms of Measure() below exercise the same
        // portable path -- the numbers below are only meaningful when this runs in a Unity
        // EditMode test pass. Builds realistic entity counts (200/300/500) with a moving
        // fraction (30-60%) where each mover carries one hook-tagged record that changes
        // (almost) every tick -- the case that forces even the Burst-assisted path to make a
        // managed hook call per changed record, which the port's design notes flag as the
        // scenario most likely to erode Burst's pre-NCORE-13 ~20% win. Deliberately asserts
        // no speed threshold: only logs medians via TestContext.Progress, since asserting an
        // unverified number would block unrelated work on a result nobody has measured yet
        // (see the NCORE-13b report for how to read the logged output).
        [Test]
        public void SnapshotDeltaCodec_BurstVsPortableRealisticMixTiming()
        {
            const uint hookTypeId = 901;
            var hooks = new NetworkComponentDeltaHooks(new[] { hookTypeId },
                new INetworkComponentDelta[] { new NetworkV7Tests.DeltaTestComponent() });

            foreach (var (entityCount, movingFraction) in new[]
                     {
                         (200, 0.3), (300, 0.45), (500, 0.6),
                     })
            {
                var bufferPool = new NetworkBufferPool(32L << 20);
                var snapshotPool = new NetworkSnapshotPool(8);
                var pairs = new List<(NetworkSnapshot baseline, NetworkSnapshot target)>();
                try
                {
                    var rng = new Random(unchecked(entityCount * 7919));
                    var entities = RandomEntities(rng, entityCount);
                    var moverCount = (int)(entityCount * movingFraction);
                    for (var i = 0; i < moverCount && i < entities.Length; i++)
                    {
                        var e = entities[i];
                        var records = new List<RecordDesc>(e.Records)
                        {
                            HookRecord(hookTypeId, i, false),
                        };
                        records.Sort((a, b) => CompareRecordForTests(a, b));
                        entities[i] = new EntityDesc(e.Gid, e.Kind, e.Disabled,
                            records.ToArray());
                    }

                    var current = entities;
                    for (var tick = 0; tick < 40; tick++)
                    {
                        var next = (EntityDesc[])current.Clone();
                        for (var i = 0; i < moverCount && i < next.Length; i++)
                        {
                            var e = next[i];
                            var recordIndex = Array.FindIndex(e.Records,
                                r => r.TypeId == hookTypeId);
                            if (recordIndex < 0)
                                continue;
                            var value = ReadLeInt32(e.Records[recordIndex].Payload) +
                                        rng.Next(-5, 6);
                            var records = (RecordDesc[])e.Records.Clone();
                            records[recordIndex] = HookRecord(hookTypeId, value,
                                false);
                            next[i] = new EntityDesc(e.Gid, e.Kind, e.Disabled,
                                records);
                        }
                        var baseline = MakeSnapshot(snapshotPool, bufferPool,
                            (uint)(tick + 1), current);
                        var target = MakeSnapshot(snapshotPool, bufferPool,
                            (uint)(tick + 2), next);
                        pairs.Add((baseline, target));
                        current = next;
                    }

                    long Measure(bool burst)
                    {
                        SnapshotDeltaBurstBackend.ForcePortableForTests = !burst;
                        var start = Stopwatch.GetTimestamp();
                        foreach (var pair in pairs)
                        {
                            if (SnapshotDeltaCodec.TryEncode(bufferPool,
                                    pair.baseline, pair.target, out var delta, hooks))
                                delta.Dispose();
                        }
                        return Stopwatch.GetTimestamp() - start;
                    }

                    for (var warmup = 0; warmup < 2; warmup++)
                    {
                        Measure(false);
                        Measure(true);
                    }
                    var portableSamples = new long[5];
                    var burstSamples = new long[5];
                    for (var sample = 0; sample < portableSamples.Length; sample++)
                    {
                        portableSamples[sample] = Measure(false);
                        burstSamples[sample] = Measure(true);
                    }
                    Array.Sort(portableSamples);
                    Array.Sort(burstSamples);
                    var portableMs = portableSamples[portableSamples.Length / 2] *
                        1000d / Stopwatch.Frequency;
                    var burstMs = burstSamples[burstSamples.Length / 2] *
                        1000d / Stopwatch.Frequency;
                    TestContext.Progress.WriteLine(
                        $"NCORE-13b {entityCount} entities, {movingFraction:P0} " +
                        $"moving, {pairs.Count} ticks: portable={portableMs:F3}ms " +
                        $"burst={burstMs:F3}ms " +
                        $"speedup={(portableMs - burstMs) / portableMs:P1} " +
                        $"burstAvailable={SnapshotDeltaBurstBackend.IsAvailable}");
                }
                finally
                {
                    SnapshotDeltaBurstBackend.ForcePortableForTests = false;
                    foreach (var pair in pairs)
                    {
                        pair.baseline.Dispose();
                        pair.target.Dispose();
                    }
                    Assert.That(bufferPool.CaptureDiagnostics().OutstandingLeases,
                        Is.Zero);
                    bufferPool.Dispose();
                }
            }
        }
#endif

        // --- Fixture builders -------------------------------------------------

        private static EntityDesc[] InsertSorted(EntityDesc[] entities,
            EntityDesc added)
        {
            var list = new List<EntityDesc>(entities) { added };
            list.Sort((a, b) => CompareGidForTests(a.Gid, b.Gid));
            return list.ToArray();
        }

        private static RecordDesc PositionRecord(uint typeId, float x, float y,
            float z)
        {
            var payload = new byte[12];
            WriteFloat(payload, 0, x);
            WriteFloat(payload, 4, y);
            WriteFloat(payload, 8, z);
            return new RecordDesc(typeId, (byte)NetworkSchemaKind.Component, 0,
                false, payload);
        }

        private static RecordDesc AnimationRecord(uint typeId, int slot,
            int tick)
        {
            var payload = new byte[38];
            WriteInt(payload, 0, slot);
            WriteInt(payload, 4, tick);
            return new RecordDesc(typeId, (byte)NetworkSchemaKind.Multi, 0,
                false, payload);
        }

        // A hook-tagged record (NCORE-13b Burst differential/perf tests): a plain
        // 4-byte little-endian int payload, matching what
        // NetworkV7Tests.DeltaTestComponent's INetworkComponentDelta hook expects.
        private static RecordDesc HookRecord(uint typeId, int value, bool disabled)
        {
            var payload = new byte[4];
            WriteInt(payload, 0, value);
            return new RecordDesc(typeId, (byte)NetworkSchemaKind.Component, 0,
                disabled, payload);
        }

        private static int ReadLeInt32(byte[] bytes) =>
            bytes[0] | bytes[1] << 8 | bytes[2] << 16 | bytes[3] << 24;

        private static void WriteFloat(byte[] destination, int offset,
            float value)
        {
            var bits = BitConverter.SingleToInt32Bits(value);
            WriteInt(destination, offset, bits);
        }

        private static void WriteInt(byte[] destination, int offset, int value)
        {
            destination[offset] = (byte)value;
            destination[offset + 1] = (byte)(value >> 8);
            destination[offset + 2] = (byte)(value >> 16);
            destination[offset + 3] = (byte)(value >> 24);
        }

        private static NetworkSnapshot MakeSnapshot(NetworkSnapshotPool snapshotPool,
            NetworkBufferPool bufferPool, uint tick, EntityDesc[] entities)
        {
            var bytes = BuildCanonical(entities);
            var recordCount = entities.Sum(e => e.Records.Length);
            return snapshotPool.Rent(tick, Fingerprint, Scope,
                bufferPool.Copy(bytes), entities.Length, recordCount);
        }

        private static byte[] BuildCanonical(EntityDesc[] entities)
        {
            using var stream = new MemoryStream();
            WriteU32(stream, checked((uint)entities.Length));
            for (var i = 0; i < entities.Length; i++)
            {
                if (i > 0)
                    Assert.That(CompareGidForTests(entities[i - 1].Gid,
                        entities[i].Gid), Is.LessThan(0),
                        "test fixture entities must be strictly GID-ordered");
                var entity = entities[i];
                WriteU64(stream, entity.Gid);
                WriteU32(stream, entity.Kind);
                stream.WriteByte(entity.Disabled ? (byte)1 : (byte)0);
                WriteU16(stream, checked((ushort)entity.Records.Length));
                for (var j = 0; j < entity.Records.Length; j++)
                {
                    if (j > 0)
                        Assert.That(CompareRecordForTests(
                            entity.Records[j - 1], entity.Records[j]),
                            Is.LessThan(0),
                            "test fixture records must be strictly ordered");
                    var record = entity.Records[j];
                    WriteU32(stream, record.TypeId);
                    stream.WriteByte(record.Kind);
                    stream.WriteByte(record.Version);
                    stream.WriteByte(record.Disabled ? (byte)1 : (byte)0);
                    WriteU32(stream, checked((uint)record.Payload.Length));
                    stream.Write(record.Payload, 0, record.Payload.Length);
                }
            }
            return stream.ToArray();
        }

        private static void WriteU16(Stream stream, ushort value)
        {
            stream.WriteByte((byte)value);
            stream.WriteByte((byte)(value >> 8));
        }

        private static void WriteU32(Stream stream, uint value)
        {
            stream.WriteByte((byte)value);
            stream.WriteByte((byte)(value >> 8));
            stream.WriteByte((byte)(value >> 16));
            stream.WriteByte((byte)(value >> 24));
        }

        private static void WriteU64(Stream stream, ulong value)
        {
            for (var i = 0; i < 8; i++)
                stream.WriteByte((byte)(value >> (i * 8)));
        }

        private static int CompareGidForTests(ulong left, ulong right)
        {
            var leftGid = new EntityGID(left);
            var rightGid = new EntityGID(right);
            var cluster = leftGid.ClusterId.CompareTo(rightGid.ClusterId);
            if (cluster != 0)
                return cluster;
            var id = leftGid.Id.CompareTo(rightGid.Id);
            return id != 0 ? id : leftGid.Version.CompareTo(rightGid.Version);
        }

        private static int CompareRecordForTests(RecordDesc left,
            RecordDesc right)
        {
            var kind = left.Kind.CompareTo(right.Kind);
            return kind != 0 ? kind : left.TypeId.CompareTo(right.TypeId);
        }

        // --- Randomized generation and mutation --------------------------------

        private static EntityDesc[] RandomEntities(Random rng, int count)
        {
            var seen = new HashSet<ulong>();
            var list = new List<EntityDesc>(count);
            var guard = 0;
            while (list.Count < count && guard < count * 20 + 100)
            {
                guard++;
                var id = (uint)rng.Next(1, 2_000_000);
                var version = (ushort)rng.Next(1, 5);
                var cluster = (ushort)rng.Next(0, 4);
                var gid = new EntityGID(id, version, cluster).Raw;
                if (!seen.Add(gid))
                    continue;
                var recordCount = rng.Next(0, 6);
                list.Add(new EntityDesc(gid,
                    (uint)(1 + rng.Next(3)), rng.Next(4) == 0,
                    RandomRecords(rng, recordCount)));
            }
            list.Sort((a, b) => CompareGidForTests(a.Gid, b.Gid));
            return list.ToArray();
        }

        private static RecordDesc[] RandomRecords(Random rng, int count)
        {
            var seen = new HashSet<(byte kind, uint typeId)>();
            while (seen.Count < count)
                seen.Add(((byte)(1 + rng.Next(5)), (uint)(1 + rng.Next(50))));
            var ordered = seen.OrderBy(k => k.kind).ThenBy(k => k.typeId)
                .ToArray();
            var records = new RecordDesc[count];
            for (var i = 0; i < count; i++)
            {
                var payload = new byte[rng.Next(0, 20)];
                rng.NextBytes(payload);
                records[i] = new RecordDesc(ordered[i].typeId, ordered[i].kind,
                    0, rng.Next(3) == 0, payload);
            }
            return records;
        }

        private static EntityDesc[] Mutate(Random rng, EntityDesc[] baseline,
            int addCount, int removeCount, int patchCount)
        {
            var list = new List<EntityDesc>(baseline);
            for (var i = 0; i < removeCount && list.Count > 0; i++)
                list.RemoveAt(rng.Next(list.Count));
            for (var i = 0; i < patchCount && list.Count > 0; i++)
            {
                var index = rng.Next(list.Count);
                list[index] = MutateEntity(rng, list[index]);
            }
            var seen = new HashSet<ulong>(list.Select(e => e.Gid));
            var guard = 0;
            for (var i = 0; i < addCount && guard < addCount * 20 + 100;)
            {
                guard++;
                var candidate = RandomEntities(rng, 1);
                if (candidate.Length == 0 || !seen.Add(candidate[0].Gid))
                    continue;
                list.Add(candidate[0]);
                i++;
            }
            list.Sort((a, b) => CompareGidForTests(a.Gid, b.Gid));
            return list.ToArray();
        }

        private static EntityDesc MutateEntity(Random rng, EntityDesc entity)
        {
            var disabled = rng.Next(3) == 0 ? !entity.Disabled : entity.Disabled;
            var records = new List<RecordDesc>(entity.Records);
            switch (rng.Next(4))
            {
                case 0 when records.Count > 0:
                {
                    var index = rng.Next(records.Count);
                    var record = records[index];
                    var payload = new byte[rng.Next(0, 20)];
                    rng.NextBytes(payload);
                    records[index] = new RecordDesc(record.TypeId, record.Kind,
                        record.Version, rng.Next(3) == 0, payload);
                    break;
                }
                case 1 when records.Count > 0:
                    records.RemoveAt(rng.Next(records.Count));
                    break;
                case 2:
                {
                    var existing = new HashSet<(byte, uint)>(
                        records.Select(r => (r.Kind, r.TypeId)));
                    for (var guard = 0; guard < 30; guard++)
                    {
                        var candidate = RandomRecords(rng, 1)[0];
                        if (existing.Add((candidate.Kind, candidate.TypeId)))
                        {
                            records.Add(candidate);
                            break;
                        }
                    }
                    break;
                }
            }
            records.Sort((a, b) => CompareRecordForTests(a, b));
            return new EntityDesc(entity.Gid, entity.Kind, disabled,
                records.ToArray());
        }

        // Advances one hook-tagged "mover" entity (by GID) independently of the
        // general Mutate() sweep above, which only ever generates plain
        // (non-hook-tagged) records: this is what guarantees every iteration of the
        // NCORE-13b Burst differential test actually exercises
        // NetworkComponentDeltaHooks/INetworkComponentDelta, both its compact
        // isDelta=1 path (small steps) and its raw isDelta=0 fallback (large jumps).
        // No-ops if Mutate() already removed this entity this iteration.
        private static EntityDesc[] MutateMover(Random rng, EntityDesc[] entities,
            ulong gid, uint hookTypeId)
        {
            var index = Array.FindIndex(entities, e => e.Gid == gid);
            if (index < 0)
                return entities;
            var entity = entities[index];
            var records = new List<RecordDesc>(entity.Records);
            var recordIndex = records.FindIndex(r => r.TypeId == hookTypeId);
            var disabled = rng.Next(6) == 0 ? !entity.Disabled : entity.Disabled;
            if (recordIndex < 0)
            {
                // The hook record is currently absent (dropped below on a previous
                // iteration, or by Mutate()'s own record-remove case): usually put
                // it back so later iterations keep exercising the hook path.
                if (rng.Next(3) != 0)
                {
                    records.Add(HookRecord(hookTypeId, rng.Next(-2000, 2000),
                        rng.Next(4) == 0));
                    records.Sort((a, b) => CompareRecordForTests(a, b));
                }
            }
            else if (rng.Next(20) == 0)
            {
                // Rarely drop the hook record entirely: forces PatchFull for this
                // entity (its record set no longer matches the baseline's).
                records.RemoveAt(recordIndex);
            }
            else
            {
                var current = ReadLeInt32(records[recordIndex].Payload);
                // Mostly small steps (fit the hook's compact sbyte-delta path);
                // sometimes a large jump (forces its raw isDelta=0 fallback).
                var step = rng.Next(10) == 0
                    ? rng.Next(-100_000, 100_000)
                    : rng.Next(-5, 6);
                var next = unchecked(current + step);
                records[recordIndex] = HookRecord(hookTypeId, next,
                    rng.Next(4) == 0);
            }
            var updated = (EntityDesc[])entities.Clone();
            updated[index] = new EntityDesc(entity.Gid, entity.Kind, disabled,
                records.ToArray());
            return updated;
        }

        // --- Shared round-trip assertion ---------------------------------------

        private static void AssertRoundTrips(NetworkBufferPool bufferPool,
            NetworkSnapshot baseline, NetworkSnapshot target,
            NetworkBufferLease delta, string context = null) =>
            AssertRoundTripsWithHooks(bufferPool, baseline, target, delta,
                NetworkComponentDeltaHooks.Empty, context);

        private static void AssertRoundTripsWithHooks(NetworkBufferPool bufferPool,
            NetworkSnapshot baseline, NetworkSnapshot target,
            NetworkBufferLease delta, NetworkComponentDeltaHooks hooks,
            string context = null)
        {
            var header = new SnapshotChunkHeader
            {
                PayloadKind = SnapshotPayloadKind.Delta,
                SnapshotTick = target.ServerTick,
                BaselineTick = baseline.ServerTick,
                TotalLength = checked((uint)target.ByteLength),
                TotalHash = target.PayloadHash,
                ChunkIndex = 0,
                ChunkCount = 1,
            };
            var ok = SnapshotDeltaCodec.TryReconstruct(bufferPool, baseline,
                delta.Span, in header, Fingerprint, Scope, out var canonical,
                out var entityCount, out var recordCount, hooks);
            Assert.That(ok, Is.True,
                (context ?? "reconstruct") + ": TryReconstruct failed");
            using (canonical)
            {
                Assert.That(canonical.Span.SequenceEqual(target.Bytes.Span),
                    Is.True,
                    (context ?? "reconstruct") +
                    ": canonical bytes differ from target");
                Assert.That(entityCount, Is.EqualTo(target.EntityCount),
                    (context ?? "reconstruct") + ": entity count mismatch");
                Assert.That(recordCount, Is.EqualTo(target.RecordCount),
                    (context ?? "reconstruct") + ": record count mismatch");
            }
        }
    }
}
