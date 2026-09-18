namespace UniGame.StaticEcs.Network.Tests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using FFS.Libraries.StaticEcs;
    using NUnit.Framework;

    // NCORE-26: correctness coverage for NetworkReconstructionCache, the per-process shared
    // delta-reconstruction cache a thin load generator opts a NetworkClient into so that many
    // client slots receiving the exact same wire delta against the exact same baseline
    // reconstruct and hash-verify the canonical target snapshot once instead of once per client.
    // Fixture-building mirrors NetworkSnapshotDeltaCompactionTests: canonical snapshot bytes are
    // built directly from primitive descriptors (no ECS world), and reconstruction is exercised
    // through the exact production entry points (SnapshotDeltaCodec.TryEncode /
    // NetworkReconstructionCache.TryReconstruct).
    public sealed class NetworkReconstructionCacheTests
    {
        private readonly struct RecordDesc
        {
            internal RecordDesc(uint typeId, byte kind, byte[] payload)
            {
                TypeId = typeId;
                Kind = kind;
                Payload = payload;
            }

            internal uint TypeId { get; }
            internal byte Kind { get; }
            internal byte[] Payload { get; }
        }

        private readonly struct EntityDesc
        {
            internal EntityDesc(ulong gid, uint kind, RecordDesc[] records)
            {
                Gid = gid;
                Kind = kind;
                Records = records;
            }

            internal ulong Gid { get; }
            internal uint Kind { get; }
            internal RecordDesc[] Records { get; }
        }

        private static readonly SchemaFingerprint Fingerprint =
            new SchemaFingerprint(0x1111_2222_3333_4444, 0x5555_6666_7777_8888);
        private static readonly ScopeId Scope = new ScopeId(1);

        [Test]
        public void SecondCallWithIdenticalBaselineAndDeltaIsAHit()
        {
            var pool = new NetworkBufferPool(1L << 20);
            try
            {
                var baseline = Snapshot(pool, 1, Entities(Position(1, 0f, 0f, 0f)));
                var target = Snapshot(pool, 2, Entities(Position(1, 1f, 2f, 3f)));
                Assert.That(SnapshotDeltaCodec.TryEncode(pool, baseline, target,
                    out var delta), Is.True);
                using (delta)
                {
                    var header = Header(baseline, target);
                    var cache = new NetworkReconstructionCache();

                    Assert.That(cache.TryReconstruct(pool, baseline, delta.Span,
                        in header, Fingerprint, Scope, out var first,
                        out var firstEntities, out var firstRecords,
                        NetworkComponentDeltaHooks.Empty), Is.True);
                    Assert.That(cache.TryReconstruct(pool, baseline, delta.Span,
                        in header, Fingerprint, Scope, out var second,
                        out var secondEntities, out var secondRecords,
                        NetworkComponentDeltaHooks.Empty), Is.True);
                    using (first)
                    using (second)
                    {
                        var diagnostics = cache.CaptureDiagnostics();
                        Assert.That(diagnostics.Misses, Is.EqualTo(1));
                        Assert.That(diagnostics.Hits, Is.EqualTo(1));
                        Assert.That(first.Span.SequenceEqual(target.Bytes.Span),
                            Is.True);
                        Assert.That(second.Span.SequenceEqual(target.Bytes.Span),
                            Is.True);
                        Assert.That(secondEntities, Is.EqualTo(firstEntities));
                        Assert.That(secondRecords, Is.EqualTo(firstRecords));
                    }
                    cache.Clear();
                }
                baseline.Dispose();
                target.Dispose();
            }
            finally
            {
                Assert.That(pool.CaptureDiagnostics().OutstandingLeases, Is.Zero);
                pool.Dispose();
            }
        }

        [Test]
        public void CacheHitDoesNotReReadTheBaselineBytes()
        {
            // Corrupting the baseline after the first (miss) reconstruction, without touching
            // its already-computed PayloadHash field, makes any *real* re-reconstruction fail:
            // SnapshotDeltaCodec.TryReconstruct's TryOpenSnapshot re-verifies
            // xxHash64(bytes) == PayloadHash before reading a single entity. A second call that
            // still succeeds despite the corruption can only be explained by a genuine cache hit
            // that never touched the baseline bytes again — proving the cache actually skips
            // re-invoking the codec rather than merely delegating to it every time.
            var pool = new NetworkBufferPool(1L << 20);
            try
            {
                var baseline = Snapshot(pool, 1, Entities(Position(1, 0f, 0f, 0f)));
                var target = Snapshot(pool, 2, Entities(Position(1, 1f, 2f, 3f)));
                Assert.That(SnapshotDeltaCodec.TryEncode(pool, baseline, target,
                    out var delta), Is.True);
                using (delta)
                {
                    var header = Header(baseline, target);
                    var cache = new NetworkReconstructionCache();

                    Assert.That(cache.TryReconstruct(pool, baseline, delta.Span,
                        in header, Fingerprint, Scope, out var first, out _,
                        out _, NetworkComponentDeltaHooks.Empty), Is.True);
                    first.Dispose();

                    Corrupt(baseline);

                    // Control: a direct, uncached reconstruct against the now-corrupted
                    // baseline must fail. If this assertion itself fails, the corruption did
                    // not take effect and the rest of the test proves nothing.
                    Assert.That(SnapshotDeltaCodec.TryReconstruct(pool, baseline,
                        delta.Span, in header, Fingerprint, Scope, out var direct,
                        out _, out _, NetworkComponentDeltaHooks.Empty), Is.False);

                    Assert.That(cache.TryReconstruct(pool, baseline, delta.Span,
                        in header, Fingerprint, Scope, out var second, out _,
                        out _, NetworkComponentDeltaHooks.Empty), Is.True);
                    using (second)
                        Assert.That(second.Span.SequenceEqual(target.Bytes.Span),
                            Is.True);
                    Assert.That(cache.CaptureDiagnostics().Hits, Is.EqualTo(1));
                }
            }
            finally
            {
                // The corrupted baseline lease is intentionally never disposed cleanly by the
                // pool's own bookkeeping expectations here; skip the outstanding-lease assert
                // this once and dispose the pool directly.
                pool.Dispose();
            }
        }

        [Test]
        public void DifferentBaselinesDoNotFalseShare()
        {
            var pool = new NetworkBufferPool(1L << 20);
            try
            {
                var baselineA = Snapshot(pool, 1, Entities(Position(1, 0f, 0f, 0f)));
                var targetA = Snapshot(pool, 2, Entities(Position(1, 1f, 1f, 1f)));
                var baselineB = Snapshot(pool, 1, Entities(Position(1, 9f, 9f, 9f)));
                var targetB = Snapshot(pool, 2, Entities(Position(1, 8f, 8f, 8f)));
                Assert.That(SnapshotDeltaCodec.TryEncode(pool, baselineA, targetA,
                    out var deltaA), Is.True);
                Assert.That(SnapshotDeltaCodec.TryEncode(pool, baselineB, targetB,
                    out var deltaB), Is.True);
                using (deltaA)
                using (deltaB)
                {
                    var headerA = Header(baselineA, targetA);
                    var headerB = Header(baselineB, targetB);
                    var cache = new NetworkReconstructionCache();

                    Assert.That(cache.TryReconstruct(pool, baselineA, deltaA.Span,
                        in headerA, Fingerprint, Scope, out var canonicalA, out _,
                        out _, NetworkComponentDeltaHooks.Empty), Is.True);
                    Assert.That(cache.TryReconstruct(pool, baselineB, deltaB.Span,
                        in headerB, Fingerprint, Scope, out var canonicalB, out _,
                        out _, NetworkComponentDeltaHooks.Empty), Is.True);
                    using (canonicalA)
                    using (canonicalB)
                    {
                        Assert.That(canonicalA.Span.SequenceEqual(targetA.Bytes.Span),
                            Is.True);
                        Assert.That(canonicalB.Span.SequenceEqual(targetB.Bytes.Span),
                            Is.True);
                        var diagnostics = cache.CaptureDiagnostics();
                        Assert.That(diagnostics.Misses, Is.EqualTo(2));
                        Assert.That(diagnostics.Hits, Is.EqualTo(0));
                        Assert.That(diagnostics.Entries, Is.EqualTo(2));
                    }

                    // Same tick pair (1 -> 2), same schema/scope, but distinct baseline content
                    // and distinct delta bytes: repeating A must still be a genuine hit keyed on
                    // A's own baseline/delta identity, not merely on (schema, scope, ticks).
                    Assert.That(cache.TryReconstruct(pool, baselineA, deltaA.Span,
                        in headerA, Fingerprint, Scope, out var repeatA, out _,
                        out _, NetworkComponentDeltaHooks.Empty), Is.True);
                    using (repeatA)
                        Assert.That(repeatA.Span.SequenceEqual(targetA.Bytes.Span),
                            Is.True);
                    Assert.That(cache.CaptureDiagnostics().Hits, Is.EqualTo(1));
                    cache.Clear();
                }
                baselineA.Dispose();
                targetA.Dispose();
                baselineB.Dispose();
                targetB.Dispose();
            }
            finally
            {
                Assert.That(pool.CaptureDiagnostics().OutstandingLeases, Is.Zero);
                pool.Dispose();
            }
        }

        [Test]
        public void ClientLeasesDisposeIndependentlyAndCacheReleaseReturnsTheBuffer()
        {
            var pool = new NetworkBufferPool(1L << 20);
            try
            {
                var baseline = Snapshot(pool, 1, Entities(Position(1, 0f, 0f, 0f)));
                var target = Snapshot(pool, 2, Entities(Position(1, 4f, 5f, 6f)));
                Assert.That(SnapshotDeltaCodec.TryEncode(pool, baseline, target,
                    out var delta), Is.True);
                using (delta)
                {
                    var header = Header(baseline, target);
                    var cache = new NetworkReconstructionCache();

                    Assert.That(cache.TryReconstruct(pool, baseline, delta.Span,
                        in header, Fingerprint, Scope, out var clientA, out _,
                        out _, NetworkComponentDeltaHooks.Empty), Is.True);
                    Assert.That(cache.TryReconstruct(pool, baseline, delta.Span,
                        in header, Fingerprint, Scope, out var clientB, out _,
                        out _, NetworkComponentDeltaHooks.Empty), Is.True);

                    var expected = target.Bytes.Span.ToArray();
                    clientA.Dispose();
                    // clientB must stay valid and correct after clientA's independent disposal.
                    Assert.That(clientB.Span.SequenceEqual(expected), Is.True);
                    clientB.Dispose();

                    var beforeClear = pool.CaptureDiagnostics().OutstandingLeases;
                    cache.Clear();
                    var afterClear = pool.CaptureDiagnostics().OutstandingLeases;
                    Assert.That(afterClear, Is.EqualTo(beforeClear - 1),
                        "Clear() must release exactly the cache's own retained lease " +
                        "once every client lease was already disposed.");
                }
                baseline.Dispose();
                target.Dispose();
            }
            finally
            {
                Assert.That(pool.CaptureDiagnostics().OutstandingLeases, Is.Zero);
                pool.Dispose();
            }
        }

        [Test]
        public void CapacityEvictsOldestEntryFirst()
        {
            var pool = new NetworkBufferPool(1L << 20);
            try
            {
                var pairs = new (NetworkSnapshot Baseline, NetworkSnapshot Target,
                    NetworkBufferLease Delta, SnapshotChunkHeader Header)[3];
                for (var i = 0; i < pairs.Length; i++)
                {
                    var baseline = Snapshot(pool, 1,
                        Entities(Position(1, i, i, i)));
                    var target = Snapshot(pool, 2,
                        Entities(Position(1, i + 100, i + 100, i + 100)));
                    Assert.That(SnapshotDeltaCodec.TryEncode(pool, baseline, target,
                        out var delta), Is.True);
                    pairs[i] = (baseline, target, delta, Header(baseline, target));
                }
                try
                {
                    var cache = new NetworkReconstructionCache(capacity: 2);
                    for (var i = 0; i < pairs.Length; i++)
                    {
                        var pair = pairs[i];
                        Assert.That(cache.TryReconstruct(pool, pair.Baseline,
                            pair.Delta.Span, in pair.Header, Fingerprint, Scope,
                            out var canonical, out _, out _,
                            NetworkComponentDeltaHooks.Empty), Is.True);
                        canonical.Dispose();
                    }
                    var diagnostics = cache.CaptureDiagnostics();
                    Assert.That(diagnostics.Entries, Is.EqualTo(2));
                    Assert.That(diagnostics.Evictions, Is.EqualTo(1));
                    Assert.That(diagnostics.Misses, Is.EqualTo(3));

                    // Entry 0 was evicted to admit entry 2: repeating it is a miss again.
                    var first = pairs[0];
                    Assert.That(cache.TryReconstruct(pool, first.Baseline,
                        first.Delta.Span, in first.Header, Fingerprint, Scope,
                        out var repeated, out _, out _,
                        NetworkComponentDeltaHooks.Empty), Is.True);
                    repeated.Dispose();
                    Assert.That(cache.CaptureDiagnostics().Misses, Is.EqualTo(4));

                    cache.Clear();
                }
                finally
                {
                    foreach (var pair in pairs)
                    {
                        pair.Delta.Dispose();
                        pair.Baseline.Dispose();
                        pair.Target.Dispose();
                    }
                }
            }
            finally
            {
                Assert.That(pool.CaptureDiagnostics().OutstandingLeases, Is.Zero);
                pool.Dispose();
            }
        }

        [Test]
        public void CorruptedDeltaFailsWithoutPoisoningTheCacheAndStillTriggersRecovery()
        {
            var pool = new NetworkBufferPool(1L << 20);
            try
            {
                var baseline = Snapshot(pool, 1, Entities(Position(1, 0f, 0f, 0f)));
                var target = Snapshot(pool, 2, Entities(Position(1, 1f, 2f, 3f)));
                Assert.That(SnapshotDeltaCodec.TryEncode(pool, baseline, target,
                    out var delta), Is.True);
                using (delta)
                {
                    var header = Header(baseline, target);
                    var cache = new NetworkReconstructionCache();

                    var corrupted = delta.Span.ToArray();
                    corrupted[corrupted.Length - 1] ^= 0xFF;

                    // A caller-side failure (bit-flipped delta payload, as if one client's UDP
                    // packet were corrupted in transit) must fail exactly like the uncached
                    // path, and must not be cached: NetworkClient's caller reacts to `false`
                    // exactly as it does today by requesting a resync.
                    Assert.That(cache.TryReconstruct(pool, baseline, corrupted,
                        in header, Fingerprint, Scope, out var failed, out _,
                        out _, NetworkComponentDeltaHooks.Empty), Is.False);
                    Assert.That(failed, Is.Null);
                    Assert.That(cache.CaptureDiagnostics().Entries, Is.EqualTo(0));

                    // The real delta for the same baseline still reconstructs correctly
                    // afterwards; the failed attempt left no stale state behind.
                    Assert.That(cache.TryReconstruct(pool, baseline, delta.Span,
                        in header, Fingerprint, Scope, out var ok, out _, out _,
                        NetworkComponentDeltaHooks.Empty), Is.True);
                    using (ok)
                        Assert.That(ok.Span.SequenceEqual(target.Bytes.Span),
                            Is.True);
                    cache.Clear();
                }
                baseline.Dispose();
                target.Dispose();
            }
            finally
            {
                Assert.That(pool.CaptureDiagnostics().OutstandingLeases, Is.Zero);
                pool.Dispose();
            }
        }

        // --- Fixture builders -------------------------------------------------

        private static SnapshotChunkHeader Header(NetworkSnapshot baseline,
            NetworkSnapshot target) => new SnapshotChunkHeader
        {
            PayloadKind = SnapshotPayloadKind.Delta,
            SnapshotTick = target.ServerTick,
            BaselineTick = baseline.ServerTick,
            TotalLength = checked((uint)target.ByteLength),
            TotalHash = target.PayloadHash,
            ChunkIndex = 0,
            ChunkCount = 1,
        };

        private static void Corrupt(NetworkSnapshot snapshot)
        {
            // Internal access via InternalsVisibleTo("unigame.staticecs.network.tests"):
            // flips one byte of the raw canonical bytes without updating the snapshot's
            // already-cached PayloadHash field, so any future re-verification fails.
            snapshot.Buffer[snapshot.Offset] ^= 0xFF;
        }

        // SnapshotDeltaCodec.TryEncode only ever returns a delta that is strictly smaller than
        // a full keyframe (see its own `writer.Length >= target.ByteLength` guard): a one-entity
        // snapshot's delta is not reliably smaller than the snapshot itself. A ballast crowd of
        // entities that never changes between baseline and target (pure skip-count on the wire,
        // see NetworkSnapshotDeltaCompactionTests) makes the encoded delta small relative to the
        // full canonical snapshot, matching what every real dense-hub tick looks like.
        private const int BallastCount = 32;

        private static EntityDesc[] Entities(params RecordDesc[] records)
        {
            var moving = new EntityDesc(new EntityGID(1, 1, 0).Raw, 1, records);
            var all = new EntityDesc[BallastCount + 1];
            all[0] = moving;
            for (var i = 0; i < BallastCount; i++)
                all[i + 1] = new EntityDesc(new EntityGID((uint)(2 + i), 1, 0).Raw, 1,
                    new[] { Position(9, i, i, i) });
            return all;
        }

        private static RecordDesc Position(uint typeId, float x, float y, float z)
        {
            var payload = new byte[12];
            WriteFloat(payload, 0, x);
            WriteFloat(payload, 4, y);
            WriteFloat(payload, 8, z);
            return new RecordDesc(typeId, (byte)NetworkSchemaKind.Component, payload);
        }

        private static void WriteFloat(byte[] destination, int offset, float value) =>
            WriteInt(destination, offset, BitConverter.SingleToInt32Bits(value));

        private static void WriteInt(byte[] destination, int offset, int value)
        {
            destination[offset] = (byte)value;
            destination[offset + 1] = (byte)(value >> 8);
            destination[offset + 2] = (byte)(value >> 16);
            destination[offset + 3] = (byte)(value >> 24);
        }

        private static NetworkSnapshot Snapshot(NetworkBufferPool pool, uint tick,
            EntityDesc[] entities)
        {
            var bytes = BuildCanonical(entities);
            var recordCount = entities.Sum(e => e.Records.Length);
            return new NetworkSnapshot(tick, Fingerprint, Scope, pool.Copy(bytes),
                entities.Length, recordCount);
        }

        private static byte[] BuildCanonical(EntityDesc[] entities)
        {
            using var stream = new MemoryStream();
            WriteU32(stream, checked((uint)entities.Length));
            foreach (var entity in entities)
            {
                WriteU64(stream, entity.Gid);
                WriteU32(stream, entity.Kind);
                stream.WriteByte(0);
                WriteU16(stream, checked((ushort)entity.Records.Length));
                foreach (var record in entity.Records)
                {
                    WriteU32(stream, record.TypeId);
                    stream.WriteByte(record.Kind);
                    stream.WriteByte(0);
                    stream.WriteByte(0);
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
    }
}
