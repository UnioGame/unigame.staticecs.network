using System;
using FFS.Libraries.StaticEcs;
using FFS.Libraries.StaticPack;
using NUnit.Framework;

namespace UniGame.StaticEcs.Network.Tests
{
    // NCORE-14: covers the generic per-component value-delta hook mechanism in the
    // codec/schema itself (NetworkComponentDeltaHooks, PatchFast's tagged length field,
    // schema fingerprint sensitivity to hook capability) using a small local fixture
    // component. Component-specific hooks (PositionComponent, AnimationStateComponent)
    // are covered in the game repository's own Game.ECS tests -- this package must not
    // depend on Game.ECS.
    public sealed partial class NetworkV7Tests
    {
        // A minimal delta-capable fixture: encodes the signed difference of its 4-byte
        // int payload as 1 byte whenever that fits in an sbyte, and declines (raw
        // fallback) otherwise. Deliberately simple/deterministic so test expectations can
        // assert exact byte counts.
        public struct DeltaTestComponent : IComponent, INetworkType, INetworkComponentDelta
        {
            public int Value;

            public void Write<TWorld>(ref BinaryPackWriter writer, World<TWorld>.Entity self)
                where TWorld : struct, IWorldType => writer.WriteInt(Value);

            public void Read<TWorld>(ref BinaryPackReader reader, World<TWorld>.Entity self,
                byte version, bool disabled) where TWorld : struct, IWorldType =>
                Value = reader.ReadInt();

            public int TryWriteValueDelta(ReadOnlySpan<byte> baselinePayload,
                ReadOnlySpan<byte> targetPayload, Span<byte> destination)
            {
                if (baselinePayload.Length != 4 || targetPayload.Length != 4 ||
                    destination.Length < 1)
                    return -1;
                var baseline = ReadInt32Le(baselinePayload);
                var target = ReadInt32Le(targetPayload);
                long diff = (long)target - baseline;
                if (diff < sbyte.MinValue || diff > sbyte.MaxValue)
                    return -1;
                destination[0] = unchecked((byte)(sbyte)diff);
                return 1;
            }

            public bool TryReadValueDelta(ReadOnlySpan<byte> baselinePayload,
                ReadOnlySpan<byte> delta, Span<byte> targetPayload, out int written)
            {
                written = 0;
                if (baselinePayload.Length != 4 || delta.Length != 1 ||
                    targetPayload.Length < 4)
                    return false;
                var baseline = ReadInt32Le(baselinePayload);
                var target = baseline + unchecked((sbyte)delta[0]);
                targetPayload[0] = (byte)target;
                targetPayload[1] = (byte)(target >> 8);
                targetPayload[2] = (byte)(target >> 16);
                targetPayload[3] = (byte)(target >> 24);
                written = 4;
                return true;
            }

            private static int ReadInt32Le(ReadOnlySpan<byte> bytes) =>
                bytes[0] | bytes[1] << 8 | bytes[2] << 16 | bytes[3] << 24;
        }

        public struct DeltaHookWorld : IWorldType { }

        private static NetworkSchema<TWorld> DeltaHookSchema<TWorld>()
            where TWorld : struct, IWorldType
        {
            var factory = NetworkCompilerSupport.Create<TWorld>();
            factory.Entity<TestEntity>(new NetworkTypeId(1));
            factory.Component<DeltaTestComponent>(new NetworkTypeId(2));
            return factory.Freeze();
        }

        private static void CreateDeltaHookWorld<TWorld>() where TWorld : struct, IWorldType
        {
            World<TWorld>.Create(WorldConfig.Default());
            var types = World<TWorld>.Types();
            types.RegisterAll(typeof(NetworkOwnerComponent).Assembly);
            types.EntityType<TestEntity>();
            types.Component<DeltaTestComponent>();
            World<TWorld>.Initialize();
        }

        [Test]
        public void FreezeDetectsDeltaHookWithoutGeneratorChanges()
        {
            var schema = DeltaHookSchema<DeltaHookWorld>();
            var entry = schema.Entries[0].Kind == NetworkSchemaKind.Entity
                ? schema.Entries[1] : schema.Entries[0];
            Assert.That(entry.RuntimeType, Is.EqualTo(typeof(DeltaTestComponent)));
            Assert.That(entry.DeltaHook, Is.Not.Null);
            Assert.That(schema.DeltaHooks.TryGet(entry.TypeId.Value, out var hook),
                Is.True);
            Assert.That(hook, Is.InstanceOf<DeltaTestComponent>());
            // A plain component's schema never registers a hook, and its own
            // DeltaHooks table is the shared Empty singleton (no allocation).
            var plainSchema = Schema<AuthorityWorld>(true);
            foreach (var plainEntry in plainSchema.Entries)
                Assert.That(plainEntry.DeltaHook, Is.Null);
            Assert.That(plainSchema.DeltaHooks, Is.SameAs(
                NetworkComponentDeltaHooks.Empty));
        }

        [Test]
        public void FreezeIsDeterministicAndSensitiveToDeltaCapability()
        {
            var first = DeltaHookSchema<DeltaHookWorld>();
            var second = DeltaHookSchema<DeltaHookWorld>();
            Assert.That(first.Fingerprint, Is.EqualTo(second.Fingerprint));

            // A schema whose only difference is the presence of a value-delta hook must
            // fingerprint differently: PatchFast's length-field semantics for that typeId
            // depend on it, so a mismatched pair must fail schema negotiation rather than
            // silently misparse.
            Assert.That(first.Fingerprint, Is.Not.EqualTo(Schema<AuthorityWorld>(true)
                .Fingerprint));
        }

        [Test]
        public void PatchFastUsesTaggedDeltaEncodingWhenHookShrinksThePayload()
        {
            CreateDeltaHookWorld<DeltaHookWorld>();
            try
            {
                var pool = new NetworkBufferPool(1L << 20);
                var schema = DeltaHookSchema<DeltaHookWorld>();
                var scope = new ScopeId(1);
                var replicator = new NetworkReplicator<DeltaHookWorld>(schema,
                    static (_, _) => true, scope, bufferPool: pool);
                NetworkSnapshot baseline = null, target = null;
                NetworkBufferLease delta = null, canonical = null;
                try
                {
                    var entity = World<DeltaHookWorld>.NewEntity<TestEntity>();
                    entity.Set(new DeltaTestComponent { Value = 100 });
                    Assert.That(replicator.Capture(1, out baseline),
                        Is.EqualTo(SnapshotCaptureResult.Success));

                    entity.Set(new DeltaTestComponent { Value = 105 }); // diff = 5, fits sbyte
                    Assert.That(replicator.Capture(2, out target),
                        Is.EqualTo(SnapshotCaptureResult.Success));

                    Assert.That(SnapshotDeltaCodec.TryEncode(pool, baseline, target,
                        out delta, schema.DeltaHooks), Is.True);

                    // Wire shape: formatVersion(1) + entityCount(4) + recordCount(4) +
                    // operationCount(4) + skip(1, =0) + opcode(1, PatchFast) +
                    // mask(1 byte: 1 record -> 2 bits round up to 1 byte) +
                    // taggedLength(1 byte varint, value=(1<<1)|1=3) + deltaByte(1) = 18.
                    Assert.That(delta.Length, Is.EqualTo(18));
                    var taggedLengthByte = delta.Span[16];
                    Assert.That(taggedLengthByte & 1, Is.EqualTo(1), "isDelta tag not set");
                    Assert.That(taggedLengthByte >> 1, Is.EqualTo(1), "delta length wrong");
                    Assert.That(delta.Span[17], Is.EqualTo(5), "raw sbyte diff wrong");

                    var header = DeltaHeader(baseline, target);
                    Assert.That(SnapshotDeltaCodec.TryReconstruct(pool, baseline,
                        delta.Span, in header, schema.Fingerprint, scope,
                        out canonical, out var entities, out var records,
                        schema.DeltaHooks), Is.True);
                    var reconstructed = replicator.CreateSnapshot(header.SnapshotTick,
                        schema.Fingerprint, scope, canonical, entities, records);
                    canonical = null;
                    Assert.That(reconstructed.Bytes.Span.SequenceEqual(
                        target.Bytes.Span), Is.True, "canonical bytes must match exactly");
                    Assert.That(reconstructed.PayloadHash, Is.EqualTo(target.PayloadHash));
                    reconstructed.Dispose();
                }
                finally
                {
                    delta?.Dispose();
                    canonical?.Dispose();
                    baseline?.Dispose();
                    target?.Dispose();
                    replicator.Dispose();
                    Assert.That(pool.CaptureDiagnostics().OutstandingLeases, Is.Zero);
                    pool.Dispose();
                }
            }
            finally
            {
                World<DeltaHookWorld>.Destroy();
            }
        }

        [Test]
        public void PatchFastFallsBackToRawPayloadWhenHookDeclines()
        {
            CreateDeltaHookWorld<DeltaHookWorld>();
            try
            {
                var pool = new NetworkBufferPool(1L << 20);
                var schema = DeltaHookSchema<DeltaHookWorld>();
                var scope = new ScopeId(1);
                var replicator = new NetworkReplicator<DeltaHookWorld>(schema,
                    static (_, _) => true, scope, bufferPool: pool);
                NetworkSnapshot baseline = null, target = null;
                NetworkBufferLease delta = null, canonical = null;
                try
                {
                    var entity = World<DeltaHookWorld>.NewEntity<TestEntity>();
                    entity.Set(new DeltaTestComponent { Value = 0 });
                    Assert.That(replicator.Capture(1, out baseline),
                        Is.EqualTo(SnapshotCaptureResult.Success));

                    entity.Set(new DeltaTestComponent { Value = 100_000 }); // too large for sbyte
                    Assert.That(replicator.Capture(2, out target),
                        Is.EqualTo(SnapshotCaptureResult.Success));

                    Assert.That(SnapshotDeltaCodec.TryEncode(pool, baseline, target,
                        out delta, schema.DeltaHooks), Is.True);
                    var taggedLengthByte = delta.Span[16];
                    Assert.That(taggedLengthByte & 1, Is.EqualTo(0),
                        "hook declined; isDelta must be 0 (raw fallback)");
                    Assert.That(taggedLengthByte >> 1, Is.EqualTo(4),
                        "raw payload is still 4 bytes");

                    var header = DeltaHeader(baseline, target);
                    Assert.That(SnapshotDeltaCodec.TryReconstruct(pool, baseline,
                        delta.Span, in header, schema.Fingerprint, scope,
                        out canonical, out var entities, out var records,
                        schema.DeltaHooks), Is.True);
                    var reconstructed = replicator.CreateSnapshot(header.SnapshotTick,
                        schema.Fingerprint, scope, canonical, entities, records);
                    canonical = null;
                    Assert.That(reconstructed.Bytes.Span.SequenceEqual(
                        target.Bytes.Span), Is.True);
                    reconstructed.Dispose();
                }
                finally
                {
                    delta?.Dispose();
                    canonical?.Dispose();
                    baseline?.Dispose();
                    target?.Dispose();
                    replicator.Dispose();
                    Assert.That(pool.CaptureDiagnostics().OutstandingLeases, Is.Zero);
                    pool.Dispose();
                }
            }
            finally
            {
                World<DeltaHookWorld>.Destroy();
            }
        }

        [Test]
        public void ReconstructRejectsCorruptedDeltaTaggedPayload()
        {
            CreateDeltaHookWorld<DeltaHookWorld>();
            try
            {
                var pool = new NetworkBufferPool(1L << 20);
                var schema = DeltaHookSchema<DeltaHookWorld>();
                var scope = new ScopeId(1);
                var replicator = new NetworkReplicator<DeltaHookWorld>(schema,
                    static (_, _) => true, scope, bufferPool: pool);
                NetworkSnapshot baseline = null, target = null;
                NetworkBufferLease delta = null, canonical = null;
                try
                {
                    var entity = World<DeltaHookWorld>.NewEntity<TestEntity>();
                    entity.Set(new DeltaTestComponent { Value = 10 });
                    Assert.That(replicator.Capture(1, out baseline),
                        Is.EqualTo(SnapshotCaptureResult.Success));
                    entity.Set(new DeltaTestComponent { Value = 15 });
                    Assert.That(replicator.Capture(2, out target),
                        Is.EqualTo(SnapshotCaptureResult.Success));
                    Assert.That(SnapshotDeltaCodec.TryEncode(pool, baseline, target,
                        out delta, schema.DeltaHooks), Is.True);
                    // 13-byte delta header + skip(1) + opcode(1) + mask(1) +
                    // taggedLength(1) + 1 hook-produced delta byte = 18.
                    Assert.That(delta.Length, Is.EqualTo(18));

                    var header = DeltaHeader(baseline, target);
                    // Truncating exactly the hook's own delta byte off the end must be
                    // rejected (not enough bytes for the declared tagged length), not
                    // crash or reconstruct a wrong value.
                    Assert.That(SnapshotDeltaCodec.TryReconstruct(pool, baseline,
                        delta.Span.Slice(0, delta.Length - 1), in header,
                        schema.Fingerprint, scope, out canonical, out _, out _,
                        schema.DeltaHooks), Is.False);
                }
                finally
                {
                    delta?.Dispose();
                    canonical?.Dispose();
                    baseline?.Dispose();
                    target?.Dispose();
                    replicator.Dispose();
                    pool.Dispose();
                }
            }
            finally
            {
                World<DeltaHookWorld>.Destroy();
            }
        }
    }
}
