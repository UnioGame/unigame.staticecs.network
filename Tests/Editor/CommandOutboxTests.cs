using System;
using System.Threading;
using FFS.Libraries.StaticEcs;
using NUnit.Framework;

namespace UniGame.StaticEcs.Network.Tests
{
    public sealed class CommandOutboxTests
    {
        [SetUp]
        public void EnterPoolTestLock() => Monitor.Enter(PoolTestGate.Sync);

        [TearDown]
        public void ExitPoolTestLock() => Monitor.Exit(PoolTestGate.Sync);

        [Test]
        public void EnqueueResultValuesAndConstructorBoundsAreFrozen()
        {
            Assert.That((byte)EnqueueResult.Queued, Is.EqualTo(0));
            Assert.That((byte)EnqueueResult.Unavailable, Is.EqualTo(1));
            Assert.That((byte)EnqueueResult.Full, Is.EqualTo(2));
            Assert.That((byte)EnqueueResult.UnknownCommand, Is.EqualTo(3));
            Assert.That((byte)EnqueueResult.TooLarge, Is.EqualTo(4));
            Assert.That((byte)EnqueueResult.CodecFailed, Is.EqualTo(5));
            Assert.That((byte)EnqueueResult.SequenceExhausted, Is.EqualTo(6));

            var schema = ValueSchema();
            Assert.Throws<ArgumentNullException>(() => new CommandOutbox<OutboxWorld>(null));
            Assert.Throws<InvalidOperationException>(() => new CommandOutbox<OtherWorld>(schema));
            Assert.Throws<ArgumentOutOfRangeException>(() => new CommandOutbox<OutboxWorld>(schema, 0));
            Assert.Throws<ArgumentOutOfRangeException>(() => new CommandOutbox<OutboxWorld>(schema, ProtocolLimits.MaxCommandsPerBatch + 1));
            Assert.Throws<ArgumentOutOfRangeException>(() => new CommandOutbox<OutboxWorld>(schema, byteCapacity: 35));
            Assert.Throws<ArgumentOutOfRangeException>(() => new CommandOutbox<OutboxWorld>(schema, byteCapacity: ProtocolLimits.MaxWirePayloadBytes + 1));
        }

        [Test]
        public void BuildMatchesCanonicalCommandBatchAndTransfersIndependentOwnership()
        {
            using var outbox = new CommandOutbox<OutboxWorld>(ValueSchema(), 4, 256);
            var command = new ValueCommand { Value = 42 };
            Assert.That(outbox.Enqueue(in command, 9), Is.EqualTo(EnqueueResult.Queued));
            Assert.That(outbox.Count, Is.EqualTo(1));
            Assert.That(outbox.UnsentCount, Is.EqualTo(1));
            Assert.That(outbox.Bytes, Is.EqualTo(40));
            Assert.That(outbox.TryBuild(out var payload, out var through), Is.True);
            Assert.That(through, Is.EqualTo(1));
            AssertHex(payload.Span, "0100000000000001000000000000000000000000020000000100000009000000040000002A000000");

            var alias = payload;
            outbox.Dispose();
            Assert.That(payload.IsValid, Is.True);
            payload.Dispose();
            Assert.That(alias.IsValid, Is.False);
        }

        [Test]
        public void EnqueueFailuresAreOrderedAndTransactional()
        {
            ScratchCodec.LastDestinationLength = -1;
            using var outbox = new CommandOutbox<OutboxWorld>(CompleteSchema(), 1, 36);
            var unknown = new UnknownCommand();
            Assert.That(outbox.Enqueue(in unknown, 0), Is.EqualTo(EnqueueResult.UnknownCommand));
            var failing = new FailingCommand();
            Assert.That(outbox.Enqueue(in failing, 0), Is.EqualTo(EnqueueResult.CodecFailed));
            var invalid = new InvalidLengthCommand();
            Assert.That(outbox.Enqueue(in invalid, 0), Is.EqualTo(EnqueueResult.CodecFailed));
            var negative = new NegativeLengthCommand();
            Assert.That(outbox.Enqueue(in negative, 0), Is.EqualTo(EnqueueResult.CodecFailed));
            Assert.That(outbox.Count, Is.Zero);
            Assert.That(outbox.Bytes, Is.Zero);
            Assert.That(outbox.LastSequence, Is.Zero);
            var throwing = new ThrowingCommand();
            Assert.Throws<InvalidOperationException>(() => outbox.Enqueue(in throwing, 0));
            Assert.That(outbox.Count, Is.Zero);
            Assert.That(outbox.Bytes, Is.Zero);
            Assert.That(outbox.LastSequence, Is.Zero);

            var zero = new ZeroCommand();
            Assert.That(outbox.Enqueue(in zero, 1), Is.EqualTo(EnqueueResult.Queued));
            var large = new ScratchCommand();
            Assert.That(outbox.Enqueue(in large, 2), Is.EqualTo(EnqueueResult.TooLarge));
            Assert.That(ScratchCodec.LastDestinationLength, Is.EqualTo(64));
            Assert.That(outbox.Count, Is.EqualTo(1));
            Assert.That(outbox.LastSequence, Is.EqualTo(1));

            Assert.That(outbox.Enqueue(in zero, 3), Is.EqualTo(EnqueueResult.Full));
        }

        [Test]
        public void EmptyBuildReturnsDefaultOutputsWithoutMutation()
        {
            using var outbox = new CommandOutbox<OutboxWorld>(ValueSchema(), 1, 64);

            Assert.That(outbox.TryBuild(out var payload, out var through), Is.False);
            Assert.That(payload.IsValid, Is.False);
            Assert.That(payload, Is.EqualTo(default(PacketLease)));
            Assert.That(through, Is.Zero);
            Assert.That(outbox.Count, Is.Zero);
            Assert.That(outbox.UnsentCount, Is.Zero);
            Assert.That(outbox.Bytes, Is.Zero);
            Assert.That(outbox.LastSequence, Is.Zero);
            Assert.That(outbox.LastSentSequence, Is.Zero);
            Assert.That(outbox.AcknowledgedSequence, Is.Zero);
        }

        [Test]
        public void CompleteRegisteredMaximumCanFillConfiguredBatchExactly()
        {
            var schema = new SchemaBuilder<OutboxWorld>()
                .Command<MaximumCommand, MaximumCodec, Allow<MaximumCommand>>(Id(8), 1, Codec(8), 64)
                .Freeze();
            using var outbox = new CommandOutbox<OutboxWorld>(schema, 1, 100);
            var command = new MaximumCommand { Seed = 7 };
            Assert.That(outbox.Enqueue(in command, 4), Is.EqualTo(EnqueueResult.Queued));
            Assert.That(outbox.Bytes, Is.EqualTo(100));
            Assert.That(outbox.TryBuild(out var payload, out var through), Is.True);
            Assert.That(payload.Length, Is.EqualTo(100));
            Assert.That(through, Is.EqualTo(1));
            payload.Dispose();
        }

        [Test]
        public void FrozenPendingBuildRetriesExactPrefixUntilExactMark()
        {
            using var outbox = new CommandOutbox<OutboxWorld>(ValueSchema(), 4, 256);
            var first = new ValueCommand { Value = 1 };
            var second = new ValueCommand { Value = 2 };
            Assert.That(outbox.Enqueue(in first, 10), Is.EqualTo(EnqueueResult.Queued));
            Assert.That(outbox.TryBuild(out var initial, out var initialThrough), Is.True);
            var expected = initial.Span.ToArray();
            initial.Dispose();
            Assert.That(outbox.Enqueue(in second, 11), Is.EqualTo(EnqueueResult.Queued));
            Assert.That(outbox.TryBuild(out var retry, out var retryThrough), Is.True);
            CollectionAssert.AreEqual(expected, retry.Span.ToArray());
            Assert.That(retryThrough, Is.EqualTo(initialThrough));
            retry.Dispose();

            Assert.Throws<InvalidOperationException>(() => outbox.MarkSent(0));
            Assert.Throws<InvalidOperationException>(() => outbox.MarkSent(2));
            Assert.That(outbox.UnsentCount, Is.EqualTo(2));
            outbox.MarkSent(1);
            Assert.That(outbox.LastSentSequence, Is.EqualTo(1));
            Assert.That(outbox.UnsentCount, Is.EqualTo(1));
            Assert.Throws<InvalidOperationException>(() => outbox.MarkSent(1));

            Assert.That(outbox.TryBuild(out var suffix, out var suffixThrough), Is.True);
            Assert.That(suffixThrough, Is.EqualTo(2));
            Assert.That(PayloadCodec.TryReadCommandBatch(suffix.Span, out var batch), Is.True);
            Assert.That(batch.Commands.Length, Is.EqualTo(1));
            Assert.That(batch.Commands[0].Sequence, Is.EqualTo(2));
            Assert.That(BitConverter.ToInt32(batch.Commands[0].Payload), Is.EqualTo(2));
            suffix.Dispose();
        }

        [Test]
        public void AcknowledgementIsCumulativeBoundedAndNeverRemovesUnsentEntries()
        {
            using var outbox = new CommandOutbox<OutboxWorld>(ValueSchema(), 4, 256);
            EnqueueValue(outbox, 1);
            EnqueueValue(outbox, 2);
            Assert.That(outbox.TryBuild(out var payload, out var through), Is.True);
            payload.Dispose();
            Assert.That(outbox.Acknowledge(1), Is.False);
            Assert.That(outbox.Count, Is.EqualTo(2));
            outbox.MarkSent(through);
            Assert.That(outbox.Acknowledge(0), Is.True);
            Assert.That(outbox.Acknowledge(3), Is.False);
            Assert.That(outbox.Acknowledge(1), Is.True);
            Assert.That(outbox.Count, Is.EqualTo(1));
            Assert.That(outbox.Bytes, Is.EqualTo(40));
            Assert.That(outbox.Acknowledge(1), Is.True);
            Assert.That(outbox.Acknowledge(2), Is.True);
            Assert.That(outbox.Count, Is.Zero);
            Assert.That(outbox.Bytes, Is.Zero);
            Assert.That(outbox.AcknowledgedSequence, Is.EqualTo(2));
        }

        [Test]
        public void SplitByteRingBuildsRemainIndependentFrozenAndCanonical()
        {
            using var outbox = new CommandOutbox<OutboxWorld>(BlobSchema(), 3, 161);
            var first = new BlobCommand { Seed = 1 };
            var second = new BlobCommand { Seed = 2 };
            Assert.That(outbox.Enqueue(in first, 1), Is.EqualTo(EnqueueResult.Queued));
            Assert.That(outbox.Enqueue(in second, 2), Is.EqualTo(EnqueueResult.Queued));
            SendPending(outbox);
            Assert.That(outbox.Acknowledge(1), Is.True);

            for (var sequence = 3; sequence <= 7; sequence++)
            {
                var command = new BlobCommand { Seed = sequence };
                Assert.That(outbox.Enqueue(in command, (uint)sequence), Is.EqualTo(EnqueueResult.Queued));
                SendPending(outbox);
                Assert.That(outbox.Acknowledge((uint)(sequence - 1)), Is.True);
                Assert.That(outbox.Count, Is.EqualTo(1));
            }

            // Seven 20-byte payloads leave the tail at 140; payload eight ends at 160,
            // so payload nine begins at 160 and splits across the physical 161-byte boundary.
            var eighth = new BlobCommand { Seed = 8 };
            var ninth = new BlobCommand { Seed = 9 };
            Assert.That(outbox.Enqueue(in eighth, 8), Is.EqualTo(EnqueueResult.Queued));
            Assert.That(outbox.Enqueue(in ninth, 9), Is.EqualTo(EnqueueResult.Queued));
            Assert.That(outbox.Count, Is.EqualTo(3));
            Assert.That(outbox.UnsentCount, Is.EqualTo(2));
            Assert.That(outbox.Bytes, Is.EqualTo(160));

            Assert.That(outbox.TryBuild(out var initial, out var initialThrough), Is.True);
            Assert.That(initialThrough, Is.EqualTo(9));
            var expected = initial.Span.ToArray();
            Assert.That(outbox.TryBuild(out var retry, out var retryThrough), Is.True);
            Assert.That(retryThrough, Is.EqualTo(initialThrough));
            initial.Dispose();
            Assert.That(retry.IsValid, Is.True);
            CollectionAssert.AreEqual(expected, retry.Span.ToArray());
            Assert.That(PayloadCodec.TryReadCommandBatch(retry.Span, out var batch), Is.True);
            Assert.That(batch.Commands.Length, Is.EqualTo(2));
            Assert.That(batch.Commands[0].Sequence, Is.EqualTo(8));
            Assert.That(batch.Commands[1].Sequence, Is.EqualTo(9));
            Assert.That(batch.Commands[0].Payload, Is.EqualTo(ExpectedBlob(8)));
            Assert.That(batch.Commands[1].Payload, Is.EqualTo(ExpectedBlob(9)));
            retry.Dispose();

            outbox.MarkSent(initialThrough);
            Assert.That(outbox.Acknowledge(initialThrough), Is.True);
            Assert.That(outbox.Count, Is.Zero);
            Assert.That(outbox.Bytes, Is.Zero);
        }

        [Test]
        public void CountAndAggregateBytePressureReturnFull()
        {
            var zeroSchema = new SchemaBuilder<OutboxWorld>()
                .Command<ZeroCommand, ZeroCodec, Allow<ZeroCommand>>(Id(2), 1, Codec(2), 1)
                .Freeze();
            using (var countBound = new CommandOutbox<OutboxWorld>(zeroSchema, 1, 256))
            {
                var value = new ZeroCommand();
                Assert.That(countBound.Enqueue(in value, 0), Is.EqualTo(EnqueueResult.Queued));
                Assert.That(countBound.Enqueue(in value, 0), Is.EqualTo(EnqueueResult.Full));
            }
            using (var byteBound = new CommandOutbox<OutboxWorld>(zeroSchema, 4, 67))
            {
                var value = new ZeroCommand();
                Assert.That(byteBound.Enqueue(in value, 0), Is.EqualTo(EnqueueResult.Queued));
                Assert.That(byteBound.Enqueue(in value, 0), Is.EqualTo(EnqueueResult.Full));
            }
        }

        [Test]
        public void MaximumSequenceIsAssignedOnceThenExhausted()
        {
            using var outbox = new CommandOutbox<OutboxWorld>(ValueSchema(), 2, 128);
            outbox.ForceLastSequenceForTests(uint.MaxValue - 1);
            var command = new ValueCommand { Value = 1 };
            Assert.That(outbox.Enqueue(in command, 0), Is.EqualTo(EnqueueResult.Queued));
            Assert.That(outbox.LastSequence, Is.EqualTo(uint.MaxValue));
            Assert.That(outbox.Enqueue(in command, 0), Is.EqualTo(EnqueueResult.SequenceExhausted));
            Assert.That(outbox.Count, Is.EqualTo(1));
        }

        [Test]
        public void DisposeIsIdempotentAndAllOtherOperationsThrow()
        {
            var outbox = new CommandOutbox<OutboxWorld>(ValueSchema(), 1, 64);
            outbox.Dispose();
            outbox.Dispose();
            var command = new ValueCommand();
            Assert.Throws<ObjectDisposedException>(() => outbox.Enqueue(in command, 0));
            Assert.Throws<ObjectDisposedException>(() => outbox.TryBuild(out _, out _));
            Assert.Throws<ObjectDisposedException>(() => outbox.MarkSent(1));
            Assert.Throws<ObjectDisposedException>(() => outbox.Acknowledge(0));
            Assert.Throws<ObjectDisposedException>(() => { var _ = outbox.Count; });
            Assert.Throws<ObjectDisposedException>(() => { var _ = outbox.LastSequence; });
        }

        [Test]
        public void WarmedEnqueueBuildMarkAcknowledgeLoopAllocatesNoManagedMemory()
        {
            const int iterations = 1024;
            using var outbox = new CommandOutbox<OutboxWorld>(ValueSchema(), 1, 64);
            var warmBefore = GC.GetAllocatedBytesForCurrentThread();
            RunCycles(outbox, 256, out _, out _);
            var warmAllocated = GC.GetAllocatedBytesForCurrentThread() - warmBefore;
            var before = GC.GetAllocatedBytesForCurrentThread();
            RunCycles(outbox, iterations, out var successes, out var bytes);
            var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

            Assert.That(warmAllocated, Is.GreaterThanOrEqualTo(0));
            Assert.That(successes, Is.EqualTo(iterations));
            Assert.That(bytes, Is.EqualTo(iterations * 40));
            Assert.That(allocated, Is.Zero);
        }

        private static void RunCycles(CommandOutbox<OutboxWorld> outbox, int count, out int successes, out int bytes)
        {
            successes = 0;
            bytes = 0;
            for (var i = 0; i < count; i++)
            {
                var command = new ValueCommand { Value = i };
                if (outbox.Enqueue(in command, (uint)i) != EnqueueResult.Queued) continue;
                if (!outbox.TryBuild(out var payload, out var through)) continue;
                bytes += payload.Length;
                payload.Dispose();
                outbox.MarkSent(through);
                if (outbox.Acknowledge(through)) successes++;
            }
        }

        private static void EnqueueValue(CommandOutbox<OutboxWorld> outbox, int value)
        {
            var command = new ValueCommand { Value = value };
            Assert.That(outbox.Enqueue(in command, (uint)value), Is.EqualTo(EnqueueResult.Queued));
        }

        private static void SendPending(CommandOutbox<OutboxWorld> outbox)
        {
            Assert.That(outbox.TryBuild(out var payload, out var through), Is.True);
            payload.Dispose();
            outbox.MarkSent(through);
        }

        private static Schema ValueSchema() => new SchemaBuilder<OutboxWorld>()
            .Command<ValueCommand, ValueCodec, Allow<ValueCommand>>(Id(1), 2, Codec(1), 4)
            .Freeze();

        private static Schema BlobSchema() => new SchemaBuilder<OutboxWorld>()
            .Command<BlobCommand, BlobCodec, Allow<BlobCommand>>(Id(9), 1, Codec(9), 20)
            .Freeze();

        private static Schema CompleteSchema() => new SchemaBuilder<OutboxWorld>()
            .Command<ValueCommand, ValueCodec, Allow<ValueCommand>>(Id(1), 2, Codec(1), 4)
            .Command<ZeroCommand, ZeroCodec, Allow<ZeroCommand>>(Id(2), 1, Codec(2), 1)
            .Command<FailingCommand, FailingCodec, Allow<FailingCommand>>(Id(3), 1, Codec(3), 8)
            .Command<InvalidLengthCommand, InvalidLengthCodec, Allow<InvalidLengthCommand>>(Id(4), 1, Codec(4), 4)
            .Command<NegativeLengthCommand, NegativeLengthCodec, Allow<NegativeLengthCommand>>(Id(7), 1, Codec(7), 4)
            .Command<ThrowingCommand, ThrowingCodec, Allow<ThrowingCommand>>(Id(5), 1, Codec(5), 4)
            .Command<ScratchCommand, ScratchCodec, Allow<ScratchCommand>>(Id(6), 1, Codec(6), 64)
            .Freeze();

        private static TypeId Id(int value) => new(new Guid(value, 0, 0, new byte[8]));
        private static CodecId Codec(int value) => new(new Guid(value, 1, 0, new byte[8]));
        private static byte[] ExpectedBlob(byte value) { var bytes = new byte[20]; Array.Fill(bytes, value); return bytes; }
        private static void AssertHex(ReadOnlySpan<byte> bytes, string expected) =>
            Assert.That(BitConverter.ToString(bytes.ToArray()).Replace("-", string.Empty), Is.EqualTo(expected));

        private struct OutboxWorld : IWorldType { }
        private struct OtherWorld : IWorldType { }
        private struct ValueCommand { public int Value; }
        private struct ZeroCommand { }
        private struct FailingCommand { }
        private struct InvalidLengthCommand { }
        private struct NegativeLengthCommand { }
        private struct ThrowingCommand { }
        private struct ScratchCommand { }
        private struct MaximumCommand { public int Seed; }
        private struct BlobCommand { public int Seed; }
        private struct UnknownCommand { }

        private struct Allow<T> : ICommandAuthorizer<OutboxWorld, T> where T : unmanaged
        {
            public bool Authorize(in CommandContext context, in T command) => true;
        }

        private struct ValueCodec : ICodec<ValueCommand>
        {
            public bool TryWrite(in ValueCommand value, Span<byte> destination, out int written)
            {
                if (destination.Length < 4) { written = 0; return false; }
                BitConverter.TryWriteBytes(destination, value.Value); written = 4; return true;
            }
            public bool TryRead(ReadOnlySpan<byte> source, out ValueCommand value, out int read)
            {
                if (source.Length != 4) { value = default; read = 0; return false; }
                value = new ValueCommand { Value = BitConverter.ToInt32(source) }; read = 4; return true;
            }
        }

        private struct ZeroCodec : ICodec<ZeroCommand>
        {
            public bool TryWrite(in ZeroCommand value, Span<byte> destination, out int written) { written = 0; return true; }
            public bool TryRead(ReadOnlySpan<byte> source, out ZeroCommand value, out int read) { value = default; read = 0; return source.IsEmpty; }
        }

        private struct FailingCodec : ICodec<FailingCommand>
        {
            public bool TryWrite(in FailingCommand value, Span<byte> destination, out int written) { written = 0; return false; }
            public bool TryRead(ReadOnlySpan<byte> source, out FailingCommand value, out int read) { value = default; read = 0; return false; }
        }

        private struct InvalidLengthCodec : ICodec<InvalidLengthCommand>
        {
            public bool TryWrite(in InvalidLengthCommand value, Span<byte> destination, out int written) { written = destination.Length + 1; return true; }
            public bool TryRead(ReadOnlySpan<byte> source, out InvalidLengthCommand value, out int read) { value = default; read = 0; return false; }
        }

        private struct NegativeLengthCodec : ICodec<NegativeLengthCommand>
        {
            public bool TryWrite(in NegativeLengthCommand value, Span<byte> destination, out int written) { written = -1; return true; }
            public bool TryRead(ReadOnlySpan<byte> source, out NegativeLengthCommand value, out int read) { value = default; read = 0; return false; }
        }

        private struct ThrowingCodec : ICodec<ThrowingCommand>
        {
            public bool TryWrite(in ThrowingCommand value, Span<byte> destination, out int written) { throw new InvalidOperationException("command codec"); }
            public bool TryRead(ReadOnlySpan<byte> source, out ThrowingCommand value, out int read) { value = default; read = 0; return false; }
        }

        private struct ScratchCodec : ICodec<ScratchCommand>
        {
            internal static int LastDestinationLength;
            public bool TryWrite(in ScratchCommand value, Span<byte> destination, out int written)
            {
                LastDestinationLength = destination.Length;
                destination.Slice(0, 5).Fill(7); written = 5; return true;
            }
            public bool TryRead(ReadOnlySpan<byte> source, out ScratchCommand value, out int read) { value = default; read = source.Length; return source.Length == 5; }
        }

        private struct MaximumCodec : ICodec<MaximumCommand>
        {
            public bool TryWrite(in MaximumCommand value, Span<byte> destination, out int written)
            {
                destination.Fill((byte)value.Seed); written = destination.Length; return true;
            }
            public bool TryRead(ReadOnlySpan<byte> source, out MaximumCommand value, out int read)
            {
                value = new MaximumCommand { Seed = source.IsEmpty ? 0 : source[0] }; read = source.Length; return source.Length == 64;
            }
        }

        private struct BlobCodec : ICodec<BlobCommand>
        {
            public bool TryWrite(in BlobCommand value, Span<byte> destination, out int written)
            {
                if (destination.Length < 20) { written = 0; return false; }
                destination.Slice(0, 20).Fill((byte)value.Seed); written = 20; return true;
            }
            public bool TryRead(ReadOnlySpan<byte> source, out BlobCommand value, out int read)
            {
                value = new BlobCommand { Seed = source.IsEmpty ? 0 : source[0] }; read = source.Length; return source.Length == 20;
            }
        }
    }
}
