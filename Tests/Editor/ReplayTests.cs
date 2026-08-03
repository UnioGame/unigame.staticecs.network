using System;
using System.Collections;
using System.IO;
using System.Reflection;
using NUnit.Framework;

namespace UniGame.StaticEcs.Network.Tests
{
    public sealed class ReplayTests
    {
        [Test]
        public void TraceSaveLoadAndReplayPreserveExactCallsAndOwnership()
        {
            var inner = new ScriptTransport { SendMode = SendMode.Accept };
            inner.Inbound = new byte[] { 7, 8, 9 };
            var tape = new ReplayTape(4096);
            var trace = new TraceTransport(inner, tape);
            trace.BeginStep(11);
            var accepted = Lease(1, 2, 3);
            Assert.That(trace.TrySend(Channel.ReliableOrdered, ref accepted), Is.True);
            Assert.That(accepted.IsValid, Is.False);
            inner.SendMode = SendMode.Reject;
            var rejected = Lease(4, 5);
            Assert.That(trace.TrySend(Channel.UnreliableSequenced, ref rejected), Is.False);
            Assert.That(rejected.IsValid, Is.True);
            rejected.Dispose();
            Assert.That(trace.TryReceive(out var inboundChannel, out var inbound), Is.True);
            Assert.That(inboundChannel, Is.EqualTo(Channel.UnreliableSequenced));
            CollectionAssert.AreEqual(new byte[] { 7, 8, 9 }, inbound.Span.ToArray());
            inbound.Dispose();
            trace.Dispose();
            Assert.That(inner.DisposeCount, Is.EqualTo(1));
            Assert.That(tape.IsSealed, Is.True);
            Assert.That(tape.IsComplete, Is.True);

            using var saved = new MemoryStream();
            tape.Save(saved);
            Assert.That(saved.CanWrite, Is.True);
            var bytes = saved.ToArray();
            CollectionAssert.AreEqual(new byte[] { 0x53, 0x45, 0x43, 0x53, 0x4e, 0x45, 0x54, 0x31 }, bytes.AsSpan(0, 8).ToArray());
            Assert.That(Read32(bytes, 12), Is.EqualTo(4));
            Assert.That(Read32(bytes, 32), Is.EqualTo(1));
            Assert.That(Read32(bytes, 36), Is.Zero);

            saved.Position = 0;
            using var loaded = ReplayTape.Load(saved, 4096);
            using var replay = new ReplayTransport(loaded);
            Assert.That(replay.State, Is.EqualTo(TransportState.Connected));
            Assert.That(replay.Error, Is.EqualTo(TransportError.None));
            replay.BeginStep(11);
            var replayAccepted = Lease(1, 2, 3);
            Assert.That(replay.TrySend(Channel.ReliableOrdered, ref replayAccepted), Is.True);
            Assert.That(replayAccepted.IsValid, Is.False);
            var replayRejected = Lease(4, 5);
            Assert.That(replay.TrySend(Channel.UnreliableSequenced, ref replayRejected), Is.False);
            Assert.That(replayRejected.IsValid, Is.True);
            replayRejected.Dispose();
            Assert.That(replay.TryReceive(out var channel, out var packet), Is.True);
            Assert.That(channel, Is.EqualTo(Channel.UnreliableSequenced));
            CollectionAssert.AreEqual(new byte[] { 7, 8, 9 }, packet.Span.ToArray());
            packet.Span[0] = 99;
            packet.Dispose();
        }

        [Test]
        public void ReplayMismatchDoesNotConsumeSendAndFaultsWithoutRepeatingOnDispose()
        {
            using var tape = RecordSingleSend(new byte[] { 1, 2 }, true);
            var replay = new ReplayTransport(tape);
            var wrong = Lease(1, 3);
            Assert.Throws<InvalidOperationException>(() => replay.TrySend(Channel.ReliableOrdered, ref wrong));
            Assert.That(wrong.IsValid, Is.True);
            Assert.That(replay.State, Is.EqualTo(TransportState.Faulted));
            Assert.That(replay.Error, Is.EqualTo(TransportError.InvalidPacket));
            Assert.DoesNotThrow(replay.Dispose);
            wrong.Dispose();
        }

        [Test]
        public void EarlyReplayDisposeReportsTruncationAndReleasesBorrow()
        {
            using var tape = RecordSingleSend(new byte[] { 1 }, true);
            var replay = new ReplayTransport(tape);
            Assert.Throws<InvalidOperationException>(replay.Dispose);
            Assert.That(replay.State, Is.EqualTo(TransportState.Faulted));
            Assert.DoesNotThrow(replay.Dispose);
            using var replayAgain = new ReplayTransport(tape);
            var packet = Lease(1);
            Assert.That(replayAgain.TrySend(Channel.ReliableOrdered, ref packet), Is.True);
        }

        [Test]
        public void OverflowMarksTapeIncompleteWithoutChangingWrappedResult()
        {
            var inner = new ScriptTransport { SendMode = SendMode.Accept };
            using var tape = new ReplayTape(24);
            using (var trace = new TraceTransport(inner, tape))
            {
                trace.BeginStep(1);
                var packet = Lease(3);
                Assert.That(trace.TrySend(Channel.ReliableOrdered, ref packet), Is.True);
            }
            Assert.That(tape.IsComplete, Is.False);
            Assert.That(tape.Dropped, Is.EqualTo(1));
            Assert.Throws<InvalidOperationException>(() => tape.Save(new MemoryStream()));
            Assert.Throws<InvalidOperationException>(() => new ReplayTransport(tape));
        }

        [TestCase(SendMode.Throw)]
        [TestCase(SendMode.TransferThenThrow)]
        public void WrappedSendExceptionIsRethrownAndProducesIncompleteTerminalTrace(SendMode mode)
        {
            var inner = new ScriptTransport { SendMode = mode };
            using var tape = new ReplayTape(1024);
            using (var trace = new TraceTransport(inner, tape))
            {
                var packet = Lease(6);
                var error = Assert.Throws<InvalidOperationException>(() => trace.TrySend(Channel.ReliableOrdered, ref packet));
                Assert.That(error.Message, Is.EqualTo("send"));
                Assert.That(packet.IsValid, Is.EqualTo(mode == SendMode.Throw));
                if (packet.IsValid) packet.Dispose();
            }
            Assert.That(tape.IsComplete, Is.False);
        }

        [Test]
        public void ExternalDisposeIsDeferredUntilActiveClaimReleases()
        {
            var inner = new ScriptTransport();
            var tape = new ReplayTape(1024);
            var trace = new TraceTransport(inner, tape);
            tape.Dispose();
            Assert.Throws<ObjectDisposedException>(() => _ = tape.Bytes);
            Assert.DoesNotThrow(() => trace.BeginStep(1));
            trace.Dispose();
            Assert.Throws<ObjectDisposedException>(() => _ = tape.IsSealed);
            Assert.DoesNotThrow(tape.Dispose);
        }

        [Test]
        public void LoadRejectsCorruptionBudgetTrailingAndTruncationTransactionally()
        {
            using var tape = RecordSingleSend(new byte[] { 9 }, true);
            using var output = new MemoryStream();
            tape.Save(output);
            var bytes = output.ToArray();

            var corrupt = (byte[])bytes.Clone(); corrupt[corrupt.Length - 1] ^= 1;
            Assert.Throws<InvalidDataException>(() => ReplayTape.Load(new MemoryStream(corrupt), 1024));
            Assert.Throws<InvalidDataException>(() => ReplayTape.Load(new MemoryStream(bytes), 24));
            var trailing = new byte[bytes.Length + 1]; bytes.CopyTo(trailing, 0);
            Assert.Throws<InvalidDataException>(() => ReplayTape.Load(new MemoryStream(trailing), 1024));
            var truncated = new byte[bytes.Length - 1]; Array.Copy(bytes, truncated, truncated.Length);
            Assert.Throws<EndOfStreamException>(() => ReplayTape.Load(new MemoryStream(truncated), 1024));
        }

        [Test]
        public void ZeroPayloadSendRoundTripsThroughPersistence()
        {
            using var tape = RecordSingleSend(Array.Empty<byte>(), true);
            using var output = new MemoryStream();
            tape.Save(output);
            output.Position = 0;
            using var loaded = ReplayTape.Load(output, 1024);
            using var replay = new ReplayTransport(loaded);
            var empty = Lease(Array.Empty<byte>());
            Assert.That(replay.TrySend(Channel.ReliableOrdered, ref empty), Is.True);
            Assert.That(empty.IsValid, Is.False);
        }

        [Test]
        public void ConstructorValidationDoesNotTakeOwnershipOrClaimTape()
        {
            var tape = new ReplayTape(1024);
            var invalid = new ScriptTransport { State = TransportState.Faulted, Error = TransportError.InvalidPacket };
            Assert.Throws<InvalidOperationException>(() => new TraceTransport(invalid, tape));
            Assert.That(invalid.DisposeCount, Is.Zero);
            Assert.DoesNotThrow(tape.Seal);
            tape.Dispose();
            Assert.Throws<ArgumentOutOfRangeException>(() => new ReplayTape(23));
            Assert.Throws<ArgumentOutOfRangeException>(() => new ReplayTape((long)int.MaxValue + 1));
        }

        [Test]
        public void LoadRejectsCallStepDifferentFromLastBarrierWithValidChecksum()
        {
            var inner = new ScriptTransport { SendMode = SendMode.Accept };
            using var tape = new ReplayTape(1024);
            using (var trace = new TraceTransport(inner, tape))
            {
                trace.BeginStep(5);
                var packet = Lease(1);
                trace.TrySend(Channel.ReliableOrdered, ref packet);
            }
            using var output = new MemoryStream();
            tape.Save(output);
            var bytes = output.ToArray();
            Write64(bytes, 72, 6);
            Write64(bytes, 24, XxHash64(bytes.AsSpan(40)));
            Assert.Throws<InvalidDataException>(() => ReplayTape.Load(new MemoryStream(bytes), 1024));
        }

        [Test]
        public void ReplayIndependentlyRejectsInMemoryStepCorruptionWithoutConsumingSend()
        {
            var inner = new ScriptTransport { SendMode = SendMode.Accept };
            using var tape = new ReplayTape(1024);
            using (var trace = new TraceTransport(inner, tape))
            {
                trace.BeginStep(5);
                var packet = Lease(1);
                trace.TrySend(Channel.ReliableOrdered, ref packet);
            }
            SetRecordStep(tape, 1, 6);
            var replay = new ReplayTransport(tape);
            replay.BeginStep(5);
            var send = Lease(1);
            Assert.Throws<InvalidOperationException>(() => replay.TrySend(Channel.ReliableOrdered, ref send));
            Assert.That(send.IsValid, Is.True);
            send.Dispose();
            Assert.DoesNotThrow(replay.Dispose);
        }

        [Test]
        public void RecordedFaultedStateWithRemainingCallsStillReportsEarlyDisposeTruncation()
        {
            var inner = new ScriptTransport { SendMode = SendMode.Reject, FaultAfterSend = true };
            using var tape = new ReplayTape(1024);
            using (var trace = new TraceTransport(inner, tape))
            {
                var first = Lease(1); trace.TrySend(Channel.ReliableOrdered, ref first); first.Dispose();
                var second = Lease(2); trace.TrySend(Channel.ReliableOrdered, ref second); second.Dispose();
            }
            var replay = new ReplayTransport(tape);
            var packet = Lease(1);
            Assert.That(replay.TrySend(Channel.ReliableOrdered, ref packet), Is.False);
            Assert.That(replay.State, Is.EqualTo(TransportState.Faulted));
            packet.Dispose();
            Assert.Throws<InvalidOperationException>(replay.Dispose);
            Assert.DoesNotThrow(replay.Dispose);
        }

        [Test]
        public void FalseReceiveAndBothChannelMismatchesAreExact()
        {
            var inner = new ScriptTransport();
            using var tape = new ReplayTape(1024);
            using (var trace = new TraceTransport(inner, tape))
                Assert.That(trace.TryReceive(out _, out _), Is.False);
            using (var replay = new ReplayTransport(tape))
                Assert.That(replay.TryReceive(out _, out _), Is.False);

            using var sendTape = RecordSingleSend(new byte[] { 3 }, true);
            var wrongChannel = new ReplayTransport(sendTape);
            var send = Lease(3);
            Assert.Throws<InvalidOperationException>(() => wrongChannel.TrySend(Channel.UnreliableSequenced, ref send));
            Assert.That(send.IsValid, Is.True);
            send.Dispose();
            wrongChannel.Dispose();
        }

        [Test]
        public void CallAfterTranscriptEndFaultsAndDoesNotConsumeSend()
        {
            using var tape = RecordSingleSend(new byte[] { 1 }, true);
            var replay = new ReplayTransport(tape);
            var first = Lease(1); replay.TrySend(Channel.ReliableOrdered, ref first);
            var extra = Lease(2);
            Assert.Throws<InvalidOperationException>(() => replay.TrySend(Channel.ReliableOrdered, ref extra));
            Assert.That(extra.IsValid, Is.True);
            extra.Dispose();
            replay.Dispose();
        }

        [Test]
        public void TracePreservesBeginReceiveAndDisposeExceptionsAndAlwaysReleasesClaim()
        {
            var beginInner = new ScriptTransport { BeginThrow = true };
            using (var beginTape = new ReplayTape(1024))
            {
                var trace = new TraceTransport(beginInner, beginTape);
                Assert.Throws<InvalidOperationException>(() => trace.BeginStep(1));
                trace.Dispose();
                Assert.That(beginTape.IsComplete, Is.False);
            }

            var receiveInner = new ScriptTransport { ReceiveThrow = true, ReceiveLeaseThenThrow = true };
            using (var receiveTape = new ReplayTape(1024))
            {
                var trace = new TraceTransport(receiveInner, receiveTape);
                Assert.Throws<InvalidOperationException>(() => trace.TryReceive(out _, out _));
                Assert.That(receiveInner.LastReceiveAlias.IsValid, Is.False);
                trace.Dispose();
                Assert.That(receiveTape.IsComplete, Is.False);
            }

            var disposeInner = new ScriptTransport { DisposeThrow = true };
            using var disposeTape = new ReplayTape(1024);
            var disposeTrace = new TraceTransport(disposeInner, disposeTape);
            Assert.Throws<InvalidOperationException>(disposeTrace.Dispose);
            Assert.That(disposeTape.IsSealed, Is.True);
            Assert.That(disposeTape.IsComplete, Is.False);
        }

        [Test]
        public void SaveUsesLifecyclePrecedenceBeforeValidatingOutput()
        {
            var open = new ReplayTape(1024);
            Assert.Throws<InvalidOperationException>(() => open.Save(null));
            open.Dispose();
            Assert.Throws<ObjectDisposedException>(() => open.Save(null));

            var recording = new ReplayTape(1024);
            var trace = new TraceTransport(new ScriptTransport(), recording);
            Assert.Throws<InvalidOperationException>(() => recording.Save(null));
            Assert.Throws<InvalidOperationException>(() => recording.Save(new ReadOnlyStream()));
            recording.Dispose();
            Assert.Throws<ObjectDisposedException>(() => recording.Save(null));
            trace.Dispose();

            using var sealedTape = RecordSingleSend(new byte[] { 1 }, true);
            Assert.Throws<ArgumentNullException>(() => sealedTape.Save(null));
            Assert.Throws<ArgumentException>(() => sealedTape.Save(new ReadOnlyStream()));
            var replay = new ReplayTransport(sealedTape);
            Assert.Throws<InvalidOperationException>(() => sealedTape.Save(null));
            Assert.Throws<InvalidOperationException>(() => sealedTape.Save(new ReadOnlyStream()));
            sealedTape.Dispose();
            Assert.Throws<ObjectDisposedException>(() => sealedTape.Save(null));
            Assert.Throws<InvalidOperationException>(replay.Dispose);
        }

        [Test]
        public void VersionOneGoldenCoversFullHeaderRecordsAndPayload()
        {
            var inner = new ScriptTransport { SendMode = SendMode.Accept };
            using var tape = new ReplayTape(1024);
            using (var trace = new TraceTransport(inner, tape))
            {
                trace.BeginStep(0x0102030405060708UL);
                var packet = Lease(0xAA, 0xBB);
                Assert.That(trace.TrySend(Channel.ReliableOrdered, ref packet), Is.True);
                Assert.That(trace.TryReceive(out _, out _), Is.False);
            }
            using var output = new MemoryStream();
            tape.Save(output);
            CollectionAssert.AreEqual(GoldenBytes(), output.ToArray());
        }

        [Test]
        public void LoadSystematicallyRejectsMalformedHeaderAndRecordFields()
        {
            var golden = GoldenBytes();
            var recordCases = new[]
            {
                (40, (byte)0), (41, (byte)0), (42, (byte)0), (43, (byte)4),
                (44, (byte)5), (45, (byte)1), (60, (byte)1), (66, (byte)2),
                (80, (byte)3), (91, (byte)1), (92, (byte)0)
            };
            foreach (var test in recordCases)
            {
                var bytes = (byte[])golden.Clone();
                bytes[test.Item1] = test.Item2;
                Write64(bytes, 24, XxHash64(bytes.AsSpan(40)));
                Assert.Throws<InvalidDataException>(() => ReplayTape.Load(new MemoryStream(bytes), 1024),
                    $"offset {test.Item1}");
            }

            var count = (byte[])golden.Clone(); Write32(count, 12, 4);
            Assert.Throws<InvalidDataException>(() => ReplayTape.Load(new MemoryStream(count), 1024), "count");
            var sectionLength = (byte[])golden.Clone(); Write64(sectionLength, 16, 23);
            Assert.Throws<InvalidDataException>(() => ReplayTape.Load(new MemoryStream(sectionLength), 1024), "section length");
            var checksum = (byte[])golden.Clone(); checksum[24] ^= 1;
            Assert.Throws<InvalidDataException>(() => ReplayTape.Load(new MemoryStream(checksum), 1024), "checksum");
            var reserved = (byte[])golden.Clone(); reserved[36] = 1;
            Assert.Throws<InvalidDataException>(() => ReplayTape.Load(new MemoryStream(reserved), 1024), "header reserved");
        }

        private static ReplayTape RecordSingleSend(byte[] bytes, bool accepted)
        {
            var tape = new ReplayTape(1024);
            var inner = new ScriptTransport { SendMode = accepted ? SendMode.Accept : SendMode.Reject };
            using (var trace = new TraceTransport(inner, tape))
            {
                var packet = Lease(bytes);
                Assert.That(trace.TrySend(Channel.ReliableOrdered, ref packet), Is.EqualTo(accepted));
                if (packet.IsValid) packet.Dispose();
            }
            return tape;
        }

        private static PacketLease Lease(params byte[] bytes)
        {
            var lease = PacketLease.Rent(bytes.Length);
            bytes.AsSpan().CopyTo(lease.CapacitySpan);
            lease.SetLength(bytes.Length);
            return lease;
        }

        private static uint Read32(byte[] bytes, int offset) =>
            (uint)(bytes[offset] | bytes[offset + 1] << 8 | bytes[offset + 2] << 16 | bytes[offset + 3] << 24);

        private static void Write32(byte[] bytes, int offset, uint value)
        {
            for (var i = 0; i < 4; i++) bytes[offset + i] = (byte)(value >> (i * 8));
        }

        private static byte[] GoldenBytes() => new byte[]
        {
            0x53, 0x45, 0x43, 0x53, 0x4E, 0x45, 0x54, 0x31, 0x01, 0x00, 0x28, 0x00,
            0x03, 0x00, 0x00, 0x00, 0x4A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0xCE, 0x4F, 0x96, 0x0A, 0x7F, 0x6A, 0x57, 0xB1, 0x01, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x01, 0x01, 0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x07, 0x06, 0x05,
            0x04, 0x03, 0x02, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x02, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x07, 0x06, 0x05,
            0x04, 0x03, 0x02, 0x01, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0xAA, 0xBB,
            0x03, 0x00, 0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x07, 0x06, 0x05,
            0x04, 0x03, 0x02, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
        };

        private static void SetRecordStep(ReplayTape tape, int index, ulong step)
        {
            var field = typeof(ReplayTape).GetField("_records", BindingFlags.Instance | BindingFlags.NonPublic);
            var records = (IList)field.GetValue(tape);
            var record = records[index];
            var stepField = record.GetType().GetField("<Step>k__BackingField", BindingFlags.Instance | BindingFlags.NonPublic);
            stepField.SetValue(record, step);
        }

        private static void Write64(byte[] bytes, int offset, ulong value)
        {
            for (var i = 0; i < 8; i++) bytes[offset + i] = (byte)(value >> (i * 8));
        }

        private static ulong XxHash64(ReadOnlySpan<byte> data)
        {
            const ulong p1 = 11400714785074694791UL, p2 = 14029467366897019727UL;
            const ulong p3 = 1609587929392839161UL, p4 = 9650029242287828579UL, p5 = 2870177450012600261UL;
            var index = 0; ulong hash;
            ulong Round(ulong value, ulong input) { value += input * p2; value = value << 31 | value >> 33; return value * p1; }
            if (data.Length >= 32)
            {
                var a = unchecked(p1 + p2); var b = p2; var c = 0UL; var d = unchecked(0UL - p1);
                var limit = data.Length - 32;
                do { a = Round(a, Read64(data, index)); index += 8; b = Round(b, Read64(data, index)); index += 8; c = Round(c, Read64(data, index)); index += 8; d = Round(d, Read64(data, index)); index += 8; } while (index <= limit);
                hash = (a << 1 | a >> 63) + (b << 7 | b >> 57) + (c << 12 | c >> 52) + (d << 18 | d >> 46);
                hash ^= Round(0, a); hash = hash * p1 + p4; hash ^= Round(0, b); hash = hash * p1 + p4;
                hash ^= Round(0, c); hash = hash * p1 + p4; hash ^= Round(0, d); hash = hash * p1 + p4;
            }
            else hash = p5;
            hash += (ulong)data.Length;
            while (index <= data.Length - 8) { var value = Round(0, Read64(data, index)); hash ^= value; hash = (hash << 27 | hash >> 37) * p1 + p4; index += 8; }
            if (index <= data.Length - 4) { hash ^= Read32(data, index) * p1; hash = (hash << 23 | hash >> 41) * p2 + p3; index += 4; }
            while (index < data.Length) { hash ^= data[index] * p5; hash = (hash << 11 | hash >> 53) * p1; index++; }
            hash ^= hash >> 33; hash *= p2; hash ^= hash >> 29; hash *= p3; hash ^= hash >> 32;
            return hash;
        }

        private static uint Read32(ReadOnlySpan<byte> bytes, int offset) =>
            (uint)(bytes[offset] | bytes[offset + 1] << 8 | bytes[offset + 2] << 16 | bytes[offset + 3] << 24);

        private static ulong Read64(ReadOnlySpan<byte> bytes, int offset) =>
            Read32(bytes, offset) | (ulong)Read32(bytes, offset + 4) << 32;

        public enum SendMode { Accept, Reject, Throw, TransferThenThrow }

        private sealed class ScriptTransport : ITransport, ISteppedTransport
        {
            public TransportState State { get; set; } = TransportState.Connected;
            public TransportError Error { get; set; } = TransportError.None;
            public SendMode SendMode { get; set; } = SendMode.Reject;
            public byte[] Inbound { get; set; }
            public bool BeginThrow { get; set; }
            public bool ReceiveThrow { get; set; }
            public bool ReceiveLeaseThenThrow { get; set; }
            public bool DisposeThrow { get; set; }
            public bool FaultAfterSend { get; set; }
            public PacketLease LastReceiveAlias { get; private set; }
            public int DisposeCount { get; private set; }
            public void BeginStep(ulong stepIndex) { if (BeginThrow) throw new InvalidOperationException("begin"); }
            public bool TrySend(Channel channel, ref PacketLease packet)
            {
                if (SendMode == SendMode.Throw) throw new InvalidOperationException("send");
                if (SendMode == SendMode.TransferThenThrow)
                {
                    var owned = PacketLease.Transfer(ref packet); owned.Dispose();
                    throw new InvalidOperationException("send");
                }
                if (SendMode == SendMode.Reject)
                {
                    if (FaultAfterSend) { State = TransportState.Faulted; Error = TransportError.InvalidPacket; }
                    return false;
                }
                var accepted = PacketLease.Transfer(ref packet); accepted.Dispose();
                return true;
            }
            public bool TryReceive(out Channel channel, out PacketLease packet)
            {
                if (ReceiveThrow)
                {
                    channel = default;
                    packet = ReceiveLeaseThenThrow ? Lease(9) : default;
                    LastReceiveAlias = packet;
                    throw new InvalidOperationException("receive");
                }
                if (Inbound == null) { channel = default; packet = default; return false; }
                channel = Channel.UnreliableSequenced;
                packet = Lease(Inbound);
                Inbound = null;
                return true;
            }
            public void Dispose() { if (State == TransportState.Disposed) return; DisposeCount++; State = TransportState.Disposed; Error = TransportError.Disposed; if (DisposeThrow) throw new InvalidOperationException("dispose"); }
        }

        private sealed class ReadOnlyStream : MemoryStream
        {
            public override bool CanWrite => false;
        }
    }
}
