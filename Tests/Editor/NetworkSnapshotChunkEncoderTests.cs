namespace UniGame.StaticEcs.Network.Tests
{
    using System;
    using NUnit.Framework;

    /// <summary>Verifies one-pass snapshot chunk encoding matches canonical packet framing without leaking leases.</summary>
    public sealed class NetworkSnapshotChunkEncoderTests
    {
        [TestCase(SnapshotPayloadKind.Keyframe, 0)]
        [TestCase(SnapshotPayloadKind.Keyframe, 1)]
        [TestCase(SnapshotPayloadKind.Keyframe, SnapshotChunkHeader.Size - 1)]
        [TestCase(SnapshotPayloadKind.Keyframe, SnapshotChunkHeader.Size)]
        [TestCase(SnapshotPayloadKind.Keyframe, SnapshotChunkHeader.Size + 1)]
        [TestCase(SnapshotPayloadKind.Keyframe, 255)]
        [TestCase(SnapshotPayloadKind.Keyframe, 256)]
        [TestCase(SnapshotPayloadKind.Keyframe, 257)]
        [TestCase(SnapshotPayloadKind.Delta, 0)]
        [TestCase(SnapshotPayloadKind.Delta, 1)]
        [TestCase(SnapshotPayloadKind.Delta, SnapshotChunkHeader.Size - 1)]
        [TestCase(SnapshotPayloadKind.Delta, SnapshotChunkHeader.Size)]
        [TestCase(SnapshotPayloadKind.Delta, SnapshotChunkHeader.Size + 1)]
        [TestCase(SnapshotPayloadKind.Delta, 255)]
        [TestCase(SnapshotPayloadKind.Delta, 256)]
        [TestCase(SnapshotPayloadKind.Delta, 257)]
        public void EncoderMatchesCanonicalPacketFraming(
            SnapshotPayloadKind kind, int bodyLength)
        {
            using var pool = new NetworkBufferPool(1L << 20);
            var chunk = Chunk(kind);
            var body = Body(bodyLength);
            var canonical = CanonicalPayload(in chunk, body);

            Assert.That(NetworkPacket.TryEncode(pool, Header(), canonical,
                out var expected), Is.True);
            Assert.That(SnapshotChunkEncoder.TryEncode(pool, Header(), in chunk,
                body, out var actual), Is.True);
            try
            {
                Assert.That(actual.Length, Is.EqualTo(expected.Length));
                Assert.That(actual.Span.SequenceEqual(expected.Span), Is.True);
                Assert.That(NetworkPacket.TryDecode(actual, out _,
                    out var payload), Is.True);
                Assert.That(SnapshotChunkHeader.TryRead(payload.Span,
                    out var decoded), Is.True);
                Assert.That(decoded.ChunkIndex, Is.EqualTo(chunk.ChunkIndex));
                Assert.That(decoded.ChunkCount, Is.EqualTo(chunk.ChunkCount));
                Assert.That(decoded.TotalHash, Is.EqualTo(chunk.TotalHash));
                Assert.That(payload.Slice(SnapshotChunkHeader.Size).Span
                    .SequenceEqual(body), Is.True);
            }
            finally
            {
                actual.Dispose();
                expected.Dispose();
            }

            AssertReleased(pool);
        }

        [Test]
        public void EncoderMatchesCanonicalFramingAtMaximumWirePayload()
        {
            using var pool = new NetworkBufferPool(1L << 20);
            var chunk = Chunk(SnapshotPayloadKind.Keyframe);
            var body = Body(ProtocolLimits.MaxWirePayloadBytes -
                SnapshotChunkHeader.Size);
            var canonical = CanonicalPayload(in chunk, body);

            Assert.That(NetworkPacket.TryEncode(pool, Header(), canonical,
                out var expected), Is.True);
            Assert.That(SnapshotChunkEncoder.TryEncode(pool, Header(), in chunk,
                body, out var actual), Is.True);
            try
            {
                Assert.That(actual.Length, Is.EqualTo(expected.Length));
                Assert.That(actual.Span.SequenceEqual(expected.Span), Is.True);
            }
            finally
            {
                actual.Dispose();
                expected.Dispose();
            }

            var oversized = Body(body.Length + 1);
            Assert.That(SnapshotChunkEncoder.TryEncode(pool, Header(), in chunk,
                oversized, out var rejected), Is.False);
            Assert.That(rejected, Is.Null);
            AssertReleased(pool);
        }

        [Test]
        public void EncoderRejectsInvalidChunkHeaderWithoutLeasing()
        {
            using var pool = new NetworkBufferPool(1024);
            var chunk = Chunk(SnapshotPayloadKind.Delta);
            chunk.BaselineTick = 0;

            Assert.That(SnapshotChunkEncoder.TryEncode(pool, Header(), in chunk,
                Body(8), out var packet), Is.False);
            Assert.That(packet, Is.Null);
            AssertReleased(pool);
        }

        [Test]
        public void EncoderReleasesBufferWhenPacketHeaderValidationFails()
        {
            using var pool = new NetworkBufferPool(1024);
            var header = Header();
            header.Kind = (PacketKind)byte.MaxValue;
            var chunk = Chunk(SnapshotPayloadKind.Keyframe);

            Assert.That(SnapshotChunkEncoder.TryEncode(pool, header, in chunk,
                Body(8), out var packet), Is.False);
            Assert.That(packet, Is.Null);
            AssertReleased(pool);
            Assert.That(pool.CaptureDiagnostics().RetainedBytes,
                Is.GreaterThan(0), "failed encoding must return its rent to the pool");
        }

        [Test]
        public void EncoderRejectsNullPoolWithoutLeasing()
        {
            var chunk = Chunk(SnapshotPayloadKind.Keyframe);

            Assert.That(SnapshotChunkEncoder.TryEncode(null, Header(), in chunk,
                Body(8), out var packet), Is.False);
            Assert.That(packet, Is.Null);
        }

        // NCORE-17a: NetworkServer frames a chunk's header+body once per unique
        // baseline and wraps it with each peer's own packet header afterward.
        // These cases prove that split matches the one-shot TryEncode byte for
        // byte, and that a cached payload can be safely reused under a second,
        // different packet header (the actual peer-to-peer sharing scenario).
        [TestCase(SnapshotPayloadKind.Keyframe, 0)]
        [TestCase(SnapshotPayloadKind.Keyframe, SnapshotChunkHeader.Size - 1)]
        [TestCase(SnapshotPayloadKind.Keyframe, 256)]
        [TestCase(SnapshotPayloadKind.Delta, 0)]
        [TestCase(SnapshotPayloadKind.Delta, SnapshotChunkHeader.Size - 1)]
        [TestCase(SnapshotPayloadKind.Delta, 256)]
        public void SplitEncodingMatchesOnePassEncoding(
            SnapshotPayloadKind kind, int bodyLength)
        {
            using var pool = new NetworkBufferPool(1L << 20);
            var chunk = Chunk(kind);
            var body = Body(bodyLength);

            Assert.That(SnapshotChunkEncoder.TryEncode(pool, Header(), in chunk,
                body, out var expected), Is.True);
            Assert.That(SnapshotChunkEncoder.TryEncodePayload(pool, in chunk,
                body, out var payload, out var payloadHash), Is.True);
            Assert.That(SnapshotChunkEncoder.TryEncodeFromPayload(pool,
                Header(), payload.Memory, payloadHash, out var actual), Is.True);
            try
            {
                Assert.That(actual.Length, Is.EqualTo(expected.Length));
                Assert.That(actual.Span.SequenceEqual(expected.Span), Is.True);
            }
            finally
            {
                actual.Dispose();
                expected.Dispose();
                payload.Dispose();
            }

            AssertReleased(pool);
        }

        [Test]
        public void CachedPayloadProducesByteIdenticalFramingUnderTwoDifferentPeerHeaders()
        {
            using var pool = new NetworkBufferPool(1L << 20);
            var chunk = Chunk(SnapshotPayloadKind.Delta);
            var body = Body(512);

            Assert.That(SnapshotChunkEncoder.TryEncodePayload(pool, in chunk,
                body, out var payload, out var payloadHash), Is.True);

            var headerA = Header();
            headerA.SessionEpoch = 3;
            headerA.PacketSequence = 5;
            var headerB = Header();
            headerB.SessionEpoch = 9;
            headerB.PacketSequence = 41;

            Assert.That(SnapshotChunkEncoder.TryEncodeFromPayload(pool, headerA,
                payload.Memory, payloadHash, out var packetA), Is.True);
            Assert.That(SnapshotChunkEncoder.TryEncodeFromPayload(pool, headerB,
                payload.Memory, payloadHash, out var packetB), Is.True);
            try
            {
                Assert.That(NetworkPacket.TryDecode(packetA, out var decodedA,
                    out var payloadA), Is.True);
                Assert.That(NetworkPacket.TryDecode(packetB, out var decodedB,
                    out var payloadB), Is.True);
                // The shared chunk bytes (header + body) and their hash must be
                // byte-identical across both peers; only the fields that vary
                // per peer (session epoch and sequence here) must differ.
                Assert.That(payloadA.Span.SequenceEqual(payloadB.Span), Is.True);
                Assert.That(decodedA.PayloadHash, Is.EqualTo(decodedB.PayloadHash));
                Assert.That(decodedA.SessionEpoch,
                    Is.Not.EqualTo(decodedB.SessionEpoch));
                Assert.That(decodedA.PacketSequence,
                    Is.Not.EqualTo(decodedB.PacketSequence));
            }
            finally
            {
                packetA.Dispose();
                packetB.Dispose();
                payload.Dispose();
            }

            AssertReleased(pool);
        }

        [Test]
        public void EncodePayloadRejectsOversizedBodyWithoutLeasing()
        {
            using var pool = new NetworkBufferPool(1024);
            var chunk = Chunk(SnapshotPayloadKind.Keyframe);
            var oversized = Body(ProtocolLimits.MaxWirePayloadBytes -
                SnapshotChunkHeader.Size + 1);

            Assert.That(SnapshotChunkEncoder.TryEncodePayload(pool, in chunk,
                oversized, out var payload, out _), Is.False);
            Assert.That(payload, Is.Null);
            AssertReleased(pool);
        }

        [Test]
        public void EncodeFromPayloadRejectsNullPoolWithoutLeasing()
        {
            var chunk = Chunk(SnapshotPayloadKind.Keyframe);
            using var pool = new NetworkBufferPool(1024);
            Assert.That(SnapshotChunkEncoder.TryEncodePayload(pool, in chunk,
                Body(8), out var payload, out var payloadHash), Is.True);
            try
            {
                Assert.That(SnapshotChunkEncoder.TryEncodeFromPayload(null,
                    Header(), payload.Memory, payloadHash, out var packet),
                    Is.False);
                Assert.That(packet, Is.Null);
            }
            finally
            {
                payload.Dispose();
            }
        }

        private static PacketHeader Header() => new PacketHeader
        {
            Kind = PacketKind.SnapshotChunk,
            Flags = PacketFlags.ReliableOrdered,
            Compression = NetworkCompression.None,
            SessionEpoch = 3,
            PacketSequence = 5,
            ServerTick = 42,
            AcknowledgedSnapshotTick = PacketHeader.NoneTick,
            ServerProcessedCommandTick = 41,
            ServerProcessedCommandSequence = 7,
            SimulationFingerprint = 0x0123456789abcdefUL,
            ContentFingerprint = 0xfedcba9876543210UL
        };

        private static SnapshotChunkHeader Chunk(SnapshotPayloadKind kind) =>
            new SnapshotChunkHeader
            {
                PayloadKind = kind,
                SnapshotTick = 42,
                BaselineTick = kind == SnapshotPayloadKind.Keyframe ? 0u : 41u,
                TotalLength = 1024,
                TotalHash = 0x1020304050607080UL,
                ChunkIndex = 0,
                ChunkCount = 1,
                ResyncCorrelationId = kind == SnapshotPayloadKind.Keyframe ? 9u : 0u
            };

        private static byte[] Body(int length)
        {
            var body = new byte[length];
            for (var i = 0; i < body.Length; i++)
                body[i] = (byte)(i * 31 + 7);
            return body;
        }

        private static byte[] CanonicalPayload(in SnapshotChunkHeader chunk,
            ReadOnlySpan<byte> body)
        {
            var payload = new byte[SnapshotChunkHeader.Size + body.Length];
            Assert.That(chunk.TryWrite(payload), Is.True);
            body.CopyTo(payload.AsSpan(SnapshotChunkHeader.Size));
            return payload;
        }

        private static void AssertReleased(NetworkBufferPool pool)
        {
            var diagnostics = pool.CaptureDiagnostics();
            Assert.That(diagnostics.OutstandingLeases, Is.Zero);
            Assert.That(diagnostics.OutstandingBytes, Is.Zero);
        }
    }
}
