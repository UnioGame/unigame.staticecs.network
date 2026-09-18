namespace UniGame.StaticEcs.Network.Tests
{
    using System;
    using NUnit.Framework;

    /// <summary>
    /// Verifies that <see cref="NetworkPacket.TryDecode(NetworkBufferLease, out PacketHeader, out ReadOnlyMemory{byte})"/>
    /// caches its framing and payload-hash verification per lease instance instead of repeating
    /// it (NCORE-17b: the same received packet is decoded once by a transport's own receive-time
    /// check and again by NetworkServer/NetworkClient processing), while never reusing a stale
    /// result across a lease that has been reinitialized with different bytes.
    /// </summary>
    public sealed class NetworkPacketDecodeCacheTests
    {
        [Test]
        public void TryDecodeReusesCachedResultForRepeatedCallsOnSameLease()
        {
            using var pool = new NetworkBufferPool(1024);
            var header = ValidHeader();
            var body = new byte[] { 1, 2, 3, 4, 5 };
            Assert.That(NetworkPacket.TryEncode(pool, header, body, out var packet), Is.True);
            try
            {
                Assert.That(NetworkPacket.TryDecode(packet, out var first, out var firstPayload),
                    Is.True);
                Assert.That(NetworkPacket.TryDecode(packet, out var second, out var secondPayload),
                    Is.True);

                Assert.That(second.Kind, Is.EqualTo(first.Kind));
                Assert.That(second.PacketSequence, Is.EqualTo(first.PacketSequence));
                Assert.That(second.PayloadHash, Is.EqualTo(first.PayloadHash));
                Assert.That(secondPayload.ToArray(), Is.EqualTo(firstPayload.ToArray()));
                Assert.That(secondPayload.ToArray(), Is.EqualTo(body));
            }
            finally
            {
                packet.Dispose();
            }
        }

        [Test]
        public void TryDecodeCachesNegativeResultForCorruptedPayloadWithoutThrowing()
        {
            using var pool = new NetworkBufferPool(1024);
            var header = ValidHeader();
            var body = new byte[] { 9, 9, 9, 9 };
            // Frame a correct header for the original body, then overwrite the payload bytes so
            // the payload no longer matches the header's xxHash64 -- simulating bit corruption on
            // the wire, which must fail on every decode attempt, not just the first.
            Assert.That(NetworkPacket.TryEncode(pool, header, body, out var packet), Is.True);
            try
            {
                var corrupted = new byte[] { 1, 2, 3, 4 };
                corrupted.CopyTo(WritablePayload(packet, body.Length));

                Assert.That(NetworkPacket.TryDecode(packet, out _, out _), Is.False);
                Assert.That(NetworkPacket.TryDecode(packet, out _, out _), Is.False,
                    "a cached negative decode result must stay negative on a later call");
            }
            finally
            {
                packet.Dispose();
            }
        }

        [Test]
        public void TryDecodeDoesNotLeakCachedResultAcrossReusedLease()
        {
            using var pool = new NetworkBufferPool(1024);
            var firstHeader = ValidHeader();
            firstHeader.PacketSequence = 11;
            var firstBody = new byte[] { 1, 2, 3 };
            Assert.That(NetworkPacket.TryEncode(pool, firstHeader, firstBody,
                out var firstPacket), Is.True);
            Assert.That(NetworkPacket.TryDecode(firstPacket, out var decodedFirst, out _),
                Is.True);
            Assert.That(decodedFirst.PacketSequence, Is.EqualTo(11u));
            firstPacket.Dispose();

            var secondHeader = ValidHeader();
            secondHeader.PacketSequence = 22;
            var secondBody = new byte[] { 4, 5, 6 };
            Assert.That(NetworkPacket.TryEncode(pool, secondHeader, secondBody,
                out var secondPacket), Is.True);
            try
            {
                Assert.That(NetworkPacket.TryDecode(secondPacket, out var decodedSecond,
                    out var secondPayload), Is.True);
                Assert.That(decodedSecond.PacketSequence, Is.EqualTo(22u),
                    "a reused lease object must decode its new bytes, never the prior packet's cached result");
                Assert.That(secondPayload.ToArray(), Is.EqualTo(secondBody));
            }
            finally
            {
                secondPacket.Dispose();
            }
        }

        [Test]
        public void RetainSliceDoesNotInheritParentsDecodeCache()
        {
            using var pool = new NetworkBufferPool(1024);
            var header = ValidHeader();
            var body = new byte[] { 1, 2, 3, 4 };
            Assert.That(NetworkPacket.TryEncode(pool, header, body, out var packet), Is.True);
            try
            {
                Assert.That(NetworkPacket.TryDecode(packet, out _, out _), Is.True);

                // A command-payload slice carved from a decoded packet is a distinct lease over a
                // sub-range that is not itself a complete framed packet; it must be judged on its
                // own bytes, never short-circuited by the parent's cached "this whole buffer
                // decodes successfully" result.
                var slice = packet.RetainSlice(PacketHeader.Size, body.Length);
                try
                {
                    Assert.That(NetworkPacket.TryDecode(slice, out _, out _),
                        Is.False);
                }
                finally
                {
                    slice.Dispose();
                }
            }
            finally
            {
                packet.Dispose();
            }
        }

        private static PacketHeader ValidHeader() => new PacketHeader
        {
            Kind = PacketKind.Ping,
            Flags = PacketFlags.ReliableOrdered,
            PacketSequence = 1,
        };

        private static Span<byte> WritablePayload(NetworkBufferLease packet, int length) =>
            packet.WritableSpan.Slice(PacketHeader.Size, length);
    }
}
