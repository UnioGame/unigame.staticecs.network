using System;
using NUnit.Framework;

namespace UniGame.StaticEcs.Network.Tests
{
    public sealed class ProtocolTests
    {
        [Test]
        public void HeaderUsesGoldenOffsetsAndRoundTrips()
        {
            var header = Header(PacketKind.Hello, PacketFlags.ReliableOrdered, 20);
            var bytes = new byte[PacketHeader.Size];
            Assert.That(header.TryWrite(bytes), Is.True);
            Assert.That(BitConverter.ToString(bytes, 0, 12).Replace("-", string.Empty), Is.EqualTo("534543530100480001010000"));
            Assert.That(BitConverter.ToUInt32(bytes, 12), Is.EqualTo(7));
            Assert.That(BitConverter.ToUInt32(bytes, 24), Is.EqualTo(PacketHeader.NoneTick));
            Assert.That(PacketHeader.TryRead(bytes, out var decoded), Is.True);
            Assert.That(decoded.PayloadHash, Is.EqualTo(header.PayloadHash));
        }

        [Test]
        public void HeaderRejectsTruncationReservedFieldsAndWrongCrc()
        {
            var bytes = new byte[PacketHeader.Size]; Header(PacketKind.Ack, PacketFlags.ReliableOrdered, 0).TryWrite(bytes);
            Assert.That(PacketHeader.TryRead(bytes.AsSpan(0, 71), out _), Is.False);
            bytes[11] = 1; Assert.That(PacketHeader.TryRead(bytes, out _), Is.False);
            bytes[11] = 0; bytes[20] ^= 1; Assert.That(PacketHeader.TryRead(bytes, out _), Is.False);
        }

        [Test]
        public void EveryPayloadKindRoundTripsAndRejectsTrailingBytes()
        {
            var bytes = new byte[4096];
            Assert.That(PayloadCodec.TryWrite(new HelloPayload { Nonce = 9, MinTickRate = 20, MaxTickRate = 60, MaxWireBytes = 1024, MaxDecodedBytes = 2048, Capabilities = 3 }, bytes, out var length), Is.True);
            Assert.That(PayloadCodec.TryReadHello(bytes.AsSpan(0, length), out _), Is.True); Assert.That(PayloadCodec.TryReadHello(bytes.AsSpan(0, length + 1), out _), Is.False);
            var ack = new HelloAckPayload { Result = ConnectResult.Accepted, TickRate = 30, PeerId = 2, ServerNonce = 8, Chunks = new[] { new ChunkMapping { Chunk = 4, Cluster = 3, Role = 1 } } };
            Assert.That(PayloadCodec.TryWrite(ack, bytes, out length), Is.True); Assert.That(PayloadCodec.TryReadHelloAck(bytes.AsSpan(0, length), out _), Is.True);
            var command = new CommandBatchPayload { Commands = new[] { new CommandRecord { TypeId = Id(1), Version = 2, Sequence = 1, ClientTick = 4, Payload = new byte[] { 7 } } } };
            Assert.That(PayloadCodec.TryWrite(command, bytes, out length), Is.True); Assert.That(PayloadCodec.TryReadCommandBatch(bytes.AsSpan(0, length), out _), Is.True);
            var snapshot = new FullSnapshotPayload { Entities = new[] { new SnapshotEntity { Entity = new WireEntityId(2, 1, 3), KindId = Id(2), Records = new[] { new SnapshotRecord { TypeId = Id(3), Kind = RecordKind.Tag, Payload = Array.Empty<byte>() } } } } };
            Assert.That(PayloadCodec.TryWrite(snapshot, bytes, out length), Is.True); Assert.That(PayloadCodec.TryReadFullSnapshot(bytes.AsSpan(0, length), out _), Is.True);
            Assert.That(PayloadCodec.TryWriteAck(bytes, out length), Is.True); Assert.That(PayloadCodec.TryReadAck(bytes.AsSpan(0, length)), Is.True);
            Assert.That(PayloadCodec.TryWrite(new ResyncRequestPayload { Reason = ResyncReason.HashMismatch, LastAcceptedTick = 4 }, bytes, out length), Is.True); Assert.That(PayloadCodec.TryReadResyncRequest(bytes.AsSpan(0, length), out _), Is.True);
            Assert.That(PayloadCodec.TryWrite(new DisconnectPayload { Reason = DisconnectReason.ServerShutdown }, bytes, out length), Is.True); Assert.That(PayloadCodec.TryReadDisconnect(bytes.AsSpan(0, length), out _), Is.True);
        }

        [Test]
        public void EveryPayloadKindMatchesFrozenGoldenBytes()
        {
            var bytes = new byte[4096];
            PayloadCodec.TryWrite(new HelloPayload { Nonce = 9, MinTickRate = 20, MaxTickRate = 60, MaxWireBytes = 1024, MaxDecodedBytes = 2048, Capabilities = 3 }, bytes, out var length);
            AssertHex(bytes, length, "090000000000000014003C00000400000008000003000000");
            Assert.That(PayloadCodec.TryReadHello(bytes.AsSpan(0, length - 1), out _), Is.False);
            PayloadCodec.TryWrite(new HelloAckPayload { Result = ConnectResult.Accepted, TickRate = 30, PeerId = 2, ServerNonce = 8, Chunks = new[] { new ChunkMapping { Chunk = 4, Cluster = 3, Role = 1 } } }, bytes, out length);
            AssertHex(bytes, length, "00001E00020000000800000000000000010000000400000003000100");
            Assert.That(PayloadCodec.TryReadHelloAck(bytes.AsSpan(0, length - 1), out _), Is.False);
            PayloadCodec.TryWrite(new CommandBatchPayload { Commands = new[] { new CommandRecord { TypeId = Id(1), Version = 2, Sequence = 1, ClientTick = 4, Payload = new byte[] { 7 } } } }, bytes, out length);
            AssertHex(bytes, length, "01000000000000010000000000000000000000000200000001000000040000000100000007");
            Assert.That(PayloadCodec.TryReadCommandBatch(bytes.AsSpan(0, length - 1), out _), Is.False);
            PayloadCodec.TryWrite(new FullSnapshotPayload { Entities = new[] { new SnapshotEntity { Entity = new WireEntityId(2, 1, 3), KindId = Id(2), Records = new[] { new SnapshotRecord { TypeId = Id(3), Kind = RecordKind.Tag, Payload = Array.Empty<byte>() } } } } }, bytes, out length);
            AssertHex(bytes, length, "010000000200000001000300000000020000000000000000000000000000010000000003000000000000000000000000020000000000000000000000");
            Assert.That(PayloadCodec.TryReadFullSnapshot(bytes.AsSpan(0, length - 1), out _), Is.False);
            PayloadCodec.TryWriteAck(bytes, out length); AssertHex(bytes, length, string.Empty);
            PayloadCodec.TryWrite(new ResyncRequestPayload { Reason = ResyncReason.HashMismatch, LastAcceptedTick = 4 }, bytes, out length); AssertHex(bytes, length, "0100000004000000");
            Assert.That(PayloadCodec.TryReadResyncRequest(bytes.AsSpan(0, length - 1), out _), Is.False);
            PayloadCodec.TryWrite(new DisconnectPayload { Reason = DisconnectReason.ServerShutdown }, bytes, out length); AssertHex(bytes, length, "07000000");
            Assert.That(PayloadCodec.TryReadDisconnect(bytes.AsSpan(0, length - 1), out _), Is.False);
        }

        [Test]
        public void DisconnectReasonValuesAndRequestedGoldenRemainFrozen()
        {
            Assert.That((ushort)DisconnectReason.ProtocolViolation, Is.EqualTo(1));
            Assert.That((ushort)DisconnectReason.SchemaMismatch, Is.EqualTo(2));
            Assert.That((ushort)DisconnectReason.LimitsExceeded, Is.EqualTo(3));
            Assert.That((ushort)DisconnectReason.UnexpectedEpoch, Is.EqualTo(4));
            Assert.That((ushort)DisconnectReason.TransportClosed, Is.EqualTo(5));
            Assert.That((ushort)DisconnectReason.SequenceExhausted, Is.EqualTo(6));
            Assert.That((ushort)DisconnectReason.ServerShutdown, Is.EqualTo(7));
            Assert.That((ushort)DisconnectReason.Requested, Is.EqualTo(8));

            var bytes = new byte[4];
            Assert.That(PayloadCodec.TryWrite(new DisconnectPayload { Reason = DisconnectReason.ServerShutdown }, bytes, out var length), Is.True);
            AssertHex(bytes, length, "07000000");
            Assert.That(PayloadCodec.TryWrite(new DisconnectPayload { Reason = DisconnectReason.Requested }, bytes, out length), Is.True);
            AssertHex(bytes, length, "08000000");
            Assert.That(PayloadCodec.TryReadDisconnect(bytes, out var decoded), Is.True);
            Assert.That(decoded.Reason, Is.EqualTo(DisconnectReason.Requested));
            bytes[0] = 9;
            Assert.That(PayloadCodec.TryReadDisconnect(bytes, out _), Is.False);
        }

        [Test]
        public void RfcUuidAndEntityIdBytesAreCanonical()
        {
            var id = new TypeId(new Guid("00112233-4455-6677-8899-aabbccddeeff")); var bytes = new byte[16]; id.WriteBytes(bytes);
            AssertHex(bytes, bytes.Length, "00112233445566778899AABBCCDDEEFF"); Assert.That(TypeId.ReadBytes(bytes), Is.EqualTo(id));
            var link = new byte[8]; WriteEntity(link, 0, new WireEntityId(0x11223344, 0x5566, 0x7788));
            AssertHex(link, link.Length, "4433221166558877");
        }

        [Test]
        public void PreflightRejectsTinyDeclaredCollectionsAndReservedOrOversizedValues()
        {
            Assert.That(PayloadCodec.TryReadCommandBatch(new byte[] { 0xff, 0xff, 0, 0 }, out _), Is.False);
            Assert.That(PayloadCodec.TryReadFullSnapshot(new byte[] { 0xff, 0xff, 0, 0 }, out _), Is.False);
            var ack = new byte[20]; ack[18] = 1; Assert.That(PayloadCodec.TryReadHelloAck(ack, out _), Is.False);
            var resync = new byte[8]; resync[0] = 1; resync[2] = 1; Assert.That(PayloadCodec.TryReadResyncRequest(resync, out _), Is.False);
            var header = Header(PacketKind.Ack, PacketFlags.ReliableOrdered, 0); header.DecodedPayloadLength = ProtocolLimits.MaxDecodedPayloadBytes + 1U;
            Assert.That(header.TryWrite(new byte[PacketHeader.Size]), Is.False);
        }

        [Test]
        public void NonCanonicalCommandsEntitiesRecordsAndLinksAreRejected()
        {
            var bytes = new byte[1024];
            var commands = new CommandBatchPayload { Commands = new[] { new CommandRecord { TypeId = Id(1), Sequence = 2, Payload = Array.Empty<byte>() }, new CommandRecord { TypeId = Id(2), Sequence = 1, Payload = Array.Empty<byte>() } } };
            Assert.That(PayloadCodec.TryWrite(commands, bytes, out _), Is.False);
            var entities = new FullSnapshotPayload { Entities = new[] { new SnapshotEntity { Entity = new WireEntityId(2, 0, 1), KindId = Id(1) }, new SnapshotEntity { Entity = new WireEntityId(1, 0, 1), KindId = Id(1) } } };
            Assert.That(PayloadCodec.TryWrite(entities, bytes, out _), Is.False);
            var links = new byte[16]; WriteEntity(links, 0, new WireEntityId(2, 0, 1)); WriteEntity(links, 8, new WireEntityId(1, 0, 1));
            var snapshot = new FullSnapshotPayload
            {
                Entities = new[]
                {
                    new SnapshotEntity
                    {
                        Entity = new WireEntityId(1, 0, 1), KindId = Id(1),
                        Records = new[]
                        {
                            new SnapshotRecord { TypeId = Id(2), Kind = RecordKind.Links, ElementCount = 2, Payload = links }
                        }
                    }
                }
            };
            Assert.That(PayloadCodec.TryWrite(snapshot, bytes, out _), Is.False);
        }

        private static PacketHeader Header(PacketKind kind, PacketFlags flags, uint length) => new() { Kind = kind, Flags = flags, SessionEpoch = 7, PacketSequence = 8, ServerTick = 9, BaselineTick = PacketHeader.NoneTick, AcknowledgedSnapshotTick = 6, WirePayloadLength = length, DecodedPayloadLength = length, SchemaHash = Id(5), PayloadHash = 11, AcknowledgedCommandSequence = 4 };
        private static TypeId Id(int value) => new(new Guid(value, 0, 0, new byte[8]));
        private static void WriteEntity(byte[] bytes, int offset, WireEntityId id) { bytes[offset] = (byte)id.Id; bytes[offset + 1] = (byte)(id.Id >> 8); bytes[offset + 2] = (byte)(id.Id >> 16); bytes[offset + 3] = (byte)(id.Id >> 24); bytes[offset + 4] = (byte)id.ClusterId; bytes[offset + 5] = (byte)(id.ClusterId >> 8); bytes[offset + 6] = (byte)id.Version; bytes[offset + 7] = (byte)(id.Version >> 8); }
        private static void AssertHex(byte[] bytes, int length, string expected) => Assert.That(BitConverter.ToString(bytes, 0, length).Replace("-", string.Empty), Is.EqualTo(expected));
    }
}
