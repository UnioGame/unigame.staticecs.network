using System;
using System.Buffers;

namespace UniGame.StaticEcs.Network
{
    internal enum SessionStage : byte
    {
        SendClientHello,
        AwaitServerHello,
        AwaitHelloAck,
        SendFinalAck,
        AwaitClientHello,
        SendServerHello,
        SendHelloAck,
        AwaitFinalAck,
        Established,
        RejectionBarrier,
        RequestedClose
    }

    internal enum HeaderReadResult : byte
    {
        Success,
        Protocol,
        Limits
    }

    internal enum TransportTerminalKind : byte
    {
        None,
        Limits,
        Protocol,
        RemoteClosed,
        Disposed,
        Transport
    }

    internal struct SequenceDomains
    {
        internal uint ReliableTransmit;
        internal uint ReliableReceive;
        internal uint UnreliableTransmit;
        internal uint UnreliableReceive;

        internal bool TryNextReliableTransmit(out uint sequence)
        {
            if (ReliableTransmit == uint.MaxValue)
            {
                sequence = 0;
                return false;
            }
            sequence = ReliableTransmit + 1;
            return true;
        }

        internal bool IsNextReliableReceive(uint sequence) =>
            ReliableReceive != uint.MaxValue && sequence == ReliableReceive + 1;

        internal bool TryNextUnreliableTransmit(out uint sequence)
        {
            if (UnreliableTransmit == uint.MaxValue)
            {
                sequence = 0;
                return false;
            }
            sequence = UnreliableTransmit + 1;
            return true;
        }

        internal bool IsNewerUnreliableReceive(uint sequence) =>
            sequence != 0 && sequence > UnreliableReceive;

        internal void CommitReliableTransmit(uint sequence) => ReliableTransmit = sequence;
        internal void CommitReliableReceive(uint sequence) => ReliableReceive = sequence;
        internal void CommitUnreliableTransmit(uint sequence) => UnreliableTransmit = sequence;
        internal void CommitUnreliableReceive(uint sequence) => UnreliableReceive = sequence;
    }

    internal readonly struct PendingControl
    {
        private PendingControl(
            PacketKind kind,
            uint epoch,
            TypeId schemaHash,
            HelloPayload hello,
            ConnectResult result,
            ushort tickRate,
            uint peerId,
            ulong nonce,
            ChunkMapping[] chunks,
            DisconnectReason reason)
        {
            Kind = kind;
            Epoch = epoch;
            SchemaHash = schemaHash;
            Hello = hello;
            Result = result;
            TickRate = tickRate;
            PeerId = peerId;
            Nonce = nonce;
            Chunks = chunks;
            Reason = reason;
        }

        internal PacketKind Kind { get; }
        internal uint Epoch { get; }
        internal TypeId SchemaHash { get; }
        internal HelloPayload Hello { get; }
        internal ConnectResult Result { get; }
        internal ushort TickRate { get; }
        internal uint PeerId { get; }
        internal ulong Nonce { get; }
        internal ChunkMapping[] Chunks { get; }
        internal DisconnectReason Reason { get; }

        internal static PendingControl HelloPacket(TypeId schemaHash, in HelloPayload hello) =>
            new(PacketKind.Hello, 0, schemaHash, hello, default, 0, 0, 0, null, default);

        internal static PendingControl HelloAckPacket(
            uint epoch,
            TypeId schemaHash,
            ConnectResult result,
            ushort tickRate,
            uint peerId,
            ulong nonce,
            ChunkMapping[] chunks) =>
            new(PacketKind.HelloAck, epoch, schemaHash, default, result, tickRate, peerId, nonce, chunks, default);

        internal static PendingControl AckPacket(uint epoch, TypeId schemaHash) =>
            new(PacketKind.Ack, epoch, schemaHash, default, default, 0, 0, 0, null, default);

        internal static PendingControl DisconnectPacket(uint epoch, TypeId schemaHash, DisconnectReason reason) =>
            new(PacketKind.Disconnect, epoch, schemaHash, default, default, 0, 0, 0, null, reason);
    }

    internal static class SessionProtocol
    {
        internal static readonly IPayloadTransform ControlTransform = new NoOpTransform();

        internal static HeaderReadResult ReadHeader(
            in PacketLease packet,
            uint maxWireBytes,
            uint maxDecodedBytes,
            out PacketHeader header)
        {
            header = default;
            if (!packet.IsValid || packet.Length < PacketHeader.Size || !PacketHeader.TryRead(packet.Span, out header))
                return HeaderReadResult.Protocol;
            if ((ulong)PacketHeader.Size + header.WirePayloadLength != (ulong)packet.Length)
                return HeaderReadResult.Protocol;
            if (header.WirePayloadLength > maxWireBytes || header.DecodedPayloadLength > maxDecodedBytes)
                return HeaderReadResult.Limits;
            return HeaderReadResult.Success;
        }

        internal static bool HasCommonControlFields(in PacketHeader header) =>
            header.Flags == PacketFlags.ReliableOrdered &&
            header.TransformId == 0 &&
            header.PacketSequence != 0 &&
            header.ServerTick == PacketHeader.NoneTick &&
            header.BaselineTick == PacketHeader.NoneTick &&
            header.AcknowledgedSnapshotTick == PacketHeader.NoneTick &&
            header.AcknowledgedCommandSequence == 0;

        internal static bool TryEncode(in PendingControl control, uint sequence, out PacketLease packet)
        {
            packet = default;
            var payloadLength = control.Kind switch
            {
                PacketKind.Hello => 24,
                PacketKind.HelloAck => checked(20 + (control.Chunks?.Length ?? 0) * 8),
                PacketKind.Ack => 0,
                PacketKind.Disconnect => 4,
                _ => -1
            };
            if (payloadLength < 0) return false;

            var rented = ArrayPool<byte>.Shared.Rent(Math.Max(1, payloadLength));
            try
            {
                var payload = rented.AsSpan(0, payloadLength);
                var encoded = control.Kind switch
                {
                    PacketKind.Hello => PayloadCodec.TryWrite(control.Hello, payload, out var helloBytes) && helloBytes == payloadLength,
                    PacketKind.HelloAck => TryWriteHelloAck(in control, payload, payloadLength),
                    PacketKind.Ack => PayloadCodec.TryWriteAck(payload, out var ackBytes) && ackBytes == 0,
                    PacketKind.Disconnect => PayloadCodec.TryWrite(new DisconnectPayload { Reason = control.Reason }, payload, out var closeBytes) && closeBytes == payloadLength,
                    _ => false
                };
                if (!encoded) return false;

                var header = new PacketHeader
                {
                    Kind = control.Kind,
                    Flags = PacketFlags.ReliableOrdered,
                    SessionEpoch = control.Epoch,
                    PacketSequence = sequence,
                    ServerTick = PacketHeader.NoneTick,
                    BaselineTick = PacketHeader.NoneTick,
                    AcknowledgedSnapshotTick = PacketHeader.NoneTick,
                    SchemaHash = control.SchemaHash,
                    AcknowledgedCommandSequence = 0
                };
                return PacketFraming.TryEncode(header, payload, ControlTransform, out packet);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }

        internal static bool TryEncodeTransfer(
            PacketKind kind,
            Channel channel,
            uint epoch,
            uint sequence,
            uint serverTick,
            TypeId schemaHash,
            uint acknowledgedSnapshotTick,
            uint acknowledgedCommandSequence,
            ReadOnlySpan<byte> payload,
            Schema schema,
            out PacketLease packet)
        {
            var header = new PacketHeader
            {
                Kind = kind,
                Flags = channel == Channel.ReliableOrdered ? PacketFlags.ReliableOrdered : (PacketFlags)0,
                SessionEpoch = epoch,
                PacketSequence = sequence,
                ServerTick = serverTick,
                BaselineTick = PacketHeader.NoneTick,
                AcknowledgedSnapshotTick = acknowledgedSnapshotTick,
                SchemaHash = schemaHash,
                AcknowledgedCommandSequence = acknowledgedCommandSequence
            };
            return PacketFraming.TryEncode(header, payload, ControlTransform, schema, out packet);
        }

        internal static TransportTerminalKind MapTransport(TransportState state, TransportError error)
        {
            if (state == TransportState.Connected && error == TransportError.None) return TransportTerminalKind.None;
            if (state == TransportState.Faulted && error == TransportError.QueueOverflow) return TransportTerminalKind.Limits;
            if (state == TransportState.Faulted && error == TransportError.InvalidPacket) return TransportTerminalKind.Protocol;
            if (state == TransportState.Closed && error == TransportError.RemoteClosed) return TransportTerminalKind.RemoteClosed;
            if (state == TransportState.Disposed && error == TransportError.Disposed) return TransportTerminalKind.Disposed;
            return TransportTerminalKind.Transport;
        }

        private static bool TryWriteHelloAck(in PendingControl control, Span<byte> destination, int expected)
        {
            var payload = new HelloAckPayload
            {
                Result = control.Result,
                TickRate = control.TickRate,
                PeerId = control.PeerId,
                ServerNonce = control.Nonce,
                Chunks = control.Chunks ?? Array.Empty<ChunkMapping>()
            };
            return PayloadCodec.TryWrite(payload, destination, out var written) && written == expected;
        }
    }
}
