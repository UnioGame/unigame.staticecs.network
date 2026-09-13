using System;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Frames one snapshot chunk directly into a single pooled packet buffer.</summary>
    internal static class SnapshotChunkEncoder
    {
        /// <summary>Writes the chunk payload after the packet header, hashes exactly those bytes, and writes the packet header in place.</summary>
        internal static bool TryEncode(NetworkBufferPool pool,
            PacketHeader header, in SnapshotChunkHeader chunk,
            ReadOnlySpan<byte> body, out NetworkBufferLease packet)
        {
            packet = null;
            if (pool == null ||
                body.Length > ProtocolLimits.MaxWirePayloadBytes - SnapshotChunkHeader.Size)
                return false;
            var payloadLength = SnapshotChunkHeader.Size + body.Length;
            var lease = pool.Rent(checked(PacketHeader.Size + payloadLength));
            var destination = lease.WritableSpan;
            var payload = destination.Slice(PacketHeader.Size, payloadLength);
            if (!chunk.TryWrite(payload))
            {
                lease.Dispose();
                return false;
            }
            body.CopyTo(payload.Slice(SnapshotChunkHeader.Size));
            header.PayloadLength = (uint)payloadLength;
            header.PayloadHash = Hashing.XxHash64(payload);
            if (!header.TryWrite(destination))
            {
                lease.Dispose();
                return false;
            }
            packet = lease;
            return true;
        }
    }
}
