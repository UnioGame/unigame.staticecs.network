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

        /// <summary>Frames the fixed chunk header and body into one pooled lease and returns its hash, for reuse across peers that share the identical chunk content.</summary>
        internal static bool TryEncodePayload(NetworkBufferPool pool,
            in SnapshotChunkHeader chunk, ReadOnlySpan<byte> body,
            out NetworkBufferLease payload, out ulong payloadHash)
        {
            payload = null;
            payloadHash = 0;
            if (pool == null ||
                body.Length > ProtocolLimits.MaxWirePayloadBytes - SnapshotChunkHeader.Size)
                return false;
            var payloadLength = SnapshotChunkHeader.Size + body.Length;
            var lease = pool.Rent(payloadLength);
            var destination = lease.WritableSpan;
            if (!chunk.TryWrite(destination))
            {
                lease.Dispose();
                return false;
            }
            body.CopyTo(destination.Slice(SnapshotChunkHeader.Size));
            payloadHash = Hashing.XxHash64(destination);
            payload = lease;
            return true;
        }

        /// <summary>Wraps a previously framed chunk payload with one peer-specific packet header, reusing its precomputed hash instead of rehashing identical bytes.</summary>
        internal static bool TryEncodeFromPayload(NetworkBufferPool pool,
            PacketHeader header, ReadOnlyMemory<byte> payload,
            ulong payloadHash, out NetworkBufferLease packet)
        {
            packet = null;
            if (pool == null)
                return false;
            var lease = pool.Rent(checked(PacketHeader.Size + payload.Length));
            var destination = lease.WritableSpan;
            payload.Span.CopyTo(destination.Slice(PacketHeader.Size));
            header.PayloadLength = (uint)payload.Length;
            header.PayloadHash = payloadHash;
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
