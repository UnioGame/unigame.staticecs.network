using System;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Identifies one endpoint's protocol direction.</summary>
    public enum SessionRole : byte
    {
        /// <summary>Initiates negotiation and owns replica topology.</summary>
        Client = 0,
        /// <summary>Admits negotiation and owns authority topology.</summary>
        Server = 1
    }

    /// <summary>Identifies the public lifecycle of one protocol session.</summary>
    public enum SessionState : byte
    {
        /// <summary>The endpoint is exchanging control packets.</summary>
        Handshaking = 0,
        /// <summary>The endpoint completed admission.</summary>
        Established = 1,
        /// <summary>The endpoint is preserving a terminal delivery barrier.</summary>
        Closing = 2,
        /// <summary>The endpoint completed an orderly or rejected close.</summary>
        Closed = 3,
        /// <summary>The endpoint detected a local or peer failure.</summary>
        Faulted = 4,
        /// <summary>The endpoint and its owned transport were disposed.</summary>
        Disposed = 5
    }

    /// <summary>Classifies local session failures without adding wire values.</summary>
    public enum SessionError : byte
    {
        /// <summary>No local failure occurred.</summary>
        None = 0,
        /// <summary>Packet framing, direction, state, or semantics were invalid.</summary>
        Protocol = 1,
        /// <summary>The negotiated schema was invalid for the current phase.</summary>
        Schema = 2,
        /// <summary>A negotiated or transport capacity was exceeded.</summary>
        Limits = 3,
        /// <summary>The mapped world topology is unavailable or changed.</summary>
        Topology = 4,
        /// <summary>The negotiated session epoch was not used.</summary>
        Epoch = 5,
        /// <summary>A local transmit sequence was exhausted.</summary>
        Sequence = 6,
        /// <summary>The owned transport ended unexpectedly.</summary>
        Transport = 7
    }

    /// <summary>Reports observable work performed by one deterministic session step.</summary>
    [Flags]
    public enum StepResult : byte
    {
        /// <summary>No packet or public state change occurred.</summary>
        None = 0,
        /// <summary>One inbound packet was transferred from the transport.</summary>
        Received = 1,
        /// <summary>One outbound packet was accepted by the transport.</summary>
        Sent = 2,
        /// <summary>The public session state changed.</summary>
        StateChanged = 4
    }

    /// <summary>Contains immutable validated settings for one session endpoint.</summary>
    public sealed class SessionConfig
    {
        private SessionConfig(
            SessionRole role,
            uint epoch,
            uint peerId,
            ulong nonce,
            ushort minTickRate,
            ushort maxTickRate,
            uint maxWireBytes,
            uint maxDecodedBytes,
            ChunkMapping[] chunks)
        {
            Role = role;
            Epoch = epoch;
            PeerId = peerId;
            Nonce = nonce;
            MinTickRate = minTickRate;
            MaxTickRate = maxTickRate;
            MaxWireBytes = maxWireBytes;
            MaxDecodedBytes = maxDecodedBytes;
            Chunks = chunks;
        }

        /// <summary>Creates validated client negotiation settings.</summary>
        public static SessionConfig Client(
            ulong nonce,
            ushort minTickRate,
            ushort maxTickRate,
            uint maxWireBytes = ProtocolLimits.MaxWirePayloadBytes,
            uint maxDecodedBytes = ProtocolLimits.MaxDecodedPayloadBytes)
        {
            ValidateNonce(nonce);
            if (minTickRate == 0) throw new ArgumentOutOfRangeException(nameof(minTickRate));
            if (maxTickRate == 0 || maxTickRate < minTickRate) throw new ArgumentOutOfRangeException(nameof(maxTickRate));
            ValidateLimits(maxWireBytes, maxDecodedBytes);
            return new SessionConfig(SessionRole.Client, 0, 0, nonce, minTickRate, maxTickRate,
                maxWireBytes, maxDecodedBytes, Array.Empty<ChunkMapping>());
        }

        /// <summary>Creates validated server negotiation settings with a canonical defensive map copy.</summary>
        public static SessionConfig Server(
            uint epoch,
            uint peerId,
            ulong nonce,
            ushort tickRate,
            ReadOnlySpan<ChunkMapping> chunks,
            uint maxWireBytes = ProtocolLimits.MaxWirePayloadBytes,
            uint maxDecodedBytes = ProtocolLimits.MaxDecodedPayloadBytes)
        {
            if (epoch == 0) throw new ArgumentOutOfRangeException(nameof(epoch));
            if (peerId == 0) throw new ArgumentOutOfRangeException(nameof(peerId));
            ValidateNonce(nonce);
            if (tickRate == 0) throw new ArgumentOutOfRangeException(nameof(tickRate));
            ValidateLimits(maxWireBytes, maxDecodedBytes);
            if (chunks.Length == 0 || chunks.Length > ProtocolLimits.MaxChunkMappings)
                throw new ArgumentOutOfRangeException(nameof(chunks));

            var copy = chunks.ToArray();
            Sort(copy);
            for (var i = 0; i < copy.Length; i++)
            {
                if (copy[i].Role != 1) throw new ArgumentException("Every mapping must use authority role one.", nameof(chunks));
                if (i > 0 && copy[i].Chunk == copy[i - 1].Chunk)
                    throw new ArgumentException("Chunk mappings must be unique.", nameof(chunks));
            }

            return new SessionConfig(SessionRole.Server, epoch, peerId, nonce, tickRate, tickRate,
                maxWireBytes, maxDecodedBytes, copy);
        }

        /// <summary>Gets the endpoint direction.</summary>
        public SessionRole Role { get; }

        internal uint Epoch { get; }
        internal uint PeerId { get; }
        internal ulong Nonce { get; }
        internal ushort MinTickRate { get; }
        internal ushort MaxTickRate { get; }
        internal uint MaxWireBytes { get; }
        internal uint MaxDecodedBytes { get; }
        internal ChunkMapping[] Chunks { get; }

        private static void ValidateNonce(ulong nonce)
        {
            if (nonce == 0) throw new ArgumentOutOfRangeException(nameof(nonce));
        }

        private static void ValidateLimits(uint maxWireBytes, uint maxDecodedBytes)
        {
            if (maxWireBytes < 24 || maxWireBytes > ProtocolLimits.MaxWirePayloadBytes)
                throw new ArgumentOutOfRangeException(nameof(maxWireBytes));
            if (maxDecodedBytes < 24 || maxDecodedBytes > ProtocolLimits.MaxDecodedPayloadBytes)
                throw new ArgumentOutOfRangeException(nameof(maxDecodedBytes));
        }

        private static void Sort(ChunkMapping[] mappings)
        {
            for (var i = 1; i < mappings.Length; i++)
            {
                var value = mappings[i];
                var j = i - 1;
                while (j >= 0 && mappings[j].Chunk > value.Chunk)
                {
                    mappings[j + 1] = mappings[j];
                    j--;
                }
                mappings[j + 1] = value;
            }
        }
    }
}
