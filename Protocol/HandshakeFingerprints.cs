using System;

namespace UniGame.StaticEcs.Network
{
    /// <summary>
    /// The compatibility fingerprints a client states once, as the payload of its Hello packet.
    /// Later packets carry only <see cref="PacketHeader.CompatibilityTag"/>.
    /// </summary>
    public readonly struct HandshakeFingerprints
    {
        /// <summary>Exact encoded length: 16 bytes of schema, 8 of simulation, 8 of content.</summary>
        public const int Size = 32;

        public HandshakeFingerprints(SchemaFingerprint schema, ulong simulation, ulong content)
        {
            Schema = schema;
            Simulation = simulation;
            Content = content;
        }

        public SchemaFingerprint Schema { get; }

        /// <summary>Deterministic simulation configuration fingerprint.</summary>
        public ulong Simulation { get; }

        /// <summary>Baked grid and content fingerprint.</summary>
        public ulong Content { get; }

        public bool TryWrite(Span<byte> destination)
        {
            if (destination.Length < Size) return false;
            Schema.WriteBytes(destination.Slice(0, 16));
            Hashing.Write64(destination, 16, Simulation);
            Hashing.Write64(destination, 24, Content);
            return true;
        }

        /// <summary>Reads a payload of exactly <see cref="Size"/> bytes.</summary>
        public static bool TryRead(ReadOnlySpan<byte> source, out HandshakeFingerprints fingerprints)
        {
            fingerprints = default;
            if (source.Length != Size) return false;
            fingerprints = new HandshakeFingerprints(SchemaFingerprint.ReadBytes(source.Slice(0, 16)),
                Hashing.Read64(source, 16), Hashing.Read64(source, 24));
            return true;
        }
    }
}
