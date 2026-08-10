namespace UniGame.StaticEcs.Network
{
    using System;
    using FFS.Libraries.StaticEcs;
    using FFS.Libraries.StaticPack;

    /// <summary>Identifies a dynamic network prefab asset without a runtime object reference.</summary>
    public struct NetworkPrefabId : IComponent, INetworkType, IEquatable<NetworkPrefabId>
    {
        /// <summary>First identifier word.</summary>
        public uint A;
        /// <summary>Second identifier word.</summary>
        public uint B;
        /// <summary>Third identifier word.</summary>
        public uint C;
        /// <summary>Fourth identifier word.</summary>
        public uint D;
        /// <summary>Gets whether the identifier has no baked value.</summary>
        public bool IsEmpty => (A | B | C | D) == 0;
        /// <summary>Writes the canonical identifier payload.</summary>
        public void Write<TWorld>(ref BinaryPackWriter writer, World<TWorld>.Entity self)
            where TWorld : struct, IWorldType => NetworkObjectIdSerialization.Write(
            ref writer, A, B, C, D);
        /// <summary>Reads the canonical identifier payload.</summary>
        public void Read<TWorld>(ref BinaryPackReader reader, World<TWorld>.Entity self,
            byte version, bool disabled) where TWorld : struct, IWorldType =>
            NetworkObjectIdSerialization.Read(ref reader, out A, out B, out C, out D);
        /// <inheritdoc />
        public bool Equals(NetworkPrefabId other) => A == other.A && B == other.B &&
            C == other.C && D == other.D;
        /// <inheritdoc />
        public override bool Equals(object obj) => obj is NetworkPrefabId other && Equals(other);
        /// <inheritdoc />
        public override int GetHashCode() => HashCode.Combine(A, B, C, D);
    }

    /// <summary>Identifies one authored scene instance across authority and replica worlds.</summary>
    public struct SceneObjectId : IComponent, INetworkType, IEquatable<SceneObjectId>
    {
        /// <summary>First identifier word.</summary>
        public uint A;
        /// <summary>Second identifier word.</summary>
        public uint B;
        /// <summary>Third identifier word.</summary>
        public uint C;
        /// <summary>Fourth identifier word.</summary>
        public uint D;
        /// <summary>Gets whether the identifier has no baked value.</summary>
        public bool IsEmpty => (A | B | C | D) == 0;
        /// <summary>Writes the canonical identifier payload.</summary>
        public void Write<TWorld>(ref BinaryPackWriter writer, World<TWorld>.Entity self)
            where TWorld : struct, IWorldType => NetworkObjectIdSerialization.Write(
            ref writer, A, B, C, D);
        /// <summary>Reads the canonical identifier payload.</summary>
        public void Read<TWorld>(ref BinaryPackReader reader, World<TWorld>.Entity self,
            byte version, bool disabled) where TWorld : struct, IWorldType =>
            NetworkObjectIdSerialization.Read(ref reader, out A, out B, out C, out D);
        /// <inheritdoc />
        public bool Equals(SceneObjectId other) => A == other.A && B == other.B &&
            C == other.C && D == other.D;
        /// <inheritdoc />
        public override bool Equals(object obj) => obj is SceneObjectId other && Equals(other);
        /// <inheritdoc />
        public override int GetHashCode() => HashCode.Combine(A, B, C, D);
    }

    internal static class NetworkObjectIdSerialization
    {
        internal static void Write(ref BinaryPackWriter writer, uint a, uint b, uint c, uint d)
        {
            writer.WriteInt(unchecked((int)a));
            writer.WriteInt(unchecked((int)b));
            writer.WriteInt(unchecked((int)c));
            writer.WriteInt(unchecked((int)d));
        }

        internal static void Read(ref BinaryPackReader reader, out uint a, out uint b,
            out uint c, out uint d)
        {
            a = unchecked((uint)reader.ReadInt());
            b = unchecked((uint)reader.ReadInt());
            c = unchecked((uint)reader.ReadInt());
            d = unchecked((uint)reader.ReadInt());
        }
    }
}
