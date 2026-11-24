namespace UniGame.StaticEcs.Network
{
    using System;
    using System.Buffers.Binary;
    using System.Collections.Generic;
    using FFS.Libraries.StaticEcs;

    /// <summary>Decides which replicated entities only their owning peer may see.</summary>
    public interface INetworkOwnerStateProvider<TWorld> where TWorld : struct, IWorldType
    {
        /// <summary>Whether the entity stays out of every shared scope snapshot.</summary>
        bool IsPrivate(World<TWorld>.Entity entity);

        /// <summary>Appends every private entity <paramref name="peerId"/> owns.</summary>
        void CollectOwned(uint peerId, List<World<TWorld>.Entity> buffer);
    }

    /// <summary>World resource a gameplay feature publishes on the authority world to keep some
    /// entities owner-only. Absent, every replicated entity goes through scope snapshots.</summary>
    public sealed class NetworkOwnerStateResource<TWorld> : IResource where TWorld : struct, IWorldType
    {
        public INetworkOwnerStateProvider<TWorld> Provider;
    }

    /// <summary>Owner state packet payload: counts, then canonical entity blocks.</summary>
    internal static class OwnerStatePayload
    {
        internal const int HeaderSize = 8;

        internal static void WriteHeader(Span<byte> target, int entities, int records)
        {
            BinaryPrimitives.WriteInt32LittleEndian(target, entities);
            BinaryPrimitives.WriteInt32LittleEndian(target.Slice(4), records);
        }

        internal static bool TryReadHeader(ReadOnlySpan<byte> source, out int entities, out int records)
        {
            entities = 0;
            records = 0;
            if (source.Length < HeaderSize)
                return false;
            entities = BinaryPrimitives.ReadInt32LittleEndian(source);
            records = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(4));
            return entities >= 0 && records >= 0;
        }
    }
}
