using System;
using System.Buffers;
using FFS.Libraries.StaticEcs;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Binds immutable mapped chunks and an exact session-owned replica ledger to one Static ECS world.</summary>
    public sealed class ReplicaScope<TWorld> : IDisposable where TWorld : struct, IWorldType
    {
        private readonly ChunkMapping[] _mappings;
        private readonly ushort[] _clusters;
        private EntityGID[] _ledger;
        private int _ledgerCount;
        private bool _disposed;
        private readonly bool _initiallyValid;

        /// <summary>Creates a role-bound scope over already registered chunks without changing world topology.</summary>
        public ReplicaScope(ScopeRole role, ReadOnlySpan<ChunkMapping> map)
        {
            Role = role;
            _mappings = map.ToArray();
            SortMappings(_mappings);
            _clusters = BuildClusters(_mappings);
            _initiallyValid = ValidateInitial();
        }

        /// <summary>Gets the immutable local scope role.</summary>
        public ScopeRole Role { get; }

        internal bool IsDisposed => _disposed;
        internal ReadOnlySpan<ushort> Clusters => _clusters;
        internal ReadOnlySpan<EntityGID> Ledger => _ledger == null ? ReadOnlySpan<EntityGID>.Empty : _ledger.AsSpan(0, _ledgerCount);

        internal bool ValidateCurrent()
        {
            if (_disposed || !_initiallyValid || World<TWorld>.Status != WorldStatus.Initialized) return false;
            var owner = Role == ScopeRole.Authority ? ChunkOwnerType.Self : ChunkOwnerType.Other;
            for (var i = 0; i < _mappings.Length; i++)
            {
                var mapping = _mappings[i];
                if (!World<TWorld>.ClusterIsRegistered(mapping.Cluster) || !World<TWorld>.ChunkIsRegistered(mapping.Chunk) ||
                    World<TWorld>.GetChunkClusterId(mapping.Chunk) != mapping.Cluster || World<TWorld>.GetChunkOwner(mapping.Chunk) != owner)
                    return false;
            }
            return true;
        }

        internal bool Contains(uint chunk, ushort cluster)
        {
            var low = 0;
            var high = _mappings.Length - 1;
            while (low <= high)
            {
                var middle = low + ((high - low) >> 1);
                var value = _mappings[middle];
                if (value.Chunk == chunk) return value.Cluster == cluster;
                if (value.Chunk < chunk) low = middle + 1; else high = middle - 1;
            }
            return false;
        }

        internal bool Owns(in EntityGID entity) => ReplicationSort.Contains(Ledger, in entity);

        internal void ReplaceLedger(ReadOnlySpan<EntityGID> entities)
        {
            EntityGID[] next = null;
            if (!entities.IsEmpty)
            {
                next = ArrayPool<EntityGID>.Shared.Rent(entities.Length);
                entities.CopyTo(next);
            }
            var previous = _ledger;
            _ledger = next;
            _ledgerCount = entities.Length;
            if (previous != null) ArrayPool<EntityGID>.Shared.Return(previous);
        }

        /// <summary>Releases the pooled exact-session ledger. World chunks and entities are not modified.</summary>
        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            if (_ledger != null) ArrayPool<EntityGID>.Shared.Return(_ledger);
            _ledger = null;
            _ledgerCount = 0;
        }

        private bool ValidateInitial()
        {
            if ((Role != ScopeRole.Authority && Role != ScopeRole.Replica) || _mappings.Length == 0 ||
                _mappings.Length > ProtocolLimits.MaxChunkMappings || World<TWorld>.Status != WorldStatus.Initialized)
                return false;
            var previousChunk = uint.MaxValue;
            for (var i = 0; i < _mappings.Length; i++)
            {
                var mapping = _mappings[i];
                if (mapping.Role != 1 || (i > 0 && mapping.Chunk == previousChunk)) return false;
                previousChunk = mapping.Chunk;
            }
            if (!ValidateCurrentTopology()) return false;
            if (Role == ScopeRole.Replica)
                for (var i = 0; i < _mappings.Length; i++) if (World<TWorld>.HasEntitiesInChunk(_mappings[i].Chunk)) return false;
            return true;
        }

        private bool ValidateCurrentTopology()
        {
            var owner = Role == ScopeRole.Authority ? ChunkOwnerType.Self : ChunkOwnerType.Other;
            for (var i = 0; i < _mappings.Length; i++)
            {
                var mapping = _mappings[i];
                if (!World<TWorld>.ClusterIsRegistered(mapping.Cluster) || !World<TWorld>.ChunkIsRegistered(mapping.Chunk) ||
                    World<TWorld>.GetChunkClusterId(mapping.Chunk) != mapping.Cluster || World<TWorld>.GetChunkOwner(mapping.Chunk) != owner)
                    return false;
            }
            return true;
        }

        private static void SortMappings(ChunkMapping[] mappings)
        {
            for (var i = 1; i < mappings.Length; i++)
            {
                var value = mappings[i];
                var j = i - 1;
                while (j >= 0 && mappings[j].Chunk > value.Chunk) { mappings[j + 1] = mappings[j]; j--; }
                mappings[j + 1] = value;
            }
        }

        private static ushort[] BuildClusters(ChunkMapping[] mappings)
        {
            var values = new ushort[mappings.Length];
            var count = 0;
            for (var i = 0; i < mappings.Length; i++)
            {
                var cluster = mappings[i].Cluster;
                var found = false;
                for (var j = 0; j < count; j++) if (values[j] == cluster) { found = true; break; }
                if (!found) values[count++] = cluster;
            }
            Array.Sort(values, 0, count);
            if (count == values.Length) return values;
            var result = new ushort[count];
            Array.Copy(values, result, count);
            return result;
        }
    }
}
