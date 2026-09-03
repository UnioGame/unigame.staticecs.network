namespace UniGame.StaticEcs.Network
{
    using System;
    using System.Buffers;
    using System.Collections.Generic;
    using FFS.Libraries.StaticEcs;
    using FFS.Libraries.StaticPack;

    internal sealed class NetworkSnapshotPool
    {
        private readonly Stack<NetworkSnapshot> _snapshots;
        private readonly int _capacity;

        internal NetworkSnapshotPool(int capacity)
        {
            _capacity = Math.Max(1, capacity);
            _snapshots = new Stack<NetworkSnapshot>(_capacity);
        }

        internal NetworkSnapshot Rent(uint tick, SchemaFingerprint fingerprint,
            ScopeId scope, NetworkBufferLease bytes, int entities, int records)
        {
            var snapshot = _snapshots.Count > 0
                ? _snapshots.Pop()
                : new NetworkSnapshot();
            snapshot.Initialize(this, tick, fingerprint, scope, bytes, entities,
                records);
            return snapshot;
        }

        internal void Return(NetworkSnapshot snapshot)
        {
            if (_snapshots.Count < _capacity)
                _snapshots.Push(snapshot);
        }
    }

    /// <summary>Encodes and reconstructs deterministic canonical snapshot deltas without ECS mutation.</summary>
}
