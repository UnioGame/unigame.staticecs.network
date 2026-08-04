using System;
using System.Collections.Generic;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Stores a bounded ordered history keyed by authoritative tick.</summary>
    public sealed class NetworkHistory<T>
    {
        private readonly int _capacity;
        private readonly SortedDictionary<uint, T> _values = new SortedDictionary<uint, T>();
        /// <summary>Creates a bounded history.</summary>
        public NetworkHistory(int capacity) { if (capacity < 1) throw new ArgumentOutOfRangeException(nameof(capacity)); _capacity = capacity; }
        /// <summary>Gets retained item count.</summary>
        public int Count => _values.Count;
        /// <summary>Adds or replaces one tick and evicts the oldest tick.</summary>
        public void Store(uint tick, T value)
        {
            _values[tick] = value;
            if (_values.Count <= _capacity) return;
            using var keys = _values.Keys.GetEnumerator();
            keys.MoveNext();
            _values.Remove(keys.Current);
        }
        /// <summary>Finds one retained tick.</summary>
        public bool TryGet(uint tick, out T value) => _values.TryGetValue(tick, out value);
    }
}
