using System;
using System.Collections.Generic;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Stores a bounded ordered history keyed by authoritative tick.</summary>
    public sealed class NetworkHistory<T>
    {
        private readonly int _capacity;
        private readonly long _maxBytes;
        private readonly Func<T, int> _sizeOf;
        private readonly SortedDictionary<uint, T> _values = new SortedDictionary<uint, T>();
        private long _bytes;
        /// <summary>Creates a bounded history.</summary>
        public NetworkHistory(int capacity) : this(capacity, long.MaxValue, _ => 0) { }
        /// <summary>Creates a history bounded simultaneously by ticks and retained bytes.</summary>
        public NetworkHistory(int capacity, long maxBytes, Func<T, int> sizeOf)
        {
            if (capacity < 1) throw new ArgumentOutOfRangeException(nameof(capacity));
            if (maxBytes < 1) throw new ArgumentOutOfRangeException(nameof(maxBytes));
            _capacity = capacity; _maxBytes = maxBytes; _sizeOf = sizeOf ?? throw new ArgumentNullException(nameof(sizeOf));
        }
        /// <summary>Gets retained item count.</summary>
        public int Count => _values.Count;
        /// <summary>Gets retained byte count.</summary>
        public long Bytes => _bytes;
        /// <summary>Adds or replaces one tick and evicts oldest ticks until both bounds hold.</summary>
        public void Store(uint tick, T value)
        {
            var size = _sizeOf(value);
            if (size < 0) throw new ArgumentOutOfRangeException(nameof(value), "History size cannot be negative.");
            if (_values.TryGetValue(tick, out var replaced)) _bytes -= _sizeOf(replaced);
            _values[tick] = value;
            _bytes += size;
            while (_values.Count > _capacity || _bytes > _maxBytes)
            {
                using var keys = _values.Keys.GetEnumerator();
                keys.MoveNext();
                var oldest = keys.Current;
                _bytes -= _sizeOf(_values[oldest]);
                _values.Remove(oldest);
            }
        }
        /// <summary>Finds one retained tick.</summary>
        public bool TryGet(uint tick, out T value) => _values.TryGetValue(tick, out value);
    }
}
