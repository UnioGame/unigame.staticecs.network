using System;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Stores a bounded ordered history keyed by authoritative tick.</summary>
    public sealed class NetworkHistory<T>
    {
        private readonly int _capacity;
        private readonly long _maxBytes;
        private readonly Func<T, int> _sizeOf;
        private readonly Action<T> _release;
        private readonly uint[] _ticks;
        private readonly T[] _values;
        private readonly int[] _sizes;
        private readonly bool[] _occupied;
        private int _count;
        private long _bytes;
        /// <summary>Creates a bounded history.</summary>
        public NetworkHistory(int capacity) : this(capacity, long.MaxValue, _ => 0) { }
        /// <summary>Creates a history bounded simultaneously by ticks and retained bytes.</summary>
        public NetworkHistory(int capacity, long maxBytes, Func<T, int> sizeOf,
            Action<T> release = null)
        {
            if (capacity < 1) throw new ArgumentOutOfRangeException(nameof(capacity));
            if (maxBytes < 1) throw new ArgumentOutOfRangeException(nameof(maxBytes));
            _capacity = capacity;
            _maxBytes = maxBytes;
            _sizeOf = sizeOf ?? throw new ArgumentNullException(nameof(sizeOf));
            _release = release;
            _ticks = new uint[capacity];
            _values = new T[capacity];
            _sizes = new int[capacity];
            _occupied = new bool[capacity];
        }
        /// <summary>Gets retained item count.</summary>
        public int Count => _count;
        /// <summary>Gets retained byte count.</summary>
        public long Bytes => _bytes;
        /// <summary>Gets the configured maximum retained item count.</summary>
        public int Capacity => _capacity;
        /// <summary>Gets the configured maximum retained byte count.</summary>
        public long MaxBytes => _maxBytes;
        /// <summary>Gets the oldest retained tick, or zero when empty.</summary>
        public uint OldestTick => BoundaryTick(false);
        /// <summary>Gets the newest retained tick, or zero when empty.</summary>
        public uint NewestTick => BoundaryTick(true);
        /// <summary>Adds or replaces one tick and evicts oldest ticks until both bounds hold.</summary>
        public void Store(uint tick, T value)
        {
            var size = _sizeOf(value);
            if (size < 0) throw new ArgumentOutOfRangeException(nameof(value), "History size cannot be negative.");
            var index = (int)(tick % (uint)_capacity);
            if (_occupied[index])
            {
                _bytes -= _sizes[index];
                _release?.Invoke(_values[index]);
            }
            else
            {
                _count++;
            }
            _ticks[index] = tick;
            _values[index] = value;
            _sizes[index] = size;
            _occupied[index] = true;
            _bytes += size;
            while (_bytes > _maxBytes && _count > 0)
            {
                var oldest = FindBoundary(false);
                Evict((int)(oldest % (uint)_capacity));
            }
        }
        /// <summary>Finds one retained tick.</summary>
        public bool TryGet(uint tick, out T value)
        {
            var index = (int)(tick % (uint)_capacity);
            if (_occupied[index] && _ticks[index] == tick)
            {
                value = _values[index];
                return true;
            }
            value = default;
            return false;
        }

        /// <summary>Removes all retained items and resets the byte count.</summary>
        public void Clear()
        {
            for (var i = 0; i < _occupied.Length; i++)
                if (_occupied[i])
                    Evict(i);
            _bytes = 0;
        }

        private uint BoundaryTick(bool newest)
        {
            return _count == 0 ? 0 : FindBoundary(newest);
        }

        private uint FindBoundary(bool newest)
        {
            var found = false;
            var value = 0u;
            for (var i = 0; i < _occupied.Length; i++)
            {
                if (!_occupied[i])
                    continue;
                if (!found || newest && _ticks[i] > value || !newest && _ticks[i] < value)
                {
                    value = _ticks[i];
                    found = true;
                }
            }
            return value;
        }

        private void Evict(int index)
        {
            if (!_occupied[index])
                return;
            _release?.Invoke(_values[index]);
            _bytes -= _sizes[index];
            _values[index] = default;
            _sizes[index] = 0;
            _occupied[index] = false;
            _count--;
        }
    }
}
