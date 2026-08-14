namespace UniGame.StaticEcs.Network
{
    using System;
    using System.Collections.Generic;
    using System.Threading;

    /// <summary>Reports bounded packet-buffer ownership and reuse counters.</summary>
    public struct NetworkBufferPoolDiagnostics
    {
        /// <summary>Number of leases currently owned outside the pool.</summary>
        public int OutstandingLeases;

        /// <summary>Bytes currently owned outside the pool.</summary>
        public long OutstandingBytes;

        /// <summary>Bytes retained for reuse.</summary>
        public long RetainedBytes;

        /// <summary>Largest observed outstanding byte count.</summary>
        public long OutstandingHighWaterBytes;

        /// <summary>Largest observed retained byte count.</summary>
        public long RetainedHighWaterBytes;

        /// <summary>Number of rents that required a new managed buffer.</summary>
        public long PoolMisses;
    }

    /// <summary>Owns one immutable packet-buffer view until explicitly disposed.</summary>
    public sealed class NetworkBufferLease : IDisposable
    {
        private NetworkBufferPool _pool;
        private NetworkBufferOwner _owner;
        private int _offset;
        private int _length;

        internal NetworkBufferLease()
        {
        }

        /// <summary>Gets the immutable leased bytes.</summary>
        public ReadOnlyMemory<byte> Memory => _owner == null
            ? throw new ObjectDisposedException(nameof(NetworkBufferLease))
            : new ReadOnlyMemory<byte>(_owner.Buffer, _offset, _length);

        /// <summary>Gets the immutable leased span.</summary>
        public ReadOnlySpan<byte> Span => Memory.Span;

        /// <summary>Gets the number of visible bytes.</summary>
        public int Length => _owner == null ? 0 : _length;

        internal byte[] Buffer => _owner?.Buffer ??
            throw new ObjectDisposedException(nameof(NetworkBufferLease));

        internal int Offset => _offset;

        internal int Capacity => _owner?.Buffer.Length ?? 0;

        /// <summary>Creates another independently disposable view over the same buffer.</summary>
        public NetworkBufferLease Retain()
        {
            if (_owner == null)
                throw new ObjectDisposedException(nameof(NetworkBufferLease));
            return _pool.Retain(_owner, _offset, _length);
        }

        /// <summary>Creates another independently disposable sub-view over the same buffer.</summary>
        public NetworkBufferLease RetainSlice(int offset, int length)
        {
            if (_owner == null)
                throw new ObjectDisposedException(nameof(NetworkBufferLease));
            if ((uint)offset > (uint)_length || (uint)length > (uint)(_length - offset))
                throw new ArgumentOutOfRangeException(nameof(offset));
            return _pool.Retain(_owner, checked(_offset + offset), length);
        }

        /// <inheritdoc />
        public void Dispose()
        {
            var owner = _owner;
            if (owner == null)
                return;
            var pool = _pool;
            _pool = null;
            _owner = null;
            _offset = 0;
            _length = 0;
            pool.Release(owner, this);
        }

        internal Span<byte> WritableSpan => _owner == null
            ? throw new ObjectDisposedException(nameof(NetworkBufferLease))
            : new Span<byte>(_owner.Buffer, _offset, _length);

        internal void Initialize(NetworkBufferPool pool, NetworkBufferOwner owner,
            int offset, int length)
        {
            _pool = pool;
            _owner = owner;
            _offset = offset;
            _length = length;
        }

        internal void SetLength(int length)
        {
            if (_owner == null || length < 0 || length > _owner.Buffer.Length - _offset)
                throw new ArgumentOutOfRangeException(nameof(length));
            _length = length;
        }
    }

    internal sealed class NetworkBufferOwner
    {
        internal byte[] Buffer;
        internal int References;
    }

    /// <summary>Provides bounded reusable packet buffers and explicit lease diagnostics.</summary>
    public sealed class NetworkBufferPool : IDisposable
    {
        /// <summary>Default maximum retained bytes for a client endpoint.</summary>
        public const long DefaultClientRetainedBytes = 32L * 1024 * 1024;

        /// <summary>Default maximum retained bytes for a server endpoint.</summary>
        public const long DefaultServerRetainedBytes = 64L * 1024 * 1024;

        private const int MinimumBufferBytes = 256;
        private const int BucketCount = 18;

        private readonly object _sync = new object();
        private readonly Stack<byte[]>[] _buffers = new Stack<byte[]>[BucketCount];
        private readonly Stack<NetworkBufferOwner> _owners = new Stack<NetworkBufferOwner>();
        private readonly Stack<NetworkBufferLease> _leases = new Stack<NetworkBufferLease>();
        private readonly long _maxRetainedBytes;
        private long _retainedBytes;
        private int _outstandingLeases;
        private long _outstandingBytes;
        private long _outstandingHighWaterBytes;
        private long _retainedHighWaterBytes;
        private long _poolMisses;
        private bool _disposed;

        /// <summary>Creates a pool with one explicit retained-memory limit.</summary>
        public NetworkBufferPool(long maxRetainedBytes)
        {
            if (maxRetainedBytes < 0)
                throw new ArgumentOutOfRangeException(nameof(maxRetainedBytes));
            _maxRetainedBytes = maxRetainedBytes;
            for (var i = 0; i < _buffers.Length; i++)
                _buffers[i] = new Stack<byte[]>();
        }

        /// <summary>Rents one writable buffer whose visible length is exact.</summary>
        public NetworkBufferLease Rent(int length)
        {
            if (length < 0)
                throw new ArgumentOutOfRangeException(nameof(length));
            lock (_sync)
            {
                ThrowIfDisposed();
                var capacity = CapacityFor(length);
                var bucket = BucketFor(capacity);
                byte[] buffer;
                if (_buffers[bucket].Count > 0)
                {
                    buffer = _buffers[bucket].Pop();
                    _retainedBytes -= buffer.Length;
                }
                else
                {
                    buffer = new byte[capacity];
                    _poolMisses++;
                }

                var owner = _owners.Count > 0 ? _owners.Pop() : new NetworkBufferOwner();
                owner.Buffer = buffer;
                owner.References = 1;
                return CreateLease(owner, 0, length);
            }
        }

        /// <summary>Copies source bytes into a new immutable pooled lease.</summary>
        public NetworkBufferLease Copy(ReadOnlySpan<byte> source)
        {
            var lease = Rent(source.Length);
            source.CopyTo(lease.WritableSpan);
            return lease;
        }

        internal NetworkBufferLease Adopt(byte[] buffer, int length)
        {
            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));
            if (length < 0 || length > buffer.Length)
                throw new ArgumentOutOfRangeException(nameof(length));
            lock (_sync)
            {
                ThrowIfDisposed();
                var owner = _owners.Count > 0 ? _owners.Pop() : new NetworkBufferOwner();
                owner.Buffer = buffer;
                owner.References = 1;
                return CreateLease(owner, 0, length);
            }
        }

        /// <summary>Captures current ownership and reuse diagnostics.</summary>
        public NetworkBufferPoolDiagnostics CaptureDiagnostics()
        {
            lock (_sync)
            {
                return new NetworkBufferPoolDiagnostics
                {
                    OutstandingLeases = _outstandingLeases,
                    OutstandingBytes = _outstandingBytes,
                    RetainedBytes = _retainedBytes,
                    OutstandingHighWaterBytes = _outstandingHighWaterBytes,
                    RetainedHighWaterBytes = _retainedHighWaterBytes,
                    PoolMisses = _poolMisses,
                };
            }
        }

        /// <inheritdoc />
        public void Dispose()
        {
            lock (_sync)
            {
                if (_disposed)
                    return;
                _disposed = true;
                for (var i = 0; i < _buffers.Length; i++)
                    _buffers[i].Clear();
                _retainedBytes = 0;
            }
        }

        internal NetworkBufferLease Retain(NetworkBufferOwner owner, int offset, int length)
        {
            lock (_sync)
            {
                if (owner.Buffer == null)
                    throw new ObjectDisposedException(nameof(NetworkBufferLease));
                owner.References = checked(owner.References + 1);
                return CreateLease(owner, offset, length);
            }
        }

        internal void Release(NetworkBufferOwner owner, NetworkBufferLease lease)
        {
            lock (_sync)
            {
                _outstandingLeases--;
                _outstandingBytes -= owner.Buffer?.Length ?? 0;
                _leases.Push(lease);
                owner.References--;
                if (owner.References != 0)
                    return;

                var buffer = owner.Buffer;
                owner.Buffer = null;
                _owners.Push(owner);
                if (buffer == null || _disposed ||
                    buffer.Length > _maxRetainedBytes - _retainedBytes)
                    return;
                _buffers[BucketFor(buffer.Length)].Push(buffer);
                _retainedBytes += buffer.Length;
                if (_retainedBytes > _retainedHighWaterBytes)
                    _retainedHighWaterBytes = _retainedBytes;
            }
        }

        private NetworkBufferLease CreateLease(NetworkBufferOwner owner, int offset, int length)
        {
            var lease = _leases.Count > 0 ? _leases.Pop() : new NetworkBufferLease();
            lease.Initialize(this, owner, offset, length);
            _outstandingLeases++;
            _outstandingBytes += owner.Buffer.Length;
            if (_outstandingBytes > _outstandingHighWaterBytes)
                _outstandingHighWaterBytes = _outstandingBytes;
            return lease;
        }

        private static int CapacityFor(int length)
        {
            var capacity = MinimumBufferBytes;
            var required = Math.Max(1, length);
            while (capacity < required)
                capacity = checked(capacity << 1);
            return capacity;
        }

        private static int BucketFor(int capacity)
        {
            var bucket = 0;
            var value = MinimumBufferBytes;
            while (value < capacity && bucket < BucketCount - 1)
            {
                value <<= 1;
                bucket++;
            }
            if (value != capacity)
                throw new ArgumentOutOfRangeException(nameof(capacity));
            return bucket;
        }

        private void ThrowIfDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(NetworkBufferPool));
        }
    }
}
