using System;
using System.Buffers;
using System.Collections.Generic;
using System.Threading;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Identifies one generation of pooled packet-buffer ownership.</summary>
    public readonly struct PacketLease : IDisposable
    {
        internal sealed class State
        {
            internal byte[] Buffer;
            internal int Length;
            internal long Generation = 1;
            internal State Next;
        }

        private static readonly object PoolLock = new();
        private static State _freeState;
        private static int _freeStateCount;
        private static int _stateAllocationCount;
        private readonly State _state;
        private readonly long _generation;

        private PacketLease(State state, long generation) { _state = state; _generation = generation; }
        /// <summary>Rents a writable packet buffer with the requested capacity.</summary>
        public static PacketLease Rent(int capacity)
        {
            if (capacity < 0 || capacity > ProtocolLimits.MaxDecodedPayloadBytes + PacketHeader.Size)
                throw new ArgumentOutOfRangeException(nameof(capacity));

            var state = AcquireState();
            try
            {
                state.Buffer = ArrayPool<byte>.Shared.Rent(Math.Max(1, capacity));
                state.Length = 0;
                return new PacketLease(state, state.Generation);
            }
            catch
            {
                state.Buffer = null;
                state.Length = 0;
                ReleaseState(state);
                throw;
            }
        }
        /// <summary>Gets whether this lease still owns its storage.</summary>
        public bool IsValid => _state != null && _state.Generation == _generation && _state.Buffer != null;
        /// <summary>Gets the committed packet length.</summary>
        public int Length { get { var state = EnsureValid(); return state.Length; } }
        /// <summary>Gets writable committed storage borrowed until ownership transfers or returns.</summary>
        public Span<byte> Span { get { var state = EnsureValid(); return state.Buffer.AsSpan(0, state.Length); } }
        /// <summary>Gets writable capacity borrowed until ownership transfers or returns.</summary>
        public Span<byte> CapacitySpan { get { var state = EnsureValid(); return state.Buffer; } }
        /// <summary>Commits a new packet length within the rented capacity.</summary>
        public void SetLength(int length) { var state = EnsureValid(); if (length < 0 || length > state.Buffer.Length) throw new ArgumentOutOfRangeException(nameof(length)); state.Length = length; }
        /// <summary>Creates an independent pooled copy of the committed bytes.</summary>
        public PacketLease Copy()
        {
            var state = EnsureValid();
            var copy = Rent(state.Length);
            try
            {
                state.Buffer.AsSpan(0, state.Length).CopyTo(copy.CapacitySpan);
                copy.SetLength(state.Length);
                return copy;
            }
            catch
            {
                if (copy.IsValid) copy.Dispose();
                throw;
            }
        }
        /// <summary>Returns owned storage to the shared pool.</summary>
        public void Dispose()
        {
            var state = EnsureValid();
            var buffer = state.Buffer;
            state.Buffer = null;
            state.Length = 0;
            var retired = _generation == long.MaxValue;
            if (!retired) state.Generation = _generation + 1;
            ArrayPool<byte>.Shared.Return(buffer);
            if (!retired) ReleaseState(state);
        }

        internal static PacketLease Transfer(ref PacketLease lease)
        {
            var state = lease.EnsureValid();
            if (lease._generation < long.MaxValue)
            {
                var generation = lease._generation + 1;
                state.Generation = generation;
                lease = default;
                return new PacketLease(state, generation);
            }

            var replacement = AcquireState();
            replacement.Buffer = state.Buffer;
            replacement.Length = state.Length;
            state.Buffer = null;
            state.Length = 0;
            lease = default;
            return new PacketLease(replacement, replacement.Generation);
        }

        internal ReadOnlyMemory<byte> AsReadOnlyMemory()
        {
            var state = EnsureValid();
            return new ReadOnlyMemory<byte>(state.Buffer, 0, state.Length);
        }

        internal static int PooledStateCountForTests
        {
            get { lock (PoolLock) return _freeStateCount; }
        }

        internal static int StateAllocationCountForTests => Volatile.Read(ref _stateAllocationCount);

        internal bool IsDefault => _state == null;

        internal static bool SameGeneration(in PacketLease left, in PacketLease right) =>
            left._state != null && ReferenceEquals(left._state, right._state) && left._generation == right._generation;

        internal static void ForceGenerationForTests(ref PacketLease lease, long generation)
        {
            if (generation < 1) throw new ArgumentOutOfRangeException(nameof(generation));
            var state = lease.EnsureValid();
            if (generation < lease._generation) throw new ArgumentOutOfRangeException(nameof(generation));
            state.Generation = generation;
            lease = new PacketLease(state, generation);
        }

        private State EnsureValid()
        {
            var state = _state;
            if (state == null || state.Generation != _generation || state.Buffer == null)
                throw new InvalidOperationException("Packet storage was already returned or transferred.");
            return state;
        }

        private static State AcquireState()
        {
            lock (PoolLock)
            {
                if (_freeState != null)
                {
                    var state = _freeState;
                    _freeState = state.Next;
                    state.Next = null;
                    _freeStateCount--;
                    return state;
                }
            }

            var created = new State();
            Interlocked.Increment(ref _stateAllocationCount);
            return created;
        }

        private static void ReleaseState(State state)
        {
            lock (PoolLock)
            {
                state.Next = _freeState;
                _freeState = state;
                _freeStateCount++;
            }
        }
    }

    /// <summary>Identifies transport delivery behavior.</summary>
    public enum Channel
    {
        /// <summary>Exactly-once and in-order until disconnect.</summary>
        ReliableOrdered,
        /// <summary>May drop stale snapshots while preserving newest sequence.</summary>
        UnreliableSequenced
    }

    /// <summary>Reports transport lifecycle state.</summary>
    public enum TransportState
    {
        /// <summary>The transport accepts packets.</summary>
        Connected = 0,
        /// <summary>The transport faulted and drained ownership.</summary>
        Faulted = 1,
        /// <summary>The transport is disposed.</summary>
        Disposed = 2,
        /// <summary>The connected peer disposed its endpoint.</summary>
        Closed = 3
    }

    /// <summary>Identifies the immutable cause of the current transport state.</summary>
    public enum TransportError : byte
    {
        /// <summary>The connected transport has no error.</summary>
        None = 0,
        /// <summary>A reliable packet exceeded the bounded receive queue.</summary>
        QueueOverflow = 1,
        /// <summary>The connected peer disposed its endpoint.</summary>
        RemoteClosed = 2,
        /// <summary>An unreliable packet or channel violated the transport contract.</summary>
        InvalidPacket = 3,
        /// <summary>The local endpoint was disposed.</summary>
        Disposed = 4
    }

    /// <summary>Transfers owned packets across a delivery boundary.</summary>
    public interface ITransport : IDisposable
    {
        /// <summary>Gets transport lifecycle state.</summary>
        TransportState State { get; }
        /// <summary>Gets the immutable cause of the current transport state.</summary>
        TransportError Error { get; }
        /// <summary>Consumes a valid lease and reports whether it entered the delivery queue.</summary>
        bool TrySend(Channel channel, ref PacketLease packet);
        /// <summary>Transfers the next received lease to the caller.</summary>
        bool TryReceive(out Channel channel, out PacketLease packet);
    }

    /// <summary>Advances a transport at a deterministic finite non-blocking logical step barrier.</summary>
    public interface ISteppedTransport
    {
        /// <summary>Begins one caller-defined logical transport step.</summary>
        void BeginStep(ulong stepIndex);
    }

    /// <summary>Creates bounded, single-thread-affine in-memory transports with deterministic delivery semantics.</summary>
    public sealed class MemoryTransport : ITransport, ISteppedTransport
    {
        private readonly LinkedList<Item> _incoming = new();
        private readonly int _capacity;
        private MemoryTransport _peer;
        private uint _latestUnreliable;
        private MemoryTransport(int capacity) { _capacity = capacity; State = TransportState.Connected; Error = TransportError.None; }
        /// <summary>Gets transport lifecycle state.</summary>
        public TransportState State { get; private set; }
        /// <summary>Gets the immutable cause of the current transport state.</summary>
        public TransportError Error { get; private set; }
        /// <summary>Creates a connected pair with a bounded receive queue.</summary>
        public static void CreatePair(int queueCapacity, out MemoryTransport left, out MemoryTransport right) { if (queueCapacity <= 0) throw new ArgumentOutOfRangeException(nameof(queueCapacity)); left = new MemoryTransport(queueCapacity); right = new MemoryTransport(queueCapacity); left._peer = right; right._peer = left; }
        /// <inheritdoc />
        public void BeginStep(ulong stepIndex) { }
        /// <inheritdoc />
        public bool TrySend(Channel channel, ref PacketLease packet)
        {
            var owned = PacketLease.Transfer(ref packet);
            if (State != TransportState.Connected)
            {
                owned.Dispose();
                owned = default;
                return false;
            }

            var peer = _peer;
            if (peer == null || peer.State != TransportState.Connected)
            {
                owned.Dispose();
                owned = default;
                return false;
            }

            if (channel != Channel.ReliableOrdered && channel != Channel.UnreliableSequenced)
            {
                owned.Dispose();
                owned = default;
                TerminatePair(
                    TransportState.Faulted,
                    TransportError.InvalidPacket,
                    TransportState.Closed,
                    TransportError.RemoteClosed);
                return false;
            }

            uint sequence = 0;
            if (channel == Channel.UnreliableSequenced)
            {
                if (owned.Length < PacketHeader.Size || !PacketHeader.TryRead(owned.Span, out var header) ||
                    header.Kind != PacketKind.FullSnapshot || header.PacketSequence == 0 ||
                    owned.Length != PacketHeader.Size + header.WirePayloadLength)
                {
                    owned.Dispose();
                    owned = default;
                    TerminatePair(
                        TransportState.Faulted,
                        TransportError.InvalidPacket,
                        TransportState.Closed,
                        TransportError.RemoteClosed);
                    return false;
                }

                sequence = header.PacketSequence;
                if (sequence <= peer._latestUnreliable)
                {
                    owned.Dispose();
                    owned = default;
                    return false;
                }
            }

            Item queuedItem = null;
            LinkedListNode<Item> queuedNode = null;
            try
            {
                queuedItem = new Item(channel, ref owned);
                queuedNode = new LinkedListNode<Item>(queuedItem);
            }
            catch
            {
                if (queuedItem != null) queuedItem.Dispose();
                if (owned.IsValid) { owned.Dispose(); owned = default; }
                throw;
            }

            if (channel == Channel.UnreliableSequenced)
            {
                var node = peer._incoming.First;
                while (node != null)
                {
                    var next = node.Next;
                    if (node.Value.Channel == Channel.UnreliableSequenced)
                    {
                        peer._incoming.Remove(node);
                        node.Value.Dispose();
                    }
                    node = next;
                }
            }

            if (peer._incoming.Count >= peer._capacity)
            {
                queuedItem.Dispose();
                if (channel == Channel.ReliableOrdered)
                    TerminatePair(
                        TransportState.Faulted,
                        TransportError.QueueOverflow,
                        TransportState.Faulted,
                        TransportError.QueueOverflow);
                return false;
            }
            peer._incoming.AddLast(queuedNode);
            if (channel == Channel.UnreliableSequenced) peer._latestUnreliable = sequence;
            return true;
        }
        /// <inheritdoc />
        public bool TryReceive(out Channel channel, out PacketLease packet)
        {
            channel = default;
            packet = default;
            if (State != TransportState.Connected || _incoming.Count == 0) return false;
            var item = _incoming.First.Value;
            packet = item.Take();
            _incoming.RemoveFirst();
            channel = item.Channel;
            return true;
        }
        /// <summary>Disposes the transport and drains every queued lease.</summary>
        public void Dispose()
        {
            if (State == TransportState.Disposed) return;
            if (State == TransportState.Connected)
            {
                TerminatePair(
                    TransportState.Disposed,
                    TransportError.Disposed,
                    TransportState.Closed,
                    TransportError.RemoteClosed);
                return;
            }

            _peer = null;
            State = TransportState.Disposed;
            Error = TransportError.Disposed;
        }

        private void TerminatePair(
            TransportState localState,
            TransportError localError,
            TransportState peerState,
            TransportError peerError)
        {
            var peer = _peer;
            _peer = null;
            if (peer != null) peer._peer = null;
            TransitionFromConnected(localState, localError);
            peer?.TransitionFromConnected(peerState, peerError);
        }

        private void TransitionFromConnected(TransportState state, TransportError error)
        {
            if (State != TransportState.Connected) return;
            State = state;
            Error = error;
            Drain();
        }

        private void Drain() { while (_incoming.Count > 0) { var item = _incoming.First.Value; _incoming.RemoveFirst(); item.Dispose(); } }

        private sealed class Item : IDisposable
        {
            private PacketLease _packet;
            internal Item(Channel channel, ref PacketLease packet) { Channel = channel; _packet = PacketLease.Transfer(ref packet); }
            internal Channel Channel { get; }
            internal PacketLease Take() => PacketLease.Transfer(ref _packet);
            public void Dispose() { if (!_packet.IsValid) return; var packet = _packet; _packet = default; packet.Dispose(); }
        }
    }

    /// <summary>Transforms payload bytes within explicit decoded and encoded bounds.</summary>
    public interface IPayloadTransform
    {
        /// <summary>Gets the versioned transform identifier.</summary>
        byte Id { get; }
        /// <summary>Returns the maximum encoded bytes for a decoded length.</summary>
        int MaxEncodedLength(int decodedLength);
        /// <summary>Encodes one complete decoded payload.</summary>
        bool TryEncode(ReadOnlySpan<byte> decoded, Span<byte> destination, out int written);
        /// <summary>Decodes one complete encoded payload within the expected output bound.</summary>
        bool TryDecode(ReadOnlySpan<byte> encoded, Span<byte> destination, out int written);
    }

    /// <summary>Implements version-one transform zero as an exact bounded copy.</summary>
    public readonly struct NoOpTransform : IPayloadTransform
    {
        /// <inheritdoc />
        public byte Id => 0;
        /// <inheritdoc />
        public int MaxEncodedLength(int decodedLength) => decodedLength;
        /// <inheritdoc />
        public bool TryEncode(ReadOnlySpan<byte> decoded, Span<byte> destination, out int written) { if (decoded.Length > destination.Length) { written = 0; return false; } decoded.CopyTo(destination); written = decoded.Length; return true; }
        /// <inheritdoc />
        public bool TryDecode(ReadOnlySpan<byte> encoded, Span<byte> destination, out int written) { if (encoded.Length > destination.Length) { written = 0; return false; } encoded.CopyTo(destination); written = encoded.Length; return true; }
    }
}
