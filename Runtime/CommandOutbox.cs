using System;
using System.Buffers;
using FFS.Libraries.StaticEcs;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Reports whether a typed command entered bounded outbound retention.</summary>
    public enum EnqueueResult : byte
    {
        /// <summary>The command was retained for ordered transmission.</summary>
        Queued = 0,
        /// <summary>The current facade or session state cannot accept commands.</summary>
        Unavailable = 1,
        /// <summary>The command fits an empty outbox but not its current bounded state.</summary>
        Full = 2,
        /// <summary>The schema has no retained typed binding for the command.</summary>
        UnknownCommand = 3,
        /// <summary>The encoded command cannot fit the configured canonical batch capacity.</summary>
        TooLarge = 4,
        /// <summary>The retained codec failed within its complete registered payload bound.</summary>
        CodecFailed = 5,
        /// <summary>No further non-zero command sequence can be assigned.</summary>
        SequenceExhausted = 6
    }

    /// <summary>Retains bounded typed commands until cumulative acknowledgement.</summary>
    public sealed class CommandOutbox<TWorld> : IDisposable where TWorld : struct, IWorldType
    {
        private const int BatchPrefixSize = 4;
        private const int RecordPrefixSize = 32;

        private readonly Schema _schema;
        private readonly Entry[] _entries;
        private readonly int _byteCapacity;
        private byte[] _storage;
        private byte[] _scratch;
        private int _head;
        private int _count;
        private int _unsentCount;
        private int _bytes;
        private int _storageTail;
        private int _payloadBytes;
        private int _pendingCount;
        private uint _pendingThrough;
        private uint _lastSequence;
        private uint _lastSentSequence;
        private uint _acknowledgedSequence;
        private bool _disposed;

        /// <summary>Creates a bounded outbox for one session epoch.</summary>
        public CommandOutbox(
            Schema schema,
            int commandCapacity = ProtocolLimits.MaxCommandsPerBatch,
            int byteCapacity = ProtocolLimits.MaxWirePayloadBytes)
        {
            if (schema == null) throw new ArgumentNullException(nameof(schema));
            schema.EnsureWorld<TWorld>();
            if (commandCapacity <= 0 || commandCapacity > ProtocolLimits.MaxCommandsPerBatch)
                throw new ArgumentOutOfRangeException(nameof(commandCapacity));
            if (byteCapacity < BatchPrefixSize + RecordPrefixSize || byteCapacity > ProtocolLimits.MaxWirePayloadBytes)
                throw new ArgumentOutOfRangeException(nameof(byteCapacity));

            var maximum = 0;
            var entries = schema.RetainedEntries;
            for (var i = 0; i < entries.Length; i++)
            {
                var entry = entries[i];
                if (entry.Kind == SchemaKind.Command && entry.MaxPayload > maximum)
                    maximum = checked((int)entry.MaxPayload);
            }

            byte[] storage = null;
            byte[] scratch = null;
            try
            {
                storage = ArrayPool<byte>.Shared.Rent(byteCapacity);
                scratch = maximum == 0 ? Array.Empty<byte>() : ArrayPool<byte>.Shared.Rent(maximum);
                _schema = schema;
                _entries = new Entry[commandCapacity];
                _byteCapacity = byteCapacity;
                _storage = storage;
                _scratch = scratch;
                ScratchLength = maximum;
                storage = null;
                scratch = null;
            }
            finally
            {
                if (storage != null) ArrayPool<byte>.Shared.Return(storage);
                if (scratch != null && scratch.Length != 0) ArrayPool<byte>.Shared.Return(scratch);
            }
        }

        /// <summary>Gets the retained sent and unsent command count.</summary>
        public int Count { get { EnsureActive(); return _count; } }
        /// <summary>Gets the retained command count not yet marked sent.</summary>
        public int UnsentCount { get { EnsureActive(); return _unsentCount; } }
        /// <summary>Gets canonical retained batch bytes, including one batch prefix when non-empty.</summary>
        public int Bytes { get { EnsureActive(); return _bytes; } }
        /// <summary>Gets the last assigned command sequence.</summary>
        public uint LastSequence { get { EnsureActive(); return _lastSequence; } }
        /// <summary>Gets the last sequence successfully marked sent.</summary>
        public uint LastSentSequence { get { EnsureActive(); return _lastSentSequence; } }
        /// <summary>Gets the last cumulatively acknowledged sequence.</summary>
        public uint AcknowledgedSequence { get { EnsureActive(); return _acknowledgedSequence; } }

        private int ScratchLength { get; }

        /// <summary>Encodes and retains one typed command without mutating state on failure.</summary>
        public EnqueueResult Enqueue<T>(in T command, uint clientTick) where T : unmanaged
        {
            EnsureActive();
            if (!_schema.TryGetCommand<T>(out var schemaEntry, out var invoker)) return EnqueueResult.UnknownCommand;
            if (_lastSequence == uint.MaxValue) return EnqueueResult.SequenceExhausted;

            var destination = _scratch.AsSpan(0, checked((int)schemaEntry.MaxPayload));
            if (!invoker.TryWrite(in command, destination, out var written) || written < 0 || written > destination.Length)
                return EnqueueResult.CodecFailed;

            var recordBytes = checked(RecordPrefixSize + written);
            if (BatchPrefixSize + recordBytes > _byteCapacity) return EnqueueResult.TooLarge;
            var nextBytes = _count == 0 ? BatchPrefixSize + recordBytes : checked(_bytes + recordBytes);
            if (_count == _entries.Length || nextBytes > _byteCapacity) return EnqueueResult.Full;

            var sequence = _lastSequence + 1;
            var entryIndex = PhysicalIndex(_count);
            var offset = _storageTail;
            CopyToStorage(destination.Slice(0, written));
            _entries[entryIndex] = new Entry(schemaEntry.TypeId, schemaEntry.Version, sequence, clientTick, offset, written);
            _count++;
            _unsentCount++;
            _bytes = nextBytes;
            _lastSequence = sequence;
            return EnqueueResult.Queued;
        }

        /// <summary>Builds an owned canonical batch for the frozen pending range.</summary>
        public bool TryBuild(out PacketLease payload, out uint throughSequence)
        {
            EnsureActive();
            payload = default;
            throughSequence = 0;
            var buildCount = _pendingCount == 0 ? _unsentCount : _pendingCount;
            if (buildCount == 0) return false;

            var first = _count - _unsentCount;
            var length = BatchPrefixSize;
            for (var i = 0; i < buildCount; i++)
                length = checked(length + RecordPrefixSize + EntryAt(first + i).PayloadLength);

            var lease = PacketLease.Rent(length);
            try
            {
                lease.SetLength(length);
                var bytes = lease.Span;
                Write16(bytes, 0, checked((ushort)buildCount));
                Write16(bytes, 2, 0);
                var position = BatchPrefixSize;
                for (var i = 0; i < buildCount; i++)
                {
                    ref readonly var entry = ref EntryAt(first + i);
                    entry.TypeId.WriteBytes(bytes.Slice(position, 16));
                    Write16(bytes, position + 16, entry.Version);
                    Write16(bytes, position + 18, 0);
                    Hashing.Write32(bytes, position + 20, entry.Sequence);
                    Hashing.Write32(bytes, position + 24, entry.ClientTick);
                    Hashing.Write32(bytes, position + 28, checked((uint)entry.PayloadLength));
                    CopyFromStorage(entry.PayloadOffset, bytes.Slice(position + RecordPrefixSize, entry.PayloadLength));
                    position += RecordPrefixSize + entry.PayloadLength;
                }

                var through = EntryAt(first + buildCount - 1).Sequence;
                payload = PacketLease.Transfer(ref lease);
                throughSequence = through;
                if (_pendingCount == 0)
                {
                    _pendingCount = buildCount;
                    _pendingThrough = through;
                }
                return true;
            }
            finally
            {
                if (lease.IsValid)
                {
                    lease.Dispose();
                    lease = default;
                }
            }
        }

        /// <summary>Marks the exact frozen pending range as successfully sent.</summary>
        public void MarkSent(uint throughSequence)
        {
            EnsureActive();
            if (_pendingCount == 0 || throughSequence == 0 || throughSequence != _pendingThrough)
                throw new InvalidOperationException("Only the exact frozen pending range can be marked sent.");
            _unsentCount -= _pendingCount;
            _lastSentSequence = _pendingThrough;
            _pendingCount = 0;
            _pendingThrough = 0;
        }

        /// <summary>Applies a cumulative acknowledgement when it does not exceed sent state.</summary>
        public bool Acknowledge(uint sequence)
        {
            EnsureActive();
            if (sequence > _lastSentSequence) return false;
            if (sequence == 0 || sequence <= _acknowledgedSequence) return true;

            var sentCount = _count - _unsentCount;
            var removed = 0;
            while (removed < sentCount)
            {
                ref readonly var entry = ref _entries[_head];
                if (entry.Sequence > sequence) break;
                RemoveHead();
                removed++;
            }
            _acknowledgedSequence = sequence;
            return true;
        }

        /// <summary>Releases retained storage without affecting independently built leases.</summary>
        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            var storage = _storage;
            var scratch = _scratch;
            _storage = null;
            _scratch = null;
            Array.Clear(_entries, 0, _entries.Length);
            _head = 0;
            _count = 0;
            _unsentCount = 0;
            _bytes = 0;
            _storageTail = 0;
            _payloadBytes = 0;
            _pendingCount = 0;
            _pendingThrough = 0;
            ArrayPool<byte>.Shared.Return(storage);
            if (ScratchLength > 0) ArrayPool<byte>.Shared.Return(scratch);
        }

        internal void ForceLastSequenceForTests(uint sequence)
        {
            EnsureActive();
            if (_count != 0 || _lastSequence != 0 || _lastSentSequence != 0 || _acknowledgedSequence != 0 || _pendingCount != 0)
                throw new InvalidOperationException("Sequence exhaustion can only be forced on a fresh empty outbox.");
            _lastSequence = sequence;
        }

        private int PhysicalIndex(int logicalIndex) => (_head + logicalIndex) % _entries.Length;
        private ref readonly Entry EntryAt(int logicalIndex) => ref _entries[PhysicalIndex(logicalIndex)];

        private void RemoveHead()
        {
            ref var entry = ref _entries[_head];
            _payloadBytes -= entry.PayloadLength;
            _bytes -= RecordPrefixSize + entry.PayloadLength;
            entry = default;
            _head = (_head + 1) % _entries.Length;
            _count--;
            if (_count != 0) return;
            _bytes = 0;
            _storageTail = 0;
            _payloadBytes = 0;
        }

        private void CopyToStorage(ReadOnlySpan<byte> source)
        {
            if (source.IsEmpty) return;
            var first = Math.Min(source.Length, _byteCapacity - _storageTail);
            source.Slice(0, first).CopyTo(_storage.AsSpan(_storageTail, first));
            var remaining = source.Length - first;
            if (remaining > 0) source.Slice(first).CopyTo(_storage.AsSpan(0, remaining));
            _storageTail = (_storageTail + source.Length) % _byteCapacity;
            _payloadBytes += source.Length;
        }

        private void CopyFromStorage(int offset, Span<byte> destination)
        {
            if (destination.IsEmpty) return;
            var first = Math.Min(destination.Length, _byteCapacity - offset);
            _storage.AsSpan(offset, first).CopyTo(destination.Slice(0, first));
            var remaining = destination.Length - first;
            if (remaining > 0) _storage.AsSpan(0, remaining).CopyTo(destination.Slice(first));
        }

        private void EnsureActive()
        {
            if (_disposed) throw new ObjectDisposedException(nameof(CommandOutbox<TWorld>));
        }

        private static void Write16(Span<byte> bytes, int offset, ushort value)
        {
            bytes[offset] = (byte)value;
            bytes[offset + 1] = (byte)(value >> 8);
        }

        private readonly struct Entry
        {
            internal Entry(TypeId typeId, ushort version, uint sequence, uint clientTick, int payloadOffset, int payloadLength)
            {
                TypeId = typeId;
                Version = version;
                Sequence = sequence;
                ClientTick = clientTick;
                PayloadOffset = payloadOffset;
                PayloadLength = payloadLength;
            }

            internal TypeId TypeId { get; }
            internal ushort Version { get; }
            internal uint Sequence { get; }
            internal uint ClientTick { get; }
            internal int PayloadOffset { get; }
            internal int PayloadLength { get; }
        }
    }
}
