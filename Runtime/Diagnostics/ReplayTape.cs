using System;
using System.Collections.Generic;
using System.IO;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Owns a bounded privacy-sensitive transcript of complete transport calls and packet bytes.</summary>
    public sealed class ReplayTape : IDisposable
    {
        private const int FileHeaderSize = 40;
        private const int RecordHeaderSize = 24;
        private readonly object _sync = new();
        private readonly long _byteCapacity;
        private readonly List<Record> _records = new();
        private TapeState _state;
        private long _bytes;
        private ulong _dropped;
        private bool _complete = true;

        /// <summary>Creates an open bounded transcript.</summary>
        public ReplayTape(long byteCapacity)
        {
            if (byteCapacity < RecordHeaderSize || byteCapacity > int.MaxValue)
                throw new ArgumentOutOfRangeException(nameof(byteCapacity));
            _byteCapacity = byteCapacity;
        }

        /// <summary>Gets whether every attempted transcript record was retained.</summary>
        public bool IsComplete { get { lock (_sync) { EnsurePublic(); return _complete; } } }
        /// <summary>Gets whether the transcript is immutable and available for save or replay.</summary>
        public bool IsSealed { get { lock (_sync) { EnsurePublic(); return _state == TapeState.Sealed; } } }
        /// <summary>Gets the number of records rejected by the byte budget.</summary>
        public ulong Dropped { get { lock (_sync) { EnsurePublic(); return _dropped; } } }
        /// <summary>Gets charged record-section bytes.</summary>
        public long Bytes { get { lock (_sync) { EnsurePublic(); return _bytes; } } }

        /// <summary>Seals an unclaimed open transcript.</summary>
        public void Seal()
        {
            lock (_sync)
            {
                EnsurePublic();
                if (_state == TapeState.Sealed) return;
                if (_state != TapeState.Open) throw new InvalidOperationException("An active tape cannot be sealed.");
                _state = TapeState.Sealed;
            }
        }

        /// <summary>Writes a sealed complete transcript using the version-one little-endian format.</summary>
        public void Save(Stream output)
        {
            byte[] section;
            byte[] header;
            lock (_sync)
            {
                EnsurePublic();
                if (_state != TapeState.Sealed || !_complete)
                    throw new InvalidOperationException("Only a sealed complete tape can be saved.");
                if (_bytes > int.MaxValue)
                    throw new InvalidOperationException("The tape is too large for the version-one format.");

                section = new byte[(int)_bytes];
                var offset = 0;
                for (var i = 0; i < _records.Count; i++) offset += WriteRecord(_records[i], section.AsSpan(offset));
                header = new byte[FileHeaderSize];
                header[0] = 0x53; header[1] = 0x45; header[2] = 0x43; header[3] = 0x53;
                header[4] = 0x4e; header[5] = 0x45; header[6] = 0x54; header[7] = 0x31;
                Hashing.Write16(header, 8, 1); Hashing.Write16(header, 10, FileHeaderSize);
                Hashing.Write32(header, 12, (uint)_records.Count); Hashing.Write64(header, 16, (ulong)_bytes);
                Hashing.Write64(header, 24, Hashing.XxHash64(section)); Hashing.Write32(header, 32, 1);
            }
            if (output == null) throw new ArgumentNullException(nameof(output));
            if (!output.CanWrite) throw new ArgumentException("The output stream must be writable.", nameof(output));
            output.Write(header, 0, header.Length);
            output.Write(section, 0, section.Length);
        }

        /// <summary>Loads and transactionally validates one complete version-one transcript.</summary>
        public static ReplayTape Load(Stream input, long byteCapacity)
        {
            if (input == null) throw new ArgumentNullException(nameof(input));
            if (!input.CanRead) throw new ArgumentException("The input stream must be readable.", nameof(input));
            if (byteCapacity < RecordHeaderSize || byteCapacity > int.MaxValue)
                throw new ArgumentOutOfRangeException(nameof(byteCapacity));
            var header = new byte[FileHeaderSize];
            ReadExactly(input, header, 0, header.Length);
            if (header[0] != 0x53 || header[1] != 0x45 || header[2] != 0x43 || header[3] != 0x53 ||
                header[4] != 0x4e || header[5] != 0x45 || header[6] != 0x54 || header[7] != 0x31 ||
                Read16(header, 8) != 1 || Read16(header, 10) != FileHeaderSize ||
                Hashing.Read32(header, 32) != 1 || Hashing.Read32(header, 36) != 0)
                throw new InvalidDataException("Invalid replay header.");
            var count = Hashing.Read32(header, 12);
            var bytes64 = Hashing.Read64(header, 16);
            if (bytes64 > (ulong)byteCapacity || bytes64 > int.MaxValue || (ulong)count * RecordHeaderSize > bytes64)
                throw new InvalidDataException("Replay record bounds are invalid.");
            var section = new byte[(int)bytes64];
            ReadExactly(input, section, 0, section.Length);
            if (input.ReadByte() != -1) throw new InvalidDataException("Replay contains trailing bytes.");
            if (Hashing.XxHash64(section) != Hashing.Read64(header, 24))
                throw new InvalidDataException("Replay checksum mismatch.");

            if (count > int.MaxValue) throw new InvalidDataException("Replay record count is too large.");
            var records = new List<Record>((int)count);
            var offset = 0;
            var currentStep = ulong.MaxValue;
            try
            {
                for (uint i = 0; i < count; i++)
                {
                    var record = ReadRecord(section, ref offset);
                    if (record.Tag == 1) currentStep = record.Step;
                    else if (record.Step != currentStep)
                        throw new InvalidDataException("Replay call step does not match the last successful barrier.");
                    records.Add(record);
                }
                if (offset != section.Length) throw new InvalidDataException("Replay section length mismatch.");
                var tape = new ReplayTape(byteCapacity) { _state = TapeState.Sealed, _bytes = section.Length };
                tape._records.AddRange(records);
                return tape;
            }
            catch
            {
                records.Clear();
                throw;
            }
        }

        /// <summary>Requests disposal and defers cleanup while a trace or replay owns an internal claim.</summary>
        public void Dispose()
        {
            lock (_sync)
            {
                if (_state == TapeState.Disposed || _state == TapeState.DisposeRequested) return;
                if (_state == TapeState.Recording || _state == TapeState.Borrowed)
                {
                    _state = TapeState.DisposeRequested;
                    return;
                }
                Cleanup();
            }
        }

        internal void ClaimWriter()
        {
            lock (_sync)
            {
                EnsurePublic();
                if (_state != TapeState.Open) throw new InvalidOperationException("The tape is not open.");
                _state = TapeState.Recording;
            }
        }

        internal void ReleaseWriter()
        {
            lock (_sync)
            {
                if (_state == TapeState.DisposeRequested) Cleanup();
                else if (_state == TapeState.Recording) _state = TapeState.Sealed;
            }
        }

        internal void MarkIncomplete()
        {
            lock (_sync) _complete = false;
        }

        internal void Append(byte tag, byte flags, byte channel, TransportState state, TransportError error,
            ulong step, byte[] payload)
        {
            lock (_sync)
            {
                if (_state != TapeState.Recording && _state != TapeState.DisposeRequested)
                    throw new InvalidOperationException("The tape has no active writer.");
                var length = payload?.Length ?? 0;
                if (!ValidRecord(tag, flags, channel, state, error, length)) _complete = false;
                var charge = checked((long)RecordHeaderSize + length);
                if (charge > _byteCapacity - _bytes)
                {
                    _complete = false;
                    _dropped++;
                    return;
                }
                _records.Add(new Record(tag, flags, channel, state, error, step, payload ?? Array.Empty<byte>()));
                _bytes += charge;
            }
        }

        internal void Borrow()
        {
            lock (_sync)
            {
                EnsurePublic();
                if (_state != TapeState.Sealed || !_complete) throw new InvalidOperationException("Replay requires a sealed complete tape.");
                _state = TapeState.Borrowed;
            }
        }

        internal Record RecordAt(int index)
        {
            lock (_sync)
            {
                if (_state != TapeState.Borrowed && _state != TapeState.DisposeRequested)
                    throw new InvalidOperationException("The tape is not borrowed.");
                return index < _records.Count ? _records[index] : null;
            }
        }

        internal void ReleaseBorrow()
        {
            lock (_sync)
            {
                if (_state == TapeState.DisposeRequested) Cleanup();
                else if (_state == TapeState.Borrowed) _state = TapeState.Sealed;
            }
        }

        private void EnsurePublic()
        {
            if (_state == TapeState.DisposeRequested || _state == TapeState.Disposed)
                throw new ObjectDisposedException(nameof(ReplayTape));
        }

        private void Cleanup()
        {
            _records.Clear();
            _bytes = 0;
            _state = TapeState.Disposed;
        }

        private static int WriteRecord(Record value, Span<byte> destination)
        {
            var length = value.Payload.Length;
            destination.Slice(0, RecordHeaderSize).Clear();
            destination[0] = value.Tag; destination[1] = value.Flags; destination[2] = value.Channel;
            destination[3] = (byte)value.State; destination[4] = (byte)value.Error;
            Hashing.Write64(destination, 8, value.Step); Hashing.Write32(destination, 16, (uint)length);
            value.Payload.AsSpan().CopyTo(destination.Slice(RecordHeaderSize));
            return RecordHeaderSize + length;
        }

        private static bool ValidRecord(byte tag, byte flags, byte channel, TransportState state,
            TransportError error, int length)
        {
            if (tag < 1 || tag > 3 || flags > 2 ||
                state < TransportState.Connected || state > TransportState.Closed ||
                error < TransportError.None || error > TransportError.Disposed ||
                length < 0 || length > ProtocolLimits.MaxWirePayloadBytes + PacketHeader.Size)
                return false;
            if (tag == 1) return (flags == 1 || flags == 2) && channel == byte.MaxValue && length == 0;
            if (tag == 2) return channel <= 1;
            if (flags == 0) return channel == byte.MaxValue && length == 0;
            if (flags == 1) return channel <= 1;
            return channel == byte.MaxValue && length == 0;
        }

        private static Record ReadRecord(byte[] section, ref int offset)
        {
            if (section.Length - offset < RecordHeaderSize) throw new InvalidDataException("Truncated replay record.");
            var span = section.AsSpan(offset);
            var tag = span[0]; var flags = span[1]; var channel = span[2];
            var state = (TransportState)span[3]; var error = (TransportError)span[4];
            if (span[5] != 0 || span[6] != 0 || span[7] != 0 || Hashing.Read32(span, 20) != 0 ||
                tag < 1 || tag > 3 || flags > 1 || state < TransportState.Connected || state > TransportState.Closed ||
                error < TransportError.None || error > TransportError.Disposed)
                throw new InvalidDataException("Invalid replay record fields.");
            var step = Hashing.Read64(span, 8);
            var length = Hashing.Read32(span, 16);
            if (length > ProtocolLimits.MaxWirePayloadBytes + PacketHeader.Size || length > section.Length - offset - RecordHeaderSize)
                throw new InvalidDataException("Invalid replay payload length.");
            if (tag == 1 && (flags != 1 || channel != byte.MaxValue || length != 0) ||
                tag == 2 && channel > 1 ||
                tag == 3 && (flags == 0 ? channel != byte.MaxValue || length != 0 : channel > 1))
                throw new InvalidDataException("Invalid replay tag invariant.");
            var payload = length == 0 ? Array.Empty<byte>() : span.Slice(RecordHeaderSize, (int)length).ToArray();
            offset = checked(offset + RecordHeaderSize + (int)length);
            return new Record(tag, flags, channel, state, error, step, payload);
        }

        private static void ReadExactly(Stream stream, byte[] buffer, int offset, int count)
        {
            while (count != 0)
            {
                var read = stream.Read(buffer, offset, count);
                if (read <= 0) throw new EndOfStreamException();
                offset += read; count -= read;
            }
        }

        private static ushort Read16(ReadOnlySpan<byte> value, int offset) => (ushort)(value[offset] | value[offset + 1] << 8);

        internal sealed class Record
        {
            internal Record(byte tag, byte flags, byte channel, TransportState state, TransportError error, ulong step, byte[] payload)
            { Tag = tag; Flags = flags; Channel = channel; State = state; Error = error; Step = step; Payload = payload; }
            internal byte Tag { get; }
            internal byte Flags { get; }
            internal byte Channel { get; }
            internal TransportState State { get; }
            internal TransportError Error { get; }
            internal ulong Step { get; }
            internal byte[] Payload { get; }
        }

        private enum TapeState : byte { Open, Recording, Sealed, Borrowed, DisposeRequested, Disposed }
    }
}
