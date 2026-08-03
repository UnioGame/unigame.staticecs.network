using System;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Reproduces a sealed complete call transcript with exact call-order validation.</summary>
    public sealed class ReplayTransport : ITransport, ISteppedTransport
    {
        private readonly ReplayTape _tape;
        private int _index;
        private ulong _currentStep = ulong.MaxValue;
        private bool _mismatched;
        private bool _disposed;

        /// <summary>Atomically borrows a sealed complete tape.</summary>
        public ReplayTransport(ReplayTape tape)
        {
            if (tape == null) throw new ArgumentNullException(nameof(tape));
            tape.Borrow();
            _tape = tape;
            State = TransportState.Connected;
            Error = TransportError.None;
        }

        /// <summary>Gets the replayed transport lifecycle state.</summary>
        public TransportState State { get; private set; }
        /// <summary>Gets the replayed transport error.</summary>
        public TransportError Error { get; private set; }

        /// <summary>Consumes one exactly matching recorded step call.</summary>
        public void BeginStep(ulong stepIndex)
        {
            EnsureActive();
            var record = Peek();
            if (record.Tag != 1 || record.Step != stepIndex) Mismatch();
            Consume(record);
            _currentStep = stepIndex;
        }

        /// <summary>Consumes one exactly matching recorded outbound call and packet.</summary>
        public bool TrySend(Channel channel, ref PacketLease packet)
        {
            EnsureActive();
            var record = Peek();
            if (record.Tag != 2 || record.Step != _currentStep || record.Channel != (byte)channel ||
                !packet.Span.SequenceEqual(record.Payload))
                Mismatch();
            if (record.Flags == 1)
            {
                var owned = PacketLease.Transfer(ref packet);
                owned.Dispose();
            }
            Consume(record);
            return record.Flags == 1;
        }

        /// <summary>Consumes one recorded inbound call and returns independently owned packet bytes.</summary>
        public bool TryReceive(out Channel channel, out PacketLease packet)
        {
            EnsureActive();
            var record = Peek();
            if (record.Tag != 3 || record.Step != _currentStep) Mismatch();
            Consume(record);
            if (record.Flags == 0)
            {
                channel = default;
                packet = default;
                return false;
            }
            channel = (Channel)record.Channel;
            packet = PacketLease.Rent(record.Payload.Length);
            try
            {
                record.Payload.AsSpan().CopyTo(packet.CapacitySpan);
                packet.SetLength(record.Payload.Length);
                return true;
            }
            catch
            {
                if (packet.IsValid) packet.Dispose();
                packet = default;
                throw;
            }
        }

        /// <summary>Releases the tape borrow without taking tape ownership.</summary>
        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            var truncated = !_mismatched && _tape.RecordAt(_index) != null;
            try
            {
                if (truncated)
                {
                    State = TransportState.Faulted;
                    Error = TransportError.InvalidPacket;
                    throw new InvalidOperationException("The replay ended before the transcript was exhausted.");
                }
                State = TransportState.Disposed;
                Error = TransportError.Disposed;
            }
            finally
            {
                _tape.ReleaseBorrow();
            }
        }

        private ReplayTape.Record Peek()
        {
            var record = _tape.RecordAt(_index);
            if (record == null) Mismatch();
            return record;
        }

        private void Consume(ReplayTape.Record record)
        {
            _index++;
            State = record.State;
            Error = record.Error;
        }

        private void Mismatch()
        {
            _mismatched = true;
            State = TransportState.Faulted;
            Error = TransportError.InvalidPacket;
            throw new InvalidOperationException("The replay call does not match the transcript.");
        }

        private void EnsureActive()
        {
            if (_disposed) throw new ObjectDisposedException(nameof(ReplayTransport));
        }
    }
}
