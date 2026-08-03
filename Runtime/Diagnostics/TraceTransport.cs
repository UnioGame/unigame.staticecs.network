using System;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Records exact transport calls and packet bytes without changing wrapped outcomes.</summary>
    public sealed class TraceTransport : ITransport, ISteppedTransport
    {
        private readonly ITransport _inner;
        private readonly ISteppedTransport _stepped;
        private readonly ReplayTape _tape;
        private ulong _currentStep = ulong.MaxValue;
        private bool _disposed;

        /// <summary>Claims an open tape and takes ownership of a connected stepped transport.</summary>
        public TraceTransport(ITransport inner, ReplayTape tape)
        {
            if (inner == null) throw new ArgumentNullException(nameof(inner));
            if (tape == null) throw new ArgumentNullException(nameof(tape));
            if (inner is not ISteppedTransport stepped)
                throw new ArgumentException("The traced transport must be stepped.", nameof(inner));
            if (inner.State != TransportState.Connected || inner.Error != TransportError.None)
                throw new InvalidOperationException("The traced transport must initially be connected and error-free.");
            tape.ClaimWriter();
            _inner = inner;
            _stepped = stepped;
            _tape = tape;
        }

        /// <summary>Gets the wrapped transport lifecycle state.</summary>
        public TransportState State => _inner.State;
        /// <summary>Gets the wrapped transport error.</summary>
        public TransportError Error => _inner.Error;

        /// <summary>Records one exact step barrier call.</summary>
        public void BeginStep(ulong stepIndex)
        {
            EnsureActive();
            try
            {
                _stepped.BeginStep(stepIndex);
                _currentStep = stepIndex;
                _tape.Append(1, 1, byte.MaxValue, _inner.State, _inner.Error, stepIndex, null);
            }
            catch
            {
                _tape.MarkIncomplete();
                _tape.Append(1, 2, byte.MaxValue, _inner.State, _inner.Error, stepIndex, null);
                throw;
            }
        }

        /// <summary>Records exact outbound bytes before forwarding ownership.</summary>
        public bool TrySend(Channel channel, ref PacketLease packet)
        {
            EnsureActive();
            var bytes = packet.Span.ToArray();
            try
            {
                var result = _inner.TrySend(channel, ref packet);
                _tape.Append(2, result ? (byte)1 : (byte)0, (byte)channel,
                    _inner.State, _inner.Error, _currentStep, bytes);
                return result;
            }
            catch
            {
                _tape.MarkIncomplete();
                _tape.Append(2, 2, (byte)channel, _inner.State, _inner.Error, _currentStep, bytes);
                throw;
            }
        }

        /// <summary>Records exact inbound bytes after forwarding one receive attempt.</summary>
        public bool TryReceive(out Channel channel, out PacketLease packet)
        {
            EnsureActive();
            channel = default;
            packet = default;
            try
            {
                var result = _inner.TryReceive(out channel, out packet);
                var bytes = result ? packet.Span.ToArray() : null;
                _tape.Append(3, result ? (byte)1 : (byte)0, result ? (byte)channel : byte.MaxValue,
                    _inner.State, _inner.Error, _currentStep, bytes);
                return result;
            }
            catch
            {
                if (packet.IsValid) packet.Dispose();
                channel = default;
                packet = default;
                _tape.MarkIncomplete();
                _tape.Append(3, 2, byte.MaxValue, _inner.State, _inner.Error, _currentStep, null);
                throw;
            }
        }

        /// <summary>Disposes the owned transport and releases the tape claim even when disposal throws.</summary>
        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            try { _inner.Dispose(); }
            catch { _tape.MarkIncomplete(); throw; }
            finally { _tape.ReleaseWriter(); }
        }

        private void EnsureActive()
        {
            if (_disposed) throw new ObjectDisposedException(nameof(TraceTransport));
        }
    }
}
