using System;
using System.Collections.Generic;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Exposes complete wire packet limits for one transport.</summary>
    public interface INetworkTransportCapabilities
    {
        /// <summary>Gets the maximum complete reliable packet bytes, including <see cref="PacketHeader"/>.</summary>
        int MaxReliablePayloadBytes { get; }
        /// <summary>Gets the maximum complete unreliable packet bytes, including <see cref="PacketHeader"/>.</summary>
        int MaxUnreliablePayloadBytes { get; }
    }

    /// <summary>Defines an exact-packet transport with transport-owned connection identity.</summary>
    public interface INetworkTransport : INetworkTransportCapabilities, IDisposable
    {
        /// <summary>Gets transport-owned identity.</summary>
        ConnectionId Connection { get; }
        /// <summary>Consumes the packet lease on every result; <c>true</c> means accepted locally, not delivered remotely.</summary>
        bool TrySend(NetworkBufferLease packet);
        /// <summary>Receives the next exact packet.</summary>
        bool TryReceive(out NetworkBufferLease packet);
    }

    /// <summary>Provides deterministic in-memory transport for tests and editor sandboxes.</summary>
    public sealed class MemoryNetworkTransport : INetworkTransport
    {
        private readonly Queue<NetworkBufferLease> _incoming;
        private readonly Queue<NetworkBufferLease> _outgoing;
        private bool _disposed;
        internal MemoryNetworkTransport(ConnectionId connection,
            Queue<NetworkBufferLease> incoming, Queue<NetworkBufferLease> outgoing)
        { Connection = connection; _incoming = incoming; _outgoing = outgoing; }
        /// <inheritdoc />
        public ConnectionId Connection { get; }
        /// <inheritdoc />
        public int MaxReliablePayloadBytes =>
            PacketHeader.Size + ProtocolLimits.MaxWirePayloadBytes;
        /// <inheritdoc />
        public int MaxUnreliablePayloadBytes =>
            PacketHeader.Size + ProtocolLimits.MaxWirePayloadBytes;
        /// <inheritdoc />
        public bool TrySend(NetworkBufferLease packet)
        {
            if (_disposed || packet == null)
            {
                packet?.Dispose();
                return false;
            }
            _outgoing.Enqueue(packet);
            return true;
        }
        /// <inheritdoc />
        public bool TryReceive(out NetworkBufferLease packet)
        {
            if (!_disposed && _incoming.Count > 0) { packet = _incoming.Dequeue(); return true; }
            packet = null;
            return false;
        }
        /// <inheritdoc />
        public void Dispose()
        {
            if (_disposed)
                return;
            _disposed = true;
            while (_incoming.Count > 0)
                _incoming.Dequeue().Dispose();
        }

        /// <summary>Creates two connected endpoints for one transport connection.</summary>
        public static void CreatePair(ConnectionId connection, out MemoryNetworkTransport client, out MemoryNetworkTransport server)
        {
            var clientIncoming = new Queue<NetworkBufferLease>();
            var serverIncoming = new Queue<NetworkBufferLease>();
            client = new MemoryNetworkTransport(connection, clientIncoming, serverIncoming);
            server = new MemoryNetworkTransport(connection, serverIncoming, clientIncoming);
        }
    }

    /// <summary>Creates a deterministic two-client server mock with isolated connection queues.</summary>
    public sealed class TwoClientNetworkMock : IDisposable
    {
        /// <summary>Creates two independent transport pairs.</summary>
        public TwoClientNetworkMock()
        {
            MemoryNetworkTransport.CreatePair(new ConnectionId(1), out var clientA, out var serverA);
            MemoryNetworkTransport.CreatePair(new ConnectionId(2), out var clientB, out var serverB);
            ClientA = clientA; ServerA = serverA; ClientB = clientB; ServerB = serverB;
        }
        /// <summary>Gets first client endpoint.</summary>
        public MemoryNetworkTransport ClientA { get; }
        /// <summary>Gets first server endpoint.</summary>
        public MemoryNetworkTransport ServerA { get; }
        /// <summary>Gets second client endpoint.</summary>
        public MemoryNetworkTransport ClientB { get; }
        /// <summary>Gets second server endpoint.</summary>
        public MemoryNetworkTransport ServerB { get; }
        /// <inheritdoc />
        public void Dispose() { ClientA.Dispose(); ServerA.Dispose(); ClientB.Dispose(); ServerB.Dispose(); }
    }
}
