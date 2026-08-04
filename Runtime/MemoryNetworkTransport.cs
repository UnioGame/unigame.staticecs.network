using System;
using System.Collections.Generic;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Defines an exact-packet transport with transport-owned connection identity.</summary>
    public interface INetworkTransport : IDisposable
    {
        /// <summary>Gets transport-owned identity.</summary>
        ConnectionId Connection { get; }
        /// <summary>Sends an exact immutable packet copy.</summary>
        bool TrySend(byte[] packet);
        /// <summary>Receives the next exact packet.</summary>
        bool TryReceive(out byte[] packet);
    }

    /// <summary>Provides deterministic in-memory transport for tests and editor sandboxes.</summary>
    public sealed class MemoryNetworkTransport : INetworkTransport
    {
        private readonly Queue<byte[]> _incoming;
        private readonly Queue<byte[]> _outgoing;
        private bool _disposed;
        internal MemoryNetworkTransport(ConnectionId connection, Queue<byte[]> incoming, Queue<byte[]> outgoing) { Connection = connection; _incoming = incoming; _outgoing = outgoing; }
        /// <inheritdoc />
        public ConnectionId Connection { get; }
        /// <inheritdoc />
        public bool TrySend(byte[] packet)
        {
            if (_disposed || packet == null) return false;
            var copy = new byte[packet.Length];
            packet.CopyTo(copy, 0);
            _outgoing.Enqueue(copy);
            return true;
        }
        /// <inheritdoc />
        public bool TryReceive(out byte[] packet)
        {
            if (!_disposed && _incoming.Count > 0) { packet = _incoming.Dequeue(); return true; }
            packet = null;
            return false;
        }
        /// <inheritdoc />
        public void Dispose() => _disposed = true;

        /// <summary>Creates two connected endpoints for one transport connection.</summary>
        public static void CreatePair(ConnectionId connection, out MemoryNetworkTransport client, out MemoryNetworkTransport server)
        {
            var clientIncoming = new Queue<byte[]>();
            var serverIncoming = new Queue<byte[]>();
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
