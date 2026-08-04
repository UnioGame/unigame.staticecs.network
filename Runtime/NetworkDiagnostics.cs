using System;
using System.Globalization;
using System.IO;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Identifies a measured networking phase.</summary>
    public enum NetworkPhase : byte
    {
        /// <summary>Transport receive work.</summary>
        Receive,
        /// <summary>Wire decoding work.</summary>
        Decode,
        /// <summary>Command dispatch work.</summary>
        CommandDispatch,
        /// <summary>Snapshot application work.</summary>
        SnapshotApply,
        /// <summary>Snapshot capture work.</summary>
        SnapshotCapture,
        /// <summary>Transport send work.</summary>
        Send
    }
    /// <summary>Identifies trace event timing semantics.</summary>
    public enum NetworkTraceKind : byte
    {
        /// <summary>Begins a measured interval.</summary>
        Begin,
        /// <summary>Ends a measured interval.</summary>
        End,
        /// <summary>Records an instantaneous measurement.</summary>
        Point
    }
    /// <summary>Classifies a trace result without exposing private payload data.</summary>
    public enum NetworkResultCategory : byte
    {
        /// <summary>No result was recorded.</summary>
        None,
        /// <summary>The operation succeeded.</summary>
        Success,
        /// <summary>The operation was rejected.</summary>
        Rejected,
        /// <summary>The input was malformed.</summary>
        Malformed,
        /// <summary>The schema was incompatible.</summary>
        Schema,
        /// <summary>A negotiated limit was exceeded.</summary>
        Limits,
        /// <summary>The transport failed.</summary>
        Transport,
        /// <summary>A command policy rejected the operation.</summary>
        Policy,
        /// <summary>The Static ECS world rejected the operation.</summary>
        World
    }

    /// <summary>Receives bounded immutable network telemetry.</summary>
    public interface INetworkObserver
    {
        /// <summary>Observes one privacy-safe event.</summary>
        void Observe(in NetworkTraceEvent value);
    }

    /// <summary>Contains immutable privacy-safe endpoint telemetry.</summary>
    public readonly struct NetworkTraceEvent
    {
        /// <summary>Creates one immutable trace event.</summary>
        public NetworkTraceEvent(NetworkPhase phase, NetworkTraceKind kind, NetworkResultCategory result,
            NetworkRole role, uint connectionId, uint peerId, uint epoch, uint serverTick, uint targetTick,
            int bytes, int packets, int entities, int records, int commands, int queueSize, int historySize,
            int activeConnections, int activePeers, long timestamp)
        {
            Phase = phase; Kind = kind; Result = result; Role = role; ConnectionId = connectionId; PeerId = peerId;
            Epoch = epoch; ServerTick = serverTick; TargetTick = targetTick; Bytes = bytes; Packets = packets;
            Entities = entities; Records = records; Commands = commands; QueueSize = queueSize; HistorySize = historySize;
            ActiveConnections = activeConnections; ActivePeers = activePeers; Timestamp = timestamp;
        }
        /// <summary>Gets the measured phase.</summary>
        public NetworkPhase Phase { get; }
        /// <summary>Gets event timing semantics.</summary>
        public NetworkTraceKind Kind { get; }
        /// <summary>Gets the result category.</summary>
        public NetworkResultCategory Result { get; }
        /// <summary>Gets endpoint role.</summary>
        public NetworkRole Role { get; }
        /// <summary>Gets transport-owned connection id.</summary>
        public uint ConnectionId { get; }
        /// <summary>Gets server-assigned peer id.</summary>
        public uint PeerId { get; }
        /// <summary>Gets admitted epoch.</summary>
        public uint Epoch { get; }
        /// <summary>Gets authoritative simulation time.</summary>
        public uint ServerTick { get; }
        /// <summary>Gets command target time.</summary>
        public uint TargetTick { get; }
        /// <summary>Gets byte count.</summary>
        public int Bytes { get; }
        /// <summary>Gets packet count.</summary>
        public int Packets { get; }
        /// <summary>Gets entity count.</summary>
        public int Entities { get; }
        /// <summary>Gets record count.</summary>
        public int Records { get; }
        /// <summary>Gets command count.</summary>
        public int Commands { get; }
        /// <summary>Gets queue size.</summary>
        public int QueueSize { get; }
        /// <summary>Gets history size.</summary>
        public int HistorySize { get; }
        /// <summary>Gets active connection count.</summary>
        public int ActiveConnections { get; }
        /// <summary>Gets active peer count.</summary>
        public int ActivePeers { get; }
        /// <summary>Gets Stopwatch ticks.</summary>
        public long Timestamp { get; }
    }

    /// <summary>Buffers strict privacy-safe NDJSON without blocking the producer.</summary>
    public sealed class NetworkNdjsonLog : INetworkObserver, IDisposable
    {
        private readonly StreamWriter _writer;
        private readonly NetworkTraceEvent[] _events;
        private int _read;
        private int _count;
        private int _dropped;
        private bool _disposed;

        /// <summary>Creates a bounded logger over a caller-owned stream.</summary>
        public NetworkNdjsonLog(Stream stream, int capacity = 4096)
        {
            if (stream == null) throw new ArgumentNullException(nameof(stream));
            if (capacity < 1) throw new ArgumentOutOfRangeException(nameof(capacity));
            _writer = new StreamWriter(stream, new System.Text.UTF8Encoding(false), 1024, true);
            _events = new NetworkTraceEvent[capacity];
        }

        /// <inheritdoc />
        public void Observe(in NetworkTraceEvent value)
        {
            if (_disposed) return;
            if (_count == _events.Length) { _dropped++; return; }
            _events[(_read + _count) % _events.Length] = value;
            _count++;
        }

        /// <summary>Writes the retained prefix and one explicit gap for dropped events.</summary>
        public void Flush()
        {
            if (_disposed) throw new ObjectDisposedException(nameof(NetworkNdjsonLog));
            while (_count > 0)
            {
                var value = _events[_read];
                _read = (_read + 1) % _events.Length;
                _count--;
                _writer.WriteLine(ToJson(in value));
            }
            if (_dropped > 0) { _writer.WriteLine("{\"kind\":\"gap\",\"dropped\":" + _dropped.ToString(CultureInfo.InvariantCulture) + "}"); _dropped = 0; }
            _writer.Flush();
        }

        /// <inheritdoc />
        public void Dispose() { if (_disposed) return; Flush(); _disposed = true; _writer.Dispose(); }

        private static string ToJson(in NetworkTraceEvent v) =>
            "{\"phase\":\"" + v.Phase.ToString().ToLowerInvariant() + "\",\"kind\":\"" + v.Kind.ToString().ToLowerInvariant() +
            "\",\"result\":\"" + v.Result.ToString().ToLowerInvariant() + "\",\"role\":\"" + v.Role.ToString().ToLowerInvariant() +
            "\",\"connection\":" + v.ConnectionId + ",\"peer\":" + v.PeerId + ",\"epoch\":" + v.Epoch +
            ",\"serverTick\":" + v.ServerTick + ",\"targetTick\":" + v.TargetTick + ",\"bytes\":" + v.Bytes +
            ",\"packets\":" + v.Packets + ",\"entities\":" + v.Entities + ",\"records\":" + v.Records +
            ",\"commands\":" + v.Commands + ",\"queue\":" + v.QueueSize + ",\"history\":" + v.HistorySize +
            ",\"connections\":" + v.ActiveConnections + ",\"peers\":" + v.ActivePeers + ",\"timestamp\":" + v.Timestamp + "}";
    }
}
