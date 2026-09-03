using System;
using System.Globalization;
using System.IO;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Identifies the packet kind associated with a diagnostic event.</summary>
    public enum NetworkPacketKind : byte
    {
        None = 0,
        Hello = 1,
        Ready = 2,
        CommandBatch = 3,
        SnapshotChunk = 8,
        Ack = 5,
        ResyncRequest = 6,
        Disconnect = 7,
        Ping = 9,
        Pong = 10,
        TransactionCommand = 11,
        TransactionReceipt = 12
    }
    /// <summary>Identifies a measured networking phase.</summary>
    public enum NetworkPhase : byte
    {
        Receive,
        Decode,
        CommandDispatch,
        SnapshotApply,
        SnapshotCapture,
        Send,
        ServerTick
    }
    /// <summary>Identifies trace event timing semantics.</summary>
    public enum NetworkTraceKind : byte
    {
        Begin,
        End,
        Point
    }
    /// <summary>Classifies a trace result without exposing private payload data.</summary>
    public enum NetworkResultCategory : byte
    {
        None,
        Success,
        Rejected,
        Malformed,
        Protocol,
        Schema,
        Limits,
        Transport,
        Policy,
        World
    }

    /// <summary>Identifies why a resynchronization request was emitted locally.</summary>
    public enum NetworkResyncReason : byte
    {
        None,
        PredictionHistoryUnavailable,
        SnapshotRejected,
        SnapshotApplyFailed,
        ProtocolIncompatible,
        ServerEmptyPayload,
        ServerInvalidCommandCount,
        ServerTruncatedCommandHeader,
        ServerInvalidCommandEnvelope,
        ServerTrailingPayloadBytes,
        ServerCommandQueueRejected,
    }

    /// <summary>Identifies the local trace-only origin of a resynchronization request.</summary>
    public enum NetworkResyncSource : byte
    {
        None,
        ClientIncomingResyncEcho,
        ClientSnapshotValidation,
        ClientSnapshotAssemblyTimeout,
        ClientPrediction,
        ServerCommandDecode,
    }

    /// <summary>Reports bounded endpoint memory and queue ownership.</summary>
    public struct NetworkMemoryDiagnostics
    {
        public NetworkBufferPoolDiagnostics Buffers;

        public long HistoryBytes;

        public int PendingCommands;

        public long PendingCommandBytes;

        public int PendingCommandsHighWater;

        public long PendingCommandBytesHighWater;
    }

    /// <summary>Receives bounded immutable network telemetry.</summary>
    public interface INetworkObserver
    {
        void Observe(in NetworkTraceEvent value);
    }

    /// <summary>Optionally receives privacy-safe session and snapshot metadata in addition to phase events.</summary>
    public interface INetworkDiagnosticsObserver : INetworkObserver
    {
        void ObserveSession(in NetworkSessionDiagnostics value);

        void ObserveSnapshot(in NetworkSnapshotDiagnostics value);
    }

    /// <summary>Contains immutable session state and protocol cursors without payload or ECS references.</summary>
    public readonly struct NetworkSessionDiagnostics
    {
        public NetworkSessionDiagnostics(NetworkRole role, NetworkSessionState state, uint connectionId, uint peerId,
            uint epoch, ScopeId scope, uint serverTick, uint acknowledgedSnapshotTick, uint serverProcessedCommandSequence,
            uint nextSendCommandSequence, uint nextReceiveCommandSequence, uint nextReceivePacketSequence,
            uint nextSendPacketSequence)
        {
            Role = role; State = state; ConnectionId = connectionId; PeerId = peerId; Epoch = epoch; Scope = scope;
            ServerTick = serverTick; AcknowledgedSnapshotTick = acknowledgedSnapshotTick;
            ServerProcessedCommandSequence = serverProcessedCommandSequence; NextSendCommandSequence = nextSendCommandSequence;
            NextReceiveCommandSequence = nextReceiveCommandSequence; NextReceivePacketSequence = nextReceivePacketSequence;
            NextSendPacketSequence = nextSendPacketSequence;
        }

        public NetworkRole Role { get; }
        public NetworkSessionState State { get; }
        /// <summary>Gets transport-owned connection id.</summary>
        public uint ConnectionId { get; }
        /// <summary>Gets server-assigned peer id.</summary>
        public uint PeerId { get; }
        public uint Epoch { get; }
        public ScopeId Scope { get; }
        public uint ServerTick { get; }
        public uint AcknowledgedSnapshotTick { get; }
        /// <summary>Gets the latest command sequence processed into authoritative state.</summary>
        public uint ServerProcessedCommandSequence { get; }
        public uint NextSendCommandSequence { get; }
        public uint NextReceiveCommandSequence { get; }
        public uint NextReceivePacketSequence { get; }
        public uint NextSendPacketSequence { get; }
    }

    /// <summary>Contains immutable snapshot identity, counts, and retained-history bounds.</summary>
    public readonly struct NetworkSnapshotDiagnostics
    {
        public NetworkSnapshotDiagnostics(NetworkRole role, uint connectionId, uint peerId, uint epoch, ScopeId scope,
            uint serverTick, SchemaFingerprint fingerprint, ulong payloadHash, int bytes, int entities, int records,
            int historyTicks, long historyBytes, uint oldestHistoryTick, uint newestHistoryTick, int historyCapacity,
            long historyMaxBytes)
        {
            Role = role; ConnectionId = connectionId; PeerId = peerId; Epoch = epoch; Scope = scope; ServerTick = serverTick;
            SchemaFingerprint = fingerprint; PayloadHash = payloadHash; Bytes = bytes; Entities = entities; Records = records;
            HistoryTicks = historyTicks; HistoryBytes = historyBytes; OldestHistoryTick = oldestHistoryTick;
            NewestHistoryTick = newestHistoryTick; HistoryCapacity = historyCapacity; HistoryMaxBytes = historyMaxBytes;
        }

        public NetworkRole Role { get; }
        /// <summary>Gets transport-owned connection id.</summary>
        public uint ConnectionId { get; }
        /// <summary>Gets server-assigned peer id.</summary>
        public uint PeerId { get; }
        public uint Epoch { get; }
        public ScopeId Scope { get; }
        public uint ServerTick { get; }
        public SchemaFingerprint SchemaFingerprint { get; }
        public ulong PayloadHash { get; }
        public int Bytes { get; }
        public int Entities { get; }
        public int Records { get; }
        public int HistoryTicks { get; }
        public long HistoryBytes { get; }
        /// <summary>Gets the oldest retained history tick, or zero when empty.</summary>
        public uint OldestHistoryTick { get; }
        /// <summary>Gets the newest retained history tick, or zero when empty.</summary>
        public uint NewestHistoryTick { get; }
        public int HistoryCapacity { get; }
        public long HistoryMaxBytes { get; }
    }

    /// <summary>Contains immutable privacy-safe endpoint telemetry.</summary>
    public readonly struct NetworkTraceEvent
    {
        public NetworkTraceEvent(NetworkPhase phase, NetworkTraceKind kind, NetworkResultCategory result,
            NetworkRole role, uint connectionId, uint peerId, uint epoch, uint serverTick, uint targetTick,
            int bytes, int packets, int entities, int records, int commands, int queueSize, int historyTicks,
            int activeConnections, int activePeers, long timestamp, NetworkPacketKind packetKind = NetworkPacketKind.None,
            long historyBytes = 0, int clientServerTickGap = 0, long durationNanoseconds = 0, SchemaFingerprint fingerprint = default,
            int acceptedCommands = 0, int rejectedCommands = 0,
            long? managedAllocatedBytes = null,
            NetworkResyncReason resyncReason = NetworkResyncReason.None,
            NetworkResyncSource resyncSource = NetworkResyncSource.None,
            uint resyncCorrelationId = 0,
            NetworkCommandResult? commandResult = null,
            SnapshotApplyResult? snapshotResult = null,
            PacketValidationResult? packetValidationResult = null,
            uint sequence = 0,
            uint acknowledgedSnapshotTick = 0,
            uint oldestHistoryTick = 0,
            uint newestHistoryTick = 0)
        {
            Phase = phase; Kind = kind; Result = result; Role = role; ConnectionId = connectionId; PeerId = peerId;
            Epoch = epoch; ServerTick = serverTick; TargetTick = targetTick; Bytes = bytes; Packets = packets;
            Entities = entities; Records = records; Commands = commands; QueueSize = queueSize; HistoryTicks = historyTicks;
            ActiveConnections = activeConnections; ActivePeers = activePeers; Timestamp = timestamp;
            PacketKind = packetKind; HistoryBytes = historyBytes; ClientServerTickGap = clientServerTickGap; DurationNanoseconds = durationNanoseconds; SchemaFingerprint = fingerprint;
            AcceptedCommands = acceptedCommands; RejectedCommands = rejectedCommands;
            ManagedAllocatedBytes = managedAllocatedBytes;
            ResyncReason = resyncReason;
            ResyncSource = resyncSource;
            ResyncCorrelationId = resyncCorrelationId;
            CommandResult = commandResult;
            SnapshotResult = snapshotResult;
            PacketValidationResult = packetValidationResult;
            Sequence = sequence;
            AcknowledgedSnapshotTick = acknowledgedSnapshotTick;
            OldestHistoryTick = oldestHistoryTick;
            NewestHistoryTick = newestHistoryTick;
        }
        public NetworkPhase Phase { get; }
        public NetworkTraceKind Kind { get; }
        public NetworkResultCategory Result { get; }
        public NetworkRole Role { get; }
        /// <summary>Gets transport-owned connection id.</summary>
        public uint ConnectionId { get; }
        /// <summary>Gets server-assigned peer id.</summary>
        public uint PeerId { get; }
        public uint Epoch { get; }
        public uint ServerTick { get; }
        public uint TargetTick { get; }
        public int Bytes { get; }
        public int Packets { get; }
        public int Entities { get; }
        public int Records { get; }
        public int Commands { get; }
        public int AcceptedCommands { get; }
        public int RejectedCommands { get; }
        public int QueueSize { get; }
        public int HistoryTicks { get; }
        public long HistoryBytes { get; }
        public int ActiveConnections { get; }
        public int ActivePeers { get; }
        /// <summary>Gets Stopwatch ticks.</summary>
        public long Timestamp { get; }
        public long DurationNanoseconds { get; }
        public NetworkPacketKind PacketKind { get; }
        public int ClientServerTickGap { get; }
        /// <summary>Gets managed bytes allocated on the measured thread, when available.</summary>
        public long? ManagedAllocatedBytes { get; }
        public NetworkResyncReason ResyncReason { get; }
        public NetworkResyncSource ResyncSource { get; }
        /// <summary>Gets the non-zero resynchronization correlation identifier, or zero when unrelated.</summary>
        public uint ResyncCorrelationId { get; }
        public NetworkCommandResult? CommandResult { get; }
        public SnapshotApplyResult? SnapshotResult { get; }
        public PacketValidationResult? PacketValidationResult { get; }
        public uint Sequence { get; }
        public uint AcknowledgedSnapshotTick { get; }
        /// <summary>Gets the oldest retained history tick, or zero when empty.</summary>
        public uint OldestHistoryTick { get; }
        /// <summary>Gets the newest retained history tick, or zero when empty.</summary>
        public uint NewestHistoryTick { get; }
        public SchemaFingerprint SchemaFingerprint { get; }
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

        public void Dispose() { if (_disposed) return; Flush(); _disposed = true; _writer.Dispose(); }

        private static string ToJson(in NetworkTraceEvent v) =>
            "{\"phase\":\"" + PhaseName(v.Phase) + "\",\"kind\":\"" + v.Kind.ToString().ToLowerInvariant() + "\",\"packet_kind\":\"" + PacketName(v.PacketKind) +
            "\",\"error_category\":\"" + v.Result.ToString().ToLowerInvariant() + "\",\"role\":\"" + v.Role.ToString().ToLowerInvariant() +
            "\",\"connection\":" + v.ConnectionId + ",\"peer\":" + v.PeerId + ",\"epoch\":" + v.Epoch +
            ",\"server_tick\":" + v.ServerTick + ",\"target_tick\":" + v.TargetTick + ",\"bytes\":" + v.Bytes +
            ",\"packets\":" + v.Packets + ",\"entities\":" + v.Entities + ",\"records\":" + v.Records +
            ",\"commands\":" + v.Commands + ",\"accepted_commands\":" + v.AcceptedCommands + ",\"rejected_commands\":" + v.RejectedCommands + ",\"queue_size\":" + v.QueueSize + ",\"history_ticks\":" + v.HistoryTicks + ",\"history_bytes\":" + v.HistoryBytes +
            ",\"active_connections\":" + v.ActiveConnections + ",\"active_peers\":" + v.ActivePeers + ",\"timestamp\":" + v.Timestamp +
            ",\"client_server_tick_gap\":" + v.ClientServerTickGap + ",\"duration_ns\":" + v.DurationNanoseconds +
            (v.ManagedAllocatedBytes.HasValue ? ",\"managed_allocated_bytes\":" + v.ManagedAllocatedBytes.Value.ToString(CultureInfo.InvariantCulture) : string.Empty) +
            (v.ResyncCorrelationId != 0 ? ",\"resync_correlation_id\":" + v.ResyncCorrelationId.ToString(CultureInfo.InvariantCulture) : string.Empty) +
            (v.CommandResult.HasValue ? ",\"command_result\":\"" + v.CommandResult.Value.ToString().ToLowerInvariant() + "\"" : string.Empty) +
            (v.SnapshotResult.HasValue ? ",\"snapshot_result\":\"" + v.SnapshotResult.Value.ToString().ToLowerInvariant() + "\"" : string.Empty) +
            (v.PacketValidationResult.HasValue ? ",\"packet_validation_result\":\"" + v.PacketValidationResult.Value.ToString().ToLowerInvariant() + "\"" : string.Empty) +
            ",\"sequence\":" + v.Sequence +
            ",\"acknowledged_snapshot_tick\":" + v.AcknowledgedSnapshotTick +
            ",\"oldest_history_tick\":" + v.OldestHistoryTick +
            ",\"newest_history_tick\":" + v.NewestHistoryTick +
            ",\"schema_fingerprint\":\"" + v.SchemaFingerprint + "\"}";

        private static string PhaseName(NetworkPhase phase) => phase == NetworkPhase.CommandDispatch ? "command_dispatch" :
            phase == NetworkPhase.SnapshotApply ? "snapshot_apply" :
            phase == NetworkPhase.SnapshotCapture ? "snapshot_capture" :
            phase == NetworkPhase.ServerTick ? "server_tick" : phase.ToString().ToLowerInvariant();
        private static string PacketName(NetworkPacketKind kind) => kind == NetworkPacketKind.CommandBatch ? "command_batch" : kind == NetworkPacketKind.SnapshotChunk ? "snapshot_chunk" : kind == NetworkPacketKind.ResyncRequest ? "resync_request" : kind == NetworkPacketKind.TransactionCommand ? "transaction_command" : kind == NetworkPacketKind.TransactionReceipt ? "transaction_receipt" : kind.ToString().ToLowerInvariant();
    }
}
