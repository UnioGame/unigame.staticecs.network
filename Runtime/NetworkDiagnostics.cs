using System;
using System.Globalization;
using System.IO;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Identifies the packet kind associated with a diagnostic event.</summary>
    public enum NetworkPacketKind : byte
    {
        /// <summary>No packet is associated with the event.</summary>
        None = 0,
        /// <summary>A handshake hello packet.</summary>
        Hello = 1,
        /// <summary>A handshake ready packet.</summary>
        Ready = 2,
        /// <summary>A command batch packet.</summary>
        CommandBatch = 3,
        /// <summary>A keyframe or delta snapshot chunk.</summary>
        SnapshotChunk = 8,
        /// <summary>An acknowledgement packet.</summary>
        Ack = 5,
        /// <summary>A resynchronization request packet.</summary>
        ResyncRequest = 6,
        /// <summary>A disconnect packet.</summary>
        Disconnect = 7,
        /// <summary>A clock synchronization request.</summary>
        Ping = 9,
        /// <summary>A clock synchronization response.</summary>
        Pong = 10,
        TransactionCommand = 11,
        TransactionReceipt = 12
    }
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
        Send,
        /// <summary>Complete authoritative simulation tick.</summary>
        ServerTick
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
        /// <summary>The packet or command was malformed.</summary>
        Malformed,
        /// <summary>The packet violated session, epoch, or sequence state.</summary>
        Protocol,
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

    /// <summary>Identifies why a resynchronization request was emitted locally.</summary>
    public enum NetworkResyncReason : byte
    {
        /// <summary>No resynchronization reason was recorded.</summary>
        None,
        /// <summary>The local prediction history cannot satisfy reconciliation.</summary>
        PredictionHistoryUnavailable,
        /// <summary>A received snapshot was rejected before application.</summary>
        SnapshotRejected,
        /// <summary>Snapshot application failed after the apply path started.</summary>
        SnapshotApplyFailed,
        /// <summary>The remote protocol is incompatible with this endpoint.</summary>
        ProtocolIncompatible,
        /// <summary>The server received an empty command payload.</summary>
        ServerEmptyPayload,
        /// <summary>The server received an invalid command count.</summary>
        ServerInvalidCommandCount,
        /// <summary>The server received a truncated command header.</summary>
        ServerTruncatedCommandHeader,
        /// <summary>The server received an invalid command envelope.</summary>
        ServerInvalidCommandEnvelope,
        /// <summary>The server received trailing command payload bytes.</summary>
        ServerTrailingPayloadBytes,
        /// <summary>The server command queue rejected an envelope.</summary>
        ServerCommandQueueRejected,
    }

    /// <summary>Identifies the local trace-only origin of a resynchronization request.</summary>
    public enum NetworkResyncSource : byte
    {
        /// <summary>No resynchronization origin was recorded.</summary>
        None,
        /// <summary>The client echoed an incoming resynchronization request.</summary>
        ClientIncomingResyncEcho,
        /// <summary>The client rejected a snapshot during validation.</summary>
        ClientSnapshotValidation,
        /// <summary>The client timed out an incomplete snapshot assembly.</summary>
        ClientSnapshotAssemblyTimeout,
        /// <summary>The client could not replay local prediction history.</summary>
        ClientPrediction,
        /// <summary>The server rejected a decoded command batch.</summary>
        ServerCommandDecode,
    }

    /// <summary>Reports bounded endpoint memory and queue ownership.</summary>
    public struct NetworkMemoryDiagnostics
    {
        /// <summary>Packet-buffer pool counters.</summary>
        public NetworkBufferPoolDiagnostics Buffers;

        /// <summary>Bytes retained by active snapshot histories.</summary>
        public long HistoryBytes;

        /// <summary>Commands retained for redundancy or authority dispatch.</summary>
        public int PendingCommands;

        /// <summary>Payload bytes retained by pending commands.</summary>
        public long PendingCommandBytes;

        /// <summary>Largest observed pending command count.</summary>
        public int PendingCommandsHighWater;

        /// <summary>Largest observed pending command payload byte count.</summary>
        public long PendingCommandBytesHighWater;
    }

    /// <summary>Receives bounded immutable network telemetry.</summary>
    public interface INetworkObserver
    {
        /// <summary>Observes one privacy-safe event.</summary>
        void Observe(in NetworkTraceEvent value);
    }

    /// <summary>Optionally receives privacy-safe session and snapshot metadata in addition to phase events.</summary>
    public interface INetworkDiagnosticsObserver : INetworkObserver
    {
        /// <summary>Observes one immutable session-state sample.</summary>
        void ObserveSession(in NetworkSessionDiagnostics value);

        /// <summary>Observes one immutable snapshot and history sample.</summary>
        void ObserveSnapshot(in NetworkSnapshotDiagnostics value);
    }

    /// <summary>Contains immutable session state and protocol cursors without payload or ECS references.</summary>
    public readonly struct NetworkSessionDiagnostics
    {
        /// <summary>Creates one session diagnostics sample.</summary>
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

        /// <summary>Gets endpoint role.</summary>
        public NetworkRole Role { get; }
        /// <summary>Gets admission state.</summary>
        public NetworkSessionState State { get; }
        /// <summary>Gets transport-owned connection id.</summary>
        public uint ConnectionId { get; }
        /// <summary>Gets server-assigned peer id.</summary>
        public uint PeerId { get; }
        /// <summary>Gets admitted epoch.</summary>
        public uint Epoch { get; }
        /// <summary>Gets replication scope.</summary>
        public ScopeId Scope { get; }
        /// <summary>Gets the current authoritative tick known to the endpoint.</summary>
        public uint ServerTick { get; }
        /// <summary>Gets the latest acknowledged snapshot tick.</summary>
        public uint AcknowledgedSnapshotTick { get; }
        /// <summary>Gets the latest command sequence processed into authoritative state.</summary>
        public uint ServerProcessedCommandSequence { get; }
        /// <summary>Gets the next command sequence assigned by this endpoint.</summary>
        public uint NextSendCommandSequence { get; }
        /// <summary>Gets the next command sequence accepted by this endpoint.</summary>
        public uint NextReceiveCommandSequence { get; }
        /// <summary>Gets the next packet sequence accepted by this endpoint.</summary>
        public uint NextReceivePacketSequence { get; }
        /// <summary>Gets the next packet sequence assigned by this endpoint.</summary>
        public uint NextSendPacketSequence { get; }
    }

    /// <summary>Contains immutable snapshot identity, counts, and retained-history bounds.</summary>
    public readonly struct NetworkSnapshotDiagnostics
    {
        /// <summary>Creates one snapshot diagnostics sample.</summary>
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

        /// <summary>Gets endpoint role.</summary>
        public NetworkRole Role { get; }
        /// <summary>Gets transport-owned connection id.</summary>
        public uint ConnectionId { get; }
        /// <summary>Gets server-assigned peer id.</summary>
        public uint PeerId { get; }
        /// <summary>Gets admitted epoch.</summary>
        public uint Epoch { get; }
        /// <summary>Gets replication scope.</summary>
        public ScopeId Scope { get; }
        /// <summary>Gets authoritative snapshot tick.</summary>
        public uint ServerTick { get; }
        /// <summary>Gets the schema fingerprint.</summary>
        public SchemaFingerprint SchemaFingerprint { get; }
        /// <summary>Gets the canonical payload hash.</summary>
        public ulong PayloadHash { get; }
        /// <summary>Gets exact snapshot byte count.</summary>
        public int Bytes { get; }
        /// <summary>Gets snapshot entity count.</summary>
        public int Entities { get; }
        /// <summary>Gets snapshot record count.</summary>
        public int Records { get; }
        /// <summary>Gets retained history item count.</summary>
        public int HistoryTicks { get; }
        /// <summary>Gets retained history byte count.</summary>
        public long HistoryBytes { get; }
        /// <summary>Gets the oldest retained history tick, or zero when empty.</summary>
        public uint OldestHistoryTick { get; }
        /// <summary>Gets the newest retained history tick, or zero when empty.</summary>
        public uint NewestHistoryTick { get; }
        /// <summary>Gets the configured history item limit.</summary>
        public int HistoryCapacity { get; }
        /// <summary>Gets the configured history byte limit.</summary>
        public long HistoryMaxBytes { get; }
    }

    /// <summary>Contains immutable privacy-safe endpoint telemetry.</summary>
    public readonly struct NetworkTraceEvent
    {
        /// <summary>Creates one immutable trace event.</summary>
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
        /// <summary>Gets accepted command count.</summary>
        public int AcceptedCommands { get; }
        /// <summary>Gets policy-rejected command count.</summary>
        public int RejectedCommands { get; }
        /// <summary>Gets queue size.</summary>
        public int QueueSize { get; }
        /// <summary>Gets history size.</summary>
        public int HistoryTicks { get; }
        /// <summary>Gets retained history bytes.</summary>
        public long HistoryBytes { get; }
        /// <summary>Gets active connection count.</summary>
        public int ActiveConnections { get; }
        /// <summary>Gets active peer count.</summary>
        public int ActivePeers { get; }
        /// <summary>Gets Stopwatch ticks.</summary>
        public long Timestamp { get; }
        /// <summary>Gets measured Stopwatch duration ticks.</summary>
        public long DurationNanoseconds { get; }
        /// <summary>Gets the packet kind.</summary>
        public NetworkPacketKind PacketKind { get; }
        /// <summary>Gets authoritative tick minus the latest client acknowledgement.</summary>
        public int ClientServerTickGap { get; }
        /// <summary>Gets managed bytes allocated on the measured thread, when available.</summary>
        public long? ManagedAllocatedBytes { get; }
        /// <summary>Gets the local trace-only resynchronization reason.</summary>
        public NetworkResyncReason ResyncReason { get; }
        /// <summary>Gets the local trace-only resynchronization source.</summary>
        public NetworkResyncSource ResyncSource { get; }
        /// <summary>Gets the non-zero resynchronization correlation identifier, or zero when unrelated.</summary>
        public uint ResyncCorrelationId { get; }
        /// <summary>Gets the exact bounded command result when this event evaluates a command.</summary>
        public NetworkCommandResult? CommandResult { get; }
        /// <summary>Gets the exact bounded snapshot result when this event evaluates a snapshot.</summary>
        public SnapshotApplyResult? SnapshotResult { get; }
        /// <summary>Gets the exact bounded packet validation result when this event validates a session packet.</summary>
        public PacketValidationResult? PacketValidationResult { get; }
        /// <summary>Gets the packet or command sequence associated with the event.</summary>
        public uint Sequence { get; }
        /// <summary>Gets the latest acknowledged snapshot tick.</summary>
        public uint AcknowledgedSnapshotTick { get; }
        /// <summary>Gets the oldest retained history tick, or zero when empty.</summary>
        public uint OldestHistoryTick { get; }
        /// <summary>Gets the newest retained history tick, or zero when empty.</summary>
        public uint NewestHistoryTick { get; }
        /// <summary>Gets the active schema fingerprint.</summary>
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
