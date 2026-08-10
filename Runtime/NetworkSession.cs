using System;
using System.Collections.Generic;
using System.Diagnostics;
using FFS.Libraries.StaticEcs;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Identifies one transport-owned connection.</summary>
    public readonly struct ConnectionId : IEquatable<ConnectionId>
    {
        /// <summary>Creates a non-zero connection id.</summary>
        public ConnectionId(uint value) { if (value == 0) throw new ArgumentOutOfRangeException(nameof(value)); Value = value; }
        /// <summary>Gets the transport value.</summary>
        public uint Value { get; }
        /// <inheritdoc />
        public bool Equals(ConnectionId other) => Value == other.Value;
        /// <inheritdoc />
        public override bool Equals(object obj) => obj is ConnectionId other && Equals(other);
        /// <inheritdoc />
        public override int GetHashCode() => (int)Value;
        /// <summary>Tests equality.</summary>
        public static bool operator ==(ConnectionId left, ConnectionId right) => left.Equals(right);
        /// <summary>Tests inequality.</summary>
        public static bool operator !=(ConnectionId left, ConnectionId right) => !left.Equals(right);
    }

    /// <summary>Identifies peers allowed to share an immutable server capture.</summary>
    public readonly struct ScopeId : IEquatable<ScopeId>
    {
        /// <summary>Creates a scope id.</summary>
        public ScopeId(ulong value) => Value = value;
        /// <summary>Gets the scope value.</summary>
        public ulong Value { get; }
        /// <inheritdoc />
        public bool Equals(ScopeId other) => Value == other.Value;
        /// <inheritdoc />
        public override bool Equals(object obj) => obj is ScopeId other && Equals(other);
        /// <inheritdoc />
        public override int GetHashCode() => Value.GetHashCode();
        /// <summary>Tests equality.</summary>
        public static bool operator ==(ScopeId left, ScopeId right) => left.Equals(right);
        /// <summary>Tests inequality.</summary>
        public static bool operator !=(ScopeId left, ScopeId right) => !left.Equals(right);
    }

    /// <summary>Reports per-connection admission state.</summary>
    public enum NetworkSessionState : byte
    {
        /// <summary>The connection is negotiating admission.</summary>
        Handshaking,
        /// <summary>The connection passed admission.</summary>
        Established,
        /// <summary>The connection failed admission.</summary>
        Rejected,
        /// <summary>The connection is closed.</summary>
        Closed
    }
    /// <summary>Reports handshake results.</summary>
    public enum NetworkAdmissionResult : byte
    {
        /// <summary>The connection was admitted.</summary>
        Accepted,
        /// <summary>The remote schema was incompatible.</summary>
        SchemaMismatch,
        /// <summary>The peer identifier was invalid.</summary>
        InvalidPeer,
        /// <summary>The session epoch was invalid.</summary>
        InvalidEpoch,
        /// <summary>The session state did not allow admission.</summary>
        WrongRole
    }
    /// <summary>Reports command validation results.</summary>
    public enum NetworkCommandResult : byte
    {
        /// <summary>The command was queued.</summary>
        Queued,
        /// <summary>The command was dispatched.</summary>
        Dispatched,
        /// <summary>The server policy rejected the command.</summary>
        PolicyRejected,
        /// <summary>The command did not belong to the admitted session.</summary>
        WrongSession,
        /// <summary>The command schema was incompatible.</summary>
        SchemaMismatch,
        /// <summary>The command sequence was unexpected.</summary>
        Sequence,
        /// <summary>The target tick was outside the accepted window.</summary>
        TickWindow,
        /// <summary>The command payload was malformed.</summary>
        Malformed,
        /// <summary>The redundant input frame was already observed.</summary>
        Duplicate
    }

    internal enum PacketValidationResult : byte
    {
        Success,
        WrongState,
        WrongRole,
        WrongEpoch,
        Sequence
    }

    /// <summary>Owns one immutable validated command payload.</summary>
    public sealed class NetworkCommandEnvelope
    {
        private readonly byte[] _payload;

        internal NetworkCommandEnvelope(ConnectionId connection, uint peer, uint epoch, uint sequence, uint targetTick, NetworkTypeId typeId, byte version, byte[] payload, bool isInput = false)
        { Connection = connection; PeerId = peer; Epoch = epoch; Sequence = sequence; TargetTick = targetTick; TypeId = typeId; Version = version; IsInput = isInput; _payload = payload ?? throw new ArgumentNullException(nameof(payload)); }
        /// <summary>Gets transport ownership.</summary>
        public ConnectionId Connection { get; }
        /// <summary>Gets trusted admitted peer.</summary>
        public uint PeerId { get; }
        /// <summary>Gets session epoch.</summary>
        public uint Epoch { get; }
        /// <summary>Gets per-peer sequence.</summary>
        public uint Sequence { get; }
        /// <summary>Gets target server tick.</summary>
        public uint TargetTick { get; }
        /// <summary>Gets generated command id.</summary>
        public NetworkTypeId TypeId { get; }
        /// <summary>Gets command hook version.</summary>
        public byte Version { get; }
        /// <summary>Gets whether this envelope carries continuous input.</summary>
        public bool IsInput { get; }
        /// <summary>Gets immutable exact payload.</summary>
        public ReadOnlyMemory<byte> Payload => _payload;
        internal byte[] ExactPayload => _payload;
    }

    /// <summary>Owns all mutable state for one connection.</summary>
    public sealed class NetworkSession<TWorld> where TWorld : struct, IWorldType
    {
        private readonly NetworkSchema<TWorld> _schema;
        private readonly INetworkObserver _observer;
        private uint _nextSendSequence = 1;
        private uint _nextSendInputSequence = 1;
        private uint _nextReceiveSequence = 1;
        private uint _nextReceiveInputSequence = 1;
        private uint _nextReceivePacketSequence = 1;
        private uint _lastReceiveInputPacketSequence;

        /// <summary>Creates a handshaking per-connection session.</summary>
        internal NetworkSession(ConnectionId connection, NetworkRole role, NetworkSchema<TWorld> schema, INetworkObserver observer = null)
        { Connection = connection; Role = role; _schema = schema ?? throw new ArgumentNullException(nameof(schema)); _observer = observer; State = NetworkSessionState.Handshaking; }
        /// <summary>Gets transport-owned connection.</summary>
        public ConnectionId Connection { get; }
        /// <summary>Gets endpoint role.</summary>
        public NetworkRole Role { get; }
        /// <summary>Gets admission state.</summary>
        public NetworkSessionState State { get; private set; }
        /// <summary>Gets server-assigned peer.</summary>
        public uint PeerId { get; private set; }
        /// <summary>Gets server-assigned epoch.</summary>
        public uint Epoch { get; private set; }
        /// <summary>Gets caller-selected replication scope.</summary>
        public ScopeId Scope { get; private set; }

        /// <summary>Completes the v3 handshake after exact shared-manifest fingerprint comparison.</summary>
        internal NetworkAdmissionResult Admit(SchemaFingerprint remoteFingerprint, uint peerId, uint epoch, ScopeId scope)
        {
            if (State != NetworkSessionState.Handshaking) return NetworkAdmissionResult.WrongRole;
            if (remoteFingerprint != _schema.Fingerprint) return Reject(NetworkAdmissionResult.SchemaMismatch);
            if (peerId == 0) return Reject(NetworkAdmissionResult.InvalidPeer);
            if (epoch == 0) return Reject(NetworkAdmissionResult.InvalidEpoch);
            PeerId = peerId; Epoch = epoch; Scope = scope; State = NetworkSessionState.Established;
            return NetworkAdmissionResult.Accepted;
        }

        /// <summary>Serializes one client command through its Static ECS event hook.</summary>
        internal NetworkCommandResult CreateCommand<TCommand>(in TCommand command, uint targetTick, out NetworkCommandEnvelope envelope)
            where TCommand : struct, IEvent, INetworkCommand
        {
            envelope = null;
            if (Role != NetworkRole.Client || State != NetworkSessionState.Established) return NetworkCommandResult.WrongSession;
            if (command is INetworkInput) return NetworkCommandResult.SchemaMismatch;
            return CreateEnvelope(command, targetTick, _nextSendSequence++, false, out envelope);
        }

        /// <summary>Serializes one continuous client input through its Static ECS event hook.</summary>
        internal NetworkCommandResult CreateInput<TInput>(in TInput input, uint targetTick, out NetworkCommandEnvelope envelope)
            where TInput : struct, IEvent, INetworkInput
        {
            envelope = null;
            if (Role != NetworkRole.Client || State != NetworkSessionState.Established) return NetworkCommandResult.WrongSession;
            return CreateEnvelope(input, targetTick, _nextSendInputSequence++, true, out envelope);
        }

        private NetworkCommandResult CreateEnvelope<TCommand>(TCommand command, uint targetTick,
            uint sequence, bool isInput, out NetworkCommandEnvelope envelope)
            where TCommand : struct, IEvent, INetworkCommand
        {
            envelope = null;
            var entries = _schema.RetainedEntries;
            for (var i = 0; i < entries.Length; i++)
            {
                var entry = entries[i];
                if (entry.Kind != NetworkSchemaKind.Command || entry.RuntimeType != typeof(TCommand) || entry.Invoker is not ICommandNetworkInvoker<TWorld> invoker) continue;
                var payload = invoker.Capture(command, entry.MaxBytes);
                envelope = new NetworkCommandEnvelope(Connection, PeerId, Epoch, sequence,
                    targetTick, entry.TypeId, entry.Version, payload, isInput);
                return NetworkCommandResult.Queued;
            }
            return NetworkCommandResult.SchemaMismatch;
        }

        internal NetworkCommandResult Validate(NetworkCommandEnvelope envelope, uint serverTick, uint pastWindow, uint futureWindow, out NetworkSchemaEntry entry)
        {
            entry = null;
            if (Role != NetworkRole.Server || State != NetworkSessionState.Established || envelope == null || envelope.Connection != Connection || envelope.PeerId != PeerId || envelope.Epoch != Epoch) return NetworkCommandResult.WrongSession;
            if (envelope.TargetTick < serverTick - Math.Min(serverTick, pastWindow) || envelope.TargetTick > serverTick + futureWindow) return NetworkCommandResult.TickWindow;
            if (!_schema.TryGet(envelope.TypeId, out entry) || entry.Kind != NetworkSchemaKind.Command || entry.Version != envelope.Version || envelope.ExactPayload.Length > entry.MaxBytes || entry.Invoker is not ICommandNetworkInvoker<TWorld> invoker || !invoker.HasPolicy) return NetworkCommandResult.SchemaMismatch;
            bool inputType = typeof(INetworkInput).IsAssignableFrom(entry.RuntimeType);
            if (inputType != envelope.IsInput) return NetworkCommandResult.SchemaMismatch;
            if (envelope.IsInput)
            {
                if (envelope.Sequence < _nextReceiveInputSequence) return NetworkCommandResult.Duplicate;
                _nextReceiveInputSequence = checked(envelope.Sequence + 1);
            }
            else
            {
                if (envelope.Sequence != _nextReceiveSequence) return NetworkCommandResult.Sequence;
                _nextReceiveSequence++;
            }
            return NetworkCommandResult.Queued;
        }

        internal NetworkCommandResult Dispatch(NetworkCommandEnvelope envelope, NetworkSchemaEntry entry)
        {
            var context = new NetworkCommandContext(PeerId, Epoch, envelope.Sequence, envelope.TargetTick);
            return ((ICommandNetworkInvoker<TWorld>)entry.Invoker).Dispatch(envelope.ExactPayload, entry.Version, in context);
        }

        internal PacketValidationResult ValidatePacket(in PacketHeader header)
        {
            if (State == NetworkSessionState.Handshaking)
            {
                var disconnect = Role == NetworkRole.Client && header.Kind == PacketKind.Disconnect;
                if (!disconnect &&
                    (Role == NetworkRole.Server && header.Kind != PacketKind.Hello ||
                     Role == NetworkRole.Client && header.Kind != PacketKind.Ready))
                    return PacketValidationResult.WrongRole;
                if (!disconnect && header.SessionEpoch == 0 != (Role == NetworkRole.Server))
                    return PacketValidationResult.WrongEpoch;
            }
            else
            {
                if (State != NetworkSessionState.Established) return PacketValidationResult.WrongState;
                if (!IsAllowedEstablishedPacket(header.Kind)) return PacketValidationResult.WrongRole;
                if (header.SessionEpoch != Epoch) return PacketValidationResult.WrongEpoch;
            }
            if (header.Kind == PacketKind.InputBatch)
            {
                if (header.Flags != PacketFlags.UnreliableSequenced ||
                    header.PacketSequence <= _lastReceiveInputPacketSequence)
                    return PacketValidationResult.Sequence;
                _lastReceiveInputPacketSequence = header.PacketSequence;
                return PacketValidationResult.Success;
            }
            if (header.Flags != PacketFlags.ReliableOrdered ||
                header.PacketSequence != _nextReceivePacketSequence)
                return PacketValidationResult.Sequence;
            _nextReceivePacketSequence++;
            return PacketValidationResult.Success;
        }

        private bool IsAllowedEstablishedPacket(PacketKind kind) => Role == NetworkRole.Server
            ? kind == PacketKind.CommandBatch || kind == PacketKind.InputBatch ||
              kind == PacketKind.Ping || kind == PacketKind.Ack ||
              kind == PacketKind.ResyncRequest || kind == PacketKind.Disconnect
            : kind == PacketKind.FullSnapshot || kind == PacketKind.Pong ||
              kind == PacketKind.ResyncRequest || kind == PacketKind.Disconnect;

        internal void Close() => State = NetworkSessionState.Closed;

        private NetworkAdmissionResult Reject(NetworkAdmissionResult result) { State = NetworkSessionState.Rejected; return result; }
        internal void Trace(NetworkPhase phase, NetworkTraceKind kind, NetworkResultCategory result, NetworkPacketKind packetKind, uint serverTick, uint targetTick, int bytes, int historyTicks, long historyBytes, int tickGap, long durationNanoseconds, int entities = 0, int records = 0, int commands = 0, int queueSize = 0, int activeConnections = -1, int activePeers = -1, int acceptedCommands = 0, int rejectedCommands = 0)
        {
            if (_observer == null) return;
            try { var packets = phase == NetworkPhase.Receive || phase == NetworkPhase.Decode || phase == NetworkPhase.Send ? 1 : 0; var connections = activeConnections < 0 ? State == NetworkSessionState.Closed ? 0 : 1 : activeConnections; var peers = activePeers < 0 ? State == NetworkSessionState.Established ? 1 : 0 : activePeers; var value = new NetworkTraceEvent(phase, kind, result, Role, Connection.Value, PeerId, Epoch, serverTick, targetTick, bytes, packets, entities, records, commands, queueSize, historyTicks, connections, peers, Stopwatch.GetTimestamp(), packetKind, historyBytes, tickGap, durationNanoseconds, _schema.Fingerprint, acceptedCommands, rejectedCommands); _observer.Observe(in value); }
            catch { }
        }

        internal void ReportSession(uint serverTick, uint acknowledgedSnapshotTick, uint acknowledgedCommandSequence,
            uint nextSendPacketSequence)
        {
            if (_observer is not INetworkDiagnosticsObserver diagnostics) return;
            try
            {
                var value = new NetworkSessionDiagnostics(Role, State, Connection.Value, PeerId, Epoch, Scope, serverTick,
                    acknowledgedSnapshotTick, acknowledgedCommandSequence, _nextSendSequence, _nextReceiveSequence,
                    _nextReceivePacketSequence, nextSendPacketSequence);
                diagnostics.ObserveSession(in value);
            }
            catch { }
        }

        internal void ReportSnapshot(NetworkSnapshot snapshot, NetworkHistory<NetworkSnapshot> history)
        {
            if (_observer is not INetworkDiagnosticsObserver diagnostics || snapshot == null || history == null) return;
            try
            {
                var value = new NetworkSnapshotDiagnostics(Role, Connection.Value, PeerId, Epoch, snapshot.Scope,
                    snapshot.ServerTick, snapshot.SchemaFingerprint, snapshot.PayloadHash, snapshot.ByteLength,
                    snapshot.EntityCount, snapshot.RecordCount, history.Count, history.Bytes, history.OldestTick,
                    history.NewestTick, history.Capacity, history.MaxBytes);
                diagnostics.ObserveSnapshot(in value);
            }
            catch { }
        }
    }

    /// <summary>Coordinates sessions, ordered commands, and scope-shared immutable captures.</summary>
    internal sealed class NetworkServerCoordinator<TWorld> where TWorld : struct, IWorldType
    {
        private readonly Dictionary<ConnectionId, NetworkSession<TWorld>> _sessions = new Dictionary<ConnectionId, NetworkSession<TWorld>>();
        private readonly Dictionary<ScopeId, NetworkHistory<NetworkSnapshot>> _history = new Dictionary<ScopeId, NetworkHistory<NetworkSnapshot>>();
        private readonly List<PendingCommand> _commands = new List<PendingCommand>();
        private readonly Dictionary<ConnectionId, ProcessedInputCursor> _processedInputs = new Dictionary<ConnectionId, ProcessedInputCursor>();
        private readonly int _historyCapacity;
        private readonly long _historyBytes;

        /// <summary>Creates a server coordinator with bounded per-scope history.</summary>
        internal NetworkServerCoordinator(int historyCapacity = 64, long historyBytes = 32 * 1024 * 1024)
        {
            if (historyCapacity < 1) throw new ArgumentOutOfRangeException(nameof(historyCapacity));
            if (historyBytes < 1) throw new ArgumentOutOfRangeException(nameof(historyBytes));
            _historyCapacity = historyCapacity; _historyBytes = historyBytes;
        }
        internal int PendingCommandCount => _commands.Count;

        /// <summary>Adds one independently owned per-connection session.</summary>
        internal void Add(NetworkSession<TWorld> session)
        {
            if (session == null || session.Role != NetworkRole.Server) throw new ArgumentException("A server session is required.", nameof(session));
            if (_sessions.ContainsKey(session.Connection)) throw new InvalidOperationException("Connection is already admitted.");
            _sessions.Add(session.Connection, session);
        }

        /// <summary>Removes one connection and its queued commands without touching shared capture history.</summary>
        internal bool Remove(ConnectionId connection)
        {
            for (var i = _commands.Count - 1; i >= 0; i--) if (_commands[i].Session.Connection == connection) _commands.RemoveAt(i);
            _processedInputs.Remove(connection);
            return _sessions.Remove(connection);
        }

        /// <summary>Validates and queues one command for canonical cross-peer ordering.</summary>
        internal NetworkCommandResult Queue(NetworkCommandEnvelope envelope, uint serverTick, uint pastWindow = 2, uint futureWindow = 8)
        {
            if (envelope == null || !_sessions.TryGetValue(envelope.Connection, out var session)) return NetworkCommandResult.WrongSession;
            var result = session.Validate(envelope, serverTick, pastWindow, futureWindow, out var entry);
            if (result == NetworkCommandResult.Queued) _commands.Add(new PendingCommand(session, entry, envelope));
            return result;
        }

        /// <summary>Dispatches queued commands ordered by target tick, trusted peer, then sequence.</summary>
        internal NetworkDispatchSummary Dispatch(uint serverTick)
        {
            _commands.Sort((a, b) => { var tick = a.Envelope.TargetTick.CompareTo(b.Envelope.TargetTick); if (tick != 0) return tick; var peer = a.Envelope.PeerId.CompareTo(b.Envelope.PeerId); return peer != 0 ? peer : a.Envelope.Sequence.CompareTo(b.Envelope.Sequence); });
            var summary = default(NetworkDispatchSummary);
            for (var i = 0; i < _commands.Count; i++)
            {
                if (_commands[i].Envelope.TargetTick > serverTick) continue;
                var pending = _commands[i];
                var result = pending.Session.Dispatch(pending.Envelope, pending.Entry);
                summary.Add(result);
                if (pending.Envelope.IsInput &&
                    (result == NetworkCommandResult.Dispatched ||
                     result == NetworkCommandResult.PolicyRejected))
                {
                    _processedInputs[pending.Session.Connection] = new ProcessedInputCursor(
                        pending.Envelope.TargetTick,
                        pending.Envelope.Sequence);
                }
            }
            if (summary.Total > 0) _commands.RemoveRange(0, summary.Total);
            return summary;
        }

        /// <summary>Stores one immutable capture shared only within the specified scope.</summary>
        internal void StoreCapture(ScopeId scope, NetworkSnapshot snapshot)
        {
            if (snapshot == null) throw new ArgumentNullException(nameof(snapshot));
            if (snapshot.Scope != scope) throw new InvalidOperationException("Snapshot scope does not match its history key.");
            if (!_history.TryGetValue(scope, out var history)) { history = new NetworkHistory<NetworkSnapshot>(_historyCapacity, _historyBytes, value => value.ByteLength); _history.Add(scope, history); }
            history.Store(snapshot.ServerTick, snapshot);
        }

        /// <summary>Finds one scope-and-tick capture. Per-peer acknowledgement state is never shared here.</summary>
        internal bool TryGetCapture(ScopeId scope, uint serverTick, out NetworkSnapshot snapshot)
        {
            if (_history.TryGetValue(scope, out var history)) return history.TryGet(serverTick, out snapshot);
            snapshot = null;
            return false;
        }

        internal int HistoryCount(ScopeId scope) => _history.TryGetValue(scope, out var history) ? history.Count : 0;
        internal long HistoryByteCount(ScopeId scope) => _history.TryGetValue(scope, out var history) ? history.Bytes : 0;
        internal NetworkHistory<NetworkSnapshot> History(ScopeId scope) => _history.TryGetValue(scope, out var history) ? history : null;

        internal bool TryGetProcessedInput(ConnectionId connection, out ProcessedInputCursor cursor)
            => _processedInputs.TryGetValue(connection, out cursor);

        private readonly struct PendingCommand
        {
            internal PendingCommand(NetworkSession<TWorld> session, NetworkSchemaEntry entry, NetworkCommandEnvelope envelope) { Session = session; Entry = entry; Envelope = envelope; }
            internal NetworkSession<TWorld> Session { get; }
            internal NetworkSchemaEntry Entry { get; }
            internal NetworkCommandEnvelope Envelope { get; }
        }
    }

    internal readonly struct ProcessedInputCursor
    {
        internal ProcessedInputCursor(uint tick, uint sequence)
        {
            Tick = tick;
            Sequence = sequence;
        }

        internal uint Tick { get; }
        internal uint Sequence { get; }
    }

    internal struct NetworkDispatchSummary
    {
        internal int Total { get; private set; }
        internal int Accepted { get; private set; }
        internal int Rejected { get; private set; }
        internal void Add(NetworkCommandResult result) { Total++; if (result == NetworkCommandResult.Dispatched) Accepted++; else if (result == NetworkCommandResult.PolicyRejected) Rejected++; }
    }
}
