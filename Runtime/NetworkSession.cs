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
        Malformed
    }

    /// <summary>Owns one immutable validated command payload.</summary>
    public sealed class NetworkCommandEnvelope
    {
        private readonly byte[] _payload;

        internal NetworkCommandEnvelope(ConnectionId connection, uint peer, uint epoch, uint sequence, uint targetTick, NetworkTypeId typeId, byte version, byte[] payload)
        { Connection = connection; PeerId = peer; Epoch = epoch; Sequence = sequence; TargetTick = targetTick; TypeId = typeId; Version = version; _payload = payload ?? throw new ArgumentNullException(nameof(payload)); }
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
        private uint _nextReceiveSequence = 1;

        /// <summary>Creates a handshaking per-connection session.</summary>
        public NetworkSession(ConnectionId connection, NetworkRole role, NetworkSchema<TWorld> schema, INetworkObserver observer = null)
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
        /// <summary>Gets the latest mock/replay transport call order.</summary>
        public ulong Cycle { get; private set; }

        /// <summary>Completes the v2 handshake after exact shared-manifest fingerprint comparison.</summary>
        public NetworkAdmissionResult Admit(SchemaFingerprint remoteFingerprint, uint peerId, uint epoch, ScopeId scope)
        {
            if (State != NetworkSessionState.Handshaking) return NetworkAdmissionResult.WrongRole;
            if (remoteFingerprint != _schema.Fingerprint) return Reject(NetworkAdmissionResult.SchemaMismatch);
            if (peerId == 0) return Reject(NetworkAdmissionResult.InvalidPeer);
            if (epoch == 0) return Reject(NetworkAdmissionResult.InvalidEpoch);
            PeerId = peerId; Epoch = epoch; Scope = scope; State = NetworkSessionState.Established;
            return NetworkAdmissionResult.Accepted;
        }

        /// <summary>Advances connection bookkeeping without conflating cycle ordering with simulation time.</summary>
        public void Tick(uint serverTick, ulong cycle)
        {
            if (cycle <= Cycle) throw new ArgumentOutOfRangeException(nameof(cycle), "Cycle must increase monotonically.");
            Cycle = cycle;
            Observe(NetworkPhase.Receive, NetworkTraceKind.Point, NetworkResultCategory.Success, serverTick, 0, 0);
        }

        /// <summary>Serializes one client command through its Static ECS event hook.</summary>
        public NetworkCommandResult CreateCommand<TCommand>(in TCommand command, uint targetTick, out NetworkCommandEnvelope envelope)
            where TCommand : struct, IEvent, INetworkCommand
        {
            envelope = null;
            if (Role != NetworkRole.Client || State != NetworkSessionState.Established) return NetworkCommandResult.WrongSession;
            var entries = _schema.RetainedEntries;
            for (var i = 0; i < entries.Length; i++)
            {
                var entry = entries[i];
                if (entry.Kind != NetworkSchemaKind.Command || entry.RuntimeType != typeof(TCommand) || entry.Invoker is not ICommandNetworkInvoker<TWorld> invoker) continue;
                var payload = invoker.Capture(command, entry.MaxBytes);
                envelope = new NetworkCommandEnvelope(Connection, PeerId, Epoch, _nextSendSequence++, targetTick, entry.TypeId, entry.Version, payload);
                Observe(NetworkPhase.Send, NetworkTraceKind.Point, NetworkResultCategory.Success, 0, targetTick, payload.Length);
                return NetworkCommandResult.Queued;
            }
            return NetworkCommandResult.SchemaMismatch;
        }

        internal NetworkCommandResult Validate(NetworkCommandEnvelope envelope, uint serverTick, uint pastWindow, uint futureWindow, out NetworkSchemaEntry entry)
        {
            entry = null;
            if (Role != NetworkRole.Server || State != NetworkSessionState.Established || envelope == null || envelope.Connection != Connection || envelope.PeerId != PeerId || envelope.Epoch != Epoch) return NetworkCommandResult.WrongSession;
            if (envelope.Sequence != _nextReceiveSequence) return NetworkCommandResult.Sequence;
            if (envelope.TargetTick < serverTick - Math.Min(serverTick, pastWindow) || envelope.TargetTick > serverTick + futureWindow) return NetworkCommandResult.TickWindow;
            if (!_schema.TryGet(envelope.TypeId, out entry) || entry.Kind != NetworkSchemaKind.Command || entry.Version != envelope.Version || envelope.ExactPayload.Length > entry.MaxBytes || entry.Invoker is not ICommandNetworkInvoker<TWorld>) return NetworkCommandResult.SchemaMismatch;
            _nextReceiveSequence++;
            return NetworkCommandResult.Queued;
        }

        internal NetworkCommandResult Dispatch(NetworkCommandEnvelope envelope, NetworkSchemaEntry entry)
        {
            var context = new NetworkCommandContext(PeerId, Epoch, envelope.Sequence, envelope.TargetTick);
            var sent = ((ICommandNetworkInvoker<TWorld>)entry.Invoker).Dispatch(envelope.ExactPayload, entry.Version, in context);
            Observe(NetworkPhase.CommandDispatch, NetworkTraceKind.Point, sent ? NetworkResultCategory.Success : NetworkResultCategory.Policy, 0, envelope.TargetTick, envelope.ExactPayload.Length);
            return sent ? NetworkCommandResult.Dispatched : NetworkCommandResult.PolicyRejected;
        }

        /// <summary>Closes this connection without touching shared capture history.</summary>
        public void Close() => State = NetworkSessionState.Closed;

        private NetworkAdmissionResult Reject(NetworkAdmissionResult result) { State = NetworkSessionState.Rejected; return result; }
        private void Observe(NetworkPhase phase, NetworkTraceKind kind, NetworkResultCategory result, uint serverTick, uint targetTick, int bytes)
        {
            if (_observer == null) return;
            try { var value = new NetworkTraceEvent(phase, kind, result, Role, Connection.Value, PeerId, Epoch, serverTick, targetTick, bytes, bytes > 0 ? 1 : 0, 0, 0, phase == NetworkPhase.CommandDispatch ? 1 : 0, 0, 0, State == NetworkSessionState.Closed ? 0 : 1, PeerId == 0 ? 0 : 1, Stopwatch.GetTimestamp()); _observer.Observe(in value); }
            catch { }
        }
    }

    /// <summary>Coordinates sessions, ordered commands, and scope-shared immutable captures.</summary>
    public sealed class NetworkServerCoordinator<TWorld> where TWorld : struct, IWorldType
    {
        private readonly Dictionary<ConnectionId, NetworkSession<TWorld>> _sessions = new Dictionary<ConnectionId, NetworkSession<TWorld>>();
        private readonly Dictionary<ScopeId, NetworkHistory<NetworkSnapshot>> _history = new Dictionary<ScopeId, NetworkHistory<NetworkSnapshot>>();
        private readonly List<PendingCommand> _commands = new List<PendingCommand>();
        private readonly int _historyCapacity;

        /// <summary>Creates a server coordinator with bounded per-scope history.</summary>
        public NetworkServerCoordinator(int historyCapacity = 64) { if (historyCapacity < 1) throw new ArgumentOutOfRangeException(nameof(historyCapacity)); _historyCapacity = historyCapacity; }
        /// <summary>Gets active connection count.</summary>
        public int ConnectionCount => _sessions.Count;

        /// <summary>Adds one independently owned per-connection session.</summary>
        public void Add(NetworkSession<TWorld> session)
        {
            if (session == null || session.Role != NetworkRole.Server) throw new ArgumentException("A server session is required.", nameof(session));
            if (_sessions.ContainsKey(session.Connection)) throw new InvalidOperationException("Connection is already admitted.");
            _sessions.Add(session.Connection, session);
        }

        /// <summary>Validates and queues one command for canonical cross-peer ordering.</summary>
        public NetworkCommandResult Queue(NetworkCommandEnvelope envelope, uint serverTick, uint pastWindow = 2, uint futureWindow = 8)
        {
            if (envelope == null || !_sessions.TryGetValue(envelope.Connection, out var session)) return NetworkCommandResult.WrongSession;
            var result = session.Validate(envelope, serverTick, pastWindow, futureWindow, out var entry);
            if (result == NetworkCommandResult.Queued) _commands.Add(new PendingCommand(session, entry, envelope));
            return result;
        }

        /// <summary>Dispatches queued commands ordered by target tick, trusted peer, then sequence.</summary>
        public int Dispatch(uint serverTick)
        {
            _commands.Sort((a, b) => { var tick = a.Envelope.TargetTick.CompareTo(b.Envelope.TargetTick); if (tick != 0) return tick; var peer = a.Envelope.PeerId.CompareTo(b.Envelope.PeerId); return peer != 0 ? peer : a.Envelope.Sequence.CompareTo(b.Envelope.Sequence); });
            var count = 0;
            for (var i = 0; i < _commands.Count; i++) if (_commands[i].Envelope.TargetTick <= serverTick) { _commands[i].Session.Dispatch(_commands[i].Envelope, _commands[i].Entry); count++; }
            if (count > 0) _commands.RemoveRange(0, count);
            return count;
        }

        /// <summary>Stores one immutable capture shared only within the specified scope.</summary>
        public void StoreCapture(ScopeId scope, NetworkSnapshot snapshot)
        {
            if (snapshot == null) throw new ArgumentNullException(nameof(snapshot));
            if (!_history.TryGetValue(scope, out var history)) { history = new NetworkHistory<NetworkSnapshot>(_historyCapacity); _history.Add(scope, history); }
            history.Store(snapshot.ServerTick, snapshot);
        }

        /// <summary>Finds one scope-and-tick capture. Per-peer acknowledgement state is never shared here.</summary>
        public bool TryGetCapture(ScopeId scope, uint serverTick, out NetworkSnapshot snapshot)
        {
            if (_history.TryGetValue(scope, out var history)) return history.TryGet(serverTick, out snapshot);
            snapshot = null;
            return false;
        }

        private readonly struct PendingCommand
        {
            internal PendingCommand(NetworkSession<TWorld> session, NetworkSchemaEntry entry, NetworkCommandEnvelope envelope) { Session = session; Entry = entry; Envelope = envelope; }
            internal NetworkSession<TWorld> Session { get; }
            internal NetworkSchemaEntry Entry { get; }
            internal NetworkCommandEnvelope Envelope { get; }
        }
    }
}
