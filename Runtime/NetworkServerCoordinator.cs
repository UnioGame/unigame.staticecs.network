using System;
using System.Collections.Generic;
using System.Diagnostics;
using FFS.Libraries.StaticEcs;
using FFS.Libraries.StaticPack;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Coordinates sessions, ordered commands, and scope-shared immutable captures.</summary>
    internal sealed class NetworkServerCoordinator<TWorld> where TWorld : struct, IWorldType
    {
        private readonly Dictionary<ConnectionId, NetworkSession<TWorld>> _sessions = new Dictionary<ConnectionId, NetworkSession<TWorld>>();
        private readonly Dictionary<ScopeId, NetworkHistory<NetworkSnapshot>> _history = new Dictionary<ScopeId, NetworkHistory<NetworkSnapshot>>();
        private readonly List<PendingCommand> _commands = new List<PendingCommand>();
        private readonly Dictionary<ConnectionId, ProcessedCommandCursor> _processedCommands = new Dictionary<ConnectionId, ProcessedCommandCursor>();
        private readonly Dictionary<ConnectionId, int> _pendingCommandCounts = new Dictionary<ConnectionId, int>();
        private readonly Dictionary<ConnectionId, int> _pendingCommandBytes = new Dictionary<ConnectionId, int>();
        private readonly int _historyCapacity;
        private readonly long _historyBytes;
        private readonly int _maxPendingCommandsPerPeer;
        private readonly int _maxPendingBytesPerPeer;
        private long _pendingCommandBytesTotal;
        private int _pendingCommandsHighWater;
        private long _pendingCommandBytesHighWater;

        /// <summary>Creates a server coordinator with bounded per-scope history.</summary>
        internal NetworkServerCoordinator(int historyCapacity = 64,
            long historyBytes = 32 * 1024 * 1024,
            int maxPendingCommandsPerPeer = ProtocolLimits.MaxCommandsPerBatch * 3,
            int maxPendingBytesPerPeer = ProtocolLimits.MaxWirePayloadBytes)
        {
            if (historyCapacity < 1) throw new ArgumentOutOfRangeException(nameof(historyCapacity));
            if (historyBytes < 1) throw new ArgumentOutOfRangeException(nameof(historyBytes));
            if (maxPendingCommandsPerPeer < 1)
                throw new ArgumentOutOfRangeException(nameof(maxPendingCommandsPerPeer));
            if (maxPendingBytesPerPeer < 1)
                throw new ArgumentOutOfRangeException(nameof(maxPendingBytesPerPeer));
            _historyCapacity = historyCapacity; _historyBytes = historyBytes;
            _maxPendingCommandsPerPeer = maxPendingCommandsPerPeer;
            _maxPendingBytesPerPeer = maxPendingBytesPerPeer;
        }
        internal int PendingCommandCount => _commands.Count;
        internal long PendingCommandBytes => _pendingCommandBytesTotal;
        internal int PendingCommandsHighWater => _pendingCommandsHighWater;
        internal long PendingCommandBytesHighWater => _pendingCommandBytesHighWater;
        internal long HistoryBytes
        {
            get
            {
                long bytes = 0;
                foreach (var history in _history.Values)
                    bytes += history.Bytes;
                return bytes;
            }
        }

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
            ScopeId scope = default;
            if (_sessions.TryGetValue(connection, out var removedSession))
                scope = removedSession.Scope;
            for (var i = _commands.Count - 1; i >= 0; i--)
            {
                if (_commands[i].Session.Connection != connection)
                    continue;
                var envelope = _commands[i].Envelope;
                DecrementPending(envelope);
                envelope.Dispose();
                _commands.RemoveAt(i);
            }
            _pendingCommandCounts.Remove(connection);
            _pendingCommandBytes.Remove(connection);
            _processedCommands.Remove(connection);
            var removed = _sessions.Remove(connection);
            if (removed && !ScopeIsActive(scope) && _history.TryGetValue(scope, out var history))
            {
                history.Clear();
                _history.Remove(scope);
            }
            return removed;
        }

        /// <summary>Validates and queues one command for canonical cross-peer ordering.</summary>
        internal NetworkCommandResult Queue(NetworkCommandEnvelope envelope, uint serverTick, uint pastWindow = 2, uint futureWindow = 8)
        {
            if (envelope.ExactBuffer == null || !_sessions.TryGetValue(envelope.Connection, out var session)) return NetworkCommandResult.WrongSession;
            _pendingCommandCounts.TryGetValue(envelope.Connection, out var count);
            _pendingCommandBytes.TryGetValue(envelope.Connection, out var bytes);
            if (count >= _maxPendingCommandsPerPeer ||
                envelope.ExactLength > _maxPendingBytesPerPeer - bytes)
                return NetworkCommandResult.LimitExceeded;
            var result = session.Validate(envelope, serverTick, pastWindow, futureWindow, out var entry);
            if (result == NetworkCommandResult.Queued)
            {
                _commands.Add(new PendingCommand(session, entry, envelope));
                _pendingCommandCounts[envelope.Connection] = count + 1;
                _pendingCommandBytes[envelope.Connection] = bytes + envelope.ExactLength;
                _pendingCommandBytesTotal += envelope.ExactLength;
                if (_commands.Count > _pendingCommandsHighWater)
                    _pendingCommandsHighWater = _commands.Count;
                if (_pendingCommandBytesTotal > _pendingCommandBytesHighWater)
                    _pendingCommandBytesHighWater = _pendingCommandBytesTotal;
            }
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
                if (result == NetworkCommandResult.Dispatched ||
                    result == NetworkCommandResult.PolicyRejected)
                {
                    _processedCommands[pending.Session.Connection] = new ProcessedCommandCursor(
                        pending.Envelope.TargetTick,
                        pending.Envelope.Sequence);
                }
                DecrementPending(pending.Envelope);
                var envelope = pending.Envelope;
                envelope.Dispose();
            }
            if (summary.Total > 0) _commands.RemoveRange(0, summary.Total);
            return summary;
        }

        /// <summary>Stores one immutable capture shared only within the specified scope.</summary>
        internal void StoreCapture(ScopeId scope, NetworkSnapshot snapshot)
        {
            if (snapshot == null) throw new ArgumentNullException(nameof(snapshot));
            if (snapshot.Scope != scope) throw new InvalidOperationException("Snapshot scope does not match its history key.");
            if (!_history.TryGetValue(scope, out var history)) { history = new NetworkHistory<NetworkSnapshot>(_historyCapacity, _historyBytes, value => value.ByteLength, value => value.Dispose()); _history.Add(scope, history); }
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
        internal uint OldestHistoryTick(ScopeId scope) => _history.TryGetValue(scope, out var history) ? history.OldestTick : 0;
        internal uint NewestHistoryTick(ScopeId scope) => _history.TryGetValue(scope, out var history) ? history.NewestTick : 0;
        internal NetworkHistory<NetworkSnapshot> History(ScopeId scope) => _history.TryGetValue(scope, out var history) ? history : null;

        internal void Clear()
        {
            for (var i = 0; i < _commands.Count; i++)
            {
                var envelope = _commands[i].Envelope;
                envelope.Dispose();
            }
            _commands.Clear();
            _processedCommands.Clear();
            _pendingCommandCounts.Clear();
            _pendingCommandBytes.Clear();
            _pendingCommandBytesTotal = 0;
            _sessions.Clear();
            foreach (var history in _history.Values)
                history.Clear();
            _history.Clear();
        }

        private void DecrementPending(in NetworkCommandEnvelope envelope)
        {
            var connection = envelope.Connection;
            if (_pendingCommandCounts.TryGetValue(connection, out var count))
                _pendingCommandCounts[connection] = Math.Max(0, count - 1);
            if (_pendingCommandBytes.TryGetValue(connection, out var bytes))
                _pendingCommandBytes[connection] = Math.Max(0,
                    bytes - envelope.ExactLength);
            _pendingCommandBytesTotal = Math.Max(0,
                _pendingCommandBytesTotal - envelope.ExactLength);
        }

        private bool ScopeIsActive(ScopeId scope)
        {
            foreach (var session in _sessions.Values)
                if (session.Scope == scope)
                    return true;
            return false;
        }

        internal bool TryGetProcessedCommand(ConnectionId connection, out ProcessedCommandCursor cursor)
            => _processedCommands.TryGetValue(connection, out cursor);

        private readonly struct PendingCommand
        {
            internal PendingCommand(NetworkSession<TWorld> session, NetworkSchemaEntry entry, NetworkCommandEnvelope envelope) { Session = session; Entry = entry; Envelope = envelope; }
            internal NetworkSession<TWorld> Session { get; }
            internal NetworkSchemaEntry Entry { get; }
            internal NetworkCommandEnvelope Envelope { get; }
        }
    }

    internal readonly struct ProcessedCommandCursor
    {
        internal ProcessedCommandCursor(uint tick, uint sequence)
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
