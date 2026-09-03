using System;
using System.Collections.Generic;
using System.Diagnostics;
using FFS.Libraries.StaticEcs;
using FFS.Libraries.StaticPack;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Owns all mutable state for one connection.</summary>
    public sealed class NetworkSession<TWorld> where TWorld : struct, IWorldType
    {
        private readonly NetworkSchema<TWorld> _schema;
        private readonly INetworkObserver _observer;
        private readonly NetworkBufferPool _bufferPool;
        private int _commandWriteCapacity = 256;
        private uint _nextSendSequence = 1;
        private uint _nextReceiveSequence = 1;
        private uint _nextReceivePacketSequence = 1;
        private uint _lastReceiveCommandPacketSequence;

        /// <summary>Creates a handshaking per-connection session.</summary>
        internal NetworkSession(ConnectionId connection, NetworkRole role,
            NetworkSchema<TWorld> schema, NetworkBufferPool bufferPool = null,
            INetworkObserver observer = null)
        { Connection = connection; Role = role; _schema = schema ?? throw new ArgumentNullException(nameof(schema)); _bufferPool = bufferPool ?? new NetworkBufferPool(NetworkBufferPool.DefaultClientRetainedBytes); _observer = observer; State = NetworkSessionState.Handshaking; }
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

        /// <summary>Completes the v7 handshake after exact shared-manifest fingerprint comparison.</summary>
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
            envelope = default;
            if (Role != NetworkRole.Client || State != NetworkSessionState.Established) return NetworkCommandResult.WrongSession;
            if (typeof(INetworkTransactionCommand).IsAssignableFrom(typeof(TCommand)))
                return NetworkCommandResult.SchemaMismatch;
            if (_nextSendSequence == uint.MaxValue) return NetworkCommandResult.Sequence;
            return CreateEnvelope(command, targetTick, _nextSendSequence++, out envelope);
        }

        /// <summary>Serializes one reliable transaction without consuming the input sequence.</summary>
        internal NetworkCommandResult CreateTransaction<TCommand>(in TCommand command,
            out NetworkCommandEnvelope envelope)
            where TCommand : struct, IEvent, INetworkCommand
        {
            envelope = default;
            if (Role != NetworkRole.Client || State != NetworkSessionState.Established)
                return NetworkCommandResult.WrongSession;
            if (!typeof(INetworkTransactionCommand).IsAssignableFrom(typeof(TCommand)))
                return NetworkCommandResult.SchemaMismatch;
            return CreateEnvelope(command, PacketHeader.NoneTick, 0, out envelope);
        }

        private NetworkCommandResult CreateEnvelope<TCommand>(TCommand command, uint targetTick,
            uint sequence, out NetworkCommandEnvelope envelope)
            where TCommand : struct, IEvent, INetworkCommand
        {
            envelope = default;
            var entries = _schema.RetainedEntries;
            for (var i = 0; i < entries.Length; i++)
            {
                var entry = entries[i];
                if (entry.Kind != NetworkSchemaKind.Command ||
                    entry.RuntimeType != typeof(TCommand) ||
                    entry.Invoker is not ICommandNetworkInvoker<TWorld, TCommand> invoker)
                    continue;
                var payload = _bufferPool.Rent(_commandWriteCapacity);
                var writer = BinaryPackWriter.Create(payload.Buffer);
                try
                {
                    invoker.Write(in command, ref writer, entry.MaxBytes);
                }
                catch
                {
                    payload.Dispose();
                    throw;
                }
                if (!ReferenceEquals(writer.Buffer, payload.Buffer))
                {
                    payload.Dispose();
                    payload = _bufferPool.Adopt(writer.Buffer,
                        checked((int)writer.Position));
                }
                else
                {
                    payload.SetLength(checked((int)writer.Position));
                }
                _commandWriteCapacity = Math.Max(_commandWriteCapacity,
                    payload.Capacity);
                envelope = new NetworkCommandEnvelope(Connection, PeerId, Epoch, sequence,
                    targetTick, entry.TypeId, entry.Version, payload);
                return NetworkCommandResult.Queued;
            }
            return NetworkCommandResult.SchemaMismatch;
        }

        internal NetworkCommandResult Validate(NetworkCommandEnvelope envelope, uint serverTick, uint pastWindow, uint futureWindow, out NetworkSchemaEntry entry)
        {
            entry = null;
            if (Role != NetworkRole.Server || State != NetworkSessionState.Established || envelope.ExactBuffer == null || envelope.Connection != Connection || envelope.PeerId != PeerId || envelope.Epoch != Epoch) return NetworkCommandResult.WrongSession;
            if (!_schema.TryGet(envelope.TypeId, out entry) || entry.Kind != NetworkSchemaKind.Command || entry.Version != envelope.Version || envelope.ExactLength > entry.MaxBytes || entry.Invoker is not ICommandNetworkInvoker<TWorld> invoker || !invoker.HasPolicy) return NetworkCommandResult.SchemaMismatch;
            if (typeof(INetworkTransactionCommand).IsAssignableFrom(entry.RuntimeType)) return NetworkCommandResult.SchemaMismatch;
            if (envelope.Sequence < _nextReceiveSequence) return NetworkCommandResult.Duplicate;
            if (envelope.TargetTick < serverTick - Math.Min(serverTick, pastWindow) || envelope.TargetTick > serverTick + futureWindow) return NetworkCommandResult.TickWindow;
            if (envelope.Sequence == uint.MaxValue) return NetworkCommandResult.Sequence;
            _nextReceiveSequence = checked(envelope.Sequence + 1);
            return NetworkCommandResult.Queued;
        }

        internal NetworkCommandResult ValidateTransaction(
            NetworkCommandEnvelope envelope, out NetworkSchemaEntry entry)
        {
            entry = null;
            if (Role != NetworkRole.Server ||
                State != NetworkSessionState.Established ||
                envelope.ExactBuffer == null ||
                envelope.Connection != Connection || envelope.PeerId != PeerId ||
                envelope.Epoch != Epoch)
                return NetworkCommandResult.WrongSession;
            if (!_schema.TryGet(envelope.TypeId, out entry) ||
                entry.Kind != NetworkSchemaKind.Command ||
                !typeof(INetworkTransactionCommand).IsAssignableFrom(entry.RuntimeType) ||
                entry.Version != envelope.Version ||
                envelope.ExactLength > entry.MaxBytes ||
                entry.Invoker is not ICommandNetworkInvoker<TWorld> invoker ||
                !invoker.HasPolicy)
                return NetworkCommandResult.SchemaMismatch;
            return NetworkCommandResult.Queued;
        }

        internal NetworkCommandResult Dispatch(NetworkCommandEnvelope envelope, NetworkSchemaEntry entry)
        {
            return Dispatch(envelope, entry, NetworkCommandDelivery.Input, default);
        }

        internal NetworkCommandResult Dispatch(NetworkCommandEnvelope envelope,
            NetworkSchemaEntry entry, NetworkCommandDelivery delivery,
            NetworkTransactionId transactionId)
        {
            var context = new NetworkCommandContext(PeerId, Epoch, envelope.Sequence,
                envelope.TargetTick, delivery, entry.TypeId, transactionId);
            return ((ICommandNetworkInvoker<TWorld>)entry.Invoker).Dispatch(
                envelope.ExactBuffer, envelope.ExactOffset, envelope.ExactLength,
                entry.Version, in context);
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
            if (header.Kind == PacketKind.CommandBatch)
            {
                if (header.Flags != PacketFlags.UnreliableSequenced ||
                    header.PacketSequence == 0)
                    return PacketValidationResult.Sequence;
                if (header.PacketSequence <= _lastReceiveCommandPacketSequence)
                    return PacketValidationResult.Duplicate;
                _lastReceiveCommandPacketSequence = header.PacketSequence;
                return PacketValidationResult.Success;
            }
            if (header.Kind == PacketKind.TransactionCommand)
            {
                if (Role != NetworkRole.Server || header.Flags != PacketFlags.ReliableOrdered)
                    return PacketValidationResult.WrongRole;
                if (header.PacketSequence == 0)
                    return PacketValidationResult.Sequence;
                if (header.PacketSequence == _nextReceivePacketSequence)
                {
                    _nextReceivePacketSequence++;
                    return PacketValidationResult.Success;
                }
                // A transport duplicate may repeat a reliable transaction packet. The
                // transaction id cache performs exact-once filtering after framing.
                if (header.PacketSequence < _nextReceivePacketSequence)
                    return PacketValidationResult.Duplicate;
                return PacketValidationResult.Sequence;
            }
            if (header.Kind == PacketKind.TransactionReceipt)
            {
                if (Role != NetworkRole.Client || header.Flags != PacketFlags.ReliableOrdered)
                    return PacketValidationResult.WrongRole;
                if (header.PacketSequence == 0)
                    return PacketValidationResult.Sequence;
                if (header.PacketSequence != _nextReceivePacketSequence)
                    return header.PacketSequence < _nextReceivePacketSequence
                        ? PacketValidationResult.Duplicate
                        : PacketValidationResult.Sequence;
                _nextReceivePacketSequence++;
                return PacketValidationResult.Success;
            }
            if (header.Kind == PacketKind.SnapshotChunk)
            {
                if (header.Flags != PacketFlags.ReliableOrdered ||
                    header.PacketSequence == 0)
                    return PacketValidationResult.Sequence;
                return PacketValidationResult.Success;
            }
            if (header.Flags != PacketFlags.ReliableOrdered ||
                header.PacketSequence != _nextReceivePacketSequence)
                return PacketValidationResult.Sequence;
            _nextReceivePacketSequence++;
            return PacketValidationResult.Success;
        }

        private bool IsAllowedEstablishedPacket(PacketKind kind) => Role == NetworkRole.Server
            ? kind == PacketKind.CommandBatch ||
              kind == PacketKind.TransactionCommand ||
              kind == PacketKind.Ping || kind == PacketKind.Ack ||
              kind == PacketKind.ResyncRequest || kind == PacketKind.Disconnect
            : kind == PacketKind.SnapshotChunk || kind == PacketKind.Pong ||
              kind == PacketKind.TransactionReceipt ||
              kind == PacketKind.ResyncRequest || kind == PacketKind.Disconnect;

        internal void Close() => State = NetworkSessionState.Closed;

        internal uint LastReceivedPacketSequence =>
            _nextReceivePacketSequence == 0 ? uint.MaxValue :
            _nextReceivePacketSequence - 1;

        private NetworkAdmissionResult Reject(NetworkAdmissionResult result) { State = NetworkSessionState.Rejected; return result; }
        internal void Trace(NetworkPhase phase, NetworkTraceKind kind, NetworkResultCategory result, NetworkPacketKind packetKind, uint serverTick, uint targetTick, int bytes, int historyTicks, long historyBytes, int tickGap, long durationNanoseconds, int entities = 0, int records = 0, int commands = 0, int queueSize = 0, int activeConnections = -1, int activePeers = -1, int acceptedCommands = 0, int rejectedCommands = 0, NetworkResyncReason resyncReason = NetworkResyncReason.None, NetworkResyncSource resyncSource = NetworkResyncSource.None, uint resyncCorrelationId = 0, NetworkCommandResult? commandResult = null, SnapshotApplyResult? snapshotResult = null, PacketValidationResult? packetValidationResult = null, uint sequence = 0, uint acknowledgedSnapshotTick = 0, uint oldestHistoryTick = 0, uint newestHistoryTick = 0)
        {
            if (_observer == null) return;
            try { var packets = phase == NetworkPhase.Receive || phase == NetworkPhase.Decode || phase == NetworkPhase.Send ? 1 : 0; var connections = activeConnections < 0 ? State == NetworkSessionState.Closed ? 0 : 1 : activeConnections; var peers = activePeers < 0 ? State == NetworkSessionState.Established ? 1 : 0 : activePeers; var value = new NetworkTraceEvent(phase, kind, result, Role, Connection.Value, PeerId, Epoch, serverTick, targetTick, bytes, packets, entities, records, commands, queueSize, historyTicks, connections, peers, Stopwatch.GetTimestamp(), packetKind, historyBytes, tickGap, durationNanoseconds, _schema.Fingerprint, acceptedCommands, rejectedCommands, resyncReason: resyncReason, resyncSource: resyncSource, resyncCorrelationId: resyncCorrelationId, commandResult: commandResult, snapshotResult: snapshotResult, packetValidationResult: packetValidationResult, sequence: sequence, acknowledgedSnapshotTick: acknowledgedSnapshotTick, oldestHistoryTick: oldestHistoryTick, newestHistoryTick: newestHistoryTick); _observer.Observe(in value); }
            catch { }
        }

        internal void ReportSession(uint serverTick, uint acknowledgedSnapshotTick, uint serverProcessedCommandSequence,
            uint nextSendPacketSequence)
        {
            if (_observer is not INetworkDiagnosticsObserver diagnostics) return;
            try
            {
                var value = new NetworkSessionDiagnostics(Role, State, Connection.Value, PeerId, Epoch, Scope, serverTick,
                    acknowledgedSnapshotTick, serverProcessedCommandSequence, _nextSendSequence, _nextReceiveSequence,
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
}
