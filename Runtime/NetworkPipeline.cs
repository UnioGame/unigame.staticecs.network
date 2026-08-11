using System;
using System.Collections.Generic;
using System.IO;
using System.Diagnostics;
using FFS.Libraries.StaticEcs;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Runs the framed receive, decode, apply, gameplay-boundary, and send pipeline for one client connection.</summary>
    public sealed class NetworkClient<TWorld> where TWorld : struct, IWorldType
    {
        private readonly INetworkTransport _transport;
        private readonly NetworkSchema<TWorld> _schema;
        private readonly NetworkReplicator<TWorld> _replicator;
        private readonly NetworkSession<TWorld> _session;
        private readonly List<NetworkCommandEnvelope> _recentCommands = new List<NetworkCommandEnvelope>();
        private readonly int _commandRedundancy;
        private readonly int _ticksPerSecond;
        private readonly int _predictionLeadTicks;
        private readonly ulong _simulationFingerprint;
        private readonly ulong _contentFingerprint;
        private uint _packetSequence = 1;
        private uint _commandPacketSequence = 1;
        private uint _lastCommandFlushTick;
        private bool _commandsDirty;
        private long _lastServerTickTimestamp;
        private long _lastPingTimestamp;
        private double _roundTripSeconds;

        /// <summary>Creates an isolated client pipeline.</summary>
        public NetworkClient(INetworkTransport transport, NetworkSchema<TWorld> schema,
            ScopeId scope = default, INetworkObserver observer = null,
            int ticksPerSecond = 20, int predictionLeadTicks = 1,
            int commandRedundancy = 3, ulong simulationFingerprint = 0,
            ulong contentFingerprint = 0)
        {
            _transport = transport ?? throw new ArgumentNullException(nameof(transport));
            _schema = schema ?? throw new ArgumentNullException(nameof(schema));
            if (ticksPerSecond <= 0) throw new ArgumentOutOfRangeException(nameof(ticksPerSecond));
            if (predictionLeadTicks < 0) throw new ArgumentOutOfRangeException(nameof(predictionLeadTicks));
            if (commandRedundancy <= 0 || commandRedundancy > ProtocolLimits.MaxCommandsPerBatch)
                throw new ArgumentOutOfRangeException(nameof(commandRedundancy));
            _ticksPerSecond = ticksPerSecond;
            _predictionLeadTicks = predictionLeadTicks;
            _commandRedundancy = commandRedundancy;
            _simulationFingerprint = simulationFingerprint;
            _contentFingerprint = contentFingerprint;
            _replicator = new NetworkReplicator<TWorld>(schema, scope);
            _session = new NetworkSession<TWorld>(transport.Connection, NetworkRole.Client, schema, observer);
            _session.ReportSession(0, 0, 0, _packetSequence);
        }

        /// <summary>Gets the per-connection session.</summary>
        public NetworkSession<TWorld> Session => _session;
        /// <summary>Gets bounded successfully applied snapshot history.</summary>
        public NetworkHistory<NetworkSnapshot> History => _replicator.History;
        /// <summary>Gets the latest acknowledged authoritative tick.</summary>
        public uint AcknowledgedSnapshotTick { get; private set; }
        /// <summary>Gets the latest command tick confirmed as processed into an applied snapshot.</summary>
        public uint ServerProcessedCommandTick { get; private set; }
        /// <summary>Gets the latest command sequence confirmed as processed into an applied snapshot.</summary>
        public uint ServerProcessedCommandSequence { get; private set; }
        /// <summary>Gets the latest authoritative server tick from a validated packet.</summary>
        public uint ServerTick { get; private set; }
        /// <summary>Gets the estimated current authoritative tick including prediction lead.</summary>
        public uint EstimatedServerTick
        {
            get
            {
                if (ServerTick == 0) return 1;
                double elapsed = _lastServerTickTimestamp == 0
                    ? 0d
                    : (Stopwatch.GetTimestamp() - _lastServerTickTimestamp) /
                      (double)Stopwatch.Frequency;
                double ahead = (elapsed + _roundTripSeconds * 0.5d) * _ticksPerSecond +
                               _predictionLeadTicks;
                return checked(ServerTick + (uint)Math.Max(1d, Math.Ceiling(ahead)));
            }
        }
        /// <summary>Gets whether malformed or rejected network state requested resynchronization.</summary>
        public bool ResyncRequested { get; private set; }
        /// <summary>Gets whether snapshot hooks left the replica world requiring full recreation.</summary>
        public bool ReplicaResetRequired { get; private set; }

        /// <summary>Closes the session and removes all replica-owned entities from the client world.</summary>
        public void Disconnect()
        {
            _session.Close();
            _replicator.ClearReplicas();
            AcknowledgedSnapshotTick = 0;
            ServerProcessedCommandTick = 0;
            ServerProcessedCommandSequence = 0;
            ServerTick = 0;
            _lastServerTickTimestamp = 0;
            _lastPingTimestamp = 0;
            _roundTripSeconds = 0d;
            _recentCommands.Clear();
            _lastCommandFlushTick = 0;
            _commandsDirty = false;
            ResyncRequested = false;
            ReplicaResetRequired = false;
            _session.ReportSession(0, 0, 0, _packetSequence);
        }

        /// <summary>Sends the protocol-four Hello packet.</summary>
        public bool BeginHandshake() => Send(PacketKind.Hello, 0, PacketHeader.NoneTick, PacketHeader.NoneTick, ReadOnlySpan<byte>.Empty);

        /// <summary>Processes received packets using authoritative ticks carried by the wire.</summary>
        public void Process()
        {
            while (true)
            {
                var receiveStarted = Stopwatch.GetTimestamp();
                if (!_transport.TryReceive(out var packet)) break;
                _session.Trace(NetworkPhase.Receive, NetworkTraceKind.Point, NetworkResultCategory.Success, NetworkPacketKind.None, AcknowledgedSnapshotTick, 0, packet?.Length ?? 0, History.Count, History.Bytes, 0, ElapsedNanoseconds(receiveStarted));
                var started = Stopwatch.GetTimestamp();
                if (!NetworkPacket.TryDecode(packet, out var header, out var payload) ||
                    header.Kind != PacketKind.Disconnect &&
                    (header.SchemaFingerprint != _schema.Fingerprint ||
                     header.SimulationFingerprint != _simulationFingerprint ||
                     header.ContentFingerprint != _contentFingerprint) ||
                    _session.ValidatePacket(in header) != PacketValidationResult.Success) { _session.Trace(NetworkPhase.Decode, NetworkTraceKind.Point, NetworkResultCategory.Protocol, NetworkPacketKind.None, AcknowledgedSnapshotTick, 0, packet?.Length ?? 0, History.Count, History.Bytes, 0, ElapsedNanoseconds(started)); RequestResync(AcknowledgedSnapshotTick); continue; }
                if (header.ServerTick != PacketHeader.NoneTick && header.ServerTick >= ServerTick)
                {
                    ServerTick = header.ServerTick;
                    _lastServerTickTimestamp = Stopwatch.GetTimestamp();
                }
                StagedNetworkSnapshot staged = null;
                var entities = 0;
                var records = 0;
                var decodedBytes = packet.Length;
                var decodeResult = NetworkResultCategory.Success;
                if (header.Kind == PacketKind.Ready) DecodeReady(header, payload);
                else if (header.Kind == PacketKind.FullSnapshot) decodeResult = DiagnosticResult(TryStageSnapshot(header, payload, out staged, out entities, out records, out decodedBytes));
                else if (header.Kind == PacketKind.Pong) DecodePong(payload);
                else if (header.Kind == PacketKind.ResyncRequest) ResyncRequested = true;
                var disconnected = header.Kind == PacketKind.Disconnect;
                _session.Trace(NetworkPhase.Decode, NetworkTraceKind.Point, decodeResult, DiagnosticKind(header.Kind), header.ServerTick, 0, decodedBytes, History.Count, History.Bytes, unchecked((int)(header.ServerTick - AcknowledgedSnapshotTick)), ElapsedNanoseconds(started), entities, records);
                if (disconnected)
                {
                    Disconnect();
                    return;
                }
                if (header.Kind == PacketKind.FullSnapshot)
                {
                    if (staged == null) RequestResync(header.ServerTick);
                    else ApplySnapshot(staged, header, entities, records);
                }
                _session.ReportSession(ServerTick, AcknowledgedSnapshotTick, ServerProcessedCommandSequence, _packetSequence);
            }
        }

        /// <summary>Queues one command and flushes the redundant command batch immediately.</summary>
        public NetworkCommandResult SendCommand<TCommand>(in TCommand command, uint targetTick)
            where TCommand : struct, IEvent, INetworkCommand
        {
            return SendCommand(in command, targetTick, out _);
        }

        /// <summary>Queues one command, flushes the batch, and returns its assigned sequence.</summary>
        public NetworkCommandResult SendCommand<TCommand>(in TCommand command, uint targetTick,
            out uint sequence)
            where TCommand : struct, IEvent, INetworkCommand
        {
            var result = QueueCommand(in command, targetTick, out sequence);
            if (result != NetworkCommandResult.Queued) return result;
            return FlushCommands(targetTick);
        }

        /// <summary>Serializes one command into the current redundant tick batch.</summary>
        public NetworkCommandResult QueueCommand<TCommand>(in TCommand command, uint targetTick,
            out uint sequence)
            where TCommand : struct, IEvent, INetworkCommand
        {
            sequence = 0;
            PruneCommands(targetTick);
            if (_recentCommands.Count >= Math.Min((int)byte.MaxValue, ProtocolLimits.MaxCommandsPerBatch))
                return NetworkCommandResult.LimitExceeded;
            var result = _session.CreateCommand(in command, targetTick, out var envelope);
            if (result != NetworkCommandResult.Queued) return result;
            sequence = envelope.Sequence;
            _recentCommands.Add(envelope);
            _commandsDirty = true;
            return result;
        }

        /// <summary>Sends the current command batch when its tick advanced or new commands were queued.</summary>
        public NetworkCommandResult FlushCommands(uint currentTick)
        {
            if (_session.State != NetworkSessionState.Established)
                return NetworkCommandResult.WrongSession;
            PruneCommands(currentTick);
            if (!_commandsDirty && currentTick <= _lastCommandFlushTick)
                return NetworkCommandResult.Queued;
            _lastCommandFlushTick = currentTick;
            if (_recentCommands.Count == 0)
            {
                _commandsDirty = false;
                return NetworkCommandResult.Queued;
            }
            int length = 1;
            for (var i = 0; i < _recentCommands.Count; i++)
                length = checked(length + 17 + _recentCommands[i].ExactPayload.Length);
            if (_recentCommands.Count > Math.Min((int)byte.MaxValue, ProtocolLimits.MaxCommandsPerBatch) ||
                length > ProtocolLimits.MaxWirePayloadBytes)
                return NetworkCommandResult.LimitExceeded;
            var payload = new byte[length];
            payload[0] = checked((byte)_recentCommands.Count);
            int offset = 1;
            for (var i = 0; i < _recentCommands.Count; i++)
            {
                var command = _recentCommands[i];
                Hashing.Write32(payload, offset, command.Sequence);
                Hashing.Write32(payload, offset + 4, command.TargetTick);
                Hashing.Write32(payload, offset + 8, command.TypeId.Value);
                payload[offset + 12] = command.Version;
                Hashing.Write32(payload, offset + 13, checked((uint)command.ExactPayload.Length));
                command.ExactPayload.CopyTo(payload, offset + 17);
                offset += 17 + command.ExactPayload.Length;
            }
            var header = Header(PacketKind.CommandBatch, _commandPacketSequence++, ServerTick);
            header.Flags = PacketFlags.UnreliableSequenced;
            var started = Stopwatch.GetTimestamp();
            var sent = NetworkPacket.TryEncode(header, payload, out var packet) &&
                       _transport.TrySend(packet);
            _session.Trace(NetworkPhase.Send, NetworkTraceKind.Point,
                sent ? NetworkResultCategory.Success : NetworkResultCategory.Transport,
                NetworkPacketKind.CommandBatch, ServerTick, currentTick,
                packet?.Length ?? 0, History.Count, History.Bytes,
                unchecked((int)(ServerTick - AcknowledgedSnapshotTick)),
                ElapsedNanoseconds(started), commands: _recentCommands.Count);
            _session.ReportSession(ServerTick, AcknowledgedSnapshotTick,
                ServerProcessedCommandSequence, _packetSequence);
            if (!sent)
                return NetworkCommandResult.Malformed;
            _commandsDirty = false;
            return NetworkCommandResult.Queued;
        }

        /// <summary>Requests a clean full snapshot after local history or replica state became unusable.</summary>
        public void RequestFullResync()
        {
            RequestResync(ServerTick);
        }

        /// <summary>Sends a periodic clock synchronization sample for server-tick estimation.</summary>
        public bool SynchronizeClock()
        {
            if (_session.State != NetworkSessionState.Established) return false;
            long now = Stopwatch.GetTimestamp();
            if (_lastPingTimestamp != 0 &&
                now - _lastPingTimestamp < Stopwatch.Frequency)
                return false;
            _lastPingTimestamp = now;
            var payload = new byte[8];
            Hashing.Write64(payload, 0, unchecked((ulong)now));
            return Send(PacketKind.Ping, _session.Epoch, ServerTick,
                PacketHeader.NoneTick, payload);
        }

        private void PruneCommands(uint currentTick)
        {
            uint oldestTick = currentTick > (uint)_commandRedundancy
                ? currentTick - (uint)_commandRedundancy
                : 0;
            for (var i = _recentCommands.Count - 1; i >= 0; i--)
                if (_recentCommands[i].TargetTick < oldestTick)
                    _recentCommands.RemoveAt(i);
        }

        private void DecodePong(ReadOnlyMemory<byte> payload)
        {
            if (payload.Length != 8) return;
            long sent = unchecked((long)Hashing.Read64(payload.Span, 0));
            long elapsed = Stopwatch.GetTimestamp() - sent;
            if (elapsed < 0) return;
            double sample = elapsed / (double)Stopwatch.Frequency;
            _roundTripSeconds = _roundTripSeconds <= 0d
                ? sample
                : _roundTripSeconds * 0.8d + sample * 0.2d;
        }

        private void DecodeReady(PacketHeader header, ReadOnlyMemory<byte> payload)
        {
            if (_session.State != NetworkSessionState.Handshaking || header.SessionEpoch == 0 || payload.Length != 12) { ResyncRequested = true; return; }
            var bytes = payload.Span;
            var peer = Hashing.Read32(bytes, 0);
            var scope = new ScopeId(Hashing.Read64(bytes, 4));
            if (_session.Admit(header.SchemaFingerprint, peer, header.SessionEpoch, scope) != NetworkAdmissionResult.Accepted) ResyncRequested = true;
        }

        private SnapshotApplyResult TryStageSnapshot(PacketHeader header, ReadOnlyMemory<byte> payload, out StagedNetworkSnapshot staged, out int entities, out int records, out int decodedBytes)
        {
            staged = null; entities = 0; records = 0; decodedBytes = payload.Length;
            if (payload.Length < 8) return SnapshotApplyResult.Malformed;
            var bytes = payload.Span;
            entities = unchecked((int)Hashing.Read32(bytes, 0));
            records = unchecked((int)Hashing.Read32(bytes, 4));
            var exact = payload.Slice(8).ToArray();
            decodedBytes = exact.Length;
            var snapshot = new NetworkSnapshot(header.ServerTick, header.SchemaFingerprint, _session.Scope, exact, entities, records);
            return _replicator.Stage(snapshot, out staged);
        }

        private void ApplySnapshot(StagedNetworkSnapshot staged, PacketHeader header,
            int entities, int records)
        {
            var started = Stopwatch.GetTimestamp();
            SnapshotApplyResult result;
            try
            {
                result = _replicator.Apply(staged);
            }
            catch (Exception)
            {
                ReplicaResetRequired = true;
                try { _replicator.ClearReplicas(); }
                catch { }
                RequestResync(staged.ServerTick);
                return;
            }
            _session.Trace(NetworkPhase.SnapshotApply, NetworkTraceKind.Point, DiagnosticResult(result), NetworkPacketKind.FullSnapshot, staged.ServerTick, 0, staged.Snapshot.ByteLength, History.Count, History.Bytes, unchecked((int)(staged.ServerTick - AcknowledgedSnapshotTick)), ElapsedNanoseconds(started), entities, records);
            if (result != SnapshotApplyResult.Success) { RequestResync(staged.ServerTick); return; }
            AcknowledgedSnapshotTick = staged.ServerTick;
            ServerProcessedCommandTick = header.ServerProcessedCommandTick;
            ServerProcessedCommandSequence = header.ServerProcessedCommandSequence;
            ResyncRequested = false;
            _session.ReportSnapshot(staged.Snapshot, History);
            Send(PacketKind.Ack, _session.Epoch, PacketHeader.NoneTick, AcknowledgedSnapshotTick, ReadOnlySpan<byte>.Empty);
        }

        private void RequestResync(uint serverTick)
        {
            ResyncRequested = true;
            Send(PacketKind.ResyncRequest, _session.Epoch, serverTick, AcknowledgedSnapshotTick, ReadOnlySpan<byte>.Empty);
        }

        private bool Send(PacketKind kind, uint epoch, uint serverTick, uint acknowledgedTick, ReadOnlySpan<byte> payload)
        {
            var started = Stopwatch.GetTimestamp();
            var header = Header(kind, _packetSequence++, serverTick);
            header.SessionEpoch = epoch;
            header.AcknowledgedSnapshotTick = acknowledgedTick;
            var sent = NetworkPacket.TryEncode(header, payload, out var packet) && _transport.TrySend(packet);
            _session.Trace(NetworkPhase.Send, NetworkTraceKind.Point, sent ? NetworkResultCategory.Success : NetworkResultCategory.Transport, DiagnosticKind(kind), serverTick, 0, packet?.Length ?? 0, History.Count, History.Bytes, unchecked((int)(serverTick - AcknowledgedSnapshotTick)), ElapsedNanoseconds(started));
            _session.ReportSession(ServerTick, AcknowledgedSnapshotTick, ServerProcessedCommandSequence, _packetSequence);
            return sent;
        }

        private PacketHeader Header(PacketKind kind, uint sequence, uint serverTick) => new PacketHeader
        {
            Kind = kind, Flags = PacketFlags.ReliableOrdered, Compression = NetworkCompression.None,
            SessionEpoch = _session.Epoch, PacketSequence = sequence, ServerTick = serverTick,
            AcknowledgedSnapshotTick = AcknowledgedSnapshotTick,
            SchemaFingerprint = _schema.Fingerprint,
            SimulationFingerprint = _simulationFingerprint,
            ContentFingerprint = _contentFingerprint
        };
        private static NetworkPacketKind DiagnosticKind(PacketKind kind) => (NetworkPacketKind)(byte)kind;
        internal static NetworkResultCategory DiagnosticResult(SnapshotApplyResult result) => result switch
        {
            SnapshotApplyResult.Success => NetworkResultCategory.Success,
            SnapshotApplyResult.SchemaMismatch => NetworkResultCategory.Schema,
            SnapshotApplyResult.Malformed => NetworkResultCategory.Malformed,
            SnapshotApplyResult.LimitExceeded => NetworkResultCategory.Limits,
            SnapshotApplyResult.EntityConflict => NetworkResultCategory.World,
            _ => NetworkResultCategory.World
        };
        private static long ElapsedNanoseconds(long started) => (Stopwatch.GetTimestamp() - started) * 1000000000L / Stopwatch.Frequency;
    }

    /// <summary>Runs framed receive, decode, command dispatch, capture, and send for isolated server connections.</summary>
    public sealed class NetworkServer<TWorld> where TWorld : struct, IWorldType
    {
        private readonly NetworkSchema<TWorld> _schema;
        private readonly NetworkServerCoordinator<TWorld> _coordinator;
        private readonly NetworkReplicator<TWorld> _replicator;
        private readonly List<Peer> _peers = new List<Peer>();
        private readonly INetworkObserver _observer;
        private readonly INetworkPeerObserver _peerObserver;
        private readonly INetworkPeerAdmissionPolicy _admissionPolicy;
        private readonly ulong _simulationFingerprint;
        private readonly ulong _contentFingerprint;

        /// <summary>Gets the latest authoritative tick completed by this server.</summary>
        public uint ServerTick { get; private set; }

        /// <summary>Creates a multi-connection authoritative server pipeline.</summary>
        public NetworkServer(NetworkSchema<TWorld> schema, NetworkScopeSelector<TWorld> scopeSelector, int historyTicks = 64, long historyBytes = 32 * 1024 * 1024, INetworkObserver observer = null, INetworkPeerObserver peerObserver = null, INetworkPeerAdmissionPolicy admissionPolicy = null, ulong simulationFingerprint = 0, ulong contentFingerprint = 0)
        {
            _schema = schema ?? throw new ArgumentNullException(nameof(schema));
            if (scopeSelector == null) throw new ArgumentNullException(nameof(scopeSelector));
            _coordinator = new NetworkServerCoordinator<TWorld>(historyTicks, historyBytes);
            _replicator = new NetworkReplicator<TWorld>(schema, scopeSelector);
            _observer = observer;
            _peerObserver = peerObserver;
            _admissionPolicy = admissionPolicy;
            _simulationFingerprint = simulationFingerprint;
            _contentFingerprint = contentFingerprint;
        }

        /// <summary>Adds one transport-owned connection with server-assigned identity and scope.</summary>
        public NetworkSession<TWorld> AddConnection(INetworkTransport transport, uint peerId, uint epoch, ScopeId scope, INetworkObserver observer = null)
        {
            if (transport == null) throw new ArgumentNullException(nameof(transport));
            if (peerId == 0) throw new ArgumentOutOfRangeException(nameof(peerId), "Peer identity zero is reserved.");
            for (var i = 0; i < _peers.Count; i++)
            {
                if (_peers[i].Transport.Connection == transport.Connection)
                    throw new InvalidOperationException("Connection already exists.");
                if (_peers[i].PeerId == peerId)
                    throw new InvalidOperationException("Peer identity already exists.");
            }
            var session = new NetworkSession<TWorld>(transport.Connection, NetworkRole.Server, _schema, observer ?? _observer);
            var peer = new Peer(transport, session, peerId, epoch, scope);
            _peers.Add(peer);
            session.ReportSession(ServerTick, 0, 0, peer.PacketSequence);
            return session;
        }

        /// <summary>Closes and removes one connection while preserving scope-shared history.</summary>
        public bool RemoveConnection(ConnectionId connection)
        {
            for (var i = 0; i < _peers.Count; i++)
            {
                if (_peers[i].Transport.Connection != connection) continue;
                CleanupPeer(_peers[i]);
                _peers.RemoveAt(i);
                return true;
            }
            return false;
        }

        /// <summary>Dequeues and decodes all currently available transport packets without advancing simulation time.</summary>
        public void Receive()
        {
            for (var i = 0; i < _peers.Count; i++)
            {
                var peer = _peers[i];
                while (true)
                {
                    var receiveStarted = Stopwatch.GetTimestamp();
                    if (!peer.Transport.TryReceive(out var packet)) break;
                    peer.PendingPackets.Enqueue(packet);
                    peer.Session.Trace(NetworkPhase.Receive, NetworkTraceKind.Point, NetworkResultCategory.Success, NetworkPacketKind.None, ServerTick, 0, packet?.Length ?? 0, 0, 0, unchecked((int)(ServerTick - peer.AcknowledgedSnapshotTick)), ElapsedNanoseconds(receiveStarted), activeConnections: ActiveConnectionCount, activePeers: ActivePeerCount);
                }
                if (!DecodePending(peer)) continue;
                _peers.RemoveAt(i);
                i--;
            }
        }

        /// <summary>Advances exactly one authoritative tick around the supplied gameplay boundary.</summary>
        public void Tick(Action<uint> gameplay)
        {
            if (gameplay == null) throw new ArgumentNullException(nameof(gameplay));
            var serverTick = checked(ServerTick + 1);
            var dispatchStarted = Stopwatch.GetTimestamp();
            var dispatched = _coordinator.Dispatch(serverTick);
            TraceDispatch(serverTick, dispatched, ElapsedNanoseconds(dispatchStarted));
            for (var i = 0; i < _peers.Count; i++)
            {
                var peer = _peers[i];
                if (_coordinator.TryGetProcessedCommand(peer.Transport.Connection, out var cursor))
                {
                    peer.ServerProcessedCommandTick = cursor.Tick;
                    peer.ServerProcessedCommandSequence = cursor.Sequence;
                }
            }
            gameplay(serverTick);
            var captures = new Dictionary<ScopeId, NetworkSnapshot>();
            for (var i = 0; i < _peers.Count; i++)
            {
                var peer = _peers[i];
                if (peer.Session.State != NetworkSessionState.Established) continue;
                if (!captures.TryGetValue(peer.Scope, out var capture))
                {
                    var started = Stopwatch.GetTimestamp();
                    if (_replicator.Capture(serverTick, peer.Scope, out capture) != SnapshotCaptureResult.Success) { peer.Session.Trace(NetworkPhase.SnapshotCapture, NetworkTraceKind.Point, NetworkResultCategory.World, NetworkPacketKind.FullSnapshot, serverTick, 0, 0, 0, 0, 0, ElapsedNanoseconds(started)); continue; }
                    captures.Add(peer.Scope, capture);
                    _coordinator.StoreCapture(peer.Scope, capture);
                    peer.Session.Trace(NetworkPhase.SnapshotCapture, NetworkTraceKind.Point, NetworkResultCategory.Success, NetworkPacketKind.FullSnapshot, serverTick, 0, capture.ByteLength, _coordinator.HistoryCount(peer.Scope), _coordinator.HistoryByteCount(peer.Scope), unchecked((int)(serverTick - peer.AcknowledgedSnapshotTick)), ElapsedNanoseconds(started), capture.EntityCount, capture.RecordCount, activeConnections: ActiveConnectionCount, activePeers: ActivePeerCount);
                }
                peer.Session.ReportSnapshot(capture, _coordinator.History(peer.Scope));
                SendSnapshot(peer, capture);
            }
            ServerTick = serverTick;
        }

        /// <summary>Finds one immutable authoritative capture by scope and tick.</summary>
        public bool TryGetCapture(ScopeId scope, uint serverTick, out NetworkSnapshot snapshot)
            => _coordinator.TryGetCapture(scope, serverTick, out snapshot);

        private bool DecodePending(Peer peer)
        {
            while (peer.PendingPackets.Count > 0)
            {
                var packet = peer.PendingPackets.Dequeue();
                var started = Stopwatch.GetTimestamp();
                if (!NetworkPacket.TryDecode(packet, out var header, out var payload) ||
                    (header.Kind != PacketKind.Hello &&
                     (header.SchemaFingerprint != _schema.Fingerprint ||
                     header.SimulationFingerprint != _simulationFingerprint ||
                     header.ContentFingerprint != _contentFingerprint)) ||
                    peer.Session.ValidatePacket(in header) != PacketValidationResult.Success) { peer.Session.Trace(NetworkPhase.Decode, NetworkTraceKind.Point, NetworkResultCategory.Protocol, NetworkPacketKind.None, ServerTick, 0, packet?.Length ?? 0, 0, 0, unchecked((int)(ServerTick - peer.AcknowledgedSnapshotTick)), ElapsedNanoseconds(started), activeConnections: ActiveConnectionCount, activePeers: ActivePeerCount); continue; }
                if (header.Kind == PacketKind.Hello && !Admit(peer, header.SchemaFingerprint,
                        header.SimulationFingerprint, header.ContentFingerprint))
                    return true;
                else if (header.Kind == PacketKind.CommandBatch) DecodeCommands(peer, payload, checked(ServerTick + 1));
                else if (header.Kind == PacketKind.Ping) Send(peer, PacketKind.Pong,
                    ServerTick, PacketHeader.NoneTick, payload.Span);
                else if (header.Kind == PacketKind.Ack) peer.AcknowledgedSnapshotTick = header.AcknowledgedSnapshotTick;
                else if (header.Kind == PacketKind.ResyncRequest) peer.ResyncRequested = true;
                else if (header.Kind == PacketKind.Disconnect)
                {
                    CleanupPeer(peer);
                }
                peer.Session.Trace(NetworkPhase.Decode, NetworkTraceKind.Point, NetworkResultCategory.Success, DiagnosticKind(header.Kind), ServerTick, 0, packet.Length, 0, 0, unchecked((int)(ServerTick - peer.AcknowledgedSnapshotTick)), ElapsedNanoseconds(started), activeConnections: ActiveConnectionCount, activePeers: ActivePeerCount);
                peer.Session.ReportSession(ServerTick, peer.AcknowledgedSnapshotTick, peer.ServerProcessedCommandSequence, peer.PacketSequence);
                if (header.Kind == PacketKind.Disconnect)
                    return true;
            }
            return false;
        }

        private bool Admit(Peer peer, SchemaFingerprint remoteFingerprint,
            ulong simulationFingerprint, ulong contentFingerprint)
        {
            if (remoteFingerprint != _schema.Fingerprint ||
                simulationFingerprint != _simulationFingerprint ||
                contentFingerprint != _contentFingerprint)
            {
                Send(peer, PacketKind.Disconnect, 0, PacketHeader.NoneTick, ReadOnlySpan<byte>.Empty);
                peer.Session.Close();
                return false;
            }

            var data = peer.Data();
            var policyInvoked = false;
            try
            {
                if (_admissionPolicy != null)
                {
                    policyInvoked = true;
                    if (!_admissionPolicy.TryAdmit(in data, out var rejection))
                    {
                        if (rejection == NetworkAdmissionRejection.None)
                            rejection = NetworkAdmissionRejection.Rejected;
                        TraceAdmissionFailure(peer, rejection);
                        TryRollbackAdmission(in data);
                        Send(peer, PacketKind.Disconnect, 0, PacketHeader.NoneTick, ReadOnlySpan<byte>.Empty);
                        peer.Session.Close();
                        return false;
                    }
                }

                if (peer.Session.Admit(remoteFingerprint, peer.PeerId, peer.Epoch, peer.Scope) !=
                    NetworkAdmissionResult.Accepted)
                    throw new InvalidOperationException("Session rejected a validated peer admission.");

                _coordinator.Add(peer.Session);
                var payload = new byte[12];
                Hashing.Write32(payload, 0, peer.PeerId);
                Hashing.Write64(payload, 4, peer.Scope.Value);
                if (!Send(peer, PacketKind.Ready, 0, PacketHeader.NoneTick, payload))
                    throw new InvalidOperationException("Ready packet could not be sent.");
            }
            catch
            {
                TraceAdmissionFailure(peer, NetworkAdmissionRejection.PolicyError);
                _coordinator.Remove(peer.Transport.Connection);
                peer.Session.Close();
                if (policyInvoked)
                    TryRollbackAdmission(in data);
                Send(peer, PacketKind.Disconnect, 0, PacketHeader.NoneTick, ReadOnlySpan<byte>.Empty);
                return false;
            }

            peer.AdmissionNotified = true;
            try
            {
                NotifyAdmitted(peer);
                return true;
            }
            catch
            {
                Send(peer, PacketKind.Disconnect, 0, PacketHeader.NoneTick, ReadOnlySpan<byte>.Empty);
                CleanupPeer(peer);
                return false;
            }
        }

        private void TryRollbackAdmission(in NetworkPeerData peer)
        {
            try
            {
                _admissionPolicy?.Rollback(in peer);
            }
            catch
            {
                // Admission is already rejected; rollback remains best effort and idempotent.
            }
        }

        private void DecodeCommands(Peer peer, ReadOnlyMemory<byte> payload, uint serverTick)
        {
            if (payload.Length < 1)
            {
                Send(peer, PacketKind.ResyncRequest, serverTick,
                    PacketHeader.NoneTick, ReadOnlySpan<byte>.Empty);
                return;
            }

            var bytes = payload.Span;
            int count = bytes[0];
            if (count < 1 || count > ProtocolLimits.MaxCommandsPerBatch)
            {
                Send(peer, PacketKind.ResyncRequest, serverTick,
                    PacketHeader.NoneTick, ReadOnlySpan<byte>.Empty);
                return;
            }

            int offset = 1;
            var commands = new List<NetworkCommandEnvelope>(count);
            for (var i = 0; i < count; i++)
            {
                if (offset > bytes.Length - 17)
                {
                    Send(peer, PacketKind.ResyncRequest, serverTick,
                        PacketHeader.NoneTick, ReadOnlySpan<byte>.Empty);
                    return;
                }

                uint sequence = Hashing.Read32(bytes, offset);
                uint targetTick = Hashing.Read32(bytes, offset + 4);
                uint idValue = Hashing.Read32(bytes, offset + 8);
                byte version = bytes[offset + 12];
                uint payloadLength = Hashing.Read32(bytes, offset + 13);
                offset += 17;
                if (sequence == 0 || idValue == 0 ||
                    payloadLength > ProtocolLimits.MaxCommandBytes ||
                    payloadLength > (uint)(bytes.Length - offset))
                {
                    Send(peer, PacketKind.ResyncRequest, serverTick,
                        PacketHeader.NoneTick, ReadOnlySpan<byte>.Empty);
                    return;
                }

                var exact = payload.Slice(offset, checked((int)payloadLength)).ToArray();
                offset += checked((int)payloadLength);
                var envelope = new NetworkCommandEnvelope(
                    peer.Transport.Connection,
                    peer.PeerId,
                    peer.Epoch,
                    sequence,
                    targetTick,
                    new NetworkTypeId(idValue),
                    version,
                    exact);
                commands.Add(envelope);
            }

            if (offset != bytes.Length)
            {
                Send(peer, PacketKind.ResyncRequest, serverTick,
                    PacketHeader.NoneTick, ReadOnlySpan<byte>.Empty);
                return;
            }

            commands.Sort((left, right) => left.Sequence.CompareTo(right.Sequence));
            for (var i = 0; i < commands.Count; i++)
            {
                var result = _coordinator.Queue(commands[i], serverTick);
                if (result != NetworkCommandResult.Queued &&
                    result != NetworkCommandResult.Duplicate)
                {
                    Send(peer, PacketKind.ResyncRequest, serverTick,
                        PacketHeader.NoneTick, ReadOnlySpan<byte>.Empty);
                    return;
                }
            }
        }

        private void SendSnapshot(Peer peer, NetworkSnapshot snapshot)
        {
            var payload = new byte[8 + snapshot.ByteLength];
            Hashing.Write32(payload, 0, (uint)snapshot.EntityCount);
            Hashing.Write32(payload, 4, (uint)snapshot.RecordCount);
            snapshot.Bytes.Span.CopyTo(payload.AsSpan(8));
            Send(peer, PacketKind.FullSnapshot, snapshot.ServerTick, PacketHeader.NoneTick, payload);
            peer.ResyncRequested = false;
        }

        private bool Send(Peer peer, PacketKind kind, uint serverTick, uint acknowledgedTick, ReadOnlySpan<byte> payload)
        {
            var started = Stopwatch.GetTimestamp();
            var header = new PacketHeader
            {
                Kind = kind, Flags = PacketFlags.ReliableOrdered, Compression = NetworkCompression.None,
                SessionEpoch = peer.Session.Epoch, PacketSequence = peer.PacketSequence++, ServerTick = serverTick,
                AcknowledgedSnapshotTick = acknowledgedTick,
                ServerProcessedCommandTick = peer.ServerProcessedCommandTick,
                ServerProcessedCommandSequence = peer.ServerProcessedCommandSequence,
                SchemaFingerprint = _schema.Fingerprint
                , SimulationFingerprint = _simulationFingerprint
                , ContentFingerprint = _contentFingerprint
            };
            var sent = NetworkPacket.TryEncode(header, payload, out var packet) && peer.Transport.TrySend(packet);
            peer.Session.Trace(NetworkPhase.Send, NetworkTraceKind.Point, sent ? NetworkResultCategory.Success : NetworkResultCategory.Transport, DiagnosticKind(kind), serverTick, PacketHeader.NoneTick, packet?.Length ?? 0, _coordinator.HistoryCount(peer.Scope), _coordinator.HistoryByteCount(peer.Scope), unchecked((int)(serverTick - peer.AcknowledgedSnapshotTick)), ElapsedNanoseconds(started), activeConnections: ActiveConnectionCount, activePeers: ActivePeerCount);
            var reportedTick = serverTick == PacketHeader.NoneTick ? ServerTick : Math.Max(ServerTick, serverTick);
            peer.Session.ReportSession(reportedTick, peer.AcknowledgedSnapshotTick, peer.ServerProcessedCommandSequence, peer.PacketSequence);
            return sent;
        }

        private int ActiveConnectionCount { get { var count = 0; for (var i = 0; i < _peers.Count; i++) if (_peers[i].Session.State == NetworkSessionState.Handshaking || _peers[i].Session.State == NetworkSessionState.Established) count++; return count; } }
        private int ActivePeerCount { get { var count = 0; for (var i = 0; i < _peers.Count; i++) if (_peers[i].Session.State == NetworkSessionState.Established) count++; return count; } }

        private void TraceDispatch(uint serverTick, NetworkDispatchSummary summary, long durationNanoseconds)
        {
            if (_observer == null) return;
            try
            {
                var value = new NetworkTraceEvent(NetworkPhase.CommandDispatch, NetworkTraceKind.Point, summary.Rejected > 0 ? NetworkResultCategory.Policy : NetworkResultCategory.Success,
                    NetworkRole.Server, 0, 0, 0, serverTick, 0, 0, 0, 0, 0, summary.Total, _coordinator.PendingCommandCount, 0,
                    ActiveConnectionCount, ActivePeerCount, Stopwatch.GetTimestamp(), NetworkPacketKind.CommandBatch, durationNanoseconds: durationNanoseconds,
                    fingerprint: _schema.Fingerprint, acceptedCommands: summary.Accepted, rejectedCommands: summary.Rejected);
                _observer.Observe(in value);
            }
            catch { }
        }

        private void ClosePeer(Peer peer)
        {
            peer.Session.Close();
            if (peer.AdmissionNotified && !peer.DisconnectNotified)
            {
                peer.DisconnectNotified = true;
                NotifyDisconnected(peer);
            }
            peer.Session.ReportSession(ServerTick, peer.AcknowledgedSnapshotTick,
                peer.ServerProcessedCommandSequence, peer.PacketSequence);
        }

        private void CleanupPeer(Peer peer)
        {
            try
            {
                ClosePeer(peer);
            }
            finally
            {
                peer.PendingPackets.Clear();
                _coordinator.Remove(peer.Transport.Connection);
            }
        }

        private void NotifyAdmitted(Peer peer)
        {
            if (_peerObserver == null) return;
            var data = peer.Data();
            _peerObserver.Admitted(in data);
        }

        private void NotifyDisconnected(Peer peer)
        {
            if (_peerObserver == null) return;
            var data = peer.Data();
            try
            {
                _peerObserver.Disconnected(in data);
            }
            catch
            {
                // Transport/session cleanup must not be interrupted by game lifecycle hooks.
            }
        }

        private void TraceAdmissionFailure(Peer peer, NetworkAdmissionRejection rejection)
        {
            peer.Session.Trace(NetworkPhase.Decode, NetworkTraceKind.Point,
                NetworkResultCategory.Policy, NetworkPacketKind.Hello,
                ServerTick, 0, 0, _coordinator.HistoryCount(peer.Scope),
                _coordinator.HistoryByteCount(peer.Scope), 0, 0,
                activeConnections: ActiveConnectionCount, activePeers: ActivePeerCount,
                rejectedCommands: rejection == NetworkAdmissionRejection.None ? 0 : 1);
        }

        private static NetworkPacketKind DiagnosticKind(PacketKind kind) => (NetworkPacketKind)(byte)kind;
        private static long ElapsedNanoseconds(long started) => (Stopwatch.GetTimestamp() - started) * 1000000000L / Stopwatch.Frequency;

        private sealed class Peer
        {
            internal Peer(INetworkTransport transport, NetworkSession<TWorld> session, uint peerId, uint epoch, ScopeId scope)
            { Transport = transport; Session = session; PeerId = peerId; Epoch = epoch; Scope = scope; PacketSequence = 1; }
            internal readonly INetworkTransport Transport;
            internal readonly NetworkSession<TWorld> Session;
            internal readonly uint PeerId;
            internal readonly uint Epoch;
            internal readonly ScopeId Scope;
            internal uint PacketSequence;
            internal uint AcknowledgedSnapshotTick;
            internal uint ServerProcessedCommandTick;
            internal uint ServerProcessedCommandSequence;
            internal bool ResyncRequested;
            internal bool AdmissionNotified;
            internal bool DisconnectNotified;
            internal readonly Queue<byte[]> PendingPackets = new Queue<byte[]>();

            internal NetworkPeerData Data() => new NetworkPeerData
            {
                Connection = Transport.Connection,
                PeerId = PeerId,
                Epoch = Epoch,
                Scope = Scope
            };
        }
    }
}
