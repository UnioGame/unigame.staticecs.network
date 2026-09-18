using System;
using System.Collections.Generic;
using System.IO;
using System.Diagnostics;
using FFS.Libraries.StaticEcs;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Runs framed receive, decode, command dispatch, capture, and send for isolated server connections.</summary>
    public sealed class NetworkServer<TWorld> : IDisposable
        where TWorld : struct, IWorldType
    {
        private readonly NetworkSchema<TWorld> _schema;
        private readonly NetworkServerCoordinator<TWorld> _coordinator;
        private readonly NetworkReplicator<TWorld> _replicator;
        private readonly List<Peer> _peers = new List<Peer>();
        private readonly INetworkObserver _observer;
        private readonly INetworkPeerObserver _peerObserver;
        private readonly INetworkPeerAdmissionPolicy _admissionPolicy;
        private readonly INetworkScopeProvider<TWorld> _scopeProvider;
        private readonly ulong _simulationFingerprint;
        private readonly ulong _contentFingerprint;
        private readonly NetworkBufferPool _bufferPool;
        private readonly bool _ownsBufferPool;
        private readonly Dictionary<ScopeId, NetworkSnapshot> _captures =
            new Dictionary<ScopeId, NetworkSnapshot>();
        private readonly Dictionary<SnapshotDeltaKey, NetworkBufferLease> _snapshotDeltas =
            new Dictionary<SnapshotDeltaKey, NetworkBufferLease>();
        private readonly Dictionary<ChunkPayloadKey, ChunkPayloadEntry> _chunkPayloads =
            new Dictionary<ChunkPayloadKey, ChunkPayloadEntry>();
        private uint _activeTick;
        private int _activeConnectionCount;
        private int _activePeerCount;
        private bool _disposed;

        /// <summary>Gets the latest authoritative tick completed by this server.</summary>
        public uint ServerTick { get; private set; }

        public int ConnectionCount => _peers.Count;

        public NetworkBufferPoolDiagnostics CaptureBufferDiagnostics() =>
            _bufferPool.CaptureDiagnostics();

        /// <summary>Captures current bounded endpoint memory and queue ownership.</summary>
        public NetworkMemoryDiagnostics CaptureMemoryDiagnostics() => new NetworkMemoryDiagnostics
        {
            Buffers = _bufferPool.CaptureDiagnostics(),
            HistoryBytes = _coordinator.HistoryBytes,
            PendingCommands = _coordinator.PendingCommandCount,
            PendingCommandBytes = _coordinator.PendingCommandBytes,
            PendingCommandsHighWater = _coordinator.PendingCommandsHighWater,
            PendingCommandBytesHighWater = _coordinator.PendingCommandBytesHighWater,
        };

        /// <param name="scopeProvider">
        /// Opt-in, off by default (NCORE-15). See <see cref="INetworkScopeProvider{TWorld}"/> for the
        /// exact contract. When supplied, this server reassigns each established peer's scope every
        /// tick via <see cref="INetworkScopeProvider{TWorld}.TryUpdateScope"/> and forces a keyframe
        /// on any change, and <see cref="NetworkReplicator{TWorld}.Capture"/> collects each scope's
        /// entities from the provider's index instead of the whole world. Leaving this null keeps
        /// every peer on its admission-time scope forever, matching today's behavior exactly.
        /// </param>
        public NetworkServer(NetworkSchema<TWorld> schema, NetworkScopeSelector<TWorld> scopeSelector, int historyTicks = 64, long historyBytes = 32 * 1024 * 1024, INetworkObserver observer = null, INetworkPeerObserver peerObserver = null, INetworkPeerAdmissionPolicy admissionPolicy = null, ulong simulationFingerprint = 0, ulong contentFingerprint = 0, NetworkBufferPool bufferPool = null, INetworkScopeProvider<TWorld> scopeProvider = null)
        {
            _schema = schema ?? throw new ArgumentNullException(nameof(schema));
            if (scopeSelector == null) throw new ArgumentNullException(nameof(scopeSelector));
            _bufferPool = bufferPool ??
                new NetworkBufferPool(NetworkBufferPool.DefaultServerRetainedBytes);
            _ownsBufferPool = bufferPool == null;
            _coordinator = new NetworkServerCoordinator<TWorld>(historyTicks, historyBytes);
            _replicator = new NetworkReplicator<TWorld>(schema, scopeSelector,
                bufferPool: _bufferPool, scopeProvider: scopeProvider);
            _observer = observer;
            _peerObserver = peerObserver;
            _admissionPolicy = admissionPolicy;
            _scopeProvider = scopeProvider;
            _simulationFingerprint = simulationFingerprint;
            _contentFingerprint = contentFingerprint;
        }

        public void Dispose()
        {
            if (_disposed)
                return;
            _disposed = true;
            ClearSnapshotDeltas();
            ClearChunkPayloads();
            while (_peers.Count > 0)
                CleanupPeer(_peers[_peers.Count - 1]);
            _peers.Clear();
            _coordinator.Clear();
            _replicator.Dispose();
            if (_ownsBufferPool)
                _bufferPool.Dispose();
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
            var session = new NetworkSession<TWorld>(transport.Connection,
                NetworkRole.Server, _schema, _bufferPool, observer ?? _observer);
            var peer = new Peer(transport, session, peerId, epoch, scope);
            _peers.Add(peer);
            peer.ConnectionCounted = true;
            _activeConnectionCount++;
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
                    var remove = false;
                    try
                    {
                        peer.Session.Trace(NetworkPhase.Receive, NetworkTraceKind.Point, NetworkResultCategory.Success, NetworkPacketKind.None, ServerTick, 0, packet.Length, 0, 0, unchecked((int)(ServerTick - peer.AcknowledgedSnapshotTick)), ElapsedNanoseconds(receiveStarted), activeConnections: ActiveConnectionCount, activePeers: ActivePeerCount);
                        remove = DecodePacket(peer, packet);
                    }
                    finally
                    {
                        packet.Dispose();
                    }
                    if (!remove)
                        continue;
                    var removedIndex = _peers.IndexOf(peer);
                    if (removedIndex >= 0)
                        _peers.RemoveAt(removedIndex);
                    i = removedIndex >= 0 ? removedIndex - 1 : -1;
                    break;
                }
            }
        }

        /// <summary>Advances exactly one authoritative tick around the supplied gameplay boundary.</summary>
        public void Tick(Action<uint> gameplay)
        {
            if (gameplay == null) throw new ArgumentNullException(nameof(gameplay));
            var serverTick = BeginTick();
            gameplay(serverTick);
            CompleteTick();
        }

        /// <summary>Dispatches due commands and begins one authoritative ECS tick.</summary>
        public uint BeginTick()
        {
            if (_activeTick != 0)
                throw new InvalidOperationException("A server tick is already active.");
            var serverTick = checked(ServerTick + 1);
            var dispatchStarted = Stopwatch.GetTimestamp();
            var dispatched = _coordinator.Dispatch(serverTick);
            TraceDispatch(serverTick, dispatched, ElapsedNanoseconds(dispatchStarted));
            for (var i = 0; i < _peers.Count; i++)
            {
                var peer = _peers[i];
                DispatchTransactions(peer);
                if (_coordinator.TryGetProcessedCommand(peer.Transport.Connection, out var cursor))
                {
                    peer.ServerProcessedCommandTick = cursor.Tick;
                    peer.ServerProcessedCommandSequence = cursor.Sequence;
                }
            }
            _activeTick = serverTick;
            return serverTick;
        }

        /// <summary>Captures and sends authoritative state after gameplay systems complete.</summary>
        public void CompleteTick()
        {
            if (_activeTick == 0)
                throw new InvalidOperationException("No server tick is active.");
            var serverTick = _activeTick;
            try
            {
                for (var i = 0; i < _peers.Count; i++)
                {
                    CompleteTransactions(_peers[i], serverTick);
                    FlushTransactionReceipts(_peers[i]);
                }
                _captures.Clear();
                // NCORE-15: rebuild the provider's spatial index once per tick, after gameplay has
                // moved every entity, then let it reassign each established peer's scope with
                // whatever hysteresis it implements. A reassignment always forces a keyframe: the
                // peer's existing baseline history belongs to its old scope and a delta against it
                // would be meaningless (or outright rejected) for the new one.
                if (_scopeProvider != null)
                {
                    _scopeProvider.RefreshTick(serverTick);
                    for (var i = 0; i < _peers.Count; i++)
                    {
                        var peer = _peers[i];
                        if (peer.Session.State != NetworkSessionState.Established) continue;
                        var scope = peer.Scope;
                        if (_scopeProvider.TryUpdateScope(peer.PeerId, ref scope) &&
                            scope != peer.Scope)
                        {
                            peer.SetScope(scope);
                            peer.ResyncRequested = true;
                        }
                    }
                }
                for (var i = 0; i < _peers.Count; i++)
                {
                    var peer = _peers[i];
                    if (peer.Session.State != NetworkSessionState.Established) continue;
                    // A stalled reliable channel must not let snapshots overtake
                    // terminal transaction receipts. Completed transactions stay
                    // counted until their receipt is actually accepted by transport.
                    if (peer.HasPendingReceiptWork)
                        continue;
                    // Snapshots are state, so only one snapshot batch should
                    // be in flight. Waiting for the native reliable backlog to drain
                    // prevents a fast ACK from immediately replacing a packet
                    // whose delivery callback and fragments are still pending.
                    // This keeps ordered delivery bounded without changing the
                    // snapshot baseline or transaction guarantees.
                    if (peer.LastSnapshotSentTick != 0 &&
                        peer.Transport is INetworkReliableSendState reliableState &&
                        reliableState.HasPendingReliablePackets)
                        continue;
                    using var snapshotScope = NetworkDiagnosticMarkers.Measure(NetworkDiagnosticPhase.Snapshot);
                    if (!_captures.TryGetValue(peer.Scope, out var capture))
                    {
                        var started = Stopwatch.GetTimestamp();
                        if (_replicator.Capture(serverTick, peer.Scope, out capture) != SnapshotCaptureResult.Success) { peer.Session.Trace(NetworkPhase.SnapshotCapture, NetworkTraceKind.Point, NetworkResultCategory.World, NetworkPacketKind.SnapshotChunk, serverTick, 0, 0, 0, 0, 0, ElapsedNanoseconds(started)); continue; }
                        _captures.Add(peer.Scope, capture);
                        _coordinator.StoreCapture(peer.Scope, capture);
                        peer.Session.Trace(NetworkPhase.SnapshotCapture, NetworkTraceKind.Point, NetworkResultCategory.Success, NetworkPacketKind.SnapshotChunk, serverTick, 0, capture.ByteLength, _coordinator.HistoryCount(peer.Scope), _coordinator.HistoryByteCount(peer.Scope), unchecked((int)(serverTick - peer.AcknowledgedSnapshotTick)), ElapsedNanoseconds(started), capture.EntityCount, capture.RecordCount, activeConnections: ActiveConnectionCount, activePeers: ActivePeerCount);
                    }
                    using (NetworkDiagnosticMarkers.Measure(NetworkDiagnosticPhase.SnapshotDiagnostics))
                        peer.Session.ReportSnapshot(capture, _coordinator.History(peer.Scope));
                    SendSnapshot(peer, capture);
                }
                ServerTick = serverTick;
            }
            finally
            {
                try
                {
                    ClearSnapshotDeltas();
                }
                finally
                {
                    ClearChunkPayloads();
                    _activeTick = 0;
                }
            }
        }

        /// <summary>Copies one connection state without allocating a collection snapshot.</summary>
        public bool TryGetConnection(int index, out NetworkConnectionSnapshot snapshot)
        {
            if ((uint)index >= (uint)_peers.Count)
            {
                snapshot = default;
                return false;
            }
            var peer = _peers[index];
            snapshot = new NetworkConnectionSnapshot
            {
                Connection = new NetworkConnectionComponent
                {
                    Connection = peer.Transport.Connection,
                    Role = NetworkRole.Server,
                    State = peer.Session.State,
                    PeerId = peer.PeerId,
                    Epoch = peer.Epoch,
                    Scope = peer.Scope,
                },
                Ticks = new NetworkConnectionTickComponent
                {
                    ServerTick = ServerTick,
                    EstimatedServerTick = ServerTick,
                    AcknowledgedSnapshotTick = peer.AcknowledgedSnapshotTick,
                    ServerProcessedCommandTick = peer.ServerProcessedCommandTick,
                    ServerProcessedCommandSequence =
                        peer.ServerProcessedCommandSequence,
                },
            };
            return true;
        }

        /// <summary>Finds one immutable authoritative capture by scope and tick.</summary>
        public bool TryGetCapture(ScopeId scope, uint serverTick, out NetworkSnapshot snapshot)
            => _coordinator.TryGetCapture(scope, serverTick, out snapshot);

        public int PendingTransactionCount
        {
            get
            {
                var count = 0;
                for (var i = 0; i < _peers.Count; i++)
                    count += _peers[i].PendingTransactionCount;
                return count;
            }
        }

        /// <summary>Completes one pending transaction for the matching peer.</summary>
        public bool CompleteTransaction(NetworkTransactionId transactionId,
            NetworkTransactionStatus status = NetworkTransactionStatus.Applied)
        {
            if (status != NetworkTransactionStatus.Applied &&
                status != NetworkTransactionStatus.GameplayRejected)
                return false;
            NetworkServerTransaction match = null;
            for (var i = 0; i < _peers.Count; i++)
            {
                var peer = _peers[i];
                if (!peer.Transactions.TryGetValue(transactionId, out var transaction) ||
                    transaction.ReceiptSent || transaction.CompletionStatus.HasValue)
                    continue;
                if (match != null)
                    return false;
                match = transaction;
            }
            if (match == null)
                return false;
            match.CompletionStatus = status;
            return true;
        }

        /// <summary>Completes one transaction using its full connection-epoch key.</summary>
        public bool CompleteTransaction(uint peerId, uint epoch,
            NetworkTransactionId transactionId,
            NetworkTransactionStatus status = NetworkTransactionStatus.Applied)
        {
            if (status != NetworkTransactionStatus.Applied &&
                status != NetworkTransactionStatus.GameplayRejected)
                return false;
            for (var i = 0; i < _peers.Count; i++)
            {
                var peer = _peers[i];
                if (peer.PeerId != peerId || peer.Epoch != epoch ||
                    !peer.Transactions.TryGetValue(transactionId, out var transaction) ||
                    transaction.ReceiptSent || transaction.CompletionStatus.HasValue)
                    continue;
                transaction.CompletionStatus = status;
                return true;
            }
            return false;
        }

        /// <summary>Completes one pending transaction from its ECS request payload.</summary>
        public bool CompleteTransaction(in CompleteNetworkTransactionRequest request) =>
            CompleteTransaction(request.PeerId, request.Epoch,
                request.TransactionId, request.Status);

        /// <summary>Compatibility alias for ECS-facing transaction completion code.</summary>
        public bool CompleteNetworkTransaction(NetworkTransactionId transactionId,
            NetworkTransactionStatus status = NetworkTransactionStatus.Applied) =>
            CompleteTransaction(transactionId, status);

        private bool DecodePacket(Peer peer, NetworkBufferLease packet)
        {
            var started = Stopwatch.GetTimestamp();
            if (!NetworkPacket.TryDecode(packet, out var header, out var payload))
            {
                peer.Session.Trace(NetworkPhase.Decode, NetworkTraceKind.Point,
                    NetworkResultCategory.Protocol, NetworkPacketKind.None,
                    ServerTick, 0, packet.Length, 0, 0,
                    unchecked((int)(ServerTick - peer.AcknowledgedSnapshotTick)),
                    ElapsedNanoseconds(started), activeConnections: ActiveConnectionCount,
                    activePeers: ActivePeerCount);
                DisconnectPeer(peer);
                return true;
            }
            var packetValidation = peer.Session.ValidatePacket(in header);
            var duplicateCommandPacket =
                header.Kind == PacketKind.CommandBatch &&
                packetValidation == PacketValidationResult.Duplicate;
            var duplicateTransactionPacket =
                header.Kind == PacketKind.TransactionCommand &&
                packetValidation == PacketValidationResult.Duplicate;
            if ((header.Kind != PacketKind.Hello &&
                 (header.SchemaFingerprint != _schema.Fingerprint ||
                  header.SimulationFingerprint != _simulationFingerprint ||
                  header.ContentFingerprint != _contentFingerprint)) ||
                packetValidation != PacketValidationResult.Success &&
                !duplicateCommandPacket &&
                !duplicateTransactionPacket)
            {
                peer.Session.Trace(NetworkPhase.Decode, NetworkTraceKind.Point,
                    NetworkResultCategory.Protocol, DiagnosticKind(header.Kind),
                    ServerTick, 0, packet.Length, 0, 0,
                    unchecked((int)(ServerTick - peer.AcknowledgedSnapshotTick)),
                    ElapsedNanoseconds(started), activeConnections: ActiveConnectionCount,
                    activePeers: ActivePeerCount,
                    packetValidationResult: packetValidation);
                DisconnectPeer(peer);
                return true;
            }
            if (header.Kind == PacketKind.Hello && !Admit(peer,
                    header.SchemaFingerprint, header.SimulationFingerprint,
                    header.ContentFingerprint))
                return true;
            NetworkCommandResult? commandResult = null;
            var decodeResult = NetworkResultCategory.Success;
            var resyncCorrelationId = 0u;
            var disconnect = false;
            if (header.Kind == PacketKind.CommandBatch)
            {
                commandResult = duplicateCommandPacket
                    ? NetworkCommandResult.Duplicate
                    : DecodeCommands(peer, packet, payload,
                        checked(ServerTick + 1));
                if (commandResult == NetworkCommandResult.LimitExceeded)
                    decodeResult = NetworkResultCategory.Limits;
                else if (commandResult != NetworkCommandResult.Queued &&
                         commandResult != NetworkCommandResult.Duplicate &&
                         commandResult != NetworkCommandResult.TickWindow)
                {
                    decodeResult = NetworkResultCategory.Protocol;
                    disconnect = true;
                }
            }
            else if (header.Kind == PacketKind.TransactionCommand)
            {
                commandResult = DecodeTransaction(peer, packet, payload,
                    checked(ServerTick + 1), duplicateTransactionPacket);
                if (duplicateTransactionPacket &&
                    commandResult != NetworkCommandResult.Duplicate)
                    disconnect = true;
                if (commandResult == NetworkCommandResult.PolicyRejected)
                    decodeResult = NetworkResultCategory.Policy;
                else if (commandResult == NetworkCommandResult.LimitExceeded)
                    decodeResult = NetworkResultCategory.Limits;
                else if (commandResult != NetworkCommandResult.Queued &&
                         commandResult != NetworkCommandResult.Duplicate)
                {
                    decodeResult = NetworkResultCategory.Protocol;
                    disconnect = true;
                }
            }
            else if (header.Kind == PacketKind.Ping)
                Send(peer, PacketKind.Pong, ServerTick, PacketHeader.NoneTick,
                    payload.Span);
            else if (header.Kind == PacketKind.Ack)
                DecodeAcknowledgement(peer, header.AcknowledgedSnapshotTick);
            else if (header.Kind == PacketKind.ResyncRequest)
            {
                if (!ResyncRequestPayload.TryRead(payload.Span, out var request))
                {
                    peer.Session.Trace(NetworkPhase.Decode,
                        NetworkTraceKind.Point, NetworkResultCategory.Malformed,
                        NetworkPacketKind.ResyncRequest, ServerTick, 0,
                        packet.Length, 0, 0,
                        unchecked((int)(ServerTick -
                                         peer.AcknowledgedSnapshotTick)),
                        ElapsedNanoseconds(started),
                        activeConnections: ActiveConnectionCount,
                        activePeers: ActivePeerCount,
                        packetValidationResult: packetValidation,
                        sequence: header.PacketSequence,
                        acknowledgedSnapshotTick: peer.AcknowledgedSnapshotTick,
                        oldestHistoryTick: _coordinator.OldestHistoryTick(peer.Scope),
                        newestHistoryTick: _coordinator.NewestHistoryTick(peer.Scope));
                    DisconnectPeer(peer);
                    return true;
                }
                resyncCorrelationId = request.CorrelationId;
                var startsRecovery = !peer.ResyncRequested;
                peer.ResyncRequested = true;
                if (peer.ResyncCorrelationId == 0)
                {
                    peer.ResyncCorrelationId = request.CorrelationId;
                    if (startsRecovery)
                        peer.ResyncSnapshotTick = 0;
                }
            }
            else if (header.Kind == PacketKind.Disconnect)
                CleanupPeer(peer);
            peer.Session.Trace(NetworkPhase.Decode, NetworkTraceKind.Point,
                decodeResult, DiagnosticKind(header.Kind),
                ServerTick, 0, packet.Length, 0, 0,
                unchecked((int)(ServerTick - peer.AcknowledgedSnapshotTick)),
                ElapsedNanoseconds(started), activeConnections: ActiveConnectionCount,
                activePeers: ActivePeerCount,
                resyncCorrelationId: resyncCorrelationId,
                commandResult: commandResult,
                packetValidationResult: packetValidation,
                sequence: header.PacketSequence,
                acknowledgedSnapshotTick: peer.AcknowledgedSnapshotTick,
                oldestHistoryTick: _coordinator.OldestHistoryTick(peer.Scope),
                newestHistoryTick: _coordinator.NewestHistoryTick(peer.Scope));
            peer.Session.ReportSession(ServerTick, peer.AcknowledgedSnapshotTick,
                peer.ServerProcessedCommandSequence, peer.PacketSequence);
            if (disconnect)
            {
                DisconnectPeer(peer);
                return true;
            }
            if (header.Kind == PacketKind.Disconnect)
                return true;
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
                CloseSession(peer);
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
                        CloseSession(peer);
                        return false;
                    }
                }

                var admission = peer.Session.Admit(remoteFingerprint,
                    peer.PeerId, peer.Epoch, peer.Scope);
                if (admission != NetworkAdmissionResult.Accepted)
                {
                    if (peer.Session.State == NetworkSessionState.Rejected)
                        CloseSession(peer);
                    throw new InvalidOperationException("Session rejected a validated peer admission.");
                }
                peer.PeerCounted = true;
                _activePeerCount++;

                _coordinator.Add(peer.Session);
                Span<byte> payload = stackalloc byte[12];
                Hashing.Write32(payload, 0, peer.PeerId);
                Hashing.Write64(payload, 4, peer.Scope.Value);
                if (!Send(peer, PacketKind.Ready, 0, PacketHeader.NoneTick, payload))
                    throw new InvalidOperationException("Ready packet could not be sent.");
            }
            catch
            {
                TraceAdmissionFailure(peer, NetworkAdmissionRejection.PolicyError);
                _coordinator.Remove(peer.Transport.Connection);
                CloseSession(peer);
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

        private NetworkCommandResult DecodeCommands(Peer peer,
            NetworkBufferLease packet,
            ReadOnlyMemory<byte> payload, uint serverTick)
        {
            using var commandScope = NetworkDiagnosticMarkers.Measure(NetworkDiagnosticPhase.Command);
            if (payload.Length < 1)
                return NetworkCommandResult.Malformed;

            var bytes = payload.Span;
            int count = bytes[0];
            if (count < 1 || count > ProtocolLimits.MaxCommandsPerBatch)
                return NetworkCommandResult.Malformed;

            // Command redundancy resends the same sequence across more than one batch so it
            // survives a dropped packet, so most batches on a healthy link carry mostly commands
            // this session has already queued. NextCommandSequence only advances while this method
            // and the dispatch loop below run, so any sequence at or below the value read here is
            // guaranteed to still be a duplicate once Queue -> Validate would see it. Skipping the
            // pooled-buffer retain (a locked ref-count bump) and envelope slot for that guaranteed
            // outcome, while still walking its structural fields so framing errors are still
            // caught, is the only behavior change: every genuinely new command is decoded, sorted,
            // and validated exactly as before.
            var nextReceiveSequence = peer.Session.NextCommandSequence;

            int offset = 1;
            var commands = peer.DecodedCommands;
            var decoded = 0;
            for (var i = 0; i < count; i++)
            {
                if (offset > bytes.Length - 17)
                {
                    DisposeCommands(commands, decoded);
                    return NetworkCommandResult.Malformed;
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
                    DisposeCommands(commands, decoded);
                    return NetworkCommandResult.Malformed;
                }

                var exactLength = checked((int)payloadLength);
                if (sequence < nextReceiveSequence)
                {
                    // Already queued in an earlier tick: Queue -> Validate would report
                    // Duplicate for this exact sequence no matter where in the batch it sorts,
                    // so skip retaining its payload and only step past it.
                    offset += exactLength;
                    continue;
                }
                var exact = packet.RetainSlice(PacketHeader.Size + offset, exactLength);
                offset += exactLength;
                commands[decoded++] = new NetworkCommandEnvelope(
                    peer.Transport.Connection,
                    peer.PeerId,
                    peer.Epoch,
                    sequence,
                    targetTick,
                    new NetworkTypeId(idValue),
                    version,
                    exact);
            }

            if (offset != bytes.Length)
            {
                DisposeCommands(commands, decoded);
                return NetworkCommandResult.Malformed;
            }

            // A batch made entirely of already-processed sequences decoded above without
            // retaining or queuing anything; matches the pre-existing outcome for an
            // all-duplicate batch, which fell through the loop below untouched to this same
            // result.
            if (decoded == 0)
                return NetworkCommandResult.Queued;

            Array.Sort(commands, 0, decoded, NetworkCommandEnvelopeComparer.Instance);
            for (var i = 0; i < decoded; i++)
            {
                var result = _coordinator.Queue(commands[i], serverTick);
                if (result == NetworkCommandResult.TickWindow ||
                    result == NetworkCommandResult.Duplicate)
                {
                    commands[i].Dispose();
                    commands[i] = default;
                    continue;
                }
                if (result == NetworkCommandResult.LimitExceeded)
                {
                    for (var j = i; j < decoded; j++)
                    {
                        commands[j].Dispose();
                        commands[j] = default;
                    }
                    return result;
                }
                if (result != NetworkCommandResult.Queued)
                {
                    for (var j = i; j < decoded; j++)
                    {
                        commands[j].Dispose();
                        commands[j] = default;
                    }
                    return result;
                }
                commands[i] = default;
            }
            return NetworkCommandResult.Queued;
        }

        private static void DisposeCommands(NetworkCommandEnvelope[] commands,
            int count)
        {
            for (var i = 0; i < count; i++)
            {
                var command = commands[i];
                command.Dispose();
                commands[i] = default;
            }
        }

        private void DecodeAcknowledgement(Peer peer, uint acknowledgedTick)
        {
            if (acknowledgedTick <= peer.AcknowledgedSnapshotTick)
                return;
            if (acknowledgedTick > ServerTick ||
                !_coordinator.TryGetCapture(peer.Scope, acknowledgedTick,
                    out var baseline) ||
                baseline.Scope != peer.Scope ||
                baseline.SchemaFingerprint != _schema.Fingerprint)
            {
                peer.ResyncRequested = true;
                return;
            }
            peer.AcknowledgedSnapshotTick = acknowledgedTick;
            if (peer.ResyncRequested &&
                (peer.ResyncSnapshotTick == 0 ||
                 acknowledgedTick < peer.ResyncSnapshotTick))
                return;
            peer.ResyncRequested = false;
            peer.ResyncCorrelationId = 0;
            peer.ResyncSnapshotTick = 0;
        }

        private void SendSnapshot(Peer peer, NetworkSnapshot snapshot)
        {
            var reliableLimit = peer.Transport.MaxReliablePayloadBytes;
            if (reliableLimit <= PacketHeader.Size + SnapshotChunkHeader.Size)
            {
                peer.ResyncRequested = true;
                return;
            }
            // A transport that cannot currently accept even the smallest snapshot
            // chunk is transiently backpressured. Skip baseline lookup and delta
            // encoding without requesting recovery: no application chunk was
            // rejected, so the next tick can retry against the same baseline.
            if (peer.Transport is INetworkReliableSendPreflight preflight &&
                !preflight.CanAcceptReliablePacket(
                    PacketHeader.Size + SnapshotChunkHeader.Size + 1))
                return;

            NetworkBufferLease delta = null;
            var baselineTick = peer.AcknowledgedSnapshotTick;
            NetworkSnapshot baseline = null;
            var keyframe = peer.ResyncRequested || baselineTick == 0;
            if (!keyframe)
            {
                _coordinator.TryGetCapture(peer.Scope, baselineTick,
                    out baseline);
                if (baseline == null || baseline.Scope != peer.Scope ||
                    baseline.SchemaFingerprint != _schema.Fingerprint)
                    baseline = null;
                if (!TryGetSnapshotDelta(peer.Scope, baselineTick, baseline,
                        snapshot, out delta))
                    keyframe = true;
            }
            if (keyframe)
            {
                peer.ResyncRequested = true;
                if (peer.ResyncSnapshotTick == 0)
                    peer.ResyncSnapshotTick = snapshot.ServerTick;
            }

            var body = keyframe ? snapshot.Bytes.Span : delta.Span;
            var maxBody = Math.Min(
                reliableLimit - PacketHeader.Size - SnapshotChunkHeader.Size,
                ProtocolLimits.MaxWirePayloadBytes - SnapshotChunkHeader.Size);
            if (body.Length > ProtocolLimits.MaxDecodedPayloadBytes)
            {
                peer.ResyncRequested = true;
                return;
            }
            var chunkCountLong = (body.Length + (long)maxBody - 1L) / maxBody;
            if (chunkCountLong < 1 ||
                chunkCountLong > ProtocolLimits.MaxChunkMappings)
            {
                peer.ResyncRequested = true;
                return;
            }
            var chunkCount = checked((uint)chunkCountLong);
            var acceptedChunk = false;
            for (uint chunkIndex = 0; chunkIndex < chunkCount; chunkIndex++)
            {
                var bodyOffset = checked((int)((long)chunkIndex *
                                               maxBody));
                var bodyLength = Math.Min(maxBody, body.Length - bodyOffset);
                var chunk = new SnapshotChunkHeader
                {
                    PayloadKind = keyframe
                        ? SnapshotPayloadKind.Keyframe
                        : SnapshotPayloadKind.Delta,
                    SnapshotTick = snapshot.ServerTick,
                    BaselineTick = keyframe ? 0 : baselineTick,
                    ScopeValue = peer.Scope.Value,
                    TotalLength = checked((uint)snapshot.ByteLength),
                    TotalHash = snapshot.PayloadHash,
                    ChunkIndex = chunkIndex,
                    ChunkCount = chunkCount,
                    ResyncCorrelationId = keyframe
                        ? peer.ResyncCorrelationId
                        : 0
                };
                if (!SendSnapshotChunk(peer, snapshot.ServerTick,
                        chunkIndex + 1, in chunk,
                        body.Slice(bodyOffset, bodyLength)))
                {
                    // A rejected first chunk of a normal delta carries no
                    // recovery signal: no application chunk was accepted, so
                    // the client is not awaiting a keyframe and the next tick
                    // can retry the delta against the same baseline.
                    if (!keyframe && !acceptedChunk && chunkIndex == 0)
                        return;
                    peer.ResyncRequested = true;
                    return;
                }
                acceptedChunk = true;
            }
            if (acceptedChunk)
                peer.LastSnapshotSentTick = snapshot.ServerTick;
        }

        private bool TryGetSnapshotDelta(ScopeId scope, uint baselineTick,
            NetworkSnapshot baseline, NetworkSnapshot target,
            out NetworkBufferLease delta)
        {
            var key = new SnapshotDeltaKey(scope, baselineTick,
                target.ServerTick);
            if (_snapshotDeltas.TryGetValue(key, out delta))
                return delta != null;

            NetworkBufferLease lease = null;
            try
            {
                var encoded = false;
                if (baseline != null)
                {
                    using (NetworkDiagnosticMarkers.Measure(NetworkDiagnosticPhase.SnapshotDeltaEncode))
                        encoded = SnapshotDeltaCodec.TryEncode(_bufferPool,
                            baseline, target, out lease, _schema.DeltaHooks);
                }

                if (encoded)
                {
                    _snapshotDeltas.Add(key, lease);
                    delta = lease;
                    lease = null;
                    return true;
                }

                _snapshotDeltas.Add(key, null);
                delta = null;
                return false;
            }
            finally
            {
                lease?.Dispose();
            }
        }

        private void ClearSnapshotDeltas()
        {
            foreach (var delta in _snapshotDeltas.Values)
                delta?.Dispose();
            _snapshotDeltas.Clear();
        }

        // Peers acknowledging the same baseline tick for the same scope produce a
        // byte-identical chunk payload (header + body). Framing and hashing it once
        // per tick and reusing the result across those peers turns an O(peers) cost
        // into O(distinct baselines): every later peer only pays for the per-peer
        // packet header wrap and a body copy, not another chunk-header write or a
        // full-payload xxHash64 pass. The cache is keyed on every field that can
        // change the framed bytes, so peers on different baselines, chunks, or
        // resync correlations never share an entry.
        private bool TryGetChunkPayload(ScopeId scope, in SnapshotChunkHeader chunk,
            ReadOnlySpan<byte> body, out ReadOnlyMemory<byte> payload,
            out ulong payloadHash)
        {
            var key = new ChunkPayloadKey(scope, chunk);
            if (_chunkPayloads.TryGetValue(key, out var cached))
            {
                payload = cached.Payload.Memory;
                payloadHash = cached.PayloadHash;
                return true;
            }

            if (!SnapshotChunkEncoder.TryEncodePayload(_bufferPool, in chunk, body,
                    out var lease, out var hash))
            {
                payload = default;
                payloadHash = 0;
                return false;
            }

            _chunkPayloads.Add(key, new ChunkPayloadEntry(lease, hash));
            payload = lease.Memory;
            payloadHash = hash;
            return true;
        }

        private void ClearChunkPayloads()
        {
            foreach (var entry in _chunkPayloads.Values)
                entry.Payload?.Dispose();
            _chunkPayloads.Clear();
        }

        private bool SendSnapshotChunk(Peer peer, uint serverTick,
            uint sequence, in SnapshotChunkHeader chunk,
            ReadOnlySpan<byte> body)
        {
            // History lookups and the elapsed-time sample only feed the observer
            // trace event below; Trace itself is a no-op without one. Skipping
            // them when untraced avoids four scope-history dictionary lookups and
            // a Stopwatch sample on every peer send.
            var traceEnabled = peer.Session.IsTraceEnabled;
            var started = traceEnabled ? Stopwatch.GetTimestamp() : 0L;
            // A reliable transport may be transiently backpressured for this exact
            // encoded chunk even though it accepted the minimum probe. Reject it
            // before renting and encoding a packet; TrySend stays authoritative
            // after a true preflight.
            var exactPacketBytes = checked(PacketHeader.Size +
                SnapshotChunkHeader.Size + body.Length);
            if (peer.Transport is INetworkReliableSendPreflight preflight &&
                !preflight.CanAcceptReliablePacket(exactPacketBytes))
                return false;
            // Preparation work only starts after the exact preflight accepts, so a
            // rejection emits none of the preparation, encode, or send scopes.
            using var packetScope = NetworkDiagnosticMarkers.Measure(NetworkDiagnosticPhase.PacketPreparation);
            var header = new PacketHeader
            {
                Kind = PacketKind.SnapshotChunk,
                Flags = PacketFlags.ReliableOrdered,
                Compression = NetworkCompression.None,
                SessionEpoch = peer.Session.Epoch,
                PacketSequence = sequence,
                ServerTick = serverTick,
                AcknowledgedSnapshotTick = PacketHeader.NoneTick,
                ServerProcessedCommandTick = peer.ServerProcessedCommandTick,
                ServerProcessedCommandSequence = peer.ServerProcessedCommandSequence,
                SchemaFingerprint = _schema.Fingerprint,
                SimulationFingerprint = _simulationFingerprint,
                ContentFingerprint = _contentFingerprint
            };
            NetworkBufferLease packet = null;
            var encoded = false;
            using (NetworkDiagnosticMarkers.Measure(NetworkDiagnosticPhase.SnapshotChunkEncode))
            {
                // Peers acknowledging the same baseline share byte-identical chunk
                // framing (header + body). Encode and hash it once per tick and
                // reuse that result here; only the per-peer packet header and a
                // plain body copy are produced for every additional peer.
                encoded = TryGetChunkPayload(peer.Scope, in chunk, body,
                              out var payload, out var payloadHash) &&
                          SnapshotChunkEncoder.TryEncodeFromPayload(_bufferPool,
                              header, payload, payloadHash, out packet);
            }
            var packetBytes = packet?.Length ?? 0;
            var sent = false;
            if (encoded)
            {
                using var transportScope = NetworkDiagnosticMarkers.Measure(NetworkDiagnosticPhase.TransportTrySend);
                sent = peer.Transport.TrySend(packet);
            }
            if (traceEnabled)
                peer.Session.Trace(NetworkPhase.Send, NetworkTraceKind.Point,
                    sent ? NetworkResultCategory.Success : NetworkResultCategory.Transport,
                    NetworkPacketKind.SnapshotChunk, serverTick, PacketHeader.NoneTick,
                    packetBytes, _coordinator.HistoryCount(peer.Scope),
                    _coordinator.HistoryByteCount(peer.Scope),
                    unchecked((int)(serverTick - peer.AcknowledgedSnapshotTick)),
                    ElapsedNanoseconds(started), activeConnections: ActiveConnectionCount,
                    activePeers: ActivePeerCount,
                    resyncCorrelationId: peer.ResyncCorrelationId,
                    sequence: sequence,
                    acknowledgedSnapshotTick: peer.AcknowledgedSnapshotTick,
                    oldestHistoryTick: _coordinator.OldestHistoryTick(peer.Scope),
                    newestHistoryTick: _coordinator.NewestHistoryTick(peer.Scope));
            return sent;
        }

        private NetworkCommandResult DecodeTransaction(Peer peer,
            NetworkBufferLease packet, ReadOnlyMemory<byte> payload,
            uint applicationTick, bool duplicatePacket = false)
        {
            using var commandScope = NetworkDiagnosticMarkers.Measure(NetworkDiagnosticPhase.Command);
            if (packet.Length > peer.Transport.MaxReliablePayloadBytes)
                return NetworkCommandResult.LimitExceeded;
            if (!NetworkTransactionWire.TryReadCommand(payload.Span,
                    out var transactionId, out var typeId, out var version,
                    out var payloadOffset))
                return NetworkCommandResult.Malformed;
            if (peer.Transactions.ContainsKey(transactionId))
                return NetworkCommandResult.Duplicate;
            if (peer.ReceiptLedger.TryGetValue(transactionId, out var cached))
            {
                peer.QueueReceipt(in cached);
                return NetworkCommandResult.Duplicate;
            }
            if (transactionId.Value <= peer.HighestTransactionId)
            {
                // Evicted ids are never re-applied. Keep this fallback bounded while
                // preserving the monotonic high-water mark for exact-once safety.
                var evicted = new NetworkServerTransactionReceipt(transactionId,
                    NetworkTransactionStatus.Unhandled, applicationTick);
                return peer.QueueReceipt(in evicted)
                    ? NetworkCommandResult.Duplicate
                    : NetworkCommandResult.LimitExceeded;
            }
            if (duplicatePacket)
                return NetworkCommandResult.Sequence;
            if (peer.PendingTransactionCount >=
                NetworkTransactionWire.MaxPendingTransactions)
            {
                var rejected = new NetworkServerTransactionReceipt(transactionId,
                    NetworkTransactionStatus.PolicyRejected, applicationTick);
                peer.HighestTransactionId = transactionId.Value;
                return peer.QueueReceipt(in rejected)
                    ? NetworkCommandResult.PolicyRejected
                    : NetworkCommandResult.LimitExceeded;
            }
            peer.HighestTransactionId = transactionId.Value;

            var exactLength = payload.Length - payloadOffset;
            var exact = packet.RetainSlice(PacketHeader.Size + payloadOffset,
                exactLength);
            var envelope = new NetworkCommandEnvelope(peer.Transport.Connection,
                peer.PeerId, peer.Epoch, peer.LastReceivedPacketSequence,
                applicationTick, typeId, version, exact);
            var validation = peer.Session.ValidateTransaction(envelope,
                out var entry);
            if (validation != NetworkCommandResult.Queued)
            {
                envelope.Dispose();
                return validation;
            }
            peer.Transactions.Add(transactionId,
                new NetworkServerTransaction(transactionId, envelope, entry,
                    applicationTick));
            return NetworkCommandResult.Queued;
        }

        private static void DispatchTransactions(Peer peer)
        {
            using var commandScope = NetworkDiagnosticMarkers.Measure(NetworkDiagnosticPhase.Command);
            foreach (var transaction in peer.Transactions.Values)
            {
                if (transaction.Dispatched || transaction.ReceiptSent)
                    continue;
                transaction.Dispatched = true;
                try
                {
                    var result = peer.Session.Dispatch(transaction.Envelope,
                        transaction.Entry, NetworkCommandDelivery.Transaction,
                        transaction.TransactionId);
                    if (result == NetworkCommandResult.PolicyRejected)
                        transaction.CompletionStatus =
                            NetworkTransactionStatus.PolicyRejected;
                    else if (result != NetworkCommandResult.Dispatched)
                        transaction.CompletionStatus =
                            NetworkTransactionStatus.Unhandled;
                }
                catch
                {
                    transaction.CompletionStatus =
                        NetworkTransactionStatus.PolicyRejected;
                }
                finally
                {
                    transaction.Dispose();
                }
            }
        }

        private void CompleteTransactions(Peer peer, uint serverTick)
        {
            foreach (var transaction in peer.Transactions.Values)
            {
                if (!transaction.Dispatched || transaction.ReceiptSent)
                    continue;
                transaction.CompletionStatus ??=
                    NetworkTransactionStatus.Unhandled;
                var receipt = new NetworkServerTransactionReceipt(
                    transaction.TransactionId, transaction.CompletionStatus.Value,
                    transaction.ApplicationTick);
                peer.QueueReceipt(in receipt);
            }
        }

        private void FlushTransactionReceipts(Peer peer)
        {
            QueueCompletedTransactionReceipts(peer);
            while (peer.PendingReceipts.Count > 0)
            {
                var receipt = peer.PendingReceipts.Peek();
                Span<byte> payload = stackalloc byte[NetworkTransactionWire.ReceiptSize];
                if (!NetworkTransactionWire.TryWriteReceipt(payload,
                        receipt.TransactionId, receipt.Status,
                        receipt.ApplicationTick) ||
                    !Send(peer, PacketKind.TransactionReceipt,
                        receipt.ApplicationTick, PacketHeader.NoneTick, payload))
                    return;
                peer.PendingReceipts.Dequeue();
                peer.QueuedReceiptIds.Remove(receipt.TransactionId);
                if (peer.Transactions.TryGetValue(receipt.TransactionId,
                        out var transaction) &&
                    transaction.CompletionStatus.HasValue)
                {
                    transaction.ReceiptSent = true;
                    peer.Transactions.Remove(receipt.TransactionId);
                }
                QueueCompletedTransactionReceipts(peer);
            }
        }

        private static void QueueCompletedTransactionReceipts(Peer peer)
        {
            foreach (var transaction in peer.Transactions.Values)
            {
                if (!transaction.Dispatched || transaction.ReceiptSent ||
                    !transaction.CompletionStatus.HasValue)
                    continue;
                var receipt = new NetworkServerTransactionReceipt(
                    transaction.TransactionId,
                    transaction.CompletionStatus.Value,
                    transaction.ApplicationTick);
                peer.QueueReceipt(in receipt);
            }
        }

        private bool Send(Peer peer, PacketKind kind, uint serverTick,
            uint acknowledgedTick, ReadOnlySpan<byte> payload,
            NetworkResyncReason resyncReason = NetworkResyncReason.None,
            NetworkResyncSource resyncSource = NetworkResyncSource.None,
            uint resyncCorrelationId = 0,
            NetworkCommandResult? commandResult = null)
        {
            var started = Stopwatch.GetTimestamp();
            var sequence = peer.PacketSequence;
            var header = new PacketHeader
            {
                Kind = kind, Flags = PacketFlags.ReliableOrdered, Compression = NetworkCompression.None,
                SessionEpoch = peer.Session.Epoch, PacketSequence = sequence, ServerTick = serverTick,
                AcknowledgedSnapshotTick = acknowledgedTick,
                ServerProcessedCommandTick = peer.ServerProcessedCommandTick,
                ServerProcessedCommandSequence = peer.ServerProcessedCommandSequence,
                SchemaFingerprint = _schema.Fingerprint
                , SimulationFingerprint = _simulationFingerprint
                , ContentFingerprint = _contentFingerprint
            };
            NetworkBufferLease packet = null;
            var encoded = sequence != uint.MaxValue &&
                NetworkPacket.TryEncode(_bufferPool, header, payload,
                    out packet);
            var packetBytes = packet?.Length ?? 0;
            var sent = encoded && peer.Transport.TrySend(packet);
            if (sent)
                peer.PacketSequence = sequence + 1;
            peer.Session.Trace(NetworkPhase.Send, NetworkTraceKind.Point, sent ? NetworkResultCategory.Success : NetworkResultCategory.Transport, DiagnosticKind(kind), serverTick, PacketHeader.NoneTick, packetBytes, _coordinator.HistoryCount(peer.Scope), _coordinator.HistoryByteCount(peer.Scope), unchecked((int)(serverTick - peer.AcknowledgedSnapshotTick)), ElapsedNanoseconds(started), activeConnections: ActiveConnectionCount, activePeers: ActivePeerCount, resyncReason: resyncReason, resyncSource: resyncSource, resyncCorrelationId: resyncCorrelationId, commandResult: commandResult, sequence: sequence, acknowledgedSnapshotTick: peer.AcknowledgedSnapshotTick, oldestHistoryTick: _coordinator.OldestHistoryTick(peer.Scope), newestHistoryTick: _coordinator.NewestHistoryTick(peer.Scope));
            var reportedTick = serverTick == PacketHeader.NoneTick ? ServerTick : Math.Max(ServerTick, serverTick);
            peer.Session.ReportSession(reportedTick, peer.AcknowledgedSnapshotTick, peer.ServerProcessedCommandSequence, peer.PacketSequence);
            return sent;
        }

        private int ActiveConnectionCount => _activeConnectionCount;
        private int ActivePeerCount => _activePeerCount;

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

        private void DisconnectPeer(Peer peer)
        {
            Send(peer, PacketKind.Disconnect, ServerTick, PacketHeader.NoneTick,
                ReadOnlySpan<byte>.Empty);
            CleanupPeer(peer);
        }

        private void CloseSession(Peer peer)
        {
            peer.Session.Close();
            if (peer.PeerCounted)
            {
                peer.PeerCounted = false;
                _activePeerCount--;
            }
            if (peer.ConnectionCounted)
            {
                peer.ConnectionCounted = false;
                _activeConnectionCount--;
            }
        }

        private void ClosePeer(Peer peer)
        {
            _peers.Remove(peer);
            CloseSession(peer);
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
                foreach (var transaction in peer.Transactions.Values)
                    transaction.Dispose();
                peer.Transactions.Clear();
                peer.ReceiptLedger.Clear();
                peer.PendingReceipts.Clear();
                peer.QueuedReceiptIds.Clear();
                peer.ReceiptOrder.Clear();
                peer.CompletedTransactionIds.Clear();
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

        private readonly struct SnapshotDeltaKey : IEquatable<SnapshotDeltaKey>
        {
            internal SnapshotDeltaKey(ScopeId scope, uint baselineTick,
                uint targetTick)
            {
                Scope = scope;
                BaselineTick = baselineTick;
                TargetTick = targetTick;
            }

            private ScopeId Scope { get; }
            private uint BaselineTick { get; }
            private uint TargetTick { get; }

            public bool Equals(SnapshotDeltaKey other) => Scope == other.Scope &&
                BaselineTick == other.BaselineTick && TargetTick == other.TargetTick;

            public override bool Equals(object obj) =>
                obj is SnapshotDeltaKey other && Equals(other);

            public override int GetHashCode() => unchecked(
                (Scope.GetHashCode() * 397) ^ (int)BaselineTick ^
                ((int)TargetTick * 397));
        }

        // Every field that participates in SnapshotChunkHeader.TryWrite (plus the
        // scope, since the header itself carries no scope) is included, so two
        // peers only ever share a cache entry when TryGetChunkPayload would have
        // framed identical bytes for both of them.
        private readonly struct ChunkPayloadKey : IEquatable<ChunkPayloadKey>
        {
            internal ChunkPayloadKey(ScopeId scope, in SnapshotChunkHeader chunk)
            {
                Scope = scope;
                PayloadKind = chunk.PayloadKind;
                SnapshotTick = chunk.SnapshotTick;
                BaselineTick = chunk.BaselineTick;
                TotalHash = chunk.TotalHash;
                ChunkIndex = chunk.ChunkIndex;
                ChunkCount = chunk.ChunkCount;
                ResyncCorrelationId = chunk.ResyncCorrelationId;
            }

            private ScopeId Scope { get; }
            private SnapshotPayloadKind PayloadKind { get; }
            private uint SnapshotTick { get; }
            private uint BaselineTick { get; }
            private ulong TotalHash { get; }
            private uint ChunkIndex { get; }
            private uint ChunkCount { get; }
            private uint ResyncCorrelationId { get; }

            public bool Equals(ChunkPayloadKey other) => Scope == other.Scope &&
                PayloadKind == other.PayloadKind &&
                SnapshotTick == other.SnapshotTick &&
                BaselineTick == other.BaselineTick &&
                TotalHash == other.TotalHash &&
                ChunkIndex == other.ChunkIndex &&
                ChunkCount == other.ChunkCount &&
                ResyncCorrelationId == other.ResyncCorrelationId;

            public override bool Equals(object obj) =>
                obj is ChunkPayloadKey other && Equals(other);

            public override int GetHashCode()
            {
                unchecked
                {
                    var hash = Scope.GetHashCode();
                    hash = (hash * 397) ^ (int)PayloadKind;
                    hash = (hash * 397) ^ (int)SnapshotTick;
                    hash = (hash * 397) ^ (int)BaselineTick;
                    hash = (hash * 397) ^ TotalHash.GetHashCode();
                    hash = (hash * 397) ^ (int)ChunkIndex;
                    hash = (hash * 397) ^ (int)ChunkCount;
                    hash = (hash * 397) ^ (int)ResyncCorrelationId;
                    return hash;
                }
            }
        }

        private readonly struct ChunkPayloadEntry
        {
            internal ChunkPayloadEntry(NetworkBufferLease payload, ulong payloadHash)
            {
                Payload = payload;
                PayloadHash = payloadHash;
            }

            internal readonly NetworkBufferLease Payload;
            internal readonly ulong PayloadHash;
        }

        private sealed class Peer
        {
            internal Peer(INetworkTransport transport, NetworkSession<TWorld> session, uint peerId, uint epoch, ScopeId scope)
            { Transport = transport; Session = session; PeerId = peerId; Epoch = epoch; Scope = scope; PacketSequence = 1; }
            internal readonly INetworkTransport Transport;
            internal readonly NetworkSession<TWorld> Session;
            internal readonly uint PeerId;
            internal readonly uint Epoch;
            internal ScopeId Scope { get; private set; }
            /// <summary>Reassigns this peer's scope (NCORE-15) and keeps its session in sync.</summary>
            internal void SetScope(ScopeId scope) { Scope = scope; Session.SetScope(scope); }
            internal uint PacketSequence;
            internal uint AcknowledgedSnapshotTick;
            internal uint LastSnapshotSentTick;
            internal uint ServerProcessedCommandTick;
            internal uint ServerProcessedCommandSequence;
            internal bool ResyncRequested;
            internal uint ResyncCorrelationId;
            internal uint ResyncSnapshotTick;
            internal bool ConnectionCounted;
            internal bool PeerCounted;
            internal bool AdmissionNotified;
            internal bool DisconnectNotified;
            internal uint LastReceivedPacketSequence =>
                Session.LastReceivedPacketSequence;
            internal readonly Dictionary<NetworkTransactionId,
                NetworkServerTransaction> Transactions =
                new Dictionary<NetworkTransactionId, NetworkServerTransaction>();
            internal readonly Dictionary<NetworkTransactionId,
                NetworkServerTransactionReceipt> ReceiptLedger =
                new Dictionary<NetworkTransactionId, NetworkServerTransactionReceipt>();
            internal readonly Queue<NetworkServerTransactionReceipt> PendingReceipts =
                new Queue<NetworkServerTransactionReceipt>();
            internal readonly HashSet<NetworkTransactionId> QueuedReceiptIds =
                new HashSet<NetworkTransactionId>();
            internal readonly Queue<NetworkTransactionId> ReceiptOrder =
                new Queue<NetworkTransactionId>();
            internal readonly List<NetworkTransactionId> CompletedTransactionIds =
                new List<NetworkTransactionId>();
            internal ulong HighestTransactionId;
            internal int PendingTransactionCount
            {
                get
                {
                    return Transactions.Count;
                }
            }

            internal bool HasPendingReceiptWork
            {
                get
                {
                    if (PendingReceipts.Count != 0)
                        return true;
                    foreach (var transaction in Transactions.Values)
                    {
                        if (transaction.CompletionStatus.HasValue &&
                            !transaction.ReceiptSent)
                            return true;
                    }
                    return false;
                }
            }

            internal void CacheReceipt(in NetworkServerTransactionReceipt receipt)
            {
                if (ReceiptLedger.ContainsKey(receipt.TransactionId))
                    return;
                ReceiptLedger.Add(receipt.TransactionId, receipt);
                ReceiptOrder.Enqueue(receipt.TransactionId);
                while (ReceiptOrder.Count > NetworkTransactionWire.ReceiptLedgerCapacity)
                {
                    var evicted = ReceiptOrder.Dequeue();
                    ReceiptLedger.Remove(evicted);
                }
            }

            internal bool QueueReceipt(in NetworkServerTransactionReceipt receipt)
            {
                CacheReceipt(in receipt);
                if (!QueuedReceiptIds.Add(receipt.TransactionId))
                    return true;
                if (PendingReceipts.Count >=
                    NetworkTransactionWire.MaxPendingTransactions)
                {
                    QueuedReceiptIds.Remove(receipt.TransactionId);
                    return false;
                }
                PendingReceipts.Enqueue(receipt);
                return true;
            }
            internal readonly NetworkCommandEnvelope[] DecodedCommands =
                new NetworkCommandEnvelope[ProtocolLimits.MaxCommandsPerBatch];
            internal NetworkPeerData Data() => new NetworkPeerData
            {
                Connection = Transport.Connection,
                PeerId = PeerId,
                Epoch = Epoch,
                Scope = Scope
            };
        }

        private sealed class NetworkCommandEnvelopeComparer :
            IComparer<NetworkCommandEnvelope>
        {
            internal static readonly NetworkCommandEnvelopeComparer Instance =
                new NetworkCommandEnvelopeComparer();

            public int Compare(NetworkCommandEnvelope left,
                NetworkCommandEnvelope right) =>
                left.Sequence.CompareTo(right.Sequence);
        }
    }
}
