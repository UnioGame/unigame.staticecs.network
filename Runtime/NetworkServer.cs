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
        private readonly ulong _simulationFingerprint;
        private readonly ulong _contentFingerprint;
        private readonly NetworkBufferPool _bufferPool;
        private readonly bool _ownsBufferPool;
        private readonly Dictionary<ScopeId, NetworkSnapshot> _captures =
            new Dictionary<ScopeId, NetworkSnapshot>();
        private uint _activeTick;
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

        public NetworkServer(NetworkSchema<TWorld> schema, NetworkScopeSelector<TWorld> scopeSelector, int historyTicks = 64, long historyBytes = 32 * 1024 * 1024, INetworkObserver observer = null, INetworkPeerObserver peerObserver = null, INetworkPeerAdmissionPolicy admissionPolicy = null, ulong simulationFingerprint = 0, ulong contentFingerprint = 0, NetworkBufferPool bufferPool = null)
        {
            _schema = schema ?? throw new ArgumentNullException(nameof(schema));
            if (scopeSelector == null) throw new ArgumentNullException(nameof(scopeSelector));
            _bufferPool = bufferPool ??
                new NetworkBufferPool(NetworkBufferPool.DefaultServerRetainedBytes);
            _ownsBufferPool = bufferPool == null;
            _coordinator = new NetworkServerCoordinator<TWorld>(historyTicks, historyBytes);
            _replicator = new NetworkReplicator<TWorld>(schema, scopeSelector,
                bufferPool: _bufferPool);
            _observer = observer;
            _peerObserver = peerObserver;
            _admissionPolicy = admissionPolicy;
            _simulationFingerprint = simulationFingerprint;
            _contentFingerprint = contentFingerprint;
        }

        public void Dispose()
        {
            if (_disposed)
                return;
            _disposed = true;
            for (var i = _peers.Count - 1; i >= 0; i--)
                CleanupPeer(_peers[i]);
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
                    _peers.RemoveAt(i);
                    i--;
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
                for (var i = 0; i < _peers.Count; i++)
                {
                    var peer = _peers[i];
                    if (peer.Session.State != NetworkSessionState.Established) continue;
                    // A stalled reliable channel must not let snapshots overtake
                    // terminal transaction receipts. Completed transactions stay
                    // counted until their receipt is actually accepted by transport.
                    if (peer.HasPendingReceiptWork)
                        continue;
                    if (!_captures.TryGetValue(peer.Scope, out var capture))
                    {
                        var started = Stopwatch.GetTimestamp();
                        if (_replicator.Capture(serverTick, peer.Scope, out capture) != SnapshotCaptureResult.Success) { peer.Session.Trace(NetworkPhase.SnapshotCapture, NetworkTraceKind.Point, NetworkResultCategory.World, NetworkPacketKind.SnapshotChunk, serverTick, 0, 0, 0, 0, 0, ElapsedNanoseconds(started)); continue; }
                        _captures.Add(peer.Scope, capture);
                        _coordinator.StoreCapture(peer.Scope, capture);
                        peer.Session.Trace(NetworkPhase.SnapshotCapture, NetworkTraceKind.Point, NetworkResultCategory.Success, NetworkPacketKind.SnapshotChunk, serverTick, 0, capture.ByteLength, _coordinator.HistoryCount(peer.Scope), _coordinator.HistoryByteCount(peer.Scope), unchecked((int)(serverTick - peer.AcknowledgedSnapshotTick)), ElapsedNanoseconds(started), capture.EntityCount, capture.RecordCount, activeConnections: ActiveConnectionCount, activePeers: ActivePeerCount);
                    }
                    peer.Session.ReportSnapshot(capture, _coordinator.History(peer.Scope));
                    SendSnapshot(peer, capture);
                }
                ServerTick = serverTick;
            }
            finally
            {
                _activeTick = 0;
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
                if (!PacketHeader.HasForeignProtocolVersion(packet.Span))
                    return false;
                CleanupPeer(peer);
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
                CleanupPeer(peer);
                return true;
            }
            if (header.Kind == PacketKind.Hello && !Admit(peer,
                    header.SchemaFingerprint, header.SimulationFingerprint,
                    header.ContentFingerprint))
                return true;
            NetworkCommandResult? commandResult = null;
            var decodeResult = NetworkResultCategory.Success;
            var resyncCorrelationId = 0u;
            if (header.Kind == PacketKind.CommandBatch)
            {
                commandResult = duplicateCommandPacket
                    ? NetworkCommandResult.Duplicate
                    : DecodeCommands(peer, packet, payload,
                        checked(ServerTick + 1));
                if (commandResult != NetworkCommandResult.Queued &&
                    commandResult != NetworkCommandResult.Duplicate)
                    decodeResult = NetworkResultCategory.Malformed;
            }
            else if (header.Kind == PacketKind.TransactionCommand)
            {
                commandResult = DecodeTransaction(peer, packet, payload,
                    checked(ServerTick + 1), duplicateTransactionPacket);
                if (duplicateTransactionPacket &&
                    commandResult != NetworkCommandResult.Duplicate)
                {
                    CleanupPeer(peer);
                    return true;
                }
                if (commandResult == NetworkCommandResult.PolicyRejected)
                    decodeResult = NetworkResultCategory.Policy;
                else if (commandResult != NetworkCommandResult.Queued &&
                         commandResult != NetworkCommandResult.Duplicate)
                    decodeResult = NetworkResultCategory.Malformed;
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
                    CleanupPeer(peer);
                    return true;
                }
                resyncCorrelationId = request.CorrelationId;
                peer.ResyncRequested = true;
                if (peer.ResyncCorrelationId == 0)
                {
                    peer.ResyncCorrelationId = request.CorrelationId;
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

        private NetworkCommandResult DecodeCommands(Peer peer,
            NetworkBufferLease packet,
            ReadOnlyMemory<byte> payload, uint serverTick)
        {
            if (payload.Length < 1)
            {
                SendResync(peer, serverTick,
                    NetworkResyncReason.ServerEmptyPayload,
                    NetworkResyncSource.ServerCommandDecode,
                    NetworkCommandResult.Malformed);
                return NetworkCommandResult.Malformed;
            }

            var bytes = payload.Span;
            int count = bytes[0];
            if (count < 1 || count > ProtocolLimits.MaxCommandsPerBatch)
            {
                SendResync(peer, serverTick,
                    NetworkResyncReason.ServerInvalidCommandCount,
                    NetworkResyncSource.ServerCommandDecode,
                    count > ProtocolLimits.MaxCommandsPerBatch
                        ? NetworkCommandResult.LimitExceeded
                        : NetworkCommandResult.Malformed);
                return NetworkCommandResult.Malformed;
            }

            int offset = 1;
            var commands = peer.DecodedCommands;
            var decoded = 0;
            for (var i = 0; i < count; i++)
            {
                if (offset > bytes.Length - 17)
                {
                    DisposeCommands(commands, decoded);
                    SendResync(peer, serverTick,
                        NetworkResyncReason.ServerTruncatedCommandHeader,
                        NetworkResyncSource.ServerCommandDecode,
                        NetworkCommandResult.Malformed);
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
                    SendResync(peer, serverTick,
                        NetworkResyncReason.ServerInvalidCommandEnvelope,
                        NetworkResyncSource.ServerCommandDecode,
                        payloadLength > ProtocolLimits.MaxCommandBytes
                            ? NetworkCommandResult.LimitExceeded
                            : NetworkCommandResult.Malformed);
                    return payloadLength > ProtocolLimits.MaxCommandBytes
                        ? NetworkCommandResult.LimitExceeded
                        : NetworkCommandResult.Malformed;
                }

                var exactLength = checked((int)payloadLength);
                var exact = packet.RetainSlice(PacketHeader.Size + offset, exactLength);
                offset += exactLength;
                var envelope = new NetworkCommandEnvelope(
                    peer.Transport.Connection,
                    peer.PeerId,
                    peer.Epoch,
                    sequence,
                    targetTick,
                    new NetworkTypeId(idValue),
                    version,
                    exact);
                commands[decoded++] = envelope;
            }

            if (offset != bytes.Length)
            {
                DisposeCommands(commands, decoded);
                SendResync(peer, serverTick,
                    NetworkResyncReason.ServerTrailingPayloadBytes,
                    NetworkResyncSource.ServerCommandDecode,
                    NetworkCommandResult.Malformed);
                return NetworkCommandResult.Malformed;
            }

            Array.Sort(commands, 0, decoded, NetworkCommandEnvelopeComparer.Instance);
            for (var i = 0; i < decoded; i++)
            {
                var result = _coordinator.Queue(commands[i], serverTick);
                if (result != NetworkCommandResult.Queued &&
                    result != NetworkCommandResult.Duplicate)
                {
                    for (var j = i; j < decoded; j++)
                    {
                        var remaining = commands[j];
                        remaining.Dispose();
                        commands[j] = default;
                    }
                    SendResync(peer, serverTick,
                        NetworkResyncReason.ServerCommandQueueRejected,
                        NetworkResyncSource.ServerCommandDecode,
                        result);
                    return result;
                }
                if (result == NetworkCommandResult.Duplicate)
                {
                    var duplicate = commands[i];
                    duplicate.Dispose();
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
            if (acknowledgedTick == 0)
            {
                peer.AcknowledgedSnapshotTick = 0;
                peer.ResyncRequested = true;
                return;
            }
            if (acknowledgedTick < peer.AcknowledgedSnapshotTick ||
                acknowledgedTick > ServerTick ||
                !_coordinator.TryGetCapture(peer.Scope, acknowledgedTick,
                    out var baseline) ||
                baseline.Scope != peer.Scope ||
                baseline.SchemaFingerprint != _schema.Fingerprint)
            {
                peer.ResyncRequested = true;
                return;
            }
            peer.AcknowledgedSnapshotTick = acknowledgedTick;
            if (peer.ResyncCorrelationId != 0 &&
                (peer.ResyncSnapshotTick == 0 ||
                 acknowledgedTick < peer.ResyncSnapshotTick))
            {
                peer.ResyncRequested = true;
                return;
            }
            peer.ResyncRequested = false;
            peer.ResyncCorrelationId = 0;
            peer.ResyncSnapshotTick = 0;
        }

        private void SendSnapshot(Peer peer, NetworkSnapshot snapshot)
        {
            NetworkBufferLease delta = null;
            try
            {
                var baselineTick = peer.AcknowledgedSnapshotTick;
                NetworkSnapshot baseline = null;
                var keyframe = peer.ResyncRequested || baselineTick == 0 ||
                    !_coordinator.TryGetCapture(peer.Scope, baselineTick,
                        out baseline) ||
                    baseline.Scope != peer.Scope ||
                    baseline.SchemaFingerprint != _schema.Fingerprint;
                if (!keyframe)
                {
                    if (!SnapshotDeltaCodec.TryEncode(_bufferPool, baseline,
                            snapshot, out delta) ||
                        delta.Length >= snapshot.ByteLength)
                    {
                        delta?.Dispose();
                        delta = null;
                        keyframe = true;
                    }
                }
                if (keyframe)
                    peer.ResyncRequested = true;

                var body = keyframe ? snapshot.Bytes.Span : delta.Span;
                var reliableLimit = peer.Transport.MaxReliablePayloadBytes;
                if (reliableLimit <= PacketHeader.Size + SnapshotChunkHeader.Size)
                {
                    peer.ResyncRequested = true;
                    return;
                }
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
                for (uint chunkIndex = 0; chunkIndex < chunkCount; chunkIndex++)
                {
                    var bodyOffset = checked((int)((long)chunkIndex *
                                                   maxBody));
                    var bodyLength = Math.Min(maxBody, body.Length - bodyOffset);
                    var payload = _bufferPool.Rent(checked(
                        SnapshotChunkHeader.Size + bodyLength));
                    try
                    {
                        var chunk = new SnapshotChunkHeader
                        {
                            PayloadKind = keyframe
                                ? SnapshotPayloadKind.Keyframe
                                : SnapshotPayloadKind.Delta,
                            SnapshotTick = snapshot.ServerTick,
                            BaselineTick = keyframe ? 0 : baselineTick,
                            TotalLength = checked((uint)snapshot.ByteLength),
                            TotalHash = snapshot.PayloadHash,
                            ChunkIndex = chunkIndex,
                            ChunkCount = chunkCount,
                            ResyncCorrelationId = keyframe
                                ? peer.ResyncCorrelationId
                                : 0
                        };
                        if (!chunk.TryWrite(payload.WritableSpan))
                        {
                            peer.ResyncRequested = true;
                            return;
                        }
                        body.Slice(bodyOffset, bodyLength).CopyTo(
                            payload.WritableSpan.Slice(SnapshotChunkHeader.Size));
                        if (!SendSnapshotChunk(peer, snapshot.ServerTick,
                                chunkIndex + 1, payload.Span))
                        {
                            peer.ResyncRequested = true;
                            return;
                        }
                    }
                    finally
                    {
                        payload.Dispose();
                    }
                }
                if (keyframe && peer.ResyncCorrelationId != 0)
                    peer.ResyncSnapshotTick = snapshot.ServerTick;
            }
            finally
            {
                delta?.Dispose();
            }
        }

        private bool SendSnapshotChunk(Peer peer, uint serverTick,
            uint sequence, ReadOnlySpan<byte> payload)
        {
            var started = Stopwatch.GetTimestamp();
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
            var encoded = NetworkPacket.TryEncode(_bufferPool, header, payload,
                out var packet);
            var packetBytes = packet?.Length ?? 0;
            var sent = encoded && peer.Transport.TrySend(packet);
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
                // Keep the policy result in the same bounded transaction ledger
                // as accepted commands. It cannot be lost when reliable send is
                // stalled, and it still consumes one of the 256 pending slots.
                peer.Transactions.Add(transactionId,
                    new NetworkServerTransaction(transactionId, default,
                        default, applicationTick)
                    {
                        Dispatched = true,
                        CompletionStatus = NetworkTransactionStatus.PolicyRejected
                    });
                return NetworkCommandResult.PolicyRejected;
            }
            peer.Transactions.Add(transactionId,
                new NetworkServerTransaction(transactionId, envelope, entry,
                    applicationTick));
            return NetworkCommandResult.Queued;
        }

        private static void DispatchTransactions(Peer peer)
        {
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

        private bool SendResync(Peer peer, uint serverTick,
            NetworkResyncReason resyncReason,
            NetworkResyncSource resyncSource,
            NetworkCommandResult? commandResult = null)
        {
            var correlationId = peer.ResyncCorrelationId;
            if (correlationId == 0)
            {
                correlationId = peer.PacketSequence;
                if (correlationId == 0) return false;
                peer.ResyncCorrelationId = correlationId;
                peer.ResyncSnapshotTick = 0;
            }
            peer.ResyncRequested = true;
            Span<byte> payload = stackalloc byte[ResyncRequestPayload.Size];
            if (!new ResyncRequestPayload(correlationId).TryWrite(payload))
                return false;
            return Send(peer, PacketKind.ResyncRequest, serverTick,
                PacketHeader.NoneTick, payload, resyncReason, resyncSource,
                correlationId, commandResult);
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
            internal uint ResyncCorrelationId;
            internal uint ResyncSnapshotTick;
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
