using System;
using System.Collections.Generic;
using System.IO;
using System.Diagnostics;
using FFS.Libraries.StaticEcs;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Runs the framed receive, decode, apply, gameplay-boundary, and send pipeline for one client connection.</summary>
    public sealed class NetworkClient<TWorld> : IDisposable
        where TWorld : struct, IWorldType
    {
        private static readonly long SnapshotAssemblyTimeoutTicks =
            Stopwatch.Frequency * 2L;
        private readonly INetworkTransport _transport;
        private readonly NetworkSchema<TWorld> _schema;
        private readonly NetworkReplicator<TWorld> _replicator;
        private readonly NetworkSession<TWorld> _session;
        private readonly NetworkBufferPool _bufferPool;
        private readonly bool _ownsBufferPool;
        private readonly List<NetworkCommandEnvelope> _recentCommands = new List<NetworkCommandEnvelope>();
        private readonly Dictionary<NetworkTransactionId, NetworkClientTransaction> _transactions =
            new Dictionary<NetworkTransactionId, NetworkClientTransaction>();
        private readonly Queue<NetworkClientTransactionResult> _transactionResults =
            new Queue<NetworkClientTransactionResult>();
        private readonly int _commandRedundancy;
        private readonly int _ticksPerSecond;
        private readonly int _predictionLeadTicks;
        private readonly ulong _simulationFingerprint;
        private readonly ulong _contentFingerprint;
        private readonly NetworkBufferLease[] _snapshotChunks =
            new NetworkBufferLease[ProtocolLimits.MaxChunkMappings];
        private uint _packetSequence = 1;
        private uint _commandPacketSequence = 1;
        private ulong _nextTransactionId = 1;
        private uint _lastCommandFlushTick;
        private bool _commandsDirty;
        private bool _disposed;
        private long _recentCommandBytes;
        private int _recentCommandsHighWater;
        private long _recentCommandBytesHighWater;
        private long _lastServerTickTimestamp;
        private long _lastPingTimestamp;
        private double _roundTripSeconds;
        private bool _handshakeStarted;
        private NetworkRecoveryTransition _recoveryTransition;
        private bool _hasRecoveryTransition;
        private PacketHeader _snapshotAssemblyHeader;
        private SnapshotChunkHeader _snapshotAssemblyChunk;
        private int _snapshotAssemblyReceived;
        private int _snapshotAssemblyBytes;
        private long _snapshotAssemblyDeadline;
        private uint _snapshotDiscardThroughTick;
        private uint _resyncCorrelationId;

        public NetworkClient(INetworkTransport transport, NetworkSchema<TWorld> schema,
            ScopeId scope = default, INetworkObserver observer = null,
            int ticksPerSecond = 20, int predictionLeadTicks = 1,
            int commandRedundancy = NetworkSimulationConfig.DefaultCommandRedundancy,
            ulong simulationFingerprint = 0,
            ulong contentFingerprint = 0, NetworkBufferPool bufferPool = null)
        {
            _transport = transport ?? throw new ArgumentNullException(nameof(transport));
            _schema = schema ?? throw new ArgumentNullException(nameof(schema));
            if (ticksPerSecond <= 0) throw new ArgumentOutOfRangeException(nameof(ticksPerSecond));
            if (predictionLeadTicks < 0) throw new ArgumentOutOfRangeException(nameof(predictionLeadTicks));
            if (commandRedundancy < NetworkSimulationConfig.MinimumCommandRedundancy ||
                commandRedundancy > NetworkSimulationConfig.MaximumCommandRedundancy)
                throw new ArgumentOutOfRangeException(nameof(commandRedundancy));
            _ticksPerSecond = ticksPerSecond;
            _predictionLeadTicks = predictionLeadTicks;
            _commandRedundancy = commandRedundancy;
            _simulationFingerprint = simulationFingerprint;
            _contentFingerprint = contentFingerprint;
            _bufferPool = bufferPool ??
                new NetworkBufferPool(NetworkBufferPool.DefaultClientRetainedBytes);
            _ownsBufferPool = bufferPool == null;
            _replicator = new NetworkReplicator<TWorld>(schema, scope,
                bufferPool: _bufferPool);
            _session = new NetworkSession<TWorld>(transport.Connection,
                NetworkRole.Client, schema, _bufferPool, observer);
            _session.ReportSession(0, 0, 0, _packetSequence);
        }

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

        /// <summary>Consumes the next recovery transition without retaining ECS state in the pipeline.</summary>
        public bool TryConsumeRecoveryTransition(out NetworkRecoveryTransition transition)
        {
            if (!_hasRecoveryTransition)
            {
                transition = default;
                return false;
            }

            transition = _recoveryTransition;
            _recoveryTransition = default;
            _hasRecoveryTransition = false;
            return true;
        }

        public NetworkBufferPoolDiagnostics CaptureBufferDiagnostics() =>
            _bufferPool.CaptureDiagnostics();

        /// <summary>Captures current bounded endpoint memory and queue ownership.</summary>
        public NetworkMemoryDiagnostics CaptureMemoryDiagnostics() => new NetworkMemoryDiagnostics
        {
            Buffers = _bufferPool.CaptureDiagnostics(),
            HistoryBytes = History.Bytes,
            PendingCommands = _recentCommands.Count,
            PendingCommandBytes = _recentCommandBytes,
            PendingCommandsHighWater = _recentCommandsHighWater,
            PendingCommandBytesHighWater = _recentCommandBytesHighWater,
        };

        public int PendingTransactionCount => _transactions.Count;

        public int PendingTransactionResultCount => _transactionResults.Count;

        /// <summary>Gets trusted submission context while a transaction is pending.</summary>
        public bool TryGetTransactionContext(NetworkTransactionId transactionId,
            out NetworkCommandContext context)
        {
            if (_transactions.TryGetValue(transactionId, out var transaction))
            {
                context = transaction.Context;
                return true;
            }
            context = default;
            return false;
        }

        public bool TryDequeueTransactionResult(out NetworkTransactionResult result)
        {
            if (_transactionResults.Count == 0)
            {
                result = default;
                return false;
            }
            var item = _transactionResults.Dequeue();
            result = new NetworkTransactionResult(item.TransactionId,
                item.Status, item.ApplicationTick, item.TypeId);
            return true;
        }

        /// <summary>Dequeues the next terminal result when it belongs to the requested command type.</summary>
        public bool TryDequeueTransactionResult<TCommand>(
            out NetworkTransactionResultEvent<TCommand> result)
            where TCommand : struct, IEvent, INetworkTransactionCommand
        {
            if (_transactionResults.Count == 0)
            {
                result = default;
                return false;
            }
            // Results can contain several command types. Rotate the queue instead
            // of requiring every ECS projector to share one type order.
            var count = _transactionResults.Count;
            for (var i = 0; i < count; i++)
            {
                var item = _transactionResults.Dequeue();
                if (item.Command is TCommand command)
                {
                    result = new NetworkTransactionResultEvent<TCommand>
                    {
                        Command = command,
                        TransactionId = item.TransactionId,
                        Status = item.Status,
                        Context = item.Context
                    };
                    return true;
                }
                _transactionResults.Enqueue(item);
            }
            result = default;
            return false;
        }

        /// <summary>Closes the session and removes all replica-owned entities from the client world.</summary>
        public void Disconnect()
        {
            ClearSnapshotAssembly();
            _snapshotDiscardThroughTick = 0;
            CompletePendingTransactions(NetworkTransactionStatus.SessionLost);
            _session.Close();
            _replicator.ClearReplicas();
            AcknowledgedSnapshotTick = 0;
            ServerProcessedCommandTick = 0;
            ServerProcessedCommandSequence = 0;
            ServerTick = 0;
            _lastServerTickTimestamp = 0;
            _lastPingTimestamp = 0;
            _roundTripSeconds = 0d;
            for (var i = 0; i < _recentCommands.Count; i++)
            {
                var command = _recentCommands[i];
                command.Dispose();
            }
            _recentCommands.Clear();
            _recentCommandBytes = 0;
            _lastCommandFlushTick = 0;
            _commandsDirty = false;
            _nextTransactionId = 1;
            _handshakeStarted = false;
            _recoveryTransition = default;
            _hasRecoveryTransition = false;
            _resyncCorrelationId = 0;
            _session.ReportSession(0, 0, 0, _packetSequence);
        }

        public void Dispose()
        {
            if (_disposed)
                return;
            _disposed = true;
            Disconnect();
            _replicator.Dispose();
            if (_ownsBufferPool)
                _bufferPool.Dispose();
        }

        /// <summary>Sends the protocol-seven Hello packet.</summary>
        public bool BeginHandshake()
        {
            if (_handshakeStarted || _session.State != NetworkSessionState.Handshaking)
                return false;
            _handshakeStarted = Send(PacketKind.Hello, 0, PacketHeader.NoneTick,
                PacketHeader.NoneTick, ReadOnlySpan<byte>.Empty);
            return _handshakeStarted;
        }

        /// <summary>Copies protocol service state for projection onto a connection entity.</summary>
        public NetworkConnectionSnapshot CaptureConnection()
        {
            return new NetworkConnectionSnapshot
            {
                Connection = new NetworkConnectionComponent
                {
                    Connection = _session.Connection,
                    Role = NetworkRole.Client,
                    State = _session.State,
                    PeerId = _session.PeerId,
                    Epoch = _session.Epoch,
                    Scope = _session.Scope,
                },
                Ticks = new NetworkConnectionTickComponent
                {
                    ServerTick = ServerTick,
                    EstimatedServerTick = EstimatedServerTick,
                    AcknowledgedSnapshotTick = AcknowledgedSnapshotTick,
                    ServerProcessedCommandTick = ServerProcessedCommandTick,
                    ServerProcessedCommandSequence = ServerProcessedCommandSequence,
                },
                Clock = new NetworkConnectionClockComponent
                {
                    RoundTripSeconds = _roundTripSeconds,
                    LastServerTickTimestamp = _lastServerTickTimestamp,
                    LastPingTimestamp = _lastPingTimestamp,
                },
            };
        }

        /// <summary>Processes received packets using authoritative ticks carried by the wire.</summary>
        public void Process()
        {
            Process(Stopwatch.GetTimestamp());
        }

        internal void Process(long timestamp)
        {
            InspectSnapshotAssemblyTimeout(timestamp);
            while (true)
            {
                var receiveStarted = Stopwatch.GetTimestamp();
                if (!_transport.TryReceive(out var packet)) break;
                try
                {
                    _session.Trace(NetworkPhase.Receive, NetworkTraceKind.Point, NetworkResultCategory.Success, NetworkPacketKind.None, AcknowledgedSnapshotTick, 0, packet.Length, History.Count, History.Bytes, 0, ElapsedNanoseconds(receiveStarted));
                    var started = Stopwatch.GetTimestamp();
                    if (!NetworkPacket.TryDecode(packet, out var header, out var payload))
                    {
                        _session.Trace(NetworkPhase.Decode, NetworkTraceKind.Point,
                            NetworkResultCategory.Protocol, NetworkPacketKind.None,
                            AcknowledgedSnapshotTick, 0, packet.Length, History.Count,
                            History.Bytes, 0, ElapsedNanoseconds(started));
                        if (PacketHeader.HasForeignProtocolVersion(packet.Span))
                        {
                            RequestDisconnect(
                                NetworkRecoveryReason.ProtocolIncompatible);
                            return;
                        }
                        RequestResync(AcknowledgedSnapshotTick,
                            NetworkRecoveryReason.SnapshotRejected,
                            NetworkResyncSource.ClientSnapshotValidation);
                        continue;
                    }
                    if (header.Kind != PacketKind.Disconnect &&
                        (header.SchemaFingerprint != _schema.Fingerprint ||
                         header.SimulationFingerprint != _simulationFingerprint ||
                         header.ContentFingerprint != _contentFingerprint))
                    {
                        _session.Trace(NetworkPhase.Decode, NetworkTraceKind.Point,
                            NetworkResultCategory.Protocol, NetworkPacketKind.None,
                            AcknowledgedSnapshotTick, 0, packet.Length, History.Count,
                            History.Bytes, 0, ElapsedNanoseconds(started));
                        RequestDisconnect(NetworkRecoveryReason.ProtocolIncompatible);
                        return;
                    }
                    var packetValidation = _session.ValidatePacket(in header);
                    if (packetValidation != PacketValidationResult.Success &&
                        !(header.Kind == PacketKind.TransactionReceipt &&
                          packetValidation == PacketValidationResult.Duplicate))
                    {
                        _session.Trace(NetworkPhase.Decode, NetworkTraceKind.Point,
                            NetworkResultCategory.Protocol, NetworkPacketKind.None,
                            AcknowledgedSnapshotTick, 0, packet.Length, History.Count,
                            History.Bytes, 0, ElapsedNanoseconds(started),
                            packetValidationResult: packetValidation);
                        RequestDisconnect(NetworkRecoveryReason.ProtocolIncompatible);
                        return;
                    }
                if (header.ServerTick != PacketHeader.NoneTick && header.ServerTick >= ServerTick)
                {
                    ServerTick = header.ServerTick;
                    _lastServerTickTimestamp = Stopwatch.GetTimestamp();
                }
                StagedNetworkSnapshot staged = default;
                var entities = 0;
                var records = 0;
                var decodedBytes = packet.Length;
                var decodeResult = NetworkResultCategory.Success;
                var resyncReason = NetworkResyncReason.None;
                var resyncSource = NetworkResyncSource.None;
                var snapshotKind = default(SnapshotPayloadKind);
                var awaitingSnapshotChunks = false;
                var discardRejectedSnapshot = false;
                SnapshotApplyResult? snapshotResult = null;
                uint resyncCorrelationId = 0;
                if (header.Kind == PacketKind.Ready) DecodeReady(header, payload);
                else if (header.Kind == PacketKind.SnapshotChunk)
                {
                    if (SnapshotChunkHeader.TryRead(payload.Span, out var preview))
                    {
                        resyncCorrelationId = preview.ResyncCorrelationId;
                        var stale = preview.SnapshotTick <= Math.Max(
                            AcknowledgedSnapshotTick,
                            _snapshotDiscardThroughTick);
                        if (!stale &&
                            preview.PayloadKind == SnapshotPayloadKind.Keyframe &&
                            resyncCorrelationId != 0 &&
                            _resyncCorrelationId == 0)
                            _resyncCorrelationId = resyncCorrelationId;
                        if (!stale && ((resyncCorrelationId != 0 &&
                             preview.PayloadKind != SnapshotPayloadKind.Keyframe) ||
                            (preview.PayloadKind == SnapshotPayloadKind.Keyframe &&
                             _resyncCorrelationId != 0 &&
                             resyncCorrelationId != _resyncCorrelationId)))
                        {
                            RequestDisconnect(NetworkRecoveryReason.ProtocolIncompatible);
                            return;
                        }
                    }
                    snapshotResult = TryStageSnapshot(packet, header, payload,
                        out staged, out entities, out records, out decodedBytes,
                        out snapshotKind, out awaitingSnapshotChunks,
                        out discardRejectedSnapshot);
                    decodeResult = DiagnosticResult(snapshotResult.Value);
                }
                else if (header.Kind == PacketKind.TransactionReceipt)
                {
                    if (!DecodeTransactionReceipt(payload))
                    {
                        RequestDisconnect(NetworkRecoveryReason.ProtocolIncompatible);
                        return;
                    }
                }
                else if (header.Kind == PacketKind.Pong) DecodePong(payload);
                else if (header.Kind == PacketKind.ResyncRequest)
                {
                    if (!ResyncRequestPayload.TryRead(payload.Span,
                            out var request))
                    {
                        _session.Trace(NetworkPhase.Decode,
                            NetworkTraceKind.Point,
                            NetworkResultCategory.Malformed,
                            NetworkPacketKind.ResyncRequest, header.ServerTick,
                            0, payload.Length, History.Count, History.Bytes,
                            unchecked((int)(header.ServerTick -
                                             AcknowledgedSnapshotTick)),
                            ElapsedNanoseconds(started),
                            packetValidationResult: packetValidation,
                            sequence: header.PacketSequence,
                            acknowledgedSnapshotTick: AcknowledgedSnapshotTick,
                            oldestHistoryTick: History.OldestTick,
                            newestHistoryTick: History.NewestTick);
                        RequestDisconnect(NetworkRecoveryReason.ProtocolIncompatible);
                        return;
                    }
                    resyncCorrelationId = request.CorrelationId;
                    RequestResync(header.ServerTick,
                        NetworkRecoveryReason.SnapshotRejected,
                        NetworkResyncSource.ClientIncomingResyncEcho,
                        request.CorrelationId);
                }
                var disconnected = header.Kind == PacketKind.Disconnect;
                _session.Trace(NetworkPhase.Decode, NetworkTraceKind.Point, decodeResult, DiagnosticKind(header.Kind), header.ServerTick, 0, decodedBytes, History.Count, History.Bytes, unchecked((int)(header.ServerTick - AcknowledgedSnapshotTick)), ElapsedNanoseconds(started), entities, records, resyncReason: resyncReason, resyncSource: resyncSource, resyncCorrelationId: resyncCorrelationId, snapshotResult: snapshotResult, packetValidationResult: packetValidation, sequence: header.PacketSequence, acknowledgedSnapshotTick: AcknowledgedSnapshotTick, oldestHistoryTick: History.OldestTick, newestHistoryTick: History.NewestTick);
                if (disconnected)
                {
                    Disconnect();
                    return;
                }
                if (header.Kind == PacketKind.SnapshotChunk)
                {
                    if (!awaitingSnapshotChunks)
                    {
                        if (staged.Snapshot == null)
                        {
                            if (discardRejectedSnapshot)
                                _snapshotDiscardThroughTick = Math.Max(
                                    _snapshotDiscardThroughTick,
                                    header.ServerTick);
                            RequestResync(header.ServerTick,
                                NetworkRecoveryReason.SnapshotRejected,
                                NetworkResyncSource.ClientSnapshotValidation);
                        }
                        else if (!ApplySnapshot(staged, header, entities, records,
                                     snapshotKind, resyncCorrelationId)) return;
                    }
                }
                _session.ReportSession(ServerTick, AcknowledgedSnapshotTick, ServerProcessedCommandSequence, _packetSequence);
                }
                finally
                {
                    packet.Dispose();
                }
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

        /// <summary>Submits one reliable transaction and returns its epoch-scoped id.</summary>
        public NetworkCommandResult SubmitTransaction<TCommand>(in TCommand command,
            out NetworkTransactionId transactionId)
            where TCommand : struct, IEvent, INetworkTransactionCommand =>
            SubmitTransaction(in command, ServerTick, out transactionId);

        /// <summary>Attempts one reliable transaction submission without throwing on transport failure.</summary>
        public bool TrySubmitTransaction<TCommand>(in TCommand command,
            out NetworkTransactionId transactionId)
            where TCommand : struct, IEvent, INetworkTransactionCommand =>
            SubmitTransaction(in command, out transactionId) == NetworkCommandResult.Queued;

        /// <summary>Submits one reliable transaction; the server chooses its application tick.</summary>
        public NetworkCommandResult SubmitTransaction<TCommand>(in TCommand command,
            uint targetTick, out NetworkTransactionId transactionId)
            where TCommand : struct, IEvent, INetworkTransactionCommand
        {
            transactionId = default;
            if (_session.State != NetworkSessionState.Established)
                return NetworkCommandResult.WrongSession;
            if (_transactions.Count >= NetworkTransactionWire.MaxPendingTransactions)
                return NetworkCommandResult.LimitExceeded;
            if (_nextTransactionId == 0)
                return NetworkCommandResult.Sequence;

            var idValue = _nextTransactionId;
            _nextTransactionId = idValue == ulong.MaxValue ? 0 : idValue + 1;
            transactionId = new NetworkTransactionId(idValue);
            NetworkCommandEnvelope envelope;
            NetworkCommandResult result;
            try
            {
                result = _session.CreateTransaction(in command, out envelope);
            }
            catch (InvalidOperationException)
            {
                return NetworkCommandResult.LimitExceeded;
            }
            if (result != NetworkCommandResult.Queued)
                return result;
            var length = checked(NetworkTransactionWire.CommandHeaderSize +
                                 envelope.ExactLength);
            if (length > ProtocolLimits.MaxWirePayloadBytes ||
                PacketHeader.Size + length > _transport.MaxReliablePayloadBytes)
            {
                envelope.Dispose();
                return NetworkCommandResult.LimitExceeded;
            }

            var payload = _bufferPool.Rent(length);
            try
            {
                if (!NetworkTransactionWire.TryWriteCommand(payload.WritableSpan,
                        transactionId, envelope.TypeId, envelope.Version,
                        envelope.Payload.Span))
                    return NetworkCommandResult.LimitExceeded;
                var packetSequence = _packetSequence;
                var sent = Send(PacketKind.TransactionCommand, _session.Epoch,
                    ServerTick, PacketHeader.NoneTick, payload.Span);
                var context = new NetworkCommandContext(_session.PeerId,
                    _session.Epoch, packetSequence, targetTick,
                    NetworkCommandDelivery.Transaction, envelope.TypeId,
                    transactionId);
                if (!sent)
                {
                    QueueTransactionResult(new NetworkClientTransaction(
                        transactionId, command, envelope.TypeId, in context),
                        NetworkTransactionStatus.SubmissionFailed, ServerTick);
                    return NetworkCommandResult.SubmissionFailed;
                }
                _transactions.Add(transactionId, new NetworkClientTransaction(
                    transactionId, command, envelope.TypeId, in context));
                return NetworkCommandResult.Queued;
            }
            finally
            {
                payload.Dispose();
                envelope.Dispose();
            }
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
            var projectedCount = _recentCommands.Count + 1;
            var projectedPayloadBytes = 1L + 17L * projectedCount +
                                        _recentCommandBytes + envelope.ExactLength;
            if (projectedPayloadBytes > ProtocolLimits.MaxWirePayloadBytes ||
                PacketHeader.Size + projectedPayloadBytes > _transport.MaxUnreliablePayloadBytes)
            {
                envelope.Dispose();
                return NetworkCommandResult.LimitExceeded;
            }
            sequence = envelope.Sequence;
            _recentCommands.Add(envelope);
            _recentCommandBytes += envelope.ExactLength;
            if (_recentCommands.Count > _recentCommandsHighWater)
                _recentCommandsHighWater = _recentCommands.Count;
            if (_recentCommandBytes > _recentCommandBytesHighWater)
                _recentCommandBytesHighWater = _recentCommandBytes;
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
            long length = 1;
            for (var i = 0; i < _recentCommands.Count; i++)
                length += 17L + _recentCommands[i].ExactLength;
            if (_recentCommands.Count > Math.Min((int)byte.MaxValue, ProtocolLimits.MaxCommandsPerBatch) ||
                length > ProtocolLimits.MaxWirePayloadBytes ||
                PacketHeader.Size + length > _transport.MaxUnreliablePayloadBytes)
            {
                for (var i = 0; i < _recentCommands.Count; i++)
                {
                    var command = _recentCommands[i];
                    command.Dispose();
                }
                _recentCommands.Clear();
                _recentCommandBytes = 0;
                _commandsDirty = false;
                return NetworkCommandResult.LimitExceeded;
            }
            var payload = _bufferPool.Rent(checked((int)length));
            var payloadBytes = payload.WritableSpan;
            payloadBytes[0] = checked((byte)_recentCommands.Count);
            int offset = 1;
            for (var i = 0; i < _recentCommands.Count; i++)
            {
                var command = _recentCommands[i];
                Hashing.Write32(payloadBytes, offset, command.Sequence);
                Hashing.Write32(payloadBytes, offset + 4, command.TargetTick);
                Hashing.Write32(payloadBytes, offset + 8, command.TypeId.Value);
                payloadBytes[offset + 12] = command.Version;
                Hashing.Write32(payloadBytes, offset + 13,
                    checked((uint)command.ExactLength));
                command.Payload.Span.CopyTo(payloadBytes.Slice(offset + 17));
                offset += 17 + command.ExactLength;
            }
            var header = Header(PacketKind.CommandBatch, _commandPacketSequence++, ServerTick);
            header.Flags = PacketFlags.UnreliableSequenced;
            var started = Stopwatch.GetTimestamp();
            var encoded = NetworkPacket.TryEncode(_bufferPool, header, payload.Span,
                out var packet);
            payload.Dispose();
            var packetBytes = packet?.Length ?? 0;
            var sent = encoded && _transport.TrySend(packet);
            _session.Trace(NetworkPhase.Send, NetworkTraceKind.Point,
                sent ? NetworkResultCategory.Success : NetworkResultCategory.Transport,
                NetworkPacketKind.CommandBatch, ServerTick, currentTick,
                packetBytes, History.Count, History.Bytes,
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
        public void RequestFullResync(NetworkRecoveryReason reason)
        {
            RequestResync(ServerTick, reason);
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
            Span<byte> payload = stackalloc byte[8];
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
                {
                    var command = _recentCommands[i];
                    _recentCommandBytes -= command.ExactLength;
                    command.Dispose();
                    _recentCommands.RemoveAt(i);
                }
        }

        private bool DecodeTransactionReceipt(ReadOnlyMemory<byte> payload)
        {
            if (!NetworkTransactionWire.TryReadReceipt(payload.Span,
                    out var transactionId, out var status,
                    out var applicationTick))
                return false;
            if (!_transactions.TryGetValue(transactionId, out var transaction))
                return true;
            _transactions.Remove(transactionId);
            QueueTransactionResult(transaction, status, applicationTick);
            return true;
        }

        private void QueueTransactionResult(NetworkClientTransaction transaction,
            NetworkTransactionStatus status, uint applicationTick)
        {
            _transactionResults.Enqueue(new NetworkClientTransactionResult(
                transaction, status, applicationTick));
        }

        private void CompletePendingTransactions(NetworkTransactionStatus status)
        {
            if (_transactions.Count == 0)
                return;
            foreach (var transaction in _transactions.Values)
                QueueTransactionResult(transaction, status, ServerTick);
            _transactions.Clear();
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
            if (_session.State != NetworkSessionState.Handshaking || header.SessionEpoch == 0 || payload.Length != 12) { RequestRecovery(NetworkRecoveryPhase.AwaitingKeyframe, NetworkRecoveryReason.SnapshotRejected, header.ServerTick); return; }
            var bytes = payload.Span;
            var peer = Hashing.Read32(bytes, 0);
            var scope = new ScopeId(Hashing.Read64(bytes, 4));
            if (_session.Admit(header.SchemaFingerprint, peer, header.SessionEpoch, scope) != NetworkAdmissionResult.Accepted) RequestRecovery(NetworkRecoveryPhase.AwaitingKeyframe, NetworkRecoveryReason.SnapshotRejected, header.ServerTick);
        }

        private SnapshotApplyResult TryStageSnapshot(NetworkBufferLease packet,
            PacketHeader header, ReadOnlyMemory<byte> payload,
            out StagedNetworkSnapshot staged, out int entities, out int records,
            out int decodedBytes, out SnapshotPayloadKind payloadKind,
            out bool awaitingChunks, out bool discardRejectedTick)
        {
            staged = default;
            entities = 0;
            records = 0;
            decodedBytes = payload.Length;
            payloadKind = default;
            discardRejectedTick = false;
            NetworkBufferLease body = null;
            if (!TryAssembleSnapshot(packet, in header, payload, out var chunk,
                    out body, out awaitingChunks))
            {
                discardRejectedTick = true;
                return SnapshotApplyResult.Malformed;
            }
            if (awaitingChunks)
                return SnapshotApplyResult.Success;
            payloadKind = chunk.PayloadKind;
            try
            {
                NetworkSnapshot snapshot;
                if (chunk.PayloadKind == SnapshotPayloadKind.Keyframe)
                {
                    if (chunk.TotalLength != (uint)body.Length ||
                        Hashing.XxHash64(body.Span) != chunk.TotalHash ||
                        !SnapshotDeltaCodec.TryInspectCanonical(body.Span,
                            out entities, out records))
                    {
                        discardRejectedTick = true;
                        return SnapshotApplyResult.Malformed;
                    }
                    snapshot = _replicator.CreateSnapshot(chunk.SnapshotTick,
                        header.SchemaFingerprint, _session.Scope, body,
                        entities, records);
                    body = null;
                }
                else
                {
                    NetworkBufferLease canonical = null;
                    if (!History.TryGet(chunk.BaselineTick, out var baseline) ||
                        baseline.SchemaFingerprint != header.SchemaFingerprint ||
                        baseline.Scope != _session.Scope)
                        return SnapshotApplyResult.Malformed;
                    if (!SnapshotDeltaCodec.TryReconstruct(_bufferPool, baseline,
                            body.Span, in chunk, header.SchemaFingerprint,
                            _session.Scope, out canonical, out entities,
                            out records))
                    {
                        discardRejectedTick = true;
                        return SnapshotApplyResult.Malformed;
                    }
                    try
                    {
                        snapshot = _replicator.CreateSnapshot(chunk.SnapshotTick,
                            header.SchemaFingerprint, _session.Scope, canonical,
                            entities, records);
                        canonical = null;
                    }
                    finally
                    {
                        canonical?.Dispose();
                    }
                }
                decodedBytes = snapshot.ByteLength;
                var result = _replicator.Stage(snapshot, out staged);
                if (result != SnapshotApplyResult.Success)
                {
                    discardRejectedTick = true;
                    snapshot.Dispose();
                }
                return result;
            }
            finally
            {
                body?.Dispose();
            }
        }

        private bool TryAssembleSnapshot(NetworkBufferLease packet,
            in PacketHeader header, ReadOnlyMemory<byte> payload,
            out SnapshotChunkHeader completedChunk,
            out NetworkBufferLease completedBody, out bool awaitingChunks)
        {
            completedChunk = default;
            completedBody = null;
            awaitingChunks = false;
            var reliableLimit = _transport.MaxReliablePayloadBytes;
            if (reliableLimit <= PacketHeader.Size + SnapshotChunkHeader.Size ||
                packet.Length > reliableLimit ||
                payload.Length < SnapshotChunkHeader.Size ||
                !SnapshotChunkHeader.TryRead(payload.Span, out var chunk) ||
                chunk.ChunkCount > ProtocolLimits.MaxChunkMappings ||
                chunk.TotalLength > ProtocolLimits.MaxDecodedPayloadBytes ||
                chunk.SnapshotTick != header.ServerTick)
                return false;
            if (chunk.SnapshotTick <= Math.Max(AcknowledgedSnapshotTick,
                    _snapshotDiscardThroughTick))
            {
                awaitingChunks = true;
                return true;
            }
            var maxBody = Math.Min(
                reliableLimit - PacketHeader.Size - SnapshotChunkHeader.Size,
                ProtocolLimits.MaxWirePayloadBytes - SnapshotChunkHeader.Size);
            var maximumChunkCount =
                (chunk.TotalLength + (long)maxBody - 1L) / maxBody;
            var body = payload.Slice(SnapshotChunkHeader.Size);
            if (chunk.ChunkCount > maximumChunkCount ||
                body.Length == 0 || body.Length > maxBody ||
                chunk.ChunkIndex + 1 < chunk.ChunkCount &&
                body.Length != maxBody ||
                header.PacketSequence != chunk.ChunkIndex + 1)
                return false;
            if (chunk.SnapshotTick <= AcknowledgedSnapshotTick)
            {
                awaitingChunks = true;
                return true;
            }
            if (_snapshotAssemblyReceived != 0 &&
                chunk.SnapshotTick < _snapshotAssemblyChunk.SnapshotTick)
            {
                awaitingChunks = true;
                return true;
            }
            if (_snapshotAssemblyReceived != 0 &&
                chunk.SnapshotTick > _snapshotAssemblyChunk.SnapshotTick)
            {
                _snapshotDiscardThroughTick = Math.Max(
                    _snapshotDiscardThroughTick,
                    _snapshotAssemblyChunk.SnapshotTick);
                ClearSnapshotAssembly();
            }
            if (_snapshotAssemblyReceived == 0)
            {
                _snapshotAssemblyHeader = header;
                _snapshotAssemblyChunk = chunk;
                _snapshotAssemblyDeadline = Stopwatch.GetTimestamp() +
                                            SnapshotAssemblyTimeoutTicks;
            }
            else if (_snapshotAssemblyHeader.SessionEpoch != header.SessionEpoch ||
                     _snapshotAssemblyHeader.SchemaFingerprint != header.SchemaFingerprint ||
                     _snapshotAssemblyHeader.SimulationFingerprint != header.SimulationFingerprint ||
                     _snapshotAssemblyHeader.ContentFingerprint != header.ContentFingerprint ||
                     _snapshotAssemblyHeader.ServerProcessedCommandTick != header.ServerProcessedCommandTick ||
                     _snapshotAssemblyHeader.ServerProcessedCommandSequence != header.ServerProcessedCommandSequence ||
                     _snapshotAssemblyChunk.PayloadKind != chunk.PayloadKind ||
                     _snapshotAssemblyChunk.SnapshotTick != chunk.SnapshotTick ||
                     _snapshotAssemblyChunk.BaselineTick != chunk.BaselineTick ||
                     _snapshotAssemblyChunk.TotalLength != chunk.TotalLength ||
                     _snapshotAssemblyChunk.TotalHash != chunk.TotalHash ||
                     _snapshotAssemblyChunk.ChunkCount != chunk.ChunkCount ||
                     _snapshotAssemblyChunk.ResyncCorrelationId !=
                     chunk.ResyncCorrelationId)
            {
                return false;
            }

            var index = checked((int)chunk.ChunkIndex);
            var retained = _snapshotChunks[index];
            if (retained != null)
            {
                if (retained.Length != body.Length ||
                    !retained.Span.SequenceEqual(body.Span))
                    return false;
                awaitingChunks = true;
                return true;
            }
            if ((long)_snapshotAssemblyBytes + body.Length >
                chunk.TotalLength)
                return false;
            _snapshotChunks[index] = packet.RetainSlice(
                PacketHeader.Size + SnapshotChunkHeader.Size, body.Length);
            _snapshotAssemblyBytes += body.Length;
            _snapshotAssemblyReceived++;
            if (_snapshotAssemblyReceived != chunk.ChunkCount)
            {
                awaitingChunks = true;
                return true;
            }

            var assemblyChunk = _snapshotAssemblyChunk;
            var assemblyCount = checked((int)assemblyChunk.ChunkCount);
            NetworkBufferLease assembled = null;
            try
            {
                assembled = _bufferPool.Rent(_snapshotAssemblyBytes);
                var offset = 0;
                for (var slotIndex = 0; slotIndex < assemblyCount; slotIndex++)
                {
                    var slot = _snapshotChunks[slotIndex];
                    if (slot == null)
                        return false;
                    slot.Span.CopyTo(assembled.WritableSpan.Slice(offset));
                    offset += slot.Length;
                }
                if (offset != _snapshotAssemblyBytes)
                    return false;
                ClearSnapshotAssembly();
                assemblyChunk.ChunkIndex = 0;
                completedChunk = assemblyChunk;
                completedBody = assembled;
                assembled = null;
                return true;
            }
            finally
            {
                assembled?.Dispose();
            }
        }

        private void InspectSnapshotAssemblyTimeout(long timestamp)
        {
            if (_snapshotAssemblyReceived == 0 ||
                timestamp < _snapshotAssemblyDeadline)
                return;
            var snapshotTick = _snapshotAssemblyChunk.SnapshotTick;
            _snapshotDiscardThroughTick = Math.Max(
                _snapshotDiscardThroughTick, snapshotTick);
            ClearSnapshotAssembly();
            RequestResync(snapshotTick,
                NetworkRecoveryReason.SnapshotRejected,
                NetworkResyncSource.ClientSnapshotAssemblyTimeout);
        }

        private void ClearSnapshotAssembly()
        {
            var count = checked((int)_snapshotAssemblyChunk.ChunkCount);
            for (var index = 0; index < count; index++)
            {
                _snapshotChunks[index]?.Dispose();
                _snapshotChunks[index] = null;
            }
            _snapshotAssemblyHeader = default;
            _snapshotAssemblyChunk = default;
            _snapshotAssemblyReceived = 0;
            _snapshotAssemblyBytes = 0;
            _snapshotAssemblyDeadline = 0;
        }

        private bool ApplySnapshot(StagedNetworkSnapshot staged, PacketHeader header,
            int entities, int records, SnapshotPayloadKind payloadKind,
            uint resyncCorrelationId)
        {
            var started = Stopwatch.GetTimestamp();
            SnapshotApplyResult result;
            try
            {
                result = _replicator.Apply(in staged);
            }
            catch (Exception)
            {
                RequestRecovery(NetworkRecoveryPhase.RecreateReplicaWorld,
                    NetworkRecoveryReason.SnapshotApplyFailed, staged.ServerTick);
                staged.Snapshot.Dispose();
                staged.Dispose();
                return false;
            }
            _session.Trace(NetworkPhase.SnapshotApply, NetworkTraceKind.Point, DiagnosticResult(result), NetworkPacketKind.SnapshotChunk, staged.ServerTick, 0, staged.Snapshot.ByteLength, History.Count, History.Bytes, unchecked((int)(staged.ServerTick - AcknowledgedSnapshotTick)), ElapsedNanoseconds(started), entities, records, resyncCorrelationId: resyncCorrelationId, snapshotResult: result, sequence: header.PacketSequence, acknowledgedSnapshotTick: AcknowledgedSnapshotTick, oldestHistoryTick: History.OldestTick, newestHistoryTick: History.NewestTick);
            if (result != SnapshotApplyResult.Success)
            {
                RequestRecovery(NetworkRecoveryPhase.RecreateReplicaWorld,
                    NetworkRecoveryReason.SnapshotApplyFailed, staged.ServerTick);
                staged.Snapshot.Dispose();
                staged.Dispose();
                return false;
            }
            AcknowledgedSnapshotTick = staged.ServerTick;
            ServerProcessedCommandTick = header.ServerProcessedCommandTick;
            ServerProcessedCommandSequence = header.ServerProcessedCommandSequence;
            _session.ReportSnapshot(staged.Snapshot, History);
            var acknowledged = Send(PacketKind.Ack, _session.Epoch,
                PacketHeader.NoneTick, AcknowledgedSnapshotTick,
                ReadOnlySpan<byte>.Empty,
                resyncCorrelationId: payloadKind == SnapshotPayloadKind.Keyframe
                    ? _resyncCorrelationId
                    : 0);
            if (payloadKind == SnapshotPayloadKind.Keyframe)
            {
                RequestRecovery(NetworkRecoveryPhase.None,
                    NetworkRecoveryReason.None, staged.ServerTick);
                if (acknowledged)
                    _resyncCorrelationId = 0;
            }
            staged.Dispose();
            return true;
        }

        private void RequestResync(uint serverTick, NetworkRecoveryReason reason,
            NetworkResyncSource source = NetworkResyncSource.None,
            uint correlationId = 0)
        {
            RequestRecovery(NetworkRecoveryPhase.AwaitingKeyframe, reason, serverTick);
            if (correlationId == 0)
                correlationId = _resyncCorrelationId == 0
                    ? _packetSequence
                    : _resyncCorrelationId;
            if (correlationId == 0 || correlationId == uint.MaxValue)
            {
                RequestDisconnect(NetworkRecoveryReason.ProtocolIncompatible);
                return;
            }
            _resyncCorrelationId = correlationId;
            Span<byte> payload = stackalloc byte[ResyncRequestPayload.Size];
            if (!new ResyncRequestPayload(correlationId).TryWrite(payload))
            {
                RequestDisconnect(NetworkRecoveryReason.ProtocolIncompatible);
                return;
            }
            Send(PacketKind.ResyncRequest, _session.Epoch, serverTick,
                AcknowledgedSnapshotTick, payload,
                source == NetworkResyncSource.ClientIncomingResyncEcho
                    ? NetworkResyncReason.None
                    : DiagnosticResyncReason(reason),
                source == NetworkResyncSource.None
                    ? DiagnosticResyncSource(reason)
                    : source,
                correlationId);
        }

        private void RequestDisconnect(NetworkRecoveryReason reason)
        {
            RequestRecovery(NetworkRecoveryPhase.DisconnectRequired, reason, ServerTick);
            CompletePendingTransactions(NetworkTransactionStatus.SessionLost);
            Send(PacketKind.Disconnect, _session.Epoch, ServerTick,
                AcknowledgedSnapshotTick, ReadOnlySpan<byte>.Empty);
            _session.Close();
        }

        private void RequestRecovery(NetworkRecoveryPhase phase,
            NetworkRecoveryReason reason, uint requestedAtTick)
        {
            if (phase != NetworkRecoveryPhase.None)
                ClearSnapshotAssembly();
            if (phase == NetworkRecoveryPhase.None)
                _recoveryTransition = new NetworkRecoveryTransition(phase,
                    NetworkRecoveryReason.None, requestedAtTick);
            else
                _recoveryTransition = new NetworkRecoveryTransition(phase, reason,
                    requestedAtTick);
            _hasRecoveryTransition = true;
        }

        private bool Send(PacketKind kind, uint epoch, uint serverTick,
            uint acknowledgedTick, ReadOnlySpan<byte> payload,
            NetworkResyncReason resyncReason = NetworkResyncReason.None,
            NetworkResyncSource resyncSource = NetworkResyncSource.None,
            uint resyncCorrelationId = 0)
        {
            var started = Stopwatch.GetTimestamp();
            var sequence = _packetSequence;
            var header = Header(kind, sequence, serverTick);
            header.SessionEpoch = epoch;
            header.AcknowledgedSnapshotTick = acknowledgedTick;
            NetworkBufferLease packet = null;
            var encoded = sequence != uint.MaxValue &&
                NetworkPacket.TryEncode(_bufferPool, header, payload,
                    out packet);
            var packetBytes = packet?.Length ?? 0;
            var sent = encoded && _transport.TrySend(packet);
            if (sent)
                _packetSequence = sequence + 1;
            _session.Trace(NetworkPhase.Send, NetworkTraceKind.Point, sent ? NetworkResultCategory.Success : NetworkResultCategory.Transport, DiagnosticKind(kind), serverTick, 0, packetBytes, History.Count, History.Bytes, unchecked((int)(serverTick - AcknowledgedSnapshotTick)), ElapsedNanoseconds(started), resyncReason: resyncReason, resyncSource: resyncSource, resyncCorrelationId: resyncCorrelationId, sequence: sequence, acknowledgedSnapshotTick: AcknowledgedSnapshotTick, oldestHistoryTick: History.OldestTick, newestHistoryTick: History.NewestTick);
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
        private static NetworkResyncReason DiagnosticResyncReason(
            NetworkRecoveryReason reason) => reason switch
        {
            NetworkRecoveryReason.PredictionHistoryUnavailable => NetworkResyncReason.PredictionHistoryUnavailable,
            NetworkRecoveryReason.SnapshotRejected => NetworkResyncReason.SnapshotRejected,
            NetworkRecoveryReason.SnapshotApplyFailed => NetworkResyncReason.SnapshotApplyFailed,
            NetworkRecoveryReason.ProtocolIncompatible => NetworkResyncReason.ProtocolIncompatible,
            _ => NetworkResyncReason.None,
        };
        private static NetworkResyncSource DiagnosticResyncSource(
            NetworkRecoveryReason reason) => reason switch
        {
            NetworkRecoveryReason.PredictionHistoryUnavailable => NetworkResyncSource.ClientPrediction,
            NetworkRecoveryReason.SnapshotRejected => NetworkResyncSource.ClientSnapshotValidation,
            _ => NetworkResyncSource.None,
        };
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
}
