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
        private readonly int _commandRedundancy;
        private readonly int _ticksPerSecond;
        private readonly int _predictionLeadTicks;
        private readonly ulong _simulationFingerprint;
        private readonly ulong _contentFingerprint;
        private readonly NetworkBufferLease[] _snapshotChunks =
            new NetworkBufferLease[ProtocolLimits.MaxChunkMappings];
        private uint _packetSequence = 1;
        private uint _commandPacketSequence = 1;
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

        /// <summary>Creates an isolated client pipeline.</summary>
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

        /// <summary>Captures current packet-buffer ownership diagnostics.</summary>
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

        /// <summary>Closes the session and removes all replica-owned entities from the client world.</summary>
        public void Disconnect()
        {
            ClearSnapshotAssembly();
            _snapshotDiscardThroughTick = 0;
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
            _handshakeStarted = false;
            _recoveryTransition = default;
            _hasRecoveryTransition = false;
            _session.ReportSession(0, 0, 0, _packetSequence);
        }

        /// <inheritdoc />
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

        /// <summary>Sends the protocol-five Hello packet.</summary>
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
                        if (PacketHeader.IsProtocolVersionMismatch(packet.Span))
                        {
                            RequestDisconnect(
                                NetworkRecoveryReason.ProtocolIncompatible);
                            return;
                        }
                        RequestResync(AcknowledgedSnapshotTick,
                            NetworkRecoveryReason.SnapshotRejected);
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
                    if (_session.ValidatePacket(in header) != PacketValidationResult.Success)
                    {
                        _session.Trace(NetworkPhase.Decode, NetworkTraceKind.Point,
                            NetworkResultCategory.Protocol, NetworkPacketKind.None,
                            AcknowledgedSnapshotTick, 0, packet.Length, History.Count,
                            History.Bytes, 0, ElapsedNanoseconds(started));
                        RequestResync(AcknowledgedSnapshotTick,
                            NetworkRecoveryReason.SnapshotRejected);
                        continue;
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
                var snapshotKind = default(SnapshotPayloadKind);
                var awaitingSnapshotChunks = false;
                if (header.Kind == PacketKind.Ready) DecodeReady(header, payload);
                else if (header.Kind == PacketKind.SnapshotChunk) decodeResult = DiagnosticResult(TryStageSnapshot(packet, header, payload, out staged, out entities, out records, out decodedBytes, out snapshotKind, out awaitingSnapshotChunks));
                else if (header.Kind == PacketKind.Pong) DecodePong(payload);
                else if (header.Kind == PacketKind.ResyncRequest) RequestResync(header.ServerTick, NetworkRecoveryReason.SnapshotRejected);
                var disconnected = header.Kind == PacketKind.Disconnect;
                _session.Trace(NetworkPhase.Decode, NetworkTraceKind.Point, decodeResult, DiagnosticKind(header.Kind), header.ServerTick, 0, decodedBytes, History.Count, History.Bytes, unchecked((int)(header.ServerTick - AcknowledgedSnapshotTick)), ElapsedNanoseconds(started), entities, records);
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
                            _snapshotDiscardThroughTick = Math.Max(
                                _snapshotDiscardThroughTick,
                                header.ServerTick);
                            RequestResync(header.ServerTick, NetworkRecoveryReason.SnapshotRejected);
                        }
                        else if (!ApplySnapshot(staged, header, entities, records, snapshotKind)) return;
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
            out bool awaitingChunks)
        {
            staged = default;
            entities = 0;
            records = 0;
            decodedBytes = payload.Length;
            payloadKind = default;
            NetworkBufferLease body = null;
            if (!TryAssembleSnapshot(packet, in header, payload, out var chunk,
                    out body, out awaitingChunks))
                return SnapshotApplyResult.Malformed;
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
                        return SnapshotApplyResult.Malformed;
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
                        baseline.Scope != _session.Scope ||
                        !SnapshotDeltaCodec.TryReconstruct(_bufferPool, baseline,
                            body.Span, in chunk, header.SchemaFingerprint,
                            _session.Scope, out canonical, out entities,
                            out records))
                        return SnapshotApplyResult.Malformed;
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
                    snapshot.Dispose();
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
                     _snapshotAssemblyChunk.ChunkCount != chunk.ChunkCount)
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
                NetworkRecoveryReason.SnapshotRejected);
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
            int entities, int records, SnapshotPayloadKind payloadKind)
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
            _session.Trace(NetworkPhase.SnapshotApply, NetworkTraceKind.Point, DiagnosticResult(result), NetworkPacketKind.SnapshotChunk, staged.ServerTick, 0, staged.Snapshot.ByteLength, History.Count, History.Bytes, unchecked((int)(staged.ServerTick - AcknowledgedSnapshotTick)), ElapsedNanoseconds(started), entities, records);
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
            Send(PacketKind.Ack, _session.Epoch, PacketHeader.NoneTick, AcknowledgedSnapshotTick, ReadOnlySpan<byte>.Empty);
            if (payloadKind == SnapshotPayloadKind.Keyframe)
                RequestRecovery(NetworkRecoveryPhase.None,
                    NetworkRecoveryReason.None, staged.ServerTick);
            staged.Dispose();
            return true;
        }

        private void RequestResync(uint serverTick, NetworkRecoveryReason reason)
        {
            RequestRecovery(NetworkRecoveryPhase.AwaitingKeyframe, reason, serverTick);
            Send(PacketKind.ResyncRequest, _session.Epoch, serverTick, AcknowledgedSnapshotTick, ReadOnlySpan<byte>.Empty);
        }

        private void RequestDisconnect(NetworkRecoveryReason reason)
        {
            RequestRecovery(NetworkRecoveryPhase.DisconnectRequired, reason, ServerTick);
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

        private bool Send(PacketKind kind, uint epoch, uint serverTick, uint acknowledgedTick, ReadOnlySpan<byte> payload)
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
            _session.Trace(NetworkPhase.Send, NetworkTraceKind.Point, sent ? NetworkResultCategory.Success : NetworkResultCategory.Transport, DiagnosticKind(kind), serverTick, 0, packetBytes, History.Count, History.Bytes, unchecked((int)(serverTick - AcknowledgedSnapshotTick)), ElapsedNanoseconds(started));
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

        /// <summary>Gets the number of transport connections represented by this endpoint.</summary>
        public int ConnectionCount => _peers.Count;

        /// <summary>Captures current packet-buffer ownership diagnostics.</summary>
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

        /// <summary>Creates a multi-connection authoritative server pipeline.</summary>
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

        /// <inheritdoc />
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
                _captures.Clear();
                for (var i = 0; i < _peers.Count; i++)
                {
                    var peer = _peers[i];
                    if (peer.Session.State != NetworkSessionState.Established) continue;
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

        private bool DecodePacket(Peer peer, NetworkBufferLease packet)
        {
            var started = Stopwatch.GetTimestamp();
            if (!NetworkPacket.TryDecode(packet, out var header, out var payload) ||
                (header.Kind != PacketKind.Hello &&
                 (header.SchemaFingerprint != _schema.Fingerprint ||
                  header.SimulationFingerprint != _simulationFingerprint ||
                  header.ContentFingerprint != _contentFingerprint)) ||
                peer.Session.ValidatePacket(in header) != PacketValidationResult.Success)
            {
                peer.Session.Trace(NetworkPhase.Decode, NetworkTraceKind.Point,
                    NetworkResultCategory.Protocol, NetworkPacketKind.None,
                    ServerTick, 0, packet.Length, 0, 0,
                    unchecked((int)(ServerTick - peer.AcknowledgedSnapshotTick)),
                    ElapsedNanoseconds(started), activeConnections: ActiveConnectionCount,
                    activePeers: ActivePeerCount);
                return false;
            }
            if (header.Kind == PacketKind.Hello && !Admit(peer,
                    header.SchemaFingerprint, header.SimulationFingerprint,
                    header.ContentFingerprint))
                return true;
            if (header.Kind == PacketKind.CommandBatch)
                DecodeCommands(peer, packet, payload, checked(ServerTick + 1));
            else if (header.Kind == PacketKind.Ping)
                Send(peer, PacketKind.Pong, ServerTick, PacketHeader.NoneTick,
                    payload.Span);
            else if (header.Kind == PacketKind.Ack)
                DecodeAcknowledgement(peer, header.AcknowledgedSnapshotTick);
            else if (header.Kind == PacketKind.ResyncRequest)
                peer.ResyncRequested = true;
            else if (header.Kind == PacketKind.Disconnect)
                CleanupPeer(peer);
            peer.Session.Trace(NetworkPhase.Decode, NetworkTraceKind.Point,
                NetworkResultCategory.Success, DiagnosticKind(header.Kind),
                ServerTick, 0, packet.Length, 0, 0,
                unchecked((int)(ServerTick - peer.AcknowledgedSnapshotTick)),
                ElapsedNanoseconds(started), activeConnections: ActiveConnectionCount,
                activePeers: ActivePeerCount);
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

        private void DecodeCommands(Peer peer, NetworkBufferLease packet,
            ReadOnlyMemory<byte> payload, uint serverTick)
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
            var commands = peer.DecodedCommands;
            var decoded = 0;
            for (var i = 0; i < count; i++)
            {
                if (offset > bytes.Length - 17)
                {
                    DisposeCommands(commands, decoded);
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
                    DisposeCommands(commands, decoded);
                    Send(peer, PacketKind.ResyncRequest, serverTick,
                        PacketHeader.NoneTick, ReadOnlySpan<byte>.Empty);
                    return;
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
                Send(peer, PacketKind.ResyncRequest, serverTick,
                    PacketHeader.NoneTick, ReadOnlySpan<byte>.Empty);
                return;
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
                    Send(peer, PacketKind.ResyncRequest, serverTick,
                        PacketHeader.NoneTick, ReadOnlySpan<byte>.Empty);
                    return;
                }
                if (result == NetworkCommandResult.Duplicate)
                {
                    var duplicate = commands[i];
                    duplicate.Dispose();
                }
                commands[i] = default;
            }
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
            peer.ResyncRequested = false;
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
                            ChunkCount = chunkCount
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
                activePeers: ActivePeerCount);
            return sent;
        }

        private bool Send(Peer peer, PacketKind kind, uint serverTick, uint acknowledgedTick, ReadOnlySpan<byte> payload)
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
            peer.Session.Trace(NetworkPhase.Send, NetworkTraceKind.Point, sent ? NetworkResultCategory.Success : NetworkResultCategory.Transport, DiagnosticKind(kind), serverTick, PacketHeader.NoneTick, packetBytes, _coordinator.HistoryCount(peer.Scope), _coordinator.HistoryByteCount(peer.Scope), unchecked((int)(serverTick - peer.AcknowledgedSnapshotTick)), ElapsedNanoseconds(started), activeConnections: ActiveConnectionCount, activePeers: ActivePeerCount);
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
