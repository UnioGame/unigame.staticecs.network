using System;
using FFS.Libraries.StaticEcs;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Owns deterministic control negotiation and replication collaborators for one stepped transport.</summary>
    public sealed partial class Session<TWorld> : IDisposable where TWorld : struct, IWorldType
    {
        private readonly SessionConfig _config;
        private readonly Schema _schema;
        private readonly ITransport _transport;
        private readonly ISteppedTransport _steppedTransport;
        private SequenceDomains _sequences;
        private ReplicaScope<TWorld> _scope;
        private Replicator<TWorld> _replicator;
        private SessionStage _stage;
        private SessionState _state;
        private SessionError _error;
        private ConnectResult? _result;
        private DisconnectReason? _reason;
        private TypeId _serverSchema;
        private HelloPayload _serverHello;
        private ConnectResult _admissionResult;
        private uint _epoch;
        private uint _peerId;
        private ushort _tickRate;
        private ulong _lastStep;
        private bool _hasStep;
        private bool _wasEstablished;
        private bool _rejectionSent;
        private bool _requestedIntent;
        private bool _requestedSent;
        private bool _requestedReceived;
        private bool _disposed;

        /// <summary>Creates a control-only session and takes transport ownership only after complete validation.</summary>
        public Session(SessionConfig config, Schema schema, ITransport transport)
            : this(config, schema, transport, null)
        {
        }

        /// <summary>Creates a session with an optional caller-owned observer and takes transport ownership after validation.</summary>
        public Session(SessionConfig config, Schema schema, ITransport transport, ISessionObserver observer)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            if (schema == null) throw new ArgumentNullException(nameof(schema));
            if (transport == null) throw new ArgumentNullException(nameof(transport));
            schema.EnsureWorld<TWorld>();
            if (transport is not ISteppedTransport stepped)
                throw new ArgumentException("Session transports must expose deterministic steps.", nameof(transport));
            if (transport.State != TransportState.Connected || transport.Error != TransportError.None)
                throw new InvalidOperationException("Session transport must be connected and error-free.");
            if (World<TWorld>.Status != WorldStatus.Initialized)
                throw new InvalidOperationException($"World `{typeof(TWorld).FullName}` must be initialized.");
            if (!World<TWorld>.IsTagTypeRegistered<ReplicatedTag>())
                throw new InvalidOperationException("ReplicatedTag must be registered before Session construction.");

            ReplicaScope<TWorld> scope = null;
            Replicator<TWorld> replicator = null;
            TickHistory history = null;
            try
            {
                if (config.Role == SessionRole.Server)
                {
                    _dispatcher = new CommandDispatcher<TWorld>(schema);
                    scope = new ReplicaScope<TWorld>(ScopeRole.Authority, config.Chunks);
                    if (!scope.ValidateCurrent())
                        throw new InvalidOperationException("Server authority topology is not currently valid.");
                    replicator = new Replicator<TWorld>(schema, scope);
                }

                history = new TickHistory();

                _config = config;
                _schema = schema;
                _transport = transport;
                _steppedTransport = stepped;
                _observer = observer;
                _scope = scope;
                _replicator = replicator;
                _history = history;
                scope = null;
                replicator = null;
                history = null;
                _state = SessionState.Handshaking;
                _error = SessionError.None;
                _stage = config.Role == SessionRole.Client
                    ? SessionStage.SendClientHello
                    : SessionStage.AwaitClientHello;
                if (config.Role == SessionRole.Server)
                {
                    _epoch = config.Epoch;
                    _peerId = config.PeerId;
                    _tickRate = config.MinTickRate;
                    _serverSchema = schema.Hash;
                }
            }
            finally
            {
                replicator?.Dispose();
                scope?.Dispose();
                history?.Dispose();
            }
        }

        /// <summary>Gets the configured endpoint direction.</summary>
        public SessionRole Role => _config.Role;
        /// <summary>Gets the public session lifecycle.</summary>
        public SessionState State => _state;
        /// <summary>Gets the local terminal failure classification.</summary>
        public SessionError Error => _error;
        /// <summary>Gets the published admission result, or null while admission is unresolved.</summary>
        public ConnectResult? Result => _result;
        /// <summary>Gets the terminal wire-compatible reason when one exists.</summary>
        public DisconnectReason? Reason => _reason;
        /// <summary>Gets the configured or negotiated non-zero epoch.</summary>
        public uint Epoch => _epoch;
        /// <summary>Gets the configured or negotiated trusted peer identifier.</summary>
        public uint PeerId => _peerId;
        /// <summary>Gets the configured or negotiated exact tick rate.</summary>
        public ushort TickRate => _tickRate;

        internal bool HasScope => _scope != null;
        internal bool HasReplicator => _replicator != null;

        /// <summary>Advances one finite deterministic transport step.</summary>
        public StepResult Step(ulong stepIndex)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(Session<TWorld>));
            var started = ObserveBegin(SessionEventKind.Step, eventStep: stepIndex);
            var success = false;
            try
            {
                var result = StepCore(stepIndex);
                success = true;
                return result;
            }
            finally
            {
                _statsSteps++;
                ObserveEnd(SessionEventKind.Step, started, success, eventStep: stepIndex);
            }
        }

        private StepResult StepCore(ulong stepIndex)
        {
            if (_state == SessionState.Closed || _state == SessionState.Faulted) return StepResult.None;
            if (_hasStep && stepIndex <= _lastStep) throw new ArgumentOutOfRangeException(nameof(stepIndex));
            _hasStep = true;
            _lastStep = stepIndex;
            var initialState = _state;
            var result = StepResult.None;

            _steppedTransport.BeginStep(stepIndex);
            MapTransportTerminal();
            if (IsTerminal) return Finish(result, initialState);
            if (_wasEstablished && !ValidateScope()) return Finish(result, initialState);

            PacketLease received = default;
            var receiveStarted = ObserveBegin(SessionEventKind.Receive);
            var receiveSuccess = false;
            var receiveChannel = default(Channel);
            var receiveBytes = 0;
            try
            {
                try
                {
                    receiveSuccess = _transport.TryReceive(out receiveChannel, out received);
                    if (receiveSuccess) receiveBytes = received.Length;
                }
                finally
                {
                    ObserveEnd(SessionEventKind.Receive, receiveStarted, receiveSuccess, channel: receiveChannel,
                        wireBytes: receiveBytes, code: receiveSuccess ? (ushort)1 : (ushort)0);
                }
                if (receiveSuccess)
                {
                    _statsReceivedPackets++;
                    _statsReceivedBytes += (ulong)receiveBytes;
                    result |= StepResult.Received;
                    BeginDecodeObservation(receiveChannel);
                    try { ProcessReceived(receiveChannel, in received); }
                    finally { EndDecodeFailureIfOpen(); }
                }
            }
            finally
            {
                if (received.IsValid) received.Dispose();
            }

            if (!IsTerminal && _state == SessionState.Established && TrySendTransfer(ref result))
            {
            }
            else if (!IsTerminal && TryGetPending(out var pending))
            {
                if (!_sequences.TryNextReliableTransmit(out var sequence))
                {
                    Fault(SessionError.Sequence, DisconnectReason.SequenceExhausted);
                }
                else
                {
                    PacketLease packet = default;
                    try
                    {
                        var encodeStarted = ObserveBegin(SessionEventKind.Encode, packet: pending.Kind,
                            channel: Channel.ReliableOrdered, sequence: sequence);
                        var encoded = false;
                        try { encoded = SessionProtocol.TryEncode(in pending, sequence, out packet); }
                        finally
                        {
                            ObserveEnd(SessionEventKind.Encode, encodeStarted, encoded, packet: pending.Kind,
                                channel: Channel.ReliableOrdered, sequence: sequence,
                                wireBytes: encoded ? packet.Length : 0);
                        }
                        if (!encoded)
                        {
                            Fault(SessionError.Protocol, DisconnectReason.ProtocolViolation);
                        }
                        else if (ObservedSend(Channel.ReliableOrdered, ref packet, pending.Kind, sequence,
                                     _controlSendAttempts++ != 0))
                        {
                            result |= StepResult.Sent;
                            _sequences.CommitReliableTransmit(sequence);
                            _controlSendAttempts = 0;
                            OnSendSucceeded();
                        }
                    }
                    finally
                    {
                        if (packet.IsValid) packet.Dispose();
                    }
                }
            }

            if (!IsTerminal) MapTransportTerminal();
            return Finish(result, initialState);
        }

        /// <summary>Requests an orderly local close without synthesizing handshake traffic.</summary>
        public void Close()
        {
            var initialState = _state;
            if (_state == SessionState.Handshaking)
            {
                CloseTerminal(DisconnectReason.Requested);
                return;
            }
            if (_state != SessionState.Established) return;
            CancelTransferIntent();
            _state = SessionState.Closing;
            _stage = SessionStage.RequestedClose;
            _requestedIntent = true;
            if (_state != initialState) ObservePoint(SessionEventKind.State);
        }

        /// <summary>Immediately releases collaborators and disposes the exclusively owned transport.</summary>
        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            ReleaseCollaborators();
            _state = SessionState.Disposed;
            ObservePoint(SessionEventKind.State);
            _transport.Dispose();
        }

        private bool IsTerminal => _state == SessionState.Closed || _state == SessionState.Faulted;

        private StepResult Finish(StepResult result, SessionState initialState) =>
            _state != initialState ? result | StepResult.StateChanged : result;

        private void ProcessReceived(Channel channel, in PacketLease packet)
        {
            if (_state == SessionState.Established && TryProcessTransfer(channel, in packet)) return;
            if (channel != Channel.ReliableOrdered)
            {
                Fault(SessionError.Protocol, DisconnectReason.ProtocolViolation);
                return;
            }

            var read = SessionProtocol.ReadHeader(in packet, _config.MaxWireBytes, _config.MaxDecodedBytes, out var header);
            if (read == HeaderReadResult.Limits)
            {
                Fault(SessionError.Limits, DisconnectReason.LimitsExceeded);
                return;
            }
            if (read != HeaderReadResult.Success || !SessionProtocol.HasCommonControlFields(in header) ||
                !_sequences.IsNextReliableReceive(header.PacketSequence))
            {
                Fault(SessionError.Protocol, DisconnectReason.ProtocolViolation);
                return;
            }

            StagedPayload staged = null;
            try
            {
                if (!PacketFraming.TryDecode(in packet, SessionProtocol.ControlTransform, out var decodedHeader, out staged) ||
                    !HeaderEquals(in header, in decodedHeader))
                {
                    Fault(SessionError.Protocol, DisconnectReason.ProtocolViolation);
                    return;
                }
                RecordDecoded(in decodedHeader);

                var accepted = _config.Role == SessionRole.Client
                    ? ProcessClientPacket(in header, staged)
                    : ProcessServerPacket(in header, staged);
                if (accepted) _sequences.CommitReliableReceive(header.PacketSequence);
            }
            finally
            {
                staged?.Dispose();
            }
        }

        private bool ProcessClientPacket(in PacketHeader header, StagedPayload staged)
        {
            switch (_stage)
            {
                case SessionStage.AwaitServerHello:
                    return ProcessServerHello(in header, staged);
                case SessionStage.AwaitHelloAck:
                    return ProcessHelloAck(in header, staged);
                case SessionStage.Established:
                case SessionStage.RequestedClose:
                    return ProcessDisconnect(in header, staged);
                default:
                    Fault(SessionError.Protocol, DisconnectReason.ProtocolViolation);
                    return false;
            }
        }

        private bool ProcessServerPacket(in PacketHeader header, StagedPayload staged)
        {
            switch (_stage)
            {
                case SessionStage.AwaitClientHello:
                    return ProcessClientHello(in header, staged);
                case SessionStage.AwaitFinalAck:
                    return ProcessFinalAck(in header, staged);
                case SessionStage.Established:
                case SessionStage.RequestedClose:
                    return ProcessDisconnect(in header, staged);
                default:
                    Fault(SessionError.Protocol, DisconnectReason.ProtocolViolation);
                    return false;
            }
        }

        private bool ProcessClientHello(in PacketHeader header, StagedPayload staged)
        {
            if (header.Kind != PacketKind.Hello || header.SessionEpoch != 0 || staged.Kind != PacketKind.Hello)
                return ProtocolFailure();
            var hello = staged.Hello;
            if (hello.Nonce == 0 || hello.MinTickRate == 0 || hello.MaxTickRate == 0 || hello.MinTickRate > hello.MaxTickRate)
                return ProtocolFailure();

            var acceptedLength = checked((uint)(20 + _config.Chunks.Length * 8));
            if (header.SchemaHash != _schema.Hash) _admissionResult = ConnectResult.SchemaMismatch;
            else if (_config.MinTickRate < hello.MinTickRate || _config.MinTickRate > hello.MaxTickRate)
                _admissionResult = ConnectResult.TickRateUnsupported;
            else if (hello.Capabilities != 0 || hello.MaxWireBytes < 24 || hello.MaxDecodedBytes < 24 ||
                     hello.MaxWireBytes < acceptedLength || hello.MaxDecodedBytes < acceptedLength)
                _admissionResult = ConnectResult.LimitsRejected;
            else _admissionResult = ConnectResult.Accepted;
            _stage = SessionStage.SendServerHello;
            return true;
        }

        private bool ProcessServerHello(in PacketHeader header, StagedPayload staged)
        {
            if (header.Kind != PacketKind.Hello || header.SessionEpoch != 0 || staged.Kind != PacketKind.Hello)
                return ProtocolFailure();
            var hello = staged.Hello;
            if (hello.Nonce == 0 || hello.MinTickRate == 0 || hello.MinTickRate != hello.MaxTickRate)
                return ProtocolFailure();
            if (hello.Capabilities != 0 || hello.MaxWireBytes < 24 || hello.MaxDecodedBytes < 24)
            {
                Fault(SessionError.Limits, DisconnectReason.LimitsExceeded);
                return false;
            }

            _serverSchema = header.SchemaHash;
            _serverHello = hello;
            _stage = SessionStage.AwaitHelloAck;
            return true;
        }

        private bool ProcessHelloAck(in PacketHeader header, StagedPayload staged)
        {
            if (header.Kind != PacketKind.HelloAck || staged.Kind != PacketKind.HelloAck || header.SchemaHash != _serverSchema)
                return ProtocolFailure();
            var ack = staged.HelloAck;
            var chunks = staged.Chunks;
            if (ack.ServerNonce != _serverHello.Nonce) return ProtocolFailure();

            if (ack.Result == ConnectResult.Accepted)
            {
                if (header.SessionEpoch == 0 || ack.TickRate == 0 || ack.PeerId == 0 ||
                    ack.TickRate != _serverHello.MinTickRate || ack.TickRate < _config.MinTickRate ||
                    ack.TickRate > _config.MaxTickRate)
                    return ProtocolFailure();
                if (_serverSchema != _schema.Hash)
                {
                    Fault(SessionError.Schema, DisconnectReason.SchemaMismatch, ConnectResult.SchemaMismatch);
                    return false;
                }
                if (!IsCanonicalMap(chunks))
                {
                    Fault(SessionError.Protocol, DisconnectReason.ProtocolViolation, ConnectResult.ChunkMapRejected);
                    return false;
                }
                if (!TryCreateReplicaScope(chunks))
                {
                    Fault(SessionError.Topology, null, ConnectResult.ChunkMapRejected);
                    return false;
                }

                _epoch = header.SessionEpoch;
                _peerId = ack.PeerId;
                _tickRate = ack.TickRate;
                _stage = SessionStage.SendFinalAck;
                return true;
            }

            if (header.SessionEpoch != 0 || ack.TickRate != 0 || ack.PeerId != 0 || !chunks.IsEmpty)
                return ProtocolFailure();
            var schemaMismatch = _serverSchema != _schema.Hash;
            var tickMismatch = _serverHello.MinTickRate < _config.MinTickRate ||
                               _serverHello.MinTickRate > _config.MaxTickRate;
            var coherent = schemaMismatch
                ? ack.Result == ConnectResult.SchemaMismatch
                : tickMismatch
                    ? ack.Result == ConnectResult.TickRateUnsupported
                    : ack.Result == ConnectResult.LimitsRejected;
            if (!coherent) return ProtocolFailure();
            _result = ack.Result;
            _state = SessionState.Closed;
            _error = SessionError.None;
            _reason = null;
            ReleaseCollaborators();
            ObservePoint(SessionEventKind.State);
            return true;
        }

        private bool ProcessFinalAck(in PacketHeader header, StagedPayload staged)
        {
            if (header.Kind != PacketKind.Ack || staged.Kind != PacketKind.Ack) return ProtocolFailure();
            if (header.SchemaHash != _schema.Hash)
            {
                Fault(SessionError.Schema, DisconnectReason.SchemaMismatch);
                return false;
            }
            if (header.SessionEpoch != _config.Epoch)
            {
                Fault(SessionError.Epoch, DisconnectReason.UnexpectedEpoch);
                return false;
            }
            if (!ValidateScope()) return false;
            Establish();
            return true;
        }

        private bool ProcessDisconnect(in PacketHeader header, StagedPayload staged)
        {
            if (header.Kind != PacketKind.Disconnect || staged.Kind != PacketKind.Disconnect)
                return ProtocolFailure();
            if (header.SchemaHash != _schema.Hash)
            {
                Fault(SessionError.Schema, DisconnectReason.SchemaMismatch);
                return false;
            }
            if (header.SessionEpoch != _epoch)
            {
                Fault(SessionError.Epoch, DisconnectReason.UnexpectedEpoch);
                return false;
            }

            var reason = staged.Disconnect.Reason;
            if (reason == DisconnectReason.Requested)
            {
                CancelTransferIntent();
                _requestedReceived = true;
                if (_requestedSent)
                {
                    CloseTerminal(DisconnectReason.Requested);
                }
                else
                {
                    if (_state != SessionState.Closing)
                    {
                        _state = SessionState.Closing;
                        ObservePoint(SessionEventKind.State);
                    }
                    _stage = SessionStage.RequestedClose;
                    _requestedIntent = true;
                }
                return true;
            }
            if (reason == DisconnectReason.ServerShutdown)
            {
                if (_config.Role != SessionRole.Client) return ProtocolFailure();
                CloseTerminal(DisconnectReason.ServerShutdown);
                return true;
            }

            var error = reason switch
            {
                DisconnectReason.ProtocolViolation => SessionError.Protocol,
                DisconnectReason.SchemaMismatch => SessionError.Schema,
                DisconnectReason.LimitsExceeded => SessionError.Limits,
                DisconnectReason.UnexpectedEpoch => SessionError.Epoch,
                DisconnectReason.SequenceExhausted => SessionError.Sequence,
                DisconnectReason.TransportClosed => SessionError.Transport,
                _ => SessionError.Protocol
            };
            Fault(error, reason);
            return true;
        }

        private bool TryGetPending(out PendingControl pending)
        {
            pending = default;
            switch (_stage)
            {
                case SessionStage.SendClientHello:
                    var clientHello = new HelloPayload
                    {
                        Nonce = _config.Nonce,
                        MinTickRate = _config.MinTickRate,
                        MaxTickRate = _config.MaxTickRate,
                        MaxWireBytes = _config.MaxWireBytes,
                        MaxDecodedBytes = _config.MaxDecodedBytes,
                        Capabilities = 0
                    };
                    pending = PendingControl.HelloPacket(_schema.Hash, in clientHello);
                    return true;
                case SessionStage.SendServerHello:
                    var serverHello = new HelloPayload
                    {
                        Nonce = _config.Nonce,
                        MinTickRate = _config.MinTickRate,
                        MaxTickRate = _config.MaxTickRate,
                        MaxWireBytes = _config.MaxWireBytes,
                        MaxDecodedBytes = _config.MaxDecodedBytes,
                        Capabilities = 0
                    };
                    pending = PendingControl.HelloPacket(_schema.Hash, in serverHello);
                    return true;
                case SessionStage.SendHelloAck:
                    if (_admissionResult == ConnectResult.Accepted && !ValidateScope()) return false;
                    var accepted = _admissionResult == ConnectResult.Accepted;
                    pending = PendingControl.HelloAckPacket(
                        accepted ? _config.Epoch : 0,
                        _schema.Hash,
                        _admissionResult,
                        accepted ? _config.MinTickRate : (ushort)0,
                        accepted ? _config.PeerId : 0,
                        _config.Nonce,
                        accepted ? _config.Chunks : Array.Empty<ChunkMapping>());
                    return true;
                case SessionStage.SendFinalAck:
                    if (!ValidateScope(ConnectResult.ChunkMapRejected)) return false;
                    pending = PendingControl.AckPacket(_epoch, _serverSchema);
                    return true;
                case SessionStage.RequestedClose:
                    if (!_requestedIntent) return false;
                    pending = PendingControl.DisconnectPacket(_epoch, _schema.Hash, DisconnectReason.Requested);
                    return true;
                default:
                    return false;
            }
        }

        private void OnSendSucceeded()
        {
            switch (_stage)
            {
                case SessionStage.SendClientHello:
                    _stage = SessionStage.AwaitServerHello;
                    break;
                case SessionStage.SendServerHello:
                    _stage = SessionStage.SendHelloAck;
                    break;
                case SessionStage.SendHelloAck:
                    if (_admissionResult == ConnectResult.Accepted)
                    {
                        _stage = SessionStage.AwaitFinalAck;
                    }
                    else
                    {
                        _stage = SessionStage.RejectionBarrier;
                        _state = SessionState.Closing;
                        ObservePoint(SessionEventKind.State);
                        _rejectionSent = true;
                    }
                    break;
                case SessionStage.SendFinalAck:
                    Establish();
                    break;
                case SessionStage.RequestedClose:
                    _requestedIntent = false;
                    _requestedSent = true;
                    if (_requestedReceived) CloseTerminal(DisconnectReason.Requested);
                    break;
            }
        }

        private void Establish()
        {
            _stage = SessionStage.Established;
            _state = SessionState.Established;
            _error = SessionError.None;
            _result = ConnectResult.Accepted;
            _reason = null;
            _wasEstablished = true;
            OnTransferEstablished();
            ObservePoint(SessionEventKind.State);
        }

        private bool TryCreateReplicaScope(ReadOnlySpan<ChunkMapping> chunks)
        {
            ReplicaScope<TWorld> scope = null;
            Replicator<TWorld> replicator = null;
            CommandOutbox<TWorld> outbox = null;
            try
            {
                scope = new ReplicaScope<TWorld>(ScopeRole.Replica, chunks);
                if (!scope.ValidateCurrent()) return false;
                replicator = new Replicator<TWorld>(_schema, scope);
                outbox = new CommandOutbox<TWorld>(_schema);
                _scope = scope;
                _replicator = replicator;
                _outbox = outbox;
                scope = null;
                replicator = null;
                outbox = null;
                return true;
            }
            finally
            {
                replicator?.Dispose();
                scope?.Dispose();
                outbox?.Dispose();
            }
        }

        private bool ValidateScope(ConnectResult? admissionFailure = null)
        {
            if (_scope != null && _replicator != null &&
                World<TWorld>.Status == WorldStatus.Initialized &&
                World<TWorld>.IsTagTypeRegistered<ReplicatedTag>() &&
                _scope.ValidateCurrent())
                return true;
            Fault(SessionError.Topology, null, admissionFailure);
            return false;
        }

        internal void SetReliableTransmitHighWaterForTests(uint highWater)
        {
            if (_hasStep || _state != SessionState.Handshaking || _sequences.ReliableTransmit != 0 ||
                _sequences.ReliableReceive != 0)
                throw new InvalidOperationException("The transmit high-water test seam requires a fresh session.");
            _sequences.ReliableTransmit = highWater;
        }

        private void MapTransportTerminal()
        {
            var terminal = SessionProtocol.MapTransport(_transport.State, _transport.Error);
            switch (terminal)
            {
                case TransportTerminalKind.None:
                    return;
                case TransportTerminalKind.Limits:
                    Fault(SessionError.Limits, DisconnectReason.LimitsExceeded);
                    return;
                case TransportTerminalKind.Protocol:
                    Fault(SessionError.Protocol, DisconnectReason.ProtocolViolation);
                    return;
                case TransportTerminalKind.RemoteClosed:
                    if (_rejectionSent)
                    {
                        _result = _admissionResult;
                        _state = SessionState.Closed;
                        _error = SessionError.None;
                        _reason = null;
                        ReleaseCollaborators();
                        ObservePoint(SessionEventKind.State);
                    }
                    else if (_requestedSent || _requestedReceived)
                    {
                        CloseTerminal(DisconnectReason.Requested);
                    }
                    else
                    {
                        Fault(SessionError.Transport, DisconnectReason.TransportClosed);
                    }
                    return;
                case TransportTerminalKind.Disposed:
                case TransportTerminalKind.Transport:
                    Fault(SessionError.Transport, DisconnectReason.TransportClosed);
                    return;
            }
        }

        private bool ProtocolFailure()
        {
            Fault(SessionError.Protocol, DisconnectReason.ProtocolViolation);
            return false;
        }

        private void Fault(SessionError error, DisconnectReason? reason, ConnectResult? result = null)
        {
            if (result.HasValue) _result = result;
            _state = SessionState.Faulted;
            _error = error;
            _reason = reason;
            ReleaseCollaborators();
            _statsFaults++;
            ObservePoint(SessionEventKind.Fault, false, reason: reason.HasValue ? (ushort)reason.Value : (ushort)0);
            ObservePoint(SessionEventKind.State);
        }

        private void CloseTerminal(DisconnectReason reason)
        {
            _state = SessionState.Closed;
            _error = SessionError.None;
            _reason = reason;
            ReleaseCollaborators();
            ObservePoint(SessionEventKind.State, reason: (ushort)reason);
        }

        private void ReleaseCollaborators()
        {
            ReleaseTransferCollaborators();
            var replicator = _replicator;
            var scope = _scope;
            _replicator = null;
            _scope = null;
            replicator?.Dispose();
            scope?.Dispose();
        }

        private static bool IsCanonicalMap(ReadOnlySpan<ChunkMapping> chunks)
        {
            if (chunks.IsEmpty || chunks.Length > ProtocolLimits.MaxChunkMappings) return false;
            uint previous = 0;
            for (var i = 0; i < chunks.Length; i++)
            {
                var mapping = chunks[i];
                if (mapping.Role != 1 || i > 0 && mapping.Chunk <= previous) return false;
                previous = mapping.Chunk;
            }
            return true;
        }

        private static bool HeaderEquals(in PacketHeader left, in PacketHeader right) =>
            left.Kind == right.Kind && left.Flags == right.Flags && left.TransformId == right.TransformId &&
            left.SessionEpoch == right.SessionEpoch && left.PacketSequence == right.PacketSequence &&
            left.ServerTick == right.ServerTick && left.BaselineTick == right.BaselineTick &&
            left.AcknowledgedSnapshotTick == right.AcknowledgedSnapshotTick &&
            left.WirePayloadLength == right.WirePayloadLength &&
            left.DecodedPayloadLength == right.DecodedPayloadLength && left.SchemaHash == right.SchemaHash &&
            left.PayloadHash == right.PayloadHash &&
            left.AcknowledgedCommandSequence == right.AcknowledgedCommandSequence;
    }
}
