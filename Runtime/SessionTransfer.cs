using System;
using FFS.Libraries.StaticEcs;

namespace UniGame.StaticEcs.Network
{
    public sealed partial class Session<TWorld> where TWorld : struct, IWorldType
    {
        private enum ReliableIntentKind : byte
        {
            None,
            CommandBatch,
            ResyncRequest,
            Acknowledgement
        }

        private CommandDispatcher<TWorld> _dispatcher;
        private CommandOutbox<TWorld> _outbox;
        private TickHistory _history;
        private PacketLease _pendingSnapshot;
        private PacketLease _reliablePayload;
        private ReliableIntentKind _reliableIntent;
        private ResyncReason _queuedResyncReason;
        private uint _queuedResyncTick;
        private uint _reliableSequence;
        private uint _reliableSnapshotAck;
        private uint _reliableCommandAck;
        private uint _reliableCommandThrough;
        private uint _pendingSnapshotTick;
        private uint _pendingSnapshotSequence;
        private uint _pendingSnapshotCommandAck;
        private uint _lastCapturedTick;
        private uint _lastAcceptedSnapshotTick;
        private uint _lastSnapshotSentTick;
        private uint _lastProcessedCommand;
        private uint _peerSnapshotAck;
        private uint _lastCarriedSnapshotAck;
        private uint _lastCarriedCommandAck;
        private bool _hasCapturedTick;
        private bool _hasAcceptedSnapshot;
        private bool _hasSnapshotSent;
        private bool _hasPeerSnapshotAck;
        private bool _hasCarriedSnapshotAck;
        private bool _pendingSnapshotFrozen;
        private bool _queuedResync;
        private bool _needsSnapshot;

        /// <summary>Queues a typed command while this client session is established.</summary>
        public EnqueueResult Enqueue<T>(in T command, uint clientTick) where T : unmanaged
        {
            if (_disposed) throw new ObjectDisposedException(nameof(Session<TWorld>));
            var result = _config.Role == SessionRole.Client && _state == SessionState.Established && _outbox != null
                ? _outbox.Enqueue(in command, clientTick)
                : EnqueueResult.Unavailable;
            if (result == EnqueueResult.Queued) _statsCommandsQueued++;
            return result;
        }

        /// <summary>Captures and schedules one complete authoritative snapshot.</summary>
        public CaptureResult Capture(uint serverTick)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(Session<TWorld>));
            if (_config.Role == SessionRole.Client) return CaptureResult.WrongRole;
            if (_state != SessionState.Established || _replicator == null || _scope == null || _history == null)
                return CaptureResult.ScopeInvalid;
            if (serverTick == PacketHeader.NoneTick || _hasCapturedTick && serverTick <= _lastCapturedTick)
                throw new ArgumentOutOfRangeException(nameof(serverTick));

            PacketLease captured = default;
            PacketLease historyCopy = default;
            TickRecord record = null;
            var captureStarted = ObserveBegin(SessionEventKind.Capture, tick: serverTick);
            var captureResult = CaptureResult.ScopeInvalid;
            var committed = false;
            var capturedHash = 0UL;
            var capturedBytes = 0;
            var captureEnded = 0L;
            try
            {
                captureResult = _replicator.Capture(out captured);
                captureEnded = ObservationTimestamp();
                if (captureResult != CaptureResult.Success) return captureResult;
                capturedBytes = captured.Length;
                historyCopy = captured.Copy();
                var hash = Hashing.XxHash64(captured.Span);
                capturedHash = hash;
                PacketLease received = default;
                PacketLease postApply = default;
                record = new TickRecord(serverTick, ref historyCopy, ref received, ref postApply,
                    hash, 0, 0, 0, 0, Array.Empty<PacketLease>());
                _history.Add(record);
                record = null;

                DisposeOwned(ref _pendingSnapshot);
                _pendingSnapshot = PacketLease.Transfer(ref captured);
                _pendingSnapshotTick = serverTick;
                _pendingSnapshotFrozen = false;
                _snapshotSendAttempts = 0;
                _lastCapturedTick = serverTick;
                _hasCapturedTick = true;
                _needsSnapshot = false;
                committed = true;
                _statsSnapshotsCaptured++;
                return captureResult;
            }
            finally
            {
                ObserveEnd(SessionEventKind.Capture, captureStarted, committed, tick: serverTick,
                    decodedBytes: capturedBytes, code: (ushort)captureResult,
                    hash: committed ? capturedHash : 0, ended: captureEnded);
                record?.Dispose();
                DisposeOwned(ref historyCopy);
                DisposeOwned(ref captured);
            }
        }

        /// <summary>Gets whether the established server requires a fresh full snapshot.</summary>
        public bool NeedsSnapshot => _config.Role == SessionRole.Server && _needsSnapshot;

        internal TickHistory History => _history;

        internal static bool TryMapApplyFailure(
            ApplyResult result,
            out SessionError error,
            out DisconnectReason? reason,
            out ResyncReason resync)
        {
            error = SessionError.None;
            reason = null;
            resync = default;
            switch (result)
            {
                case ApplyResult.SchemaMismatch:
                    error = SessionError.Schema;
                    reason = DisconnectReason.SchemaMismatch;
                    return false;
                case ApplyResult.WrongPayload:
                    error = SessionError.Protocol;
                    reason = DisconnectReason.ProtocolViolation;
                    return false;
                case ApplyResult.WrongRole:
                case ApplyResult.ScopeInvalid:
                    error = SessionError.Topology;
                    return false;
                case ApplyResult.EntityConflict:
                    resync = ResyncReason.LocalStateConflict;
                    return true;
                case ApplyResult.InvalidEntity:
                case ApplyResult.MissingTarget:
                case ApplyResult.LimitExceeded:
                    resync = ResyncReason.SnapshotRejected;
                    return true;
                default:
                    throw new ArgumentOutOfRangeException(nameof(result));
            }
        }

        private void OnTransferEstablished()
        {
            if (_config.Role == SessionRole.Server) _needsSnapshot = true;
        }

        private bool TryProcessTransfer(Channel channel, in PacketLease packet)
        {
            var read = SessionProtocol.ReadHeader(in packet, _config.MaxWireBytes, _config.MaxDecodedBytes, out var header);
            if (read == HeaderReadResult.Limits)
            {
                Fault(SessionError.Limits, DisconnectReason.LimitsExceeded);
                return true;
            }
            if (read != HeaderReadResult.Success) return false;
            if (header.Kind == PacketKind.Disconnect) return false;
            if (header.Kind != PacketKind.CommandBatch && header.Kind != PacketKind.FullSnapshot &&
                header.Kind != PacketKind.Ack && header.Kind != PacketKind.ResyncRequest)
                return false;
            if (header.SessionEpoch != _epoch)
            {
                Fault(SessionError.Epoch, DisconnectReason.UnexpectedEpoch);
                return true;
            }

            var reliable = channel == Channel.ReliableOrdered && header.Flags == PacketFlags.ReliableOrdered;
            var unreliable = channel == Channel.UnreliableSequenced && header.Flags == (PacketFlags)0;
            var validSequence = reliable
                ? _sequences.IsNextReliableReceive(header.PacketSequence)
                : unreliable && _sequences.IsNewerUnreliableReceive(header.PacketSequence);
            if (!validSequence || header.TransformId != 0 || header.BaselineTick != PacketHeader.NoneTick)
            {
                Fault(SessionError.Protocol, DisconnectReason.ProtocolViolation);
                return true;
            }

            StagedPayload staged = null;
            try
            {
                var schema = header.Kind == PacketKind.CommandBatch || header.Kind == PacketKind.FullSnapshot ? _schema : null;
                if (!PacketFraming.TryDecode(in packet, SessionProtocol.ControlTransform, schema, out var decoded, out staged) ||
                    !HeaderEquals(in header, in decoded))
                {
                    var schemaMismatch = (header.Kind == PacketKind.CommandBatch || header.Kind == PacketKind.FullSnapshot) &&
                                         header.SchemaHash != _schema.Hash;
                    Fault(schemaMismatch ? SessionError.Schema : SessionError.Protocol,
                        schemaMismatch ? DisconnectReason.SchemaMismatch : DisconnectReason.ProtocolViolation);
                    return true;
                }
                RecordDecoded(in decoded);

                bool consumed;
                try
                {
                    consumed = _config.Role == SessionRole.Server
                        ? ProcessServerTransfer(in header, staged)
                        : ProcessClientTransfer(in header, staged);
                }
                catch
                {
                    Fault(SessionError.Topology, null);
                    throw;
                }
                if (consumed)
                {
                    if (reliable) _sequences.CommitReliableReceive(header.PacketSequence);
                    else _sequences.CommitUnreliableReceive(header.PacketSequence);
                }
                return true;
            }
            catch
            {
                if (!IsTerminal) Fault(SessionError.Topology, null);
                throw;
            }
            finally
            {
                staged?.Dispose();
            }
        }

        private bool ProcessServerTransfer(in PacketHeader header, StagedPayload staged)
        {
            if (header.Kind == PacketKind.CommandBatch)
            {
                if (!HasServerInboundFields(in header, true) || staged.Commands.IsEmpty) return ProtocolFailure();
                var commands = staged.Commands;
                var next = _lastProcessedCommand == uint.MaxValue ? 0 : _lastProcessedCommand + 1;
                for (var i = 0; i < commands.Length; i++)
                {
                    if (next == 0 || commands[i].Sequence != next) return ProtocolFailure();
                    next++;
                }
                for (var i = 0; i < commands.Length; i++)
                {
                    var dispatchStarted = ObserveBegin(SessionEventKind.Dispatch);
                    var dispatch = DispatchResult.InvalidCommand;
                    var dispatchReturned = false;
                    try
                    {
                        dispatch = _dispatcher.Dispatch(staged, i, _peerId);
                        dispatchReturned = true;
                    }
                    finally
                    {
                        var dispatchSuccess = dispatchReturned &&
                                              (dispatch == DispatchResult.Accepted || dispatch == DispatchResult.Rejected);
                        ObserveEnd(SessionEventKind.Dispatch, dispatchStarted, dispatchSuccess,
                            count: 1, code: (ushort)dispatch);
                    }
                    if (dispatch == DispatchResult.Accepted || dispatch == DispatchResult.Rejected)
                    {
                        if (dispatch == DispatchResult.Accepted) _statsCommandsAccepted++;
                        else _statsCommandsRejected++;
                        _lastProcessedCommand = commands[i].Sequence;
                        continue;
                    }
                    if (dispatch == DispatchResult.SchemaMismatch)
                        return Fail(SessionError.Schema, DisconnectReason.SchemaMismatch);
                    if (dispatch == DispatchResult.WrongPayload || dispatch == DispatchResult.InvalidCommand)
                        return ProtocolFailure();
                    return Fail(SessionError.Topology, null);
                }
                CommitPeerSnapshotAcknowledgement(header.AcknowledgedSnapshotTick);
                return true;
            }
            if (header.Kind == PacketKind.Ack)
            {
                if (!HasServerInboundFields(in header, false)) return ProtocolFailure();
                CommitPeerSnapshotAcknowledgement(header.AcknowledgedSnapshotTick);
                return true;
            }
            if (header.Kind == PacketKind.ResyncRequest)
            {
                if (!HasServerInboundFields(in header, false) ||
                    staged.ResyncRequest.LastAcceptedTick != header.AcknowledgedSnapshotTick)
                    return ProtocolFailure();
                CommitPeerSnapshotAcknowledgement(header.AcknowledgedSnapshotTick);
                DisposeOwned(ref _pendingSnapshot);
                _pendingSnapshotFrozen = false;
                _needsSnapshot = true;
                _statsResyncs++;
                ObservePoint(SessionEventKind.Resync, tick: staged.ResyncRequest.LastAcceptedTick,
                    reason: (ushort)staged.ResyncRequest.Reason);
                return true;
            }
            return ProtocolFailure();
        }

        private bool ProcessClientTransfer(in PacketHeader header, StagedPayload staged)
        {
            if (header.Kind == PacketKind.Ack)
            {
                if (!HasClientInboundFields(in header, false) || !_outbox.Acknowledge(header.AcknowledgedCommandSequence))
                    return ProtocolFailure();
                return true;
            }
            if (header.Kind != PacketKind.FullSnapshot || !HasClientInboundFields(in header, true))
                return ProtocolFailure();

            PacketLease received = default;
            TickRecord record = null;
            long applyStarted = 0;
            var applyObserved = false;
            var applyEnded = false;
            var apply = ApplyResult.ScopeInvalid;
            var applyCallbackEnded = 0L;
            try
            {
                received = PacketLease.Rent(staged.Payload.Length);
                staged.Payload.Span.CopyTo(received.CapacitySpan);
                received.SetLength(staged.Payload.Length);
                PacketLease generated = default;
                PacketLease postApply = default;
                record = new TickRecord(header.ServerTick, ref generated, ref received, ref postApply,
                    0, header.PayloadHash, header.PayloadHash, 0, 0, Array.Empty<PacketLease>());
                applyStarted = ObserveBegin(SessionEventKind.Apply, tick: header.ServerTick,
                    packet: PacketKind.FullSnapshot, sequence: header.PacketSequence);
                applyObserved = true;
                apply = _replicator.Apply(staged);
                applyCallbackEnded = ObservationTimestamp();
                if (apply != ApplyResult.Success)
                {
                    ObserveEnd(SessionEventKind.Apply, applyStarted, false, tick: header.ServerTick,
                        packet: PacketKind.FullSnapshot, sequence: header.PacketSequence,
                        decodedBytes: staged.Payload.Length, code: (ushort)apply, ended: applyCallbackEnded);
                    applyEnded = true;
                    record.Dispose();
                    record = null;
                    if (TryMapApplyFailure(apply, out var error, out var reason, out var resync))
                    {
                        QueueResync(resync);
                        return true;
                    }
                    return Fail(error, reason);
                }
                if (!_outbox.Acknowledge(header.AcknowledgedCommandSequence))
                    return ProtocolFailure();
                _history.Add(record);
                record = null;
                _lastAcceptedSnapshotTick = header.ServerTick;
                _hasAcceptedSnapshot = true;
                _statsSnapshotsApplied++;
                ObserveEnd(SessionEventKind.Apply, applyStarted, true, tick: header.ServerTick,
                    packet: PacketKind.FullSnapshot, sequence: header.PacketSequence,
                    decodedBytes: staged.Payload.Length, code: (ushort)apply, hash: header.PayloadHash,
                    ended: applyCallbackEnded);
                applyEnded = true;
                if (_reliableIntent != ReliableIntentKind.ResyncRequest) _queuedResync = false;
                return true;
            }
            finally
            {
                if (applyObserved && !applyEnded)
                    ObserveEnd(SessionEventKind.Apply, applyStarted, false, tick: header.ServerTick,
                        packet: PacketKind.FullSnapshot, sequence: header.PacketSequence,
                        decodedBytes: staged.Payload.Length, code: (ushort)apply);
                record?.Dispose();
                DisposeOwned(ref received);
            }
        }

        private bool HasServerInboundFields(in PacketHeader header, bool schemaRequired)
        {
            if (header.Flags != PacketFlags.ReliableOrdered || header.ServerTick != PacketHeader.NoneTick ||
                header.AcknowledgedCommandSequence != 0 || header.SchemaHash != (schemaRequired ? _schema.Hash : default) ||
                !ValidSnapshotAcknowledgement(header.AcknowledgedSnapshotTick))
                return false;
            return true;
        }

        private bool HasClientInboundFields(in PacketHeader header, bool snapshot)
        {
            if (snapshot)
            {
                if (header.Flags != (PacketFlags)0 || header.ServerTick == PacketHeader.NoneTick ||
                    header.AcknowledgedSnapshotTick != PacketHeader.NoneTick || header.SchemaHash != _schema.Hash)
                    return false;
                if (_hasAcceptedSnapshot && header.ServerTick <= _lastAcceptedSnapshotTick) return false;
            }
            else if (header.Flags != PacketFlags.ReliableOrdered || header.ServerTick != PacketHeader.NoneTick ||
                     header.AcknowledgedSnapshotTick != PacketHeader.NoneTick || header.SchemaHash != default)
                return false;
            return header.AcknowledgedCommandSequence <= _outbox.LastSentSequence;
        }

        private bool ValidSnapshotAcknowledgement(uint tick)
        {
            if (tick == PacketHeader.NoneTick) return true;
            return _hasSnapshotSent && tick <= _lastSnapshotSentTick;
        }

        private void CommitPeerSnapshotAcknowledgement(uint tick)
        {
            if (tick == PacketHeader.NoneTick) return;
            if (!_hasPeerSnapshotAck || tick > _peerSnapshotAck)
            {
                _peerSnapshotAck = tick;
                _hasPeerSnapshotAck = true;
            }
        }

        private void QueueResync(ResyncReason reason)
        {
            if (_queuedResync || _reliableIntent == ReliableIntentKind.ResyncRequest) return;
            _queuedResync = true;
            _queuedResyncReason = reason;
            _queuedResyncTick = _hasAcceptedSnapshot ? _lastAcceptedSnapshotTick : PacketHeader.NoneTick;
            _statsResyncs++;
            ObservePoint(SessionEventKind.Resync, tick: _queuedResyncTick, reason: (ushort)reason);
        }

        private bool TrySendTransfer(ref StepResult result)
        {
            if (_config.Role == SessionRole.Server && _pendingSnapshot.IsValid)
                return TrySendSnapshot(ref result);
            if (_reliableIntent == ReliableIntentKind.None && !TryFreezeReliableIntent()) return false;
            return TrySendReliableIntent(ref result);
        }

        private bool TryFreezeReliableIntent()
        {
            var intent = ReliableIntentKind.None;
            if (_config.Role == SessionRole.Client)
            {
                if (_queuedResync) intent = ReliableIntentKind.ResyncRequest;
                else if (_outbox.UnsentCount > 0) intent = ReliableIntentKind.CommandBatch;
                else if (SnapshotAcknowledgementNewerThanCarried()) intent = ReliableIntentKind.Acknowledgement;
            }
            else if (_lastProcessedCommand > _lastCarriedCommandAck)
            {
                intent = ReliableIntentKind.Acknowledgement;
            }
            if (intent == ReliableIntentKind.None) return false;
            if (!_sequences.TryNextReliableTransmit(out _reliableSequence))
            {
                Fault(SessionError.Sequence, DisconnectReason.SequenceExhausted);
                return false;
            }
            _reliableSnapshotAck = _hasAcceptedSnapshot ? _lastAcceptedSnapshotTick : PacketHeader.NoneTick;
            _reliableCommandAck = _lastProcessedCommand;
            if (_config.Role == SessionRole.Client)
            {
                if (intent == ReliableIntentKind.ResyncRequest)
                {
                    var lease = PacketLease.Rent(8);
                    try
                    {
                        var value = new ResyncRequestPayload { Reason = _queuedResyncReason, LastAcceptedTick = _queuedResyncTick };
                        if (!PayloadCodec.TryWrite(value, lease.CapacitySpan.Slice(0, 8), out var written) || written != 8)
                            return ProtocolFailure();
                        lease.SetLength(8);
                        _reliablePayload = PacketLease.Transfer(ref lease);
                    }
                    finally
                    {
                        DisposeOwned(ref lease);
                    }
                    _reliableSnapshotAck = _queuedResyncTick;
                    _queuedResync = false;
                    _reliableIntent = ReliableIntentKind.ResyncRequest;
                    return true;
                }
                if (intent == ReliableIntentKind.CommandBatch)
                {
                    if (!_outbox.TryBuild(out _reliablePayload, out _reliableCommandThrough)) return ProtocolFailure();
                    _reliableIntent = ReliableIntentKind.CommandBatch;
                    return true;
                }
                _reliableIntent = ReliableIntentKind.Acknowledgement;
                return true;
            }
            _reliableIntent = ReliableIntentKind.Acknowledgement;
            return true;
        }

        private bool TrySendReliableIntent(ref StepResult result)
        {
            var kind = _reliableIntent == ReliableIntentKind.CommandBatch ? PacketKind.CommandBatch :
                _reliableIntent == ReliableIntentKind.ResyncRequest ? PacketKind.ResyncRequest : PacketKind.Ack;
            var schema = kind == PacketKind.CommandBatch ? _schema.Hash : default;
            var snapshotAck = _config.Role == SessionRole.Client ? _reliableSnapshotAck : PacketHeader.NoneTick;
            var commandAck = _config.Role == SessionRole.Server ? _reliableCommandAck : 0;
            var payload = _reliablePayload.IsValid ? _reliablePayload.Span : ReadOnlySpan<byte>.Empty;
            PacketLease packet = default;
            try
            {
                var encodeStarted = ObserveBegin(SessionEventKind.Encode, packet: kind,
                    channel: Channel.ReliableOrdered, sequence: _reliableSequence);
                var encoded = false;
                try
                {
                    encoded = SessionProtocol.TryEncodeTransfer(kind, Channel.ReliableOrdered, _epoch,
                        _reliableSequence, PacketHeader.NoneTick, schema, snapshotAck, commandAck, payload,
                        kind == PacketKind.CommandBatch ? _schema : null, out packet);
                }
                finally
                {
                    ObserveEnd(SessionEventKind.Encode, encodeStarted, encoded, packet: kind,
                        channel: Channel.ReliableOrdered, sequence: _reliableSequence,
                        wireBytes: encoded ? packet.Length : 0);
                }
                if (!encoded)
                {
                    Fault(SessionError.Protocol, DisconnectReason.ProtocolViolation);
                    return true;
                }
                if (!ObservedSend(Channel.ReliableOrdered, ref packet, kind, _reliableSequence,
                        _reliableSendAttempts++ != 0)) return true;
                result |= StepResult.Sent;
                _sequences.CommitReliableTransmit(_reliableSequence);
                if (_reliableIntent == ReliableIntentKind.CommandBatch) _outbox.MarkSent(_reliableCommandThrough);
                if (_config.Role == SessionRole.Client) CommitCarriedSnapshotAcknowledgement(_reliableSnapshotAck);
                else if (_reliableCommandAck > _lastCarriedCommandAck) _lastCarriedCommandAck = _reliableCommandAck;
                ClearReliableIntent();
                return true;
            }
            finally
            {
                DisposeOwned(ref packet);
            }
        }

        private bool TrySendSnapshot(ref StepResult result)
        {
            if (!_pendingSnapshotFrozen)
            {
                if (!_sequences.TryNextUnreliableTransmit(out _pendingSnapshotSequence))
                {
                    Fault(SessionError.Sequence, DisconnectReason.SequenceExhausted);
                    return true;
                }
                _pendingSnapshotCommandAck = _lastProcessedCommand;
                _pendingSnapshotFrozen = true;
            }
            PacketLease packet = default;
            try
            {
                var encodeStarted = ObserveBegin(SessionEventKind.Encode, tick: _pendingSnapshotTick,
                    packet: PacketKind.FullSnapshot, channel: Channel.UnreliableSequenced,
                    sequence: _pendingSnapshotSequence);
                var encoded = false;
                try
                {
                    encoded = SessionProtocol.TryEncodeTransfer(PacketKind.FullSnapshot,
                        Channel.UnreliableSequenced, _epoch, _pendingSnapshotSequence, _pendingSnapshotTick,
                        _schema.Hash, PacketHeader.NoneTick, _pendingSnapshotCommandAck,
                        _pendingSnapshot.Span, _schema, out packet);
                }
                finally
                {
                    ObserveEnd(SessionEventKind.Encode, encodeStarted, encoded, tick: _pendingSnapshotTick,
                        packet: PacketKind.FullSnapshot, channel: Channel.UnreliableSequenced,
                        sequence: _pendingSnapshotSequence, wireBytes: encoded ? packet.Length : 0);
                }
                if (!encoded)
                {
                    Fault(SessionError.Protocol, DisconnectReason.ProtocolViolation);
                    return true;
                }
                if (!ObservedSend(Channel.UnreliableSequenced, ref packet, PacketKind.FullSnapshot,
                        _pendingSnapshotSequence, _snapshotSendAttempts++ != 0)) return true;
                result |= StepResult.Sent;
                _sequences.CommitUnreliableTransmit(_pendingSnapshotSequence);
                _lastSnapshotSentTick = _pendingSnapshotTick;
                _hasSnapshotSent = true;
                if (_pendingSnapshotCommandAck > _lastCarriedCommandAck)
                    _lastCarriedCommandAck = _pendingSnapshotCommandAck;
                DisposeOwned(ref _pendingSnapshot);
                _pendingSnapshotFrozen = false;
                _snapshotSendAttempts = 0;
                return true;
            }
            finally
            {
                DisposeOwned(ref packet);
            }
        }

        private bool SnapshotAcknowledgementNewerThanCarried()
        {
            if (!_hasAcceptedSnapshot) return false;
            return !_hasCarriedSnapshotAck || _lastAcceptedSnapshotTick > _lastCarriedSnapshotAck;
        }

        private void CommitCarriedSnapshotAcknowledgement(uint tick)
        {
            if (tick == PacketHeader.NoneTick) return;
            if (!_hasCarriedSnapshotAck || tick > _lastCarriedSnapshotAck)
            {
                _lastCarriedSnapshotAck = tick;
                _hasCarriedSnapshotAck = true;
            }
        }

        private void CancelTransferIntent()
        {
            ClearReliableIntent();
            _queuedResync = false;
        }

        private void ClearReliableIntent()
        {
            DisposeOwned(ref _reliablePayload);
            _reliableIntent = ReliableIntentKind.None;
            _reliableSequence = 0;
            _reliableSnapshotAck = PacketHeader.NoneTick;
            _reliableCommandAck = 0;
            _reliableCommandThrough = 0;
            _reliableSendAttempts = 0;
        }

        private void ReleaseTransferCollaborators()
        {
            ClearReliableIntent();
            DisposeOwned(ref _pendingSnapshot);
            _outbox?.Dispose();
            _history?.Dispose();
            _outbox = null;
            _history = null;
            _dispatcher = null;
            _queuedResync = false;
            _needsSnapshot = false;
        }

        private bool Fail(SessionError error, DisconnectReason? reason)
        {
            Fault(error, reason);
            return false;
        }

        private static void DisposeOwned(ref PacketLease lease)
        {
            if (!lease.IsValid) return;
            var owned = lease;
            lease = default;
            owned.Dispose();
        }
    }
}
