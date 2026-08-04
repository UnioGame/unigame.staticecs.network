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
        private uint _packetSequence = 1;

        /// <summary>Creates an isolated client pipeline.</summary>
        public NetworkClient(INetworkTransport transport, NetworkSchema<TWorld> schema, ScopeId scope = default, INetworkObserver observer = null)
        {
            _transport = transport ?? throw new ArgumentNullException(nameof(transport));
            _schema = schema ?? throw new ArgumentNullException(nameof(schema));
            _replicator = new NetworkReplicator<TWorld>(schema, scope);
            _session = new NetworkSession<TWorld>(transport.Connection, NetworkRole.Client, schema, observer);
        }

        /// <summary>Gets the per-connection session.</summary>
        public NetworkSession<TWorld> Session => _session;
        /// <summary>Gets bounded successfully applied snapshot history.</summary>
        public NetworkHistory<NetworkSnapshot> History => _replicator.History;
        /// <summary>Gets the latest acknowledged authoritative tick.</summary>
        public uint AcknowledgedSnapshotTick { get; private set; }
        /// <summary>Gets the latest server-acknowledged command sequence.</summary>
        public uint AcknowledgedCommandSequence { get; private set; }
        /// <summary>Gets whether malformed or rejected input requested resynchronization.</summary>
        public bool ResyncRequested { get; private set; }

        /// <summary>Sends the protocol-two Hello packet.</summary>
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
                if (!NetworkPacket.TryDecode(packet, out var header, out var payload) || header.SchemaFingerprint != _schema.Fingerprint || _session.ValidatePacket(in header) != PacketValidationResult.Success) { _session.Trace(NetworkPhase.Decode, NetworkTraceKind.Point, NetworkResultCategory.Protocol, NetworkPacketKind.None, AcknowledgedSnapshotTick, 0, packet?.Length ?? 0, History.Count, History.Bytes, 0, ElapsedNanoseconds(started)); RequestResync(AcknowledgedSnapshotTick); continue; }
                StagedNetworkSnapshot staged = null;
                var entities = 0;
                var records = 0;
                var decodedBytes = packet.Length;
                var decodeResult = NetworkResultCategory.Success;
                if (header.Kind == PacketKind.Ready) DecodeReady(header, payload);
                else if (header.Kind == PacketKind.FullSnapshot) decodeResult = DiagnosticResult(TryStageSnapshot(header, payload, out staged, out entities, out records, out decodedBytes));
                else if (header.Kind == PacketKind.ResyncRequest) ResyncRequested = true;
                else if (header.Kind == PacketKind.Disconnect) _session.Close();
                AcknowledgedCommandSequence = Math.Max(AcknowledgedCommandSequence, header.AcknowledgedCommandSequence);
                _session.Trace(NetworkPhase.Decode, NetworkTraceKind.Point, decodeResult, DiagnosticKind(header.Kind), header.ServerTick, header.TargetTick, decodedBytes, History.Count, History.Bytes, unchecked((int)(header.ServerTick - AcknowledgedSnapshotTick)), ElapsedNanoseconds(started), entities, records);
                if (header.Kind == PacketKind.FullSnapshot)
                {
                    if (staged == null) RequestResync(header.ServerTick);
                    else ApplySnapshot(staged, entities, records);
                }
            }
        }

        /// <summary>Serializes and sends one established client command packet.</summary>
        public NetworkCommandResult SendCommand<TCommand>(in TCommand command, uint targetTick) where TCommand : struct, IEvent, INetworkCommand
        {
            var result = _session.CreateCommand(in command, targetTick, out var envelope);
            if (result != NetworkCommandResult.Queued) return result;
            var payload = new byte[9 + envelope.ExactPayload.Length];
            Hashing.Write32(payload, 0, envelope.Sequence);
            Hashing.Write32(payload, 4, envelope.TypeId.Value);
            payload[8] = envelope.Version;
            envelope.ExactPayload.CopyTo(payload, 9);
            if (!Send(PacketKind.CommandBatch, _session.Epoch, 0, PacketHeader.NoneTick, targetTick, payload)) return NetworkCommandResult.Malformed;
            return result;
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

        private void ApplySnapshot(StagedNetworkSnapshot staged, int entities, int records)
        {
            var started = Stopwatch.GetTimestamp();
            var result = _replicator.Apply(staged);
            _session.Trace(NetworkPhase.SnapshotApply, NetworkTraceKind.Point, DiagnosticResult(result), NetworkPacketKind.FullSnapshot, staged.ServerTick, 0, staged.Snapshot.ByteLength, History.Count, History.Bytes, unchecked((int)(staged.ServerTick - AcknowledgedSnapshotTick)), ElapsedNanoseconds(started), entities, records);
            if (result != SnapshotApplyResult.Success) { RequestResync(staged.ServerTick); return; }
            AcknowledgedSnapshotTick = staged.ServerTick;
            ResyncRequested = false;
            Send(PacketKind.Ack, _session.Epoch, PacketHeader.NoneTick, AcknowledgedSnapshotTick, PacketHeader.NoneTick, ReadOnlySpan<byte>.Empty);
        }

        private void RequestResync(uint serverTick)
        {
            ResyncRequested = true;
            Send(PacketKind.ResyncRequest, _session.Epoch, serverTick, AcknowledgedSnapshotTick, PacketHeader.NoneTick, ReadOnlySpan<byte>.Empty);
        }

        private bool Send(PacketKind kind, uint epoch, uint serverTick, uint acknowledgedTick, ReadOnlySpan<byte> payload)
            => Send(kind, epoch, serverTick, acknowledgedTick, PacketHeader.NoneTick, payload);

        private bool Send(PacketKind kind, uint epoch, uint serverTick, uint acknowledgedTick, uint targetTick, ReadOnlySpan<byte> payload)
        {
            var started = Stopwatch.GetTimestamp();
            var header = Header(kind, _packetSequence++, serverTick, targetTick);
            header.SessionEpoch = epoch;
            header.AcknowledgedSnapshotTick = acknowledgedTick;
            header.AcknowledgedCommandSequence = AcknowledgedCommandSequence;
            var sent = NetworkPacket.TryEncode(header, payload, out var packet) && _transport.TrySend(packet);
            _session.Trace(NetworkPhase.Send, NetworkTraceKind.Point, sent ? NetworkResultCategory.Success : NetworkResultCategory.Transport, DiagnosticKind(kind), serverTick, targetTick, packet?.Length ?? 0, History.Count, History.Bytes, unchecked((int)(serverTick - AcknowledgedSnapshotTick)), ElapsedNanoseconds(started));
            return sent;
        }

        private PacketHeader Header(PacketKind kind, uint sequence, uint serverTick, uint targetTick) => new PacketHeader
        {
            Kind = kind, Flags = PacketFlags.ReliableOrdered, Compression = NetworkCompression.None,
            SessionEpoch = _session.Epoch, PacketSequence = sequence, ServerTick = serverTick, TargetTick = targetTick,
            AcknowledgedSnapshotTick = AcknowledgedSnapshotTick, AcknowledgedCommandSequence = 0,
            SchemaFingerprint = _schema.Fingerprint
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

        /// <summary>Gets the latest authoritative tick completed by this server.</summary>
        public uint ServerTick { get; private set; }

        /// <summary>Creates a multi-connection authoritative server pipeline.</summary>
        public NetworkServer(NetworkSchema<TWorld> schema, NetworkScopeSelector<TWorld> scopeSelector, int historyTicks = 64, long historyBytes = 32 * 1024 * 1024, INetworkObserver observer = null)
        {
            _schema = schema ?? throw new ArgumentNullException(nameof(schema));
            if (scopeSelector == null) throw new ArgumentNullException(nameof(scopeSelector));
            _coordinator = new NetworkServerCoordinator<TWorld>(historyTicks, historyBytes);
            _replicator = new NetworkReplicator<TWorld>(schema, scopeSelector);
            _observer = observer;
        }

        /// <summary>Adds one transport-owned connection with server-assigned identity and scope.</summary>
        public NetworkSession<TWorld> AddConnection(INetworkTransport transport, uint peerId, uint epoch, ScopeId scope, INetworkObserver observer = null)
        {
            if (transport == null) throw new ArgumentNullException(nameof(transport));
            for (var i = 0; i < _peers.Count; i++) if (_peers[i].Transport.Connection == transport.Connection) throw new InvalidOperationException("Connection already exists.");
            var session = new NetworkSession<TWorld>(transport.Connection, NetworkRole.Server, _schema, observer);
            _peers.Add(new Peer(transport, session, peerId, epoch, scope));
            return session;
        }

        /// <summary>Closes and removes one connection while preserving scope-shared history.</summary>
        public bool RemoveConnection(ConnectionId connection)
        {
            for (var i = 0; i < _peers.Count; i++)
            {
                if (_peers[i].Transport.Connection != connection) continue;
                _peers[i].Session.Close(); _peers.RemoveAt(i); _coordinator.Remove(connection); return true;
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
                DecodePending(peer);
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
                SendSnapshot(peer, capture);
            }
            ServerTick = serverTick;
        }

        /// <summary>Finds one immutable authoritative capture by scope and tick.</summary>
        public bool TryGetCapture(ScopeId scope, uint serverTick, out NetworkSnapshot snapshot)
            => _coordinator.TryGetCapture(scope, serverTick, out snapshot);

        private void DecodePending(Peer peer)
        {
            while (peer.PendingPackets.Count > 0)
            {
                var packet = peer.PendingPackets.Dequeue();
                var started = Stopwatch.GetTimestamp();
                if (!NetworkPacket.TryDecode(packet, out var header, out var payload) || header.Kind != PacketKind.Hello && header.SchemaFingerprint != _schema.Fingerprint || peer.Session.ValidatePacket(in header) != PacketValidationResult.Success) { peer.Session.Trace(NetworkPhase.Decode, NetworkTraceKind.Point, NetworkResultCategory.Protocol, NetworkPacketKind.None, ServerTick, 0, packet?.Length ?? 0, 0, 0, unchecked((int)(ServerTick - peer.AcknowledgedSnapshotTick)), ElapsedNanoseconds(started), activeConnections: ActiveConnectionCount, activePeers: ActivePeerCount); continue; }
                if (header.Kind == PacketKind.Hello) Admit(peer, header.SchemaFingerprint);
                else if (header.Kind == PacketKind.CommandBatch) DecodeCommand(peer, header, payload, checked(ServerTick + 1));
                else if (header.Kind == PacketKind.Ack) peer.AcknowledgedSnapshotTick = header.AcknowledgedSnapshotTick;
                else if (header.Kind == PacketKind.ResyncRequest) peer.ResyncRequested = true;
                else if (header.Kind == PacketKind.Disconnect) peer.Session.Close();
                peer.Session.Trace(NetworkPhase.Decode, NetworkTraceKind.Point, NetworkResultCategory.Success, DiagnosticKind(header.Kind), ServerTick, header.TargetTick, packet.Length, 0, 0, unchecked((int)(ServerTick - peer.AcknowledgedSnapshotTick)), ElapsedNanoseconds(started), activeConnections: ActiveConnectionCount, activePeers: ActivePeerCount);
            }
        }

        private void Admit(Peer peer, SchemaFingerprint remoteFingerprint)
        {
            if (peer.Session.Admit(remoteFingerprint, peer.PeerId, peer.Epoch, peer.Scope) != NetworkAdmissionResult.Accepted) { Send(peer, PacketKind.Disconnect, 0, PacketHeader.NoneTick, ReadOnlySpan<byte>.Empty); return; }
            _coordinator.Add(peer.Session);
            var payload = new byte[12];
            Hashing.Write32(payload, 0, peer.PeerId);
            Hashing.Write64(payload, 4, peer.Scope.Value);
            Send(peer, PacketKind.Ready, 0, PacketHeader.NoneTick, payload);
        }

        private void DecodeCommand(Peer peer, PacketHeader header, ReadOnlyMemory<byte> payload, uint serverTick)
        {
            if (payload.Length < 9) { Send(peer, PacketKind.ResyncRequest, serverTick, PacketHeader.NoneTick, ReadOnlySpan<byte>.Empty); return; }
            var bytes = payload.Span;
            var commandSequence = Hashing.Read32(bytes, 0);
            var idValue = Hashing.Read32(bytes, 4);
            if (idValue == 0) { Send(peer, PacketKind.ResyncRequest, serverTick, PacketHeader.NoneTick, ReadOnlySpan<byte>.Empty); return; }
            var envelope = new NetworkCommandEnvelope(peer.Transport.Connection, peer.PeerId, peer.Epoch, commandSequence, header.TargetTick, new NetworkTypeId(idValue), bytes[8], payload.Slice(9).ToArray());
            var result = _coordinator.Queue(envelope, serverTick);
            if (result != NetworkCommandResult.Queued) Send(peer, PacketKind.ResyncRequest, serverTick, PacketHeader.NoneTick, ReadOnlySpan<byte>.Empty);
            else peer.AcknowledgedCommandSequence = commandSequence;
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
                TargetTick = PacketHeader.NoneTick, AcknowledgedSnapshotTick = acknowledgedTick,
                AcknowledgedCommandSequence = peer.AcknowledgedCommandSequence, SchemaFingerprint = _schema.Fingerprint
            };
            var sent = NetworkPacket.TryEncode(header, payload, out var packet) && peer.Transport.TrySend(packet);
            peer.Session.Trace(NetworkPhase.Send, NetworkTraceKind.Point, sent ? NetworkResultCategory.Success : NetworkResultCategory.Transport, DiagnosticKind(kind), serverTick, PacketHeader.NoneTick, packet?.Length ?? 0, _coordinator.HistoryCount(peer.Scope), _coordinator.HistoryByteCount(peer.Scope), unchecked((int)(serverTick - peer.AcknowledgedSnapshotTick)), ElapsedNanoseconds(started), activeConnections: ActiveConnectionCount, activePeers: ActivePeerCount);
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
            internal uint AcknowledgedCommandSequence;
            internal bool ResyncRequested;
            internal readonly Queue<byte[]> PendingPackets = new Queue<byte[]>();
        }
    }
}
