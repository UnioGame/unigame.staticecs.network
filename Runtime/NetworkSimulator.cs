using System;
using System.Collections.Generic;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Provides an explicit-clock deterministic in-memory network link.</summary>
    public sealed class NetworkSimulator : INetworkSimulatorControl, IDisposable
    {
        private const uint ZeroSeedFallback = 0x6D2B79F5u;

        private readonly ConnectionId _connection;
        private readonly DirectionState _clientToServer = new DirectionState(NetworkSimulationDirection.ClientToServer);
        private readonly DirectionState _serverToClient = new DirectionState(NetworkSimulationDirection.ServerToClient);
        private readonly List<NetworkSimulationDecision> _decisions = new List<NetworkSimulationDecision>();
        private readonly List<NetworkSimulationDecision> _recording = new List<NetworkSimulationDecision>();
        private IReadOnlyList<NetworkSimulationDecision> _replay = Array.Empty<NetworkSimulationDecision>();
        private NetworkSimulationConfig _config;
        private uint _random;
        private int _replayIndex;
        private long _timeMilliseconds;
        private ulong _cycle;
        private ulong _connectionGeneration = 1;
        private long _replayErrors;
        private bool _connected = true;
        private bool _paused;
        private bool _recordingActive;
        private bool _replaying;
        private bool _disposed;

        /// <summary>Creates one connected simulator link and its client/server endpoints.</summary>
        public NetworkSimulator(ConnectionId connection, in NetworkSimulationConfig config)
        {
            _connection = connection;
            ApplyConfig(in config);
            Client = new Endpoint(this, true);
            Server = new Endpoint(this, false);
        }

        /// <summary>Gets the client-side transport endpoint.</summary>
        public readonly INetworkTransport Client;

        /// <summary>Gets the server-side transport endpoint.</summary>
        public readonly INetworkTransport Server;

        /// <inheritdoc />
        public NetworkSimulationConfig CaptureConfig() => _config;

        /// <inheritdoc />
        public void ApplyConfig(in NetworkSimulationConfig config)
        {
            Validate(in config);
            _config = config;
            _random = config.Seed == 0 ? ZeroSeedFallback : config.Seed;
            TrimDecisions();
        }

        /// <inheritdoc />
        public NetworkSimulationStats CaptureStats()
        {
            var stats = new NetworkSimulationStats
            {
                TimeMilliseconds = _timeMilliseconds,
                Cycle = _cycle,
                ConnectionGeneration = _connectionGeneration,
                Connected = _connected,
                Paused = _paused,
                Recording = _recordingActive,
                Replaying = _replaying,
                ReplayErrors = _replayErrors,
                ClientToServer = _clientToServer.Capture(),
                ServerToClient = _serverToClient.Capture()
            };
            return stats;
        }

        /// <inheritdoc />
        public IReadOnlyList<NetworkSimulationDecision> CaptureDecisions() => _decisions.ToArray();

        /// <inheritdoc />
        public void Advance(long elapsedMilliseconds)
        {
            ThrowIfDisposed();
            if (elapsedMilliseconds < 0)
                throw new ArgumentOutOfRangeException(nameof(elapsedMilliseconds));

            _timeMilliseconds = checked(_timeMilliseconds + elapsedMilliseconds);
            _cycle++;
            AddBandwidth(_clientToServer, elapsedMilliseconds);
            AddBandwidth(_serverToClient, elapsedMilliseconds);
            if (!_connected || _paused)
                return;
            Deliver(_clientToServer);
            Deliver(_serverToClient);
        }

        /// <inheritdoc />
        public void Connect()
        {
            ThrowIfDisposed();
            if (_connected)
                return;
            ClearQueues();
            _connected = true;
            _connectionGeneration++;
        }

        /// <inheritdoc />
        public void Disconnect()
        {
            ThrowIfDisposed();
            if (!_connected)
                return;
            _connected = false;
            _connectionGeneration++;
            ClearQueues();
        }

        /// <inheritdoc />
        public void SetPaused(bool paused)
        {
            ThrowIfDisposed();
            _paused = paused;
        }

        /// <inheritdoc />
        public void Reset()
        {
            ThrowIfDisposed();
            ClearQueues();
            _clientToServer.ClearCounters();
            _serverToClient.ClearCounters();
            _decisions.Clear();
            _recording.Clear();
            _replay = Array.Empty<NetworkSimulationDecision>();
            _replayIndex = 0;
            _replayErrors = 0;
            _timeMilliseconds = 0;
            _cycle = 0;
            _paused = false;
            _recordingActive = false;
            _replaying = false;
            _random = _config.Seed == 0 ? ZeroSeedFallback : _config.Seed;
        }

        /// <inheritdoc />
        public void StartRecording()
        {
            ThrowIfDisposed();
            _recording.Clear();
            _recordingActive = true;
        }

        /// <inheritdoc />
        public IReadOnlyList<NetworkSimulationDecision> StopRecording()
        {
            ThrowIfDisposed();
            _recordingActive = false;
            return _recording.ToArray();
        }

        /// <inheritdoc />
        public void StartReplay(IReadOnlyList<NetworkSimulationDecision> decisions)
        {
            ThrowIfDisposed();
            _replay = decisions == null ? throw new ArgumentNullException(nameof(decisions)) : Copy(decisions);
            _replayIndex = 0;
            _replaying = true;
        }

        /// <inheritdoc />
        public void StopReplay()
        {
            ThrowIfDisposed();
            _replay = Array.Empty<NetworkSimulationDecision>();
            _replayIndex = 0;
            _replaying = false;
        }

        /// <inheritdoc />
        public void Dispose()
        {
            if (_disposed)
                return;
            _disposed = true;
            _connected = false;
            ClearQueues();
        }

        private bool Send(NetworkSimulationDirection direction, byte[] packet)
        {
            ThrowIfDisposed();
            if (packet == null)
                return false;

            var state = State(direction);
            var ordinal = ++state.Ordinal;
            if (!_connected)
            {
                Record(Decision(direction, ordinal, packet.Length,
                    NetworkSimulationDecisionKind.Disconnected, _timeMilliseconds, false, false));
                return false;
            }

            if (_replaying)
                return Replay(state, packet, ordinal);

            bool reliableOrdered = PacketHeader.TryRead(packet, out var header) &&
                                   header.Flags == PacketFlags.ReliableOrdered;
            var lost = !reliableOrdered && NextUnit() < _config.LossProbability;
            var duplicated = !reliableOrdered &&
                             NextUnit() < _config.DuplicateProbability;
            var reordered = !reliableOrdered &&
                            NextUnit() < _config.ReorderProbability;
            var jitterUnit = NextUnit();
            var due = DueTime(reliableOrdered ? 0.5f : jitterUnit);
            if (lost)
            {
                state.LostPackets++;
                Record(Decision(direction, ordinal, packet.Length,
                    NetworkSimulationDecisionKind.Lost, due, reordered, false));
                return true;
            }

            if (!TrySchedule(state, packet, ordinal, 0, due, reordered))
            {
                Record(Decision(direction, ordinal, packet.Length,
                    NetworkSimulationDecisionKind.Overflow, due, reordered, false));
                return false;
            }

            var duplicateScheduled = false;
            if (duplicated)
            {
                duplicateScheduled = TrySchedule(state, packet, ordinal, 1, due, reordered);
                if (duplicateScheduled)
                    state.DuplicatePackets++;
            }

            if (reordered)
                state.ReorderedPackets++;
            Record(Decision(direction, ordinal, packet.Length,
                NetworkSimulationDecisionKind.Scheduled, due, reordered, duplicateScheduled));
            return true;
        }

        private bool Replay(DirectionState state, byte[] packet, ulong ordinal)
        {
            if (_replayIndex >= _replay.Count)
                return ReplayMismatch(state.Direction, ordinal, packet.Length);
            var decision = _replay[_replayIndex++];
            if (decision.Direction != state.Direction || decision.Ordinal != ordinal || decision.Bytes != packet.Length)
                return ReplayMismatch(state.Direction, ordinal, packet.Length);

            var replayed = decision;
            replayed.TimeMilliseconds = _timeMilliseconds;
            if (decision.Kind == NetworkSimulationDecisionKind.Lost)
            {
                state.LostPackets++;
                Record(replayed);
                return true;
            }
            if (decision.Kind == NetworkSimulationDecisionKind.Disconnected ||
                decision.Kind == NetworkSimulationDecisionKind.ReplayMismatch)
            {
                Record(replayed);
                return false;
            }
            if (decision.Kind == NetworkSimulationDecisionKind.Overflow)
            {
                state.OverflowPackets++;
                Record(replayed);
                return false;
            }
            var delay = Math.Max(0L, decision.ScheduledMilliseconds - decision.TimeMilliseconds);
            var due = checked(_timeMilliseconds + delay);
            replayed.ScheduledMilliseconds = due;
            if (!TrySchedule(state, packet, ordinal, 0, due, decision.Reordered))
                return ReplayMismatch(state.Direction, ordinal, packet.Length);
            if (decision.Duplicated && !TrySchedule(state, packet, ordinal, 1,
                    due, decision.Reordered))
                return ReplayMismatch(state.Direction, ordinal, packet.Length);
            if (decision.Duplicated)
                state.DuplicatePackets++;
            if (decision.Reordered)
                state.ReorderedPackets++;
            Record(replayed);
            return true;
        }

        private bool ReplayMismatch(NetworkSimulationDirection direction, ulong ordinal, int bytes)
        {
            _replayErrors++;
            Record(Decision(direction, ordinal, bytes,
                NetworkSimulationDecisionKind.ReplayMismatch, _timeMilliseconds, false, false));
            return false;
        }

        private bool TrySchedule(DirectionState state, byte[] packet, ulong ordinal,
            int duplicateIndex, long due, bool reordered)
        {
            if (state.QueuedPackets >= _config.MaxQueuedPackets ||
                packet.Length > _config.MaxQueuedBytes - state.QueuedBytes)
            {
                state.OverflowPackets++;
                return false;
            }

            var copy = new byte[packet.Length];
            packet.CopyTo(copy, 0);
            var scheduled = new ScheduledPacket(copy, due, reordered ? -checked((long)ordinal) : 0,
                ordinal, duplicateIndex);
            state.Scheduled.Add(scheduled);
            state.QueuedPackets++;
            state.QueuedBytes += packet.Length;
            state.ScheduledPackets++;
            return true;
        }

        private bool Receive(NetworkSimulationDirection direction, out byte[] packet)
        {
            ThrowIfDisposed();
            var state = State(direction);
            if (state.Ready.Count == 0)
            {
                packet = null;
                return false;
            }

            packet = state.Ready.Dequeue();
            state.QueuedPackets--;
            state.QueuedBytes -= packet.Length;
            return true;
        }

        private void Deliver(DirectionState state)
        {
            state.Scheduled.Sort(ScheduledPacketComparer.Instance);
            while (state.Scheduled.Count > 0)
            {
                var scheduled = state.Scheduled[0];
                if (scheduled.DueMilliseconds > _timeMilliseconds)
                    return;
                if (_config.BandwidthBytesPerSecond > 0 && state.BandwidthTokens < scheduled.Packet.Length)
                    return;
                state.Scheduled.RemoveAt(0);
                if (_config.BandwidthBytesPerSecond > 0)
                    state.BandwidthTokens -= scheduled.Packet.Length;
                state.Ready.Enqueue(scheduled.Packet);
                state.DeliveredPackets++;
            }
        }

        private void AddBandwidth(DirectionState state, long elapsedMilliseconds)
        {
            if (_config.BandwidthBytesPerSecond == 0)
                return;
            var remaining = _config.MaxQueuedBytes - state.BandwidthTokens;
            if (remaining <= 0)
                return;
            var scaled = (decimal)_config.BandwidthBytesPerSecond * elapsedMilliseconds +
                         state.BandwidthRemainder;
            var capacityScaled = (decimal)remaining * 1000;
            if (scaled >= capacityScaled)
            {
                state.BandwidthTokens = _config.MaxQueuedBytes;
                state.BandwidthRemainder = 0;
                return;
            }
            var bytes = decimal.ToInt64(decimal.Truncate(scaled / 1000));
            state.BandwidthRemainder = decimal.ToInt64(scaled - bytes * 1000);
            state.BandwidthTokens += bytes;
        }

        private long DueTime(float jitterUnit)
        {
            var jitter = 0;
            if (_config.JitterMilliseconds > 0)
            {
                var span = checked(_config.JitterMilliseconds * 2L + 1L);
                jitter = checked((int)Math.Min(span - 1, (long)(jitterUnit * span))) -
                         _config.JitterMilliseconds;
            }
            var delay = Math.Max(0L, (long)_config.LatencyMilliseconds + jitter);
            return checked(_timeMilliseconds + delay);
        }

        private float NextUnit()
        {
            var value = _random;
            value ^= value << 13;
            value ^= value >> 17;
            value ^= value << 5;
            _random = value;
            return (value >> 8) * (1f / 16777216f);
        }

        private void Record(NetworkSimulationDecision decision)
        {
            AddBounded(_decisions, decision);
            if (_recordingActive)
                AddBounded(_recording, decision);
        }

        private void AddBounded(List<NetworkSimulationDecision> values, NetworkSimulationDecision decision)
        {
            if (_config.DecisionCapacity == 0)
                return;
            if (values.Count == _config.DecisionCapacity)
                values.RemoveAt(0);
            values.Add(decision);
        }

        private void TrimDecisions()
        {
            while (_decisions.Count > _config.DecisionCapacity)
                _decisions.RemoveAt(0);
            while (_recording.Count > _config.DecisionCapacity)
                _recording.RemoveAt(0);
        }

        private void ClearQueues()
        {
            _clientToServer.ClearQueues();
            _serverToClient.ClearQueues();
        }

        private DirectionState State(NetworkSimulationDirection direction) =>
            direction == NetworkSimulationDirection.ClientToServer ? _clientToServer : _serverToClient;

        private NetworkSimulationDecision Decision(NetworkSimulationDirection direction,
            ulong ordinal, int bytes, NetworkSimulationDecisionKind kind, long due,
            bool reordered, bool duplicated)
        {
            var decision = new NetworkSimulationDecision
            {
                TimeMilliseconds = _timeMilliseconds,
                Direction = direction,
                Ordinal = ordinal,
                Bytes = bytes,
                Kind = kind,
                ScheduledMilliseconds = due,
                Reordered = reordered,
                Duplicated = duplicated
            };
            return decision;
        }

        private static IReadOnlyList<NetworkSimulationDecision> Copy(
            IReadOnlyList<NetworkSimulationDecision> source)
        {
            var result = new NetworkSimulationDecision[source.Count];
            for (var i = 0; i < source.Count; i++)
                result[i] = source[i];
            return result;
        }

        private static void Validate(in NetworkSimulationConfig config)
        {
            if (config.LatencyMilliseconds < 0 || config.JitterMilliseconds < 0 ||
                config.BandwidthBytesPerSecond < 0 || config.MaxQueuedPackets <= 0 ||
                config.MaxQueuedBytes <= 0 || config.DecisionCapacity < 0)
                throw new ArgumentOutOfRangeException(nameof(config));
            ValidateProbability(config.LossProbability, nameof(config.LossProbability));
            ValidateProbability(config.DuplicateProbability, nameof(config.DuplicateProbability));
            ValidateProbability(config.ReorderProbability, nameof(config.ReorderProbability));
        }

        private static void ValidateProbability(float value, string name)
        {
            if (float.IsNaN(value) || value < 0f || value > 1f)
                throw new ArgumentOutOfRangeException(name);
        }

        private void ThrowIfDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(NetworkSimulator));
        }

        private sealed class Endpoint : INetworkTransport
        {
            private readonly NetworkSimulator _owner;
            private readonly bool _client;
            private bool _disposed;

            internal Endpoint(NetworkSimulator owner, bool client)
            {
                _owner = owner;
                _client = client;
            }

            public ConnectionId Connection => _owner._connection;

            public bool TrySend(byte[] packet)
            {
                if (_disposed)
                    return false;
                var direction = _client
                    ? NetworkSimulationDirection.ClientToServer
                    : NetworkSimulationDirection.ServerToClient;
                return _owner.Send(direction, packet);
            }

            public bool TryReceive(out byte[] packet)
            {
                if (_disposed)
                {
                    packet = null;
                    return false;
                }
                var direction = _client
                    ? NetworkSimulationDirection.ServerToClient
                    : NetworkSimulationDirection.ClientToServer;
                return _owner.Receive(direction, out packet);
            }

            public void Dispose() => _disposed = true;
        }

        private sealed class DirectionState
        {
            internal readonly NetworkSimulationDirection Direction;
            internal readonly List<ScheduledPacket> Scheduled = new List<ScheduledPacket>();
            internal readonly Queue<byte[]> Ready = new Queue<byte[]>();
            internal ulong Ordinal;
            internal int QueuedPackets;
            internal long QueuedBytes;
            internal long ScheduledPackets;
            internal long DeliveredPackets;
            internal long LostPackets;
            internal long OverflowPackets;
            internal long DuplicatePackets;
            internal long ReorderedPackets;
            internal long BandwidthTokens;
            internal long BandwidthRemainder;

            internal DirectionState(NetworkSimulationDirection direction) => Direction = direction;

            internal NetworkSimulationDirectionStats Capture()
            {
                var stats = new NetworkSimulationDirectionStats
                {
                    QueuedPackets = QueuedPackets,
                    QueuedBytes = QueuedBytes,
                    ScheduledPackets = ScheduledPackets,
                    DeliveredPackets = DeliveredPackets,
                    LostPackets = LostPackets,
                    OverflowPackets = OverflowPackets,
                    DuplicatePackets = DuplicatePackets,
                    ReorderedPackets = ReorderedPackets
                };
                return stats;
            }

            internal void ClearQueues()
            {
                Scheduled.Clear();
                Ready.Clear();
                QueuedPackets = 0;
                QueuedBytes = 0;
                BandwidthTokens = 0;
                BandwidthRemainder = 0;
            }

            internal void ClearCounters()
            {
                Ordinal = 0;
                ScheduledPackets = 0;
                DeliveredPackets = 0;
                LostPackets = 0;
                OverflowPackets = 0;
                DuplicatePackets = 0;
                ReorderedPackets = 0;
            }
        }

        private readonly struct ScheduledPacket
        {
            internal readonly byte[] Packet;
            internal readonly long DueMilliseconds;
            internal readonly long ReorderPriority;
            internal readonly ulong Ordinal;
            internal readonly int DuplicateIndex;

            internal ScheduledPacket(byte[] packet, long dueMilliseconds, long reorderPriority,
                ulong ordinal, int duplicateIndex)
            {
                Packet = packet;
                DueMilliseconds = dueMilliseconds;
                ReorderPriority = reorderPriority;
                Ordinal = ordinal;
                DuplicateIndex = duplicateIndex;
            }
        }

        private sealed class ScheduledPacketComparer : IComparer<ScheduledPacket>
        {
            internal static readonly ScheduledPacketComparer Instance = new ScheduledPacketComparer();

            public int Compare(ScheduledPacket first, ScheduledPacket second)
            {
                var value = first.DueMilliseconds.CompareTo(second.DueMilliseconds);
                if (value != 0)
                    return value;
                value = first.ReorderPriority.CompareTo(second.ReorderPriority);
                if (value != 0)
                    return value;
                value = first.Ordinal.CompareTo(second.Ordinal);
                return value != 0 ? value : first.DuplicateIndex.CompareTo(second.DuplicateIndex);
            }
        }
    }
}
