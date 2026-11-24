using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading;

namespace UniGame.StaticEcs.Network
{
    /// <summary>
    /// Delay, jitter and loss a relayed link adds to each direction on its own: a round trip gains twice
    /// the latency and can lose a datagram on either leg.
    /// </summary>
    [Serializable]
    public struct NetworkLinkImpairment : IEquatable<NetworkLinkImpairment>
    {
        /// <summary>Largest accepted <see cref="LatencyMs"/>.</summary>
        public const float MaximumLatencyMs = 5000f;
        /// <summary>Largest accepted <see cref="JitterMs"/>.</summary>
        public const float MaximumJitterMs = 5000f;
        /// <summary>Largest accepted <see cref="LossPercent"/>.</summary>
        public const float MaximumLossPercent = 100f;

        /// <summary>Milliseconds every datagram of one direction waits in the relay.</summary>
        public float LatencyMs;
        /// <summary>
        /// Half-width in milliseconds of the uniform spread around <see cref="LatencyMs"/>. A datagram
        /// never waits less than zero, and independent waits reorder neighbouring datagrams.
        /// </summary>
        public float JitterMs;
        /// <summary>Share of the datagrams of one direction that the relay drops, 0..100.</summary>
        public float LossPercent;

        /// <summary>Gets whether the link forwards every datagram at once.</summary>
        public bool IsClean => LatencyMs <= 0f && JitterMs <= 0f && LossPercent <= 0f;

        /// <summary>Gets whether every value is finite and inside its range.</summary>
        public bool IsValid =>
            // Every comparison is false for NaN, so NaN is rejected as well.
            LatencyMs >= 0f && LatencyMs <= MaximumLatencyMs &&
            JitterMs >= 0f && JitterMs <= MaximumJitterMs &&
            LossPercent >= 0f && LossPercent <= MaximumLossPercent;

        public bool Equals(NetworkLinkImpairment other) =>
            LatencyMs.Equals(other.LatencyMs) && JitterMs.Equals(other.JitterMs) &&
            LossPercent.Equals(other.LossPercent);

        public override bool Equals(object obj) => obj is NetworkLinkImpairment other && Equals(other);

        public override int GetHashCode() => HashCode.Combine(LatencyMs, JitterMs, LossPercent);
    }

    /// <summary>Bounds of one <see cref="NetworkLinkRelay"/>; a zero field selects its default.</summary>
    public struct NetworkLinkRelaySettings
    {
        public const int DefaultSeed = 7919;
        public const int DefaultMaximumQueuedDatagrams = 8192;
        public const long DefaultMaximumQueuedBytes = 16L * 1024 * 1024;
        public const int DefaultMaximumDatagramBytes = 2048;

        /// <summary>Seed of the loss and jitter decisions.</summary>
        public int Seed;
        /// <summary>Delayed datagrams held at once across every link; a further one is dropped.</summary>
        public int MaximumQueuedDatagrams;
        /// <summary>Delayed bytes held at once across every link; a datagram that exceeds it is dropped.</summary>
        public long MaximumQueuedBytes;
        /// <summary>Largest forwarded datagram; a larger one is dropped.</summary>
        public int MaximumDatagramBytes;

        internal NetworkLinkRelaySettings Normalize()
        {
            var value = this;
            if (value.Seed == 0)
                value.Seed = DefaultSeed;
            if (value.MaximumQueuedDatagrams <= 0)
                value.MaximumQueuedDatagrams = DefaultMaximumQueuedDatagrams;
            if (value.MaximumQueuedBytes <= 0)
                value.MaximumQueuedBytes = DefaultMaximumQueuedBytes;
            if (value.MaximumDatagramBytes <= 0)
                value.MaximumDatagramBytes = DefaultMaximumDatagramBytes;
            return value;
        }
    }

    /// <summary>Cumulative counters of one direction across every link of a relay.</summary>
    public struct NetworkLinkRelayDirectionDiagnostics
    {
        /// <summary>Datagrams sent on to the far side.</summary>
        public long ForwardedPackets;
        /// <summary>Bytes of the forwarded datagrams.</summary>
        public long ForwardedBytes;
        /// <summary>Forwarded datagrams that waited in the relay first.</summary>
        public long DelayedPackets;
        /// <summary>Sum of the measured waits of the delayed datagrams.</summary>
        public long DelayMicrosecondsSum;
        /// <summary>Longest measured wait of a delayed datagram.</summary>
        public long DelayMicrosecondsMaximum;
        /// <summary>Datagrams dropped by <see cref="NetworkLinkImpairment.LossPercent"/>.</summary>
        public long LossDrops;
        /// <summary>Datagrams dropped by <see cref="NetworkLinkRelayLink.Blackout"/>.</summary>
        public long BlackoutDrops;
        /// <summary>Datagrams dropped because the delay queue was full.</summary>
        public long QueueLimitDrops;
        /// <summary>Datagrams dropped because they exceeded the largest forwarded size.</summary>
        public long OversizedDrops;
        /// <summary>Datagrams dropped because their link closed or had no client yet.</summary>
        public long ClosedDrops;
        /// <summary>Socket errors reported by an endpoint, such as an unreachable port.</summary>
        public long EndpointErrors;
    }

    /// <summary>Counters and queue levels of one relay.</summary>
    public struct NetworkLinkRelayDiagnostics
    {
        public NetworkLinkRelayDirectionDiagnostics ClientToServer;
        public NetworkLinkRelayDirectionDiagnostics ServerToClient;
        /// <summary>Open links.</summary>
        public int Links;
        /// <summary>Delayed datagrams waiting now.</summary>
        public int QueuedDatagrams;
        /// <summary>Bytes of the delayed datagrams waiting now.</summary>
        public long QueuedBytes;
        /// <summary>Most delayed datagrams that waited at once.</summary>
        public int QueuedDatagramsHighWater;
        /// <summary>Most delayed bytes that waited at once.</summary>
        public long QueuedBytesHighWater;
    }

    /// <summary>
    /// Forwards real UDP datagrams between local clients and their servers and impairs them on the way.
    /// It sits below the transport, so the transport's own retransmits, acknowledgements and round-trip
    /// estimate react as they do on a bad network. Every link is one client: the client connects to
    /// <see cref="NetworkLinkRelayLink.LocalPort"/> on the loopback address instead of the server.
    /// </summary>
    /// <remarks>
    /// Each link runs one thread per direction and the relay runs one thread for the delayed
    /// datagrams, so no call of the owner blocks on the network. Every member is thread-safe.
    /// </remarks>
    public sealed class NetworkLinkRelay : IDisposable
    {
        /// <summary>Address a client of a link connects to.</summary>
        public const string LoopbackAddress = "127.0.0.1";

        private const int IdleWaitMilliseconds = 250;
        private const int ThreadJoinMilliseconds = 2000;

        private static readonly long HalfMillisecondTicks = Stopwatch.Frequency / 2000;

        private readonly object _linksGate = new object();
        private readonly object _queueGate = new object();
        private readonly List<NetworkLinkRelayLink> _links = new List<NetworkLinkRelayLink>();
        private readonly Thread _scheduler;
        private readonly int _maximumQueuedDatagrams;
        private readonly long _maximumQueuedBytes;
        private readonly int _seed;
        private PendingDatagram[] _queue = new PendingDatagram[64];
        private int _queued;
        private long _queuedBytes;
        private int _queuedHighWater;
        private long _queuedBytesHighWater;
        private long _queueSequence;
        private int _linkSequence;
        private bool _stopping;
        private volatile bool _disposed;
        private volatile string _lastFailure;

        /// <summary>Creates a relay and starts its delay thread.</summary>
        public NetworkLinkRelay(NetworkLinkRelaySettings settings = default)
        {
            settings = settings.Normalize();
            _seed = settings.Seed;
            _maximumQueuedDatagrams = settings.MaximumQueuedDatagrams;
            _maximumQueuedBytes = settings.MaximumQueuedBytes;
            MaximumDatagramBytes = settings.MaximumDatagramBytes;
            _scheduler = new Thread(RunScheduler)
            {
                IsBackground = true,
                Name = "NetworkLinkRelay.Delay",
            };
            _scheduler.Start();
        }

        /// <summary>Gets the largest forwarded datagram.</summary>
        public int MaximumDatagramBytes { get; }

        /// <summary>Gets the message of the last unexpected failure of a relay thread, or null.</summary>
        public string LastFailure => _lastFailure;

        internal DirectionCounters ClientToServer { get; } = new DirectionCounters();

        internal DirectionCounters ServerToClient { get; } = new DirectionCounters();

        /// <summary>
        /// Opens a link to one server endpoint. Its first sender becomes its only client, so a client
        /// that reconnects from a new socket needs a new link.
        /// </summary>
        /// <param name="upstreamAddress">Server IP address or host name.</param>
        /// <param name="upstreamPort">Server UDP port.</param>
        /// <param name="impairment">Values the link starts with.</param>
        public NetworkLinkRelayLink Open(string upstreamAddress, int upstreamPort,
            in NetworkLinkImpairment impairment)
        {
            if (string.IsNullOrWhiteSpace(upstreamAddress))
                throw new ArgumentException("Upstream address is required.", nameof(upstreamAddress));
            if (upstreamPort < 1 || upstreamPort > 65535)
                throw new ArgumentOutOfRangeException(nameof(upstreamPort));
            if (!impairment.IsValid)
                throw new ArgumentOutOfRangeException(nameof(impairment));

            int index;
            lock (_linksGate)
            {
                ThrowIfDisposed();
                index = _linkSequence++;
            }

            var upstream = new IPEndPoint(ResolveAddress(upstreamAddress), upstreamPort);
            var link = new NetworkLinkRelayLink(this, upstream, impairment,
                unchecked(_seed * 31 + index * 2));
            var rejected = false;
            lock (_linksGate)
            {
                if (_disposed)
                    rejected = true;
                else
                    _links.Add(link);
            }

            if (rejected)
            {
                link.RequestClose();
                link.WaitClosed();
                ThrowIfDisposed();
            }
            return link;
        }

        /// <summary>
        /// Stops one link without waiting: its threads leave within one poll and release the sockets
        /// themselves. Datagrams the link still delays are dropped.
        /// </summary>
        public void Close(NetworkLinkRelayLink link)
        {
            if (link == null)
                return;
            lock (_linksGate)
                _links.Remove(link);
            link.RequestClose();
        }

        /// <summary>Captures the counters; safe from any thread.</summary>
        public NetworkLinkRelayDiagnostics CaptureDiagnostics()
        {
            var value = new NetworkLinkRelayDiagnostics
            {
                ClientToServer = ClientToServer.Capture(),
                ServerToClient = ServerToClient.Capture(),
            };
            lock (_linksGate)
                value.Links = _links.Count;
            lock (_queueGate)
            {
                value.QueuedDatagrams = _queued;
                value.QueuedBytes = _queuedBytes;
                value.QueuedDatagramsHighWater = _queuedHighWater;
                value.QueuedBytesHighWater = _queuedBytesHighWater;
            }
            return value;
        }

        public void Dispose()
        {
            NetworkLinkRelayLink[] links;
            lock (_linksGate)
            {
                if (_disposed)
                    return;
                _disposed = true;
                links = _links.ToArray();
                _links.Clear();
            }

            // Every link is asked first so their threads stop together instead of one poll after another.
            foreach (var link in links)
                link.RequestClose();
            foreach (var link in links)
                link.WaitClosed();

            lock (_queueGate)
            {
                _stopping = true;
                Monitor.PulseAll(_queueGate);
            }
            _scheduler.Join(ThreadJoinMilliseconds);

            lock (_queueGate)
            {
                for (var index = 0; index < _queued; index++)
                    ArrayPool<byte>.Shared.Return(_queue[index].Data);
                _queued = 0;
                _queuedBytes = 0;
            }
        }

        /// <summary>
        /// Decides one datagram: false drops it, true forwards it at <paramref name="dueTimestamp"/>,
        /// which equals <paramref name="nowTimestamp"/> when it must not wait.
        /// </summary>
        internal static bool TrySchedule(in NetworkLinkImpairment impairment, System.Random random,
            long nowTimestamp, out long dueTimestamp)
        {
            dueTimestamp = nowTimestamp;
            if (impairment.LossPercent > 0f && random.NextDouble() * 100d < impairment.LossPercent)
                return false;

            var delayMilliseconds = (double)impairment.LatencyMs;
            if (impairment.JitterMs > 0f)
                delayMilliseconds += (random.NextDouble() * 2d - 1d) * impairment.JitterMs;
            if (delayMilliseconds > 0d)
                dueTimestamp = nowTimestamp +
                    (long)(delayMilliseconds * Stopwatch.Frequency / 1000d);
            return true;
        }

        internal void Accept(NetworkLinkRelayLink link, bool towardServer, byte[] buffer, int length,
            System.Random random)
        {
            var counters = towardServer ? ClientToServer : ServerToClient;
            var now = Stopwatch.GetTimestamp();
            if (now < link.BlackoutUntilTimestamp)
            {
                Interlocked.Increment(ref counters.BlackoutDrops);
                return;
            }

            var impairment = link.Impairment;
            if (!TrySchedule(in impairment, random, now, out var due))
            {
                Interlocked.Increment(ref counters.LossDrops);
                return;
            }

            if (due == now)
            {
                Send(link, towardServer, buffer, length, counters);
                return;
            }

            var data = ArrayPool<byte>.Shared.Rent(length);
            Buffer.BlockCopy(buffer, 0, data, 0, length);
            var accepted = false;
            lock (_queueGate)
            {
                if (!_stopping && _queued < _maximumQueuedDatagrams &&
                    _queuedBytes + length <= _maximumQueuedBytes)
                {
                    accepted = true;
                    var earliest = Push(new PendingDatagram
                    {
                        DueTimestamp = due,
                        Sequence = _queueSequence++,
                        IngressTimestamp = now,
                        Link = link,
                        TowardServer = towardServer,
                        Data = data,
                        Length = length,
                    });
                    _queuedBytes += length;
                    if (_queued > _queuedHighWater)
                        _queuedHighWater = _queued;
                    if (_queuedBytes > _queuedBytesHighWater)
                        _queuedBytesHighWater = _queuedBytes;
                    if (earliest)
                        Monitor.Pulse(_queueGate);
                }
            }

            if (accepted)
                return;
            ArrayPool<byte>.Shared.Return(data);
            Interlocked.Increment(ref counters.QueueLimitDrops);
        }

        internal void RecordFailure(Exception exception) =>
            _lastFailure = exception.GetType().Name + ": " + exception.Message;

        private void RunScheduler()
        {
            try
            {
                while (true)
                {
                    PendingDatagram item;
                    lock (_queueGate)
                    {
                        while (true)
                        {
                            if (_stopping)
                                return;
                            if (_queued == 0)
                            {
                                Monitor.Wait(_queueGate, IdleWaitMilliseconds);
                                continue;
                            }

                            var remaining = _queue[0].DueTimestamp - Stopwatch.GetTimestamp();
                            // A wait is whole milliseconds, so a datagram within half of one goes now.
                            if (remaining <= HalfMillisecondTicks)
                                break;
                            Monitor.Wait(_queueGate,
                                Math.Max(1, (int)(remaining * 1000 / Stopwatch.Frequency)));
                        }

                        item = Pop();
                        _queuedBytes -= item.Length;
                    }

                    Deliver(in item);
                    ArrayPool<byte>.Shared.Return(item.Data);
                }
            }
            catch (Exception exception)
            {
                RecordFailure(exception);
            }
        }

        private void Deliver(in PendingDatagram item)
        {
            var counters = item.TowardServer ? ClientToServer : ServerToClient;
            var link = item.Link;
            if (link.Closed)
            {
                Interlocked.Increment(ref counters.ClosedDrops);
                return;
            }

            var now = Stopwatch.GetTimestamp();
            if (now < link.BlackoutUntilTimestamp)
            {
                Interlocked.Increment(ref counters.BlackoutDrops);
                return;
            }

            if (!Send(link, item.TowardServer, item.Data, item.Length, counters))
                return;
            var waited = (now - item.IngressTimestamp) * 1_000_000 / Stopwatch.Frequency;
            Interlocked.Increment(ref counters.DelayedPackets);
            Interlocked.Add(ref counters.DelayMicrosecondsSum, waited);
            DirectionCounters.RaiseMaximum(ref counters.DelayMicrosecondsMaximum, waited);
        }

        private static bool Send(NetworkLinkRelayLink link, bool towardServer, byte[] data,
            int length, DirectionCounters counters)
        {
            if (!towardServer && !link.ClientBound)
            {
                Interlocked.Increment(ref counters.ClosedDrops);
                return false;
            }

            try
            {
                (towardServer ? link.Upstream : link.Local).Send(data, 0, length, SocketFlags.None);
            }
            catch (SocketException)
            {
                Interlocked.Increment(ref counters.EndpointErrors);
                return false;
            }
            catch (ObjectDisposedException)
            {
                Interlocked.Increment(ref counters.ClosedDrops);
                return false;
            }

            Interlocked.Increment(ref counters.ForwardedPackets);
            Interlocked.Add(ref counters.ForwardedBytes, length);
            return true;
        }

        private bool Push(in PendingDatagram item)
        {
            if (_queued == _queue.Length)
                Array.Resize(ref _queue, _queue.Length * 2);

            var index = _queued++;
            while (index > 0)
            {
                var parent = (index - 1) / 2;
                if (!IsBefore(in item, in _queue[parent]))
                    break;
                _queue[index] = _queue[parent];
                index = parent;
            }
            _queue[index] = item;
            return index == 0;
        }

        private PendingDatagram Pop()
        {
            var top = _queue[0];
            var last = _queue[--_queued];
            _queue[_queued] = default;
            var index = 0;
            while (true)
            {
                var child = index * 2 + 1;
                if (child >= _queued)
                    break;
                if (child + 1 < _queued && IsBefore(in _queue[child + 1], in _queue[child]))
                    child++;
                if (!IsBefore(in _queue[child], in last))
                    break;
                _queue[index] = _queue[child];
                index = child;
            }
            if (_queued > 0)
                _queue[index] = last;
            return top;
        }

        private static bool IsBefore(in PendingDatagram left, in PendingDatagram right) =>
            left.DueTimestamp < right.DueTimestamp ||
            (left.DueTimestamp == right.DueTimestamp && left.Sequence < right.Sequence);

        private static IPAddress ResolveAddress(string address)
        {
            if (IPAddress.TryParse(address, out var parsed))
                return parsed;

            IPAddress fallback = null;
            foreach (var candidate in Dns.GetHostAddresses(address))
            {
                if (candidate.AddressFamily == AddressFamily.InterNetwork)
                    return candidate;
                fallback ??= candidate;
            }
            return fallback ?? throw new SocketException((int)SocketError.HostNotFound);
        }

        private void ThrowIfDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(NetworkLinkRelay));
        }

        private struct PendingDatagram
        {
            public long DueTimestamp;
            public long Sequence;
            public long IngressTimestamp;
            public NetworkLinkRelayLink Link;
            public bool TowardServer;
            public byte[] Data;
            public int Length;
        }

        internal sealed class DirectionCounters
        {
            public long ForwardedPackets;
            public long ForwardedBytes;
            public long DelayedPackets;
            public long DelayMicrosecondsSum;
            public long DelayMicrosecondsMaximum;
            public long LossDrops;
            public long BlackoutDrops;
            public long QueueLimitDrops;
            public long OversizedDrops;
            public long ClosedDrops;
            public long EndpointErrors;

            public NetworkLinkRelayDirectionDiagnostics Capture() =>
                new NetworkLinkRelayDirectionDiagnostics
                {
                    ForwardedPackets = Interlocked.Read(ref ForwardedPackets),
                    ForwardedBytes = Interlocked.Read(ref ForwardedBytes),
                    DelayedPackets = Interlocked.Read(ref DelayedPackets),
                    DelayMicrosecondsSum = Interlocked.Read(ref DelayMicrosecondsSum),
                    DelayMicrosecondsMaximum = Interlocked.Read(ref DelayMicrosecondsMaximum),
                    LossDrops = Interlocked.Read(ref LossDrops),
                    BlackoutDrops = Interlocked.Read(ref BlackoutDrops),
                    QueueLimitDrops = Interlocked.Read(ref QueueLimitDrops),
                    OversizedDrops = Interlocked.Read(ref OversizedDrops),
                    ClosedDrops = Interlocked.Read(ref ClosedDrops),
                    EndpointErrors = Interlocked.Read(ref EndpointErrors),
                };

            public static void RaiseMaximum(ref long target, long value)
            {
                var current = Interlocked.Read(ref target);
                while (value > current)
                {
                    var seen = Interlocked.CompareExchange(ref target, value, current);
                    if (seen == current)
                        return;
                    current = seen;
                }
            }
        }
    }

    /// <summary>One client's path through a <see cref="NetworkLinkRelay"/>.</summary>
    public sealed class NetworkLinkRelayLink
    {
        private const int PollMicroseconds = 20_000;
        private const int SocketBufferBytes = 1024 * 1024;
        private const int ThreadJoinMilliseconds = 2000;

        private readonly NetworkLinkRelay _relay;
        private readonly Thread _towardServer;
        private readonly Thread _towardClient;
        private readonly int _seed;
        private volatile ImpairmentBox _impairment;
        private volatile bool _closed;
        private volatile bool _clientBound;
        private long _blackoutUntilTimestamp;
        private int _runningPumps = 2;
        private int _socketsClosed;

        internal NetworkLinkRelayLink(NetworkLinkRelay relay, IPEndPoint upstream,
            in NetworkLinkImpairment impairment, int seed)
        {
            _relay = relay;
            _seed = seed;
            _impairment = new ImpairmentBox(impairment);
            UpstreamEndPoint = upstream;
            Socket local = null;
            Socket remote = null;
            try
            {
                local = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
                local.ReceiveBufferSize = SocketBufferBytes;
                local.SendBufferSize = SocketBufferBytes;
                // Loopback only: the relay must never accept datagrams from another machine.
                local.Bind(new IPEndPoint(IPAddress.Loopback, 0));
                remote = new Socket(upstream.AddressFamily, SocketType.Dgram, ProtocolType.Udp);
                remote.ReceiveBufferSize = SocketBufferBytes;
                remote.SendBufferSize = SocketBufferBytes;
                remote.Connect(upstream);
            }
            catch
            {
                local?.Dispose();
                remote?.Dispose();
                throw;
            }

            Local = local;
            Upstream = remote;
            LocalPort = ((IPEndPoint)local.LocalEndPoint).Port;
            _towardServer = new Thread(RunTowardServer)
            {
                IsBackground = true,
                Name = "NetworkLinkRelay.ToServer",
            };
            _towardClient = new Thread(RunTowardClient)
            {
                IsBackground = true,
                Name = "NetworkLinkRelay.ToClient",
            };
            _towardServer.Start();
            _towardClient.Start();
        }

        /// <summary>Gets the loopback UDP port the client connects to instead of the server.</summary>
        public int LocalPort { get; }

        /// <summary>Gets the server endpoint this link forwards to.</summary>
        public IPEndPoint UpstreamEndPoint { get; }

        /// <summary>Gets whether the link was closed.</summary>
        public bool Closed => _closed;

        /// <summary>
        /// Gets or sets the values applied to datagrams that arrive from now on; datagrams already
        /// waiting keep the delay they were given.
        /// </summary>
        public NetworkLinkImpairment Impairment
        {
            get => _impairment.Value;
            set
            {
                if (!value.IsValid)
                    throw new ArgumentOutOfRangeException(nameof(value));
                _impairment = new ImpairmentBox(value);
            }
        }

        internal Socket Local { get; }

        internal Socket Upstream { get; }

        internal bool ClientBound => _clientBound;

        internal long BlackoutUntilTimestamp => Interlocked.Read(ref _blackoutUntilTimestamp);

        /// <summary>Longest accepted blackout; a longer one would overflow the timestamp it ends at.</summary>
        public const float MaximumBlackoutMs = 600_000f;

        /// <summary>
        /// Drops every datagram of both directions for the given time from now, including delayed
        /// ones that come due inside it. Zero ends a running blackout.
        /// </summary>
        public void Blackout(float milliseconds)
        {
            // The negated comparison also rejects NaN.
            if (!(milliseconds >= 0f) || milliseconds > MaximumBlackoutMs)
                throw new ArgumentOutOfRangeException(nameof(milliseconds));
            Interlocked.Exchange(ref _blackoutUntilTimestamp, Stopwatch.GetTimestamp() +
                (long)((double)milliseconds * Stopwatch.Frequency / 1000d));
        }

        internal void RequestClose() => _closed = true;

        internal void WaitClosed()
        {
            _towardServer.Join(ThreadJoinMilliseconds);
            _towardClient.Join(ThreadJoinMilliseconds);
            CloseSockets();
        }

        private void CloseSockets()
        {
            if (Interlocked.Exchange(ref _socketsClosed, 1) != 0)
                return;
            Local.Dispose();
            Upstream.Dispose();
        }

        private void RunTowardServer() => Run(true);

        private void RunTowardClient() => Run(false);

        private void Run(bool towardServer)
        {
            try
            {
                Pump(towardServer);
            }
            catch (Exception exception)
            {
                if (!_closed)
                    _relay.RecordFailure(exception);
            }
            finally
            {
                // The sockets go only after both directions stopped using them.
                if (Interlocked.Decrement(ref _runningPumps) == 0)
                    CloseSockets();
            }
        }

        private void Pump(bool towardServer)
        {
            var source = towardServer ? Local : Upstream;
            var counters = towardServer ? _relay.ClientToServer : _relay.ServerToClient;
            var maximum = _relay.MaximumDatagramBytes;
            // One spare byte tells a datagram of exactly the limit from a larger one cut by the socket.
            var buffer = new byte[maximum + 1];
            var random = new System.Random(unchecked(_seed + (towardServer ? 0 : 1)));
            EndPoint anySender = new IPEndPoint(IPAddress.Loopback, 0);
            while (!_closed)
            {
                int length;
                try
                {
                    if (!source.Poll(PollMicroseconds, SelectMode.SelectRead) || _closed)
                        continue;
                    if (towardServer && !_clientBound)
                    {
                        var sender = anySender;
                        length = source.ReceiveFrom(buffer, 0, buffer.Length, SocketFlags.None,
                            ref sender);
                        // Later datagrams of other local senders are refused by the socket itself.
                        source.Connect(sender);
                        _clientBound = true;
                    }
                    else
                    {
                        length = source.Receive(buffer, 0, buffer.Length, SocketFlags.None);
                    }
                }
                catch (SocketException exception)
                {
                    if (_closed)
                        return;
                    if (exception.SocketErrorCode == SocketError.MessageSize)
                        Interlocked.Increment(ref counters.OversizedDrops);
                    else
                        Interlocked.Increment(ref counters.EndpointErrors);
                    continue;
                }
                catch (ObjectDisposedException)
                {
                    return;
                }

                if (length > maximum)
                {
                    Interlocked.Increment(ref counters.OversizedDrops);
                    continue;
                }

                _relay.Accept(this, towardServer, buffer, length, random);
            }
        }

        private sealed class ImpairmentBox
        {
            public readonly NetworkLinkImpairment Value;

            public ImpairmentBox(in NetworkLinkImpairment value) => Value = value;
        }
    }
}
