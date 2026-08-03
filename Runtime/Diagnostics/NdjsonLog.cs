using System;
using System.Globalization;
using System.IO;
using System.Text;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Buffers privacy-safe events in a bounded SPSC ring and writes strict NDJSON on demand.</summary>
    public sealed class NdjsonLog : ISessionObserver, IDisposable
    {
        private const int MaximumCapacity = 1 << 20;
        private static readonly Encoding Utf8 = new UTF8Encoding(false, true);
        private readonly Stream _output;
        private readonly SessionEvent[] _events;
        private readonly uint _source;
        private readonly bool _leaveOpen;
        private readonly object _ringSync = new();
        private int _head;
        private int _count;
        private bool _hasGap;
        private ulong _gapFirst;
        private ulong _gapLast;
        private ulong _gapCount;
        private ulong _dropped;
        private bool _faulted;
        private bool _disposed;

        /// <summary>Creates a bounded logger over a writable stream.</summary>
        public NdjsonLog(Stream output, int capacity = 4096, uint source = 0, bool leaveOpen = false)
        {
            if (output == null) throw new ArgumentNullException(nameof(output));
            if (!output.CanWrite) throw new ArgumentException("The output stream must be writable.", nameof(output));
            if (capacity <= 0 || capacity > MaximumCapacity) throw new ArgumentOutOfRangeException(nameof(capacity));
            _output = output;
            _events = new SessionEvent[capacity];
            _source = source;
            _leaveOpen = leaveOpen;
        }

        /// <summary>Gets the number of retained event records awaiting output.</summary>
        public int Pending { get { lock (_ringSync) return _count; } }
        /// <summary>Gets the cumulative number of dropped events.</summary>
        public ulong Dropped { get { lock (_ringSync) return _dropped; } }
        /// <summary>Gets whether output permanently faulted.</summary>
        public bool Faulted { get { lock (_ringSync) return _faulted; } }

        /// <summary>Retains one event without performing I/O or allocating.</summary>
        public void Observe(in SessionEvent value)
        {
            lock (_ringSync)
            {
                if (_disposed || _faulted || _hasGap)
                {
                    _dropped++;
                    if (!_disposed && !_faulted)
                    {
                        _gapLast = value.Id;
                        _gapCount++;
                    }
                    return;
                }
                if (_count == _events.Length)
                {
                    _dropped++;
                    _hasGap = true;
                    _gapFirst = value.Id;
                    _gapLast = value.Id;
                    _gapCount = 1;
                    return;
                }
                _events[(_head + _count) % _events.Length] = value;
                _count++;
            }
        }

        /// <summary>Writes retained events and the pending loss record in deterministic order.</summary>
        public void Flush()
        {
            while (true)
            {
                SessionEvent current = default;
                var hasEvent = false;
                var hasGap = false;
                ulong gapFirst = 0, gapLast = 0, gapCount = 0;
                lock (_ringSync)
                {
                    if (_disposed || _faulted) return;
                    if (_count != 0)
                    {
                        current = _events[_head];
                        _events[_head] = default;
                        _head = (_head + 1) % _events.Length;
                        _count--;
                        hasEvent = true;
                    }
                    else if (_hasGap)
                    {
                        hasGap = true;
                        gapFirst = _gapFirst; gapLast = _gapLast; gapCount = _gapCount;
                        _hasGap = false; _gapFirst = 0; _gapLast = 0; _gapCount = 0;
                    }
                }
                try
                {
                    if (hasEvent) { WriteLine(FormatEvent(in current)); continue; }
                    if (hasGap) { WriteLine(FormatGap(gapFirst, gapLast, gapCount)); continue; }
                    _output.Flush();
                    return;
                }
                catch
                {
                    EnterFault(hasEvent ? 1UL : 0UL);
                    return;
                }
            }
        }

        /// <summary>Flushes once, then optionally closes the output stream.</summary>
        public void Dispose()
        {
            lock (_ringSync) { if (_disposed) return; }
            Flush();
            lock (_ringSync)
            {
                _disposed = true;
                _dropped += (ulong)_count;
                Array.Clear(_events, 0, _events.Length);
                _head = 0;
                _count = 0;
                _hasGap = false;
                _gapFirst = 0;
                _gapLast = 0;
                _gapCount = 0;
            }
            if (_leaveOpen) return;
            try { _output.Dispose(); }
            catch { EnterFault(0); }
        }

        private string FormatEvent(in SessionEvent value)
        {
            var channel = value.Kind == SessionEventKind.Receive || value.Kind == SessionEventKind.Decode ||
                          value.Kind == SessionEventKind.Encode || value.Kind == SessionEventKind.Send
                ? ChannelToken(value.Channel)
                : "none";
            var packet = value.Packet == (PacketKind)0 ? "none" : PacketToken(value.Packet);
            return string.Concat(
                "{\"v\":1,\"source\":", U(_source),
                ",\"id\":", U(value.Id), ",\"step\":", U(value.Step),
                ",\"time_ns\":", L(ToNanoseconds(value.Timestamp)),
                ",\"elapsed_ns\":", L(ToNanoseconds(value.Elapsed)),
                ",\"role\":\"", RoleToken(value.Role), "\",\"kind\":\"", KindToken(value.Kind),
                "\",\"phase\":\"", PhaseToken(value.Phase), "\",\"state\":\"", StateToken(value.State),
                "\",\"error\":\"", ErrorToken(value.Error), "\",\"packet\":\"", packet,
                "\",\"channel\":\"", channel, "\",\"tick\":", U(value.Tick),
                ",\"packet_sequence\":", U(value.PacketSequence), ",\"wire_bytes\":", I(value.WireBytes),
                ",\"decoded_bytes\":", I(value.DecodedBytes), ",\"count\":", I(value.Count),
                ",\"code\":", U(value.Code), ",\"reason\":", U(value.Reason),
                ",\"hash\":\"", value.Hash.ToString("x16", CultureInfo.InvariantCulture),
                "\",\"success\":", value.Success ? "true" : "false",
                ",\"retry\":", value.Retry ? "true" : "false", "}");
        }

        private string FormatGap(ulong first, ulong last, ulong count) => string.Concat("{\"v\":1,\"source\":", U(_source),
            ",\"first_id\":", U(first), ",\"last_id\":", U(last),
            ",\"count\":", U(count), "}");

        private void WriteLine(string value)
        {
            var bytes = Utf8.GetBytes(value + "\n");
            _output.Write(bytes, 0, bytes.Length);
        }

        private void EnterFault(ulong extractedLoss)
        {
            lock (_ringSync)
            {
                if (_faulted) return;
                _faulted = true;
                _dropped += extractedLoss + (ulong)_count;
                Array.Clear(_events, 0, _events.Length);
                _head = 0;
                _count = 0;
                _hasGap = false;
                _gapFirst = 0;
                _gapLast = 0;
                _gapCount = 0;
            }
        }

        private static long ToNanoseconds(long ticks)
        {
            var seconds = ticks / System.Diagnostics.Stopwatch.Frequency;
            var remainder = ticks % System.Diagnostics.Stopwatch.Frequency;
            return checked(seconds * 1_000_000_000L + remainder * 1_000_000_000L / System.Diagnostics.Stopwatch.Frequency);
        }

        private static string U(ulong value) => value.ToString(CultureInfo.InvariantCulture);
        private static string U(uint value) => value.ToString(CultureInfo.InvariantCulture);
        private static string U(ushort value) => value.ToString(CultureInfo.InvariantCulture);
        private static string I(int value) => value.ToString(CultureInfo.InvariantCulture);
        private static string L(long value) => value.ToString(CultureInfo.InvariantCulture);
        private static string RoleToken(SessionRole value) => value == SessionRole.Client ? "client" : "server";
        private static string PhaseToken(SessionEventPhase value) => value == SessionEventPhase.Begin ? "begin" : value == SessionEventPhase.End ? "end" : "point";
        private static string ChannelToken(Channel value) => value == Channel.ReliableOrdered ? "reliable_ordered" : "unreliable_sequenced";
        private static string KindToken(SessionEventKind value) => value switch { SessionEventKind.Step => "step", SessionEventKind.Receive => "receive", SessionEventKind.Decode => "decode", SessionEventKind.Dispatch => "dispatch", SessionEventKind.Capture => "capture", SessionEventKind.Apply => "apply", SessionEventKind.Encode => "encode", SessionEventKind.Send => "send", SessionEventKind.State => "state", SessionEventKind.Fault => "fault", SessionEventKind.Resync => "resync", _ => "unknown" };
        private static string StateToken(SessionState value) => value switch { SessionState.Handshaking => "handshaking", SessionState.Established => "established", SessionState.Closing => "closing", SessionState.Closed => "closed", SessionState.Faulted => "faulted", SessionState.Disposed => "disposed", _ => "unknown" };
        private static string ErrorToken(SessionError value) => value switch { SessionError.None => "none", SessionError.Protocol => "protocol", SessionError.Schema => "schema", SessionError.Limits => "limits", SessionError.Topology => "topology", SessionError.Epoch => "epoch", SessionError.Sequence => "sequence", SessionError.Transport => "transport", _ => "unknown" };
        private static string PacketToken(PacketKind value) => value switch { PacketKind.Hello => "hello", PacketKind.HelloAck => "hello_ack", PacketKind.CommandBatch => "command_batch", PacketKind.FullSnapshot => "full_snapshot", PacketKind.Ack => "ack", PacketKind.ResyncRequest => "resync_request", PacketKind.Disconnect => "disconnect", _ => "unknown" };
    }
}
