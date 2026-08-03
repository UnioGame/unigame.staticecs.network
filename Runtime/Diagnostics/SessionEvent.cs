using System;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Receives privacy-safe session operation events without taking ownership.</summary>
    public interface ISessionObserver
    {
        /// <summary>Observes one immutable event.</summary>
        void Observe(in SessionEvent value);
    }

    /// <summary>Identifies an observed session operation.</summary>
    public enum SessionEventKind : byte
    {
        /// <summary>A complete deterministic session step.</summary>
        Step,
        /// <summary>A transport receive attempt.</summary>
        Receive,
        /// <summary>A packet decode attempt.</summary>
        Decode,
        /// <summary>A command dispatch attempt.</summary>
        Dispatch,
        /// <summary>A world capture attempt.</summary>
        Capture,
        /// <summary>A snapshot apply attempt.</summary>
        Apply,
        /// <summary>A packet encode attempt.</summary>
        Encode,
        /// <summary>A transport send attempt.</summary>
        Send,
        /// <summary>A public state transition.</summary>
        State,
        /// <summary>A terminal session fault.</summary>
        Fault,
        /// <summary>A semantic resynchronization request or consumption.</summary>
        Resync
    }

    /// <summary>Identifies an operation boundary or instantaneous observation.</summary>
    public enum SessionEventPhase : byte
    {
        /// <summary>The operation is beginning.</summary>
        Begin,
        /// <summary>The operation has ended.</summary>
        End,
        /// <summary>The event is instantaneous.</summary>
        Point
    }

    /// <summary>Contains one immutable privacy-safe session observation.</summary>
    public readonly struct SessionEvent
    {
        internal SessionEvent(ulong id, ulong step, long timestamp, long elapsed, uint tick,
            uint packetSequence, int wireBytes, int decodedBytes, int count, ushort code, ushort reason,
            ulong hash, SessionRole role, SessionEventKind kind, SessionEventPhase phase,
            SessionState state, SessionError error, PacketKind packet, Channel channel,
            bool success, bool retry)
        {
            Id = id; Step = step; Timestamp = timestamp; Elapsed = elapsed; Tick = tick;
            PacketSequence = packetSequence; WireBytes = wireBytes; DecodedBytes = decodedBytes;
            Count = count; Code = code; Reason = reason; Hash = hash; Role = role; Kind = kind;
            Phase = phase; State = state; Error = error; Packet = packet; Channel = channel;
            Success = success; Retry = retry;
        }

        /// <summary>Gets the session-local attempted delivery identifier.</summary>
        public ulong Id { get; }
        /// <summary>Gets the current logical session step.</summary>
        public ulong Step { get; }
        /// <summary>Gets the operation timestamp in Stopwatch ticks.</summary>
        public long Timestamp { get; }
        /// <summary>Gets paired operation duration in Stopwatch ticks.</summary>
        public long Elapsed { get; }
        /// <summary>Gets the related world tick or the absence sentinel.</summary>
        public uint Tick { get; }
        /// <summary>Gets the related packet sequence or zero.</summary>
        public uint PacketSequence { get; }
        /// <summary>Gets complete wire bytes including framing.</summary>
        public int WireBytes { get; }
        /// <summary>Gets decoded payload bytes excluding framing.</summary>
        public int DecodedBytes { get; }
        /// <summary>Gets the operation-specific item count.</summary>
        public int Count { get; }
        /// <summary>Gets the bounded operation result code.</summary>
        public ushort Code { get; }
        /// <summary>Gets the bounded wire reason code.</summary>
        public ushort Reason { get; }
        /// <summary>Gets the canonical payload hash.</summary>
        public ulong Hash { get; }
        /// <summary>Gets the endpoint role.</summary>
        public SessionRole Role { get; }
        /// <summary>Gets the operation kind.</summary>
        public SessionEventKind Kind { get; }
        /// <summary>Gets the operation phase.</summary>
        public SessionEventPhase Phase { get; }
        /// <summary>Gets the public session state.</summary>
        public SessionState State { get; }
        /// <summary>Gets the current session error.</summary>
        public SessionError Error { get; }
        /// <summary>Gets the packet kind or zero when absent.</summary>
        public PacketKind Packet { get; }
        /// <summary>Gets the channel when meaningful.</summary>
        public Channel Channel { get; }
        /// <summary>Gets whether the observed operation succeeded.</summary>
        public bool Success { get; }
        /// <summary>Gets whether this send retried a frozen intent.</summary>
        public bool Retry { get; }
    }

    /// <summary>Contains cumulative allocation-free session counters.</summary>
    public readonly struct SessionStats
    {
        internal SessionStats(ulong steps, ulong receivedPackets, ulong sentPackets, ulong receivedBytes,
            ulong sentBytes, ulong decodedBytes, ulong commandsQueued, ulong commandsAccepted,
            ulong commandsRejected, ulong snapshotsCaptured, ulong snapshotsApplied, ulong resyncs,
            ulong sendRetries, ulong faults, ulong observerErrors)
        {
            Steps = steps; ReceivedPackets = receivedPackets; SentPackets = sentPackets;
            ReceivedBytes = receivedBytes; SentBytes = sentBytes; DecodedBytes = decodedBytes;
            CommandsQueued = commandsQueued; CommandsAccepted = commandsAccepted;
            CommandsRejected = commandsRejected; SnapshotsCaptured = snapshotsCaptured;
            SnapshotsApplied = snapshotsApplied; Resyncs = resyncs; SendRetries = sendRetries;
            Faults = faults; ObserverErrors = observerErrors;
        }

        /// <summary>Gets completed step count.</summary>
        public ulong Steps { get; }
        /// <summary>Gets accepted inbound packet count.</summary>
        public ulong ReceivedPackets { get; }
        /// <summary>Gets transport-accepted outbound packet count.</summary>
        public ulong SentPackets { get; }
        /// <summary>Gets complete received wire bytes.</summary>
        public ulong ReceivedBytes { get; }
        /// <summary>Gets complete sent wire bytes.</summary>
        public ulong SentBytes { get; }
        /// <summary>Gets successfully decoded payload bytes.</summary>
        public ulong DecodedBytes { get; }
        /// <summary>Gets commands retained by the outbox.</summary>
        public ulong CommandsQueued { get; }
        /// <summary>Gets accepted dispatched commands.</summary>
        public ulong CommandsAccepted { get; }
        /// <summary>Gets authorization-rejected dispatched commands.</summary>
        public ulong CommandsRejected { get; }
        /// <summary>Gets committed authoritative captures.</summary>
        public ulong SnapshotsCaptured { get; }
        /// <summary>Gets committed replica applies.</summary>
        public ulong SnapshotsApplied { get; }
        /// <summary>Gets semantic resynchronization events.</summary>
        public ulong Resyncs { get; }
        /// <summary>Gets repeated frozen send attempts.</summary>
        public ulong SendRetries { get; }
        /// <summary>Gets terminal faults.</summary>
        public ulong Faults { get; }
        /// <summary>Gets isolated observer failures.</summary>
        public ulong ObserverErrors { get; }
    }

    /// <summary>Identifies a canonical retained snapshot by tick, hash and byte length.</summary>
    public readonly struct TickFingerprint : IEquatable<TickFingerprint>
    {
        internal TickFingerprint(uint tick, ulong hash, long bytes) { Tick = tick; Hash = hash; Bytes = bytes; }
        /// <summary>Gets the snapshot tick.</summary>
        public uint Tick { get; }
        /// <summary>Gets the canonical payload hash.</summary>
        public ulong Hash { get; }
        /// <summary>Gets the canonical payload byte length.</summary>
        public long Bytes { get; }
        /// <inheritdoc />
        public bool Equals(TickFingerprint other) => Tick == other.Tick && Hash == other.Hash && Bytes == other.Bytes;
        /// <inheritdoc />
        public override bool Equals(object obj) => obj is TickFingerprint other && Equals(other);
        /// <inheritdoc />
        public override int GetHashCode() => HashCode.Combine(Tick, Hash, Bytes);
        /// <summary>Tests value equality.</summary>
        public static bool operator ==(TickFingerprint left, TickFingerprint right) => left.Equals(right);
        /// <summary>Tests value inequality.</summary>
        public static bool operator !=(TickFingerprint left, TickFingerprint right) => !left.Equals(right);
    }
}
