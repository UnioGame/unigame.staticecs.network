using System;
using System.Collections.Generic;
using System.Diagnostics;
using FFS.Libraries.StaticEcs;
using FFS.Libraries.StaticPack;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Identifies one transport-owned connection.</summary>
    public readonly struct ConnectionId : IEquatable<ConnectionId>
    {
        /// <summary>Creates a non-zero connection id.</summary>
        public ConnectionId(uint value) { if (value == 0) throw new ArgumentOutOfRangeException(nameof(value)); Value = value; }
        public uint Value { get; }
        public bool Equals(ConnectionId other) => Value == other.Value;
        public override bool Equals(object obj) => obj is ConnectionId other && Equals(other);
        public override int GetHashCode() => (int)Value;
        public static bool operator ==(ConnectionId left, ConnectionId right) => left.Equals(right);
        public static bool operator !=(ConnectionId left, ConnectionId right) => !left.Equals(right);
    }

    /// <summary>Identifies peers allowed to share an immutable server capture.</summary>
    public readonly struct ScopeId : IEquatable<ScopeId>
    {
        public ScopeId(ulong value) => Value = value;
        public ulong Value { get; }
        public bool Equals(ScopeId other) => Value == other.Value;
        public override bool Equals(object obj) => obj is ScopeId other && Equals(other);
        public override int GetHashCode() => Value.GetHashCode();
        public static bool operator ==(ScopeId left, ScopeId right) => left.Equals(right);
        public static bool operator !=(ScopeId left, ScopeId right) => !left.Equals(right);
    }

    /// <summary>Reports per-connection admission state.</summary>
    public enum NetworkSessionState : byte
    {
        Handshaking,
        Established,
        Rejected,
        Closed
    }
    /// <summary>Reports handshake results.</summary>
    public enum NetworkAdmissionResult : byte
    {
        Accepted,
        SchemaMismatch,
        InvalidPeer,
        InvalidEpoch,
        WrongRole
    }
    /// <summary>Reports command validation results.</summary>
    public enum NetworkCommandResult : byte
    {
        Queued,
        Dispatched,
        PolicyRejected,
        WrongSession,
        SchemaMismatch,
        Sequence,
        TickWindow,
        Malformed,
        Duplicate,
        LimitExceeded,
        SubmissionFailed
    }

    /// <summary>Reports exact bounded packet session validation outcomes.</summary>
    public enum PacketValidationResult : byte
    {
        Success,
        WrongState,
        WrongRole,
        WrongEpoch,
        Sequence,
        Duplicate
    }

    /// <summary>Owns one immutable validated command payload.</summary>
    public struct NetworkCommandEnvelope : IDisposable
    {
        private NetworkBufferLease _payload;

        internal NetworkCommandEnvelope(ConnectionId connection, uint peer, uint epoch, uint sequence, uint targetTick, NetworkTypeId typeId, byte version, NetworkBufferLease payload)
        { Connection = connection; PeerId = peer; Epoch = epoch; Sequence = sequence; TargetTick = targetTick; TypeId = typeId; Version = version; _payload = payload ?? throw new ArgumentNullException(nameof(payload)); }
        public ConnectionId Connection { get; }
        public uint PeerId { get; }
        public uint Epoch { get; }
        public uint Sequence { get; }
        public uint TargetTick { get; }
        public NetworkTypeId TypeId { get; }
        public byte Version { get; }
        /// <summary>Gets immutable exact payload.</summary>
        public ReadOnlyMemory<byte> Payload => _payload?.Memory ?? ReadOnlyMemory<byte>.Empty;
        internal byte[] ExactBuffer => _payload?.Buffer;
        internal int ExactOffset => _payload?.Offset ?? 0;
        internal int ExactLength => _payload?.Length ?? 0;

        public void Dispose()
        {
            _payload?.Dispose();
            _payload = null;
        }
    }
}
