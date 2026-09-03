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
        /// <summary>Gets the transport value.</summary>
        public uint Value { get; }
        /// <inheritdoc />
        public bool Equals(ConnectionId other) => Value == other.Value;
        /// <inheritdoc />
        public override bool Equals(object obj) => obj is ConnectionId other && Equals(other);
        /// <inheritdoc />
        public override int GetHashCode() => (int)Value;
        /// <summary>Tests equality.</summary>
        public static bool operator ==(ConnectionId left, ConnectionId right) => left.Equals(right);
        /// <summary>Tests inequality.</summary>
        public static bool operator !=(ConnectionId left, ConnectionId right) => !left.Equals(right);
    }

    /// <summary>Identifies peers allowed to share an immutable server capture.</summary>
    public readonly struct ScopeId : IEquatable<ScopeId>
    {
        /// <summary>Creates a scope id.</summary>
        public ScopeId(ulong value) => Value = value;
        /// <summary>Gets the scope value.</summary>
        public ulong Value { get; }
        /// <inheritdoc />
        public bool Equals(ScopeId other) => Value == other.Value;
        /// <inheritdoc />
        public override bool Equals(object obj) => obj is ScopeId other && Equals(other);
        /// <inheritdoc />
        public override int GetHashCode() => Value.GetHashCode();
        /// <summary>Tests equality.</summary>
        public static bool operator ==(ScopeId left, ScopeId right) => left.Equals(right);
        /// <summary>Tests inequality.</summary>
        public static bool operator !=(ScopeId left, ScopeId right) => !left.Equals(right);
    }

    /// <summary>Reports per-connection admission state.</summary>
    public enum NetworkSessionState : byte
    {
        /// <summary>The connection is negotiating admission.</summary>
        Handshaking,
        /// <summary>The connection passed admission.</summary>
        Established,
        /// <summary>The connection failed admission.</summary>
        Rejected,
        /// <summary>The connection is closed.</summary>
        Closed
    }
    /// <summary>Reports handshake results.</summary>
    public enum NetworkAdmissionResult : byte
    {
        /// <summary>The connection was admitted.</summary>
        Accepted,
        /// <summary>The remote schema was incompatible.</summary>
        SchemaMismatch,
        /// <summary>The peer identifier was invalid.</summary>
        InvalidPeer,
        /// <summary>The session epoch was invalid.</summary>
        InvalidEpoch,
        /// <summary>The session state did not allow admission.</summary>
        WrongRole
    }
    /// <summary>Reports command validation results.</summary>
    public enum NetworkCommandResult : byte
    {
        /// <summary>The command was queued.</summary>
        Queued,
        /// <summary>The command was dispatched.</summary>
        Dispatched,
        /// <summary>The server policy rejected the command.</summary>
        PolicyRejected,
        /// <summary>The command did not belong to the admitted session.</summary>
        WrongSession,
        /// <summary>The command schema was incompatible.</summary>
        SchemaMismatch,
        /// <summary>The command sequence was unexpected.</summary>
        Sequence,
        /// <summary>The target tick was outside the accepted window.</summary>
        TickWindow,
        /// <summary>The command payload was malformed.</summary>
        Malformed,
        /// <summary>The redundant command was already observed.</summary>
        Duplicate,
        /// <summary>The command batch exceeded a negotiated bound.</summary>
        LimitExceeded,
        /// <summary>The transaction could not be submitted to transport.</summary>
        SubmissionFailed
    }

    /// <summary>Reports exact bounded packet session validation outcomes.</summary>
    public enum PacketValidationResult : byte
    {
        /// <summary>The packet is valid for the current session.</summary>
        Success,
        /// <summary>The session state does not accept the packet.</summary>
        WrongState,
        /// <summary>The packet kind is not valid for the endpoint role.</summary>
        WrongRole,
        /// <summary>The packet belongs to another session epoch.</summary>
        WrongEpoch,
        /// <summary>The packet sequence is invalid.</summary>
        Sequence,
        /// <summary>The reliable transaction packet was already received.</summary>
        Duplicate
    }

    /// <summary>Owns one immutable validated command payload.</summary>
    public struct NetworkCommandEnvelope : IDisposable
    {
        private NetworkBufferLease _payload;

        internal NetworkCommandEnvelope(ConnectionId connection, uint peer, uint epoch, uint sequence, uint targetTick, NetworkTypeId typeId, byte version, NetworkBufferLease payload)
        { Connection = connection; PeerId = peer; Epoch = epoch; Sequence = sequence; TargetTick = targetTick; TypeId = typeId; Version = version; _payload = payload ?? throw new ArgumentNullException(nameof(payload)); }
        /// <summary>Gets transport ownership.</summary>
        public ConnectionId Connection { get; }
        /// <summary>Gets trusted admitted peer.</summary>
        public uint PeerId { get; }
        /// <summary>Gets session epoch.</summary>
        public uint Epoch { get; }
        /// <summary>Gets per-peer sequence.</summary>
        public uint Sequence { get; }
        /// <summary>Gets target server tick.</summary>
        public uint TargetTick { get; }
        /// <summary>Gets generated command id.</summary>
        public NetworkTypeId TypeId { get; }
        /// <summary>Gets command hook version.</summary>
        public byte Version { get; }
        /// <summary>Gets immutable exact payload.</summary>
        public ReadOnlyMemory<byte> Payload => _payload?.Memory ?? ReadOnlyMemory<byte>.Empty;
        internal byte[] ExactBuffer => _payload?.Buffer;
        internal int ExactOffset => _payload?.Offset ?? 0;
        internal int ExactLength => _payload?.Length ?? 0;

        /// <inheritdoc />
        public void Dispose()
        {
            _payload?.Dispose();
            _payload = null;
        }
    }
}
