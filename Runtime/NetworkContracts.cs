using System;
using FFS.Libraries.StaticEcs;
using FFS.Libraries.StaticPack;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Marks a concrete Shared Static ECS wire type.</summary>
    public interface INetworkType { }

    /// <summary>Marks a concrete Shared Static ECS command event.</summary>
    public interface INetworkCommand { }

    /// <summary>Marks a reliable, one-shot transaction command.</summary>
    public interface INetworkTransactionCommand : INetworkCommand { }

    /// <summary>Identifies one transaction within a connection epoch.</summary>
    public readonly struct NetworkTransactionId : IEquatable<NetworkTransactionId>, IComparable<NetworkTransactionId>
    {
        private readonly ulong _value;

        /// <summary>Creates a non-zero transaction identifier.</summary>
        public NetworkTransactionId(ulong value)
        {
            if (value == 0) throw new ArgumentOutOfRangeException(nameof(value));
            _value = value;
        }

        /// <summary>Gets the identifier value.</summary>
        public ulong Value => _value;

        /// <inheritdoc />
        public bool Equals(NetworkTransactionId other) => _value == other._value;
        /// <inheritdoc />
        public override bool Equals(object obj) => obj is NetworkTransactionId other && Equals(other);
        /// <inheritdoc />
        public override int GetHashCode() => _value.GetHashCode();
        /// <inheritdoc />
        public int CompareTo(NetworkTransactionId other) => _value.CompareTo(other._value);
        /// <inheritdoc />
        public override string ToString() => _value.ToString();
        /// <summary>Tests identifier equality.</summary>
        public static bool operator ==(NetworkTransactionId left, NetworkTransactionId right) => left.Equals(right);
        /// <summary>Tests identifier inequality.</summary>
        public static bool operator !=(NetworkTransactionId left, NetworkTransactionId right) => !left.Equals(right);
    }

    /// <summary>Reports the terminal outcome of one reliable transaction.</summary>
    public enum NetworkTransactionStatus : byte
    {
        Applied = 0,
        PolicyRejected = 1,
        GameplayRejected = 2,
        Unhandled = 3,
        SessionLost = 4,
        SubmissionFailed = 5
    }

    /// <summary>Identifies the command delivery channel.</summary>
    public enum NetworkCommandDelivery : byte
    {
        Input = 0,
        Transaction = 1
    }

    /// <summary>Authorizes one decoded command using trusted server context.</summary>
    public interface INetworkCommandPolicy<TWorld, TCommand>
        where TWorld : struct, IWorldType
        where TCommand : struct, IEvent, INetworkCommand
    {
        /// <summary>Returns whether the trusted peer may dispatch the command.</summary>
        bool Authorize(in NetworkCommandContext context, in TCommand command);
    }

    /// <summary>Declares a generated endpoint in an assembly.</summary>
    [AttributeUsage(AttributeTargets.Assembly, AllowMultiple = true)]
    public sealed class NetworkEndpointAttribute : Attribute
    {
        /// <summary>Creates an endpoint declaration.</summary>
        public NetworkEndpointAttribute(string name, Type worldType, NetworkRole role,
            params Type[] rootTypes)
        {
            Name = name ?? throw new ArgumentNullException(nameof(name));
            WorldType = worldType ?? throw new ArgumentNullException(nameof(worldType));
            Role = role;
            RootTypes = rootTypes ?? Array.Empty<Type>();
        }

        /// <summary>Gets the generated endpoint identifier.</summary>
        public string Name { get; }
        /// <summary>Gets the closed Static ECS world type.</summary>
        public Type WorldType { get; }
        /// <summary>Gets the endpoint role.</summary>
        public NetworkRole Role { get; }
        /// <summary>Gets the explicit root types whose assemblies contribute wire contracts.</summary>
        public Type[] RootTypes { get; }
    }

    /// <summary>Marks a generated world-neutral manifest for compiler aggregation.</summary>
    [AttributeUsage(AttributeTargets.Class)]
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    public sealed class NetworkManifestAttribute : Attribute { }

    /// <summary>Publishes one generated world-neutral manifest record as assembly metadata.</summary>
    [AttributeUsage(AttributeTargets.Assembly, AllowMultiple = true)]
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    public sealed class NetworkManifestRecordAttribute : Attribute
    {
        /// <summary>Creates compiler-readable manifest metadata.</summary>
        public NetworkManifestRecordAttribute(uint typeId, NetworkSchemaKind kind, Type runtimeType, byte version = 0)
        { Id = typeId; Kind = kind; RuntimeType = runtimeType; Version = version; }
        /// <summary>Gets generated id.</summary>
        public uint Id { get; }
        /// <summary>Gets wire shape.</summary>
        public NetworkSchemaKind Kind { get; }
        /// <summary>Gets concrete Shared type.</summary>
        public Type RuntimeType { get; }
        /// <summary>Gets hook version.</summary>
        public byte Version { get; }
    }

    /// <summary>Publishes one generated server command policy as assembly metadata.</summary>
    [AttributeUsage(AttributeTargets.Assembly, AllowMultiple = true)]
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    public sealed class NetworkCommandPolicyManifestRecordAttribute : Attribute
    {
        /// <summary>Creates compiler-readable policy metadata.</summary>
        public NetworkCommandPolicyManifestRecordAttribute(Type worldType,
            Type commandType, Type policyType)
        {
            WorldType = worldType ?? throw new ArgumentNullException(nameof(worldType));
            CommandType = commandType ?? throw new ArgumentNullException(nameof(commandType));
            PolicyType = policyType ?? throw new ArgumentNullException(nameof(policyType));
        }

        public Type WorldType { get; }
        public Type CommandType { get; }
        public Type PolicyType { get; }
    }

    /// <summary>Identifies an endpoint role.</summary>
    public enum NetworkRole : byte
    {
        /// <summary>Produces commands and applies authoritative snapshots.</summary>
        Client = 1,
        /// <summary>Authorizes commands and produces authoritative snapshots.</summary>
        Server = 2
    }

    /// <summary>Stores server-assigned ownership. Client commands never write this value.</summary>
    public struct NetworkOwnerComponent : IComponent, INetworkType
    {
        /// <summary>Gets or sets the trusted peer identifier.</summary>
        public uint PeerId;

        /// <summary>Writes trusted ownership through the normal Static ECS hook.</summary>
        public void Write<TWorld>(ref BinaryPackWriter writer, World<TWorld>.Entity self) where TWorld : struct, IWorldType => writer.WriteUint(PeerId);

        /// <summary>Reads trusted ownership through the normal Static ECS hook.</summary>
        public void Read<TWorld>(ref BinaryPackReader reader, World<TWorld>.Entity self, byte version, bool disabled) where TWorld : struct, IWorldType
        {
            PeerId = reader.ReadUint();
        }
    }

    /// <summary>Links one local replica to its authoritative entity identity.</summary>
    public struct NetworkReplicaIdentityComponent : IComponent
    {
        /// <summary>Authoritative source entity identifier.</summary>
        public EntityGID AuthorityGid;

        /// <summary>Generated network entity-kind identifier.</summary>
        public NetworkTypeId KindId;
    }

    /// <summary>Contains trusted command ordering and authorization data.</summary>
    public readonly struct NetworkCommandContext
    {
        /// <summary>Creates a trusted command context.</summary>
        public NetworkCommandContext(uint peerId, uint epoch, uint sequence, uint targetTick)
            : this(peerId, epoch, sequence, targetTick, NetworkCommandDelivery.Input, default) { }

        /// <summary>Creates a command context with its delivery channel and generated type.</summary>
        public NetworkCommandContext(uint peerId, uint epoch, uint sequence, uint targetTick,
            NetworkCommandDelivery delivery, NetworkTypeId typeId)
            : this(peerId, epoch, sequence, targetTick, delivery, typeId, default) { }

        /// <summary>Creates a command context including a reliable transaction identity.</summary>
        public NetworkCommandContext(uint peerId, uint epoch, uint sequence, uint targetTick,
            NetworkCommandDelivery delivery, NetworkTypeId typeId,
            NetworkTransactionId transactionId)
        { PeerId = peerId; Epoch = epoch; Sequence = sequence; TargetTick = targetTick; Delivery = delivery; TypeId = typeId; TransactionId = transactionId; }
        /// <summary>Gets the admitted peer.</summary>
        public uint PeerId { get; }
        /// <summary>Gets the admitted session epoch.</summary>
        public uint Epoch { get; }
        /// <summary>Gets the per-peer sequence.</summary>
        public uint Sequence { get; }
        /// <summary>Gets the authoritative target tick.</summary>
        public uint TargetTick { get; }
        /// <summary>Gets the command delivery channel.</summary>
        public NetworkCommandDelivery Delivery { get; }
        /// <summary>Gets the generated command type identifier.</summary>
        public NetworkTypeId TypeId { get; }
        /// <summary>Gets the transaction identity, or the default value for input commands.</summary>
        public NetworkTransactionId TransactionId { get; }
    }

    /// <summary>Reports an accepted typed command to server systems.</summary>
    public struct NetworkCommandAcceptedEvent<TCommand> : IEvent where TCommand : struct, IEvent, INetworkCommand
    {
        /// <summary>Gets or sets the decoded command.</summary>
        public TCommand Command;
        /// <summary>Gets or sets trusted context.</summary>
        public NetworkCommandContext Context;
    }

    /// <summary>Reports a policy-rejected typed command to server systems.</summary>
    public struct NetworkCommandRejectedEvent<TCommand> : IEvent where TCommand : struct, IEvent, INetworkCommand
    {
        /// <summary>Gets or sets the decoded command.</summary>
        public TCommand Command;
        /// <summary>Gets or sets trusted context.</summary>
        public NetworkCommandContext Context;
    }

    /// <summary>Reports a transaction submitted to the reliable command channel.</summary>
    public struct NetworkTransactionSubmittedEvent<TCommand> : IEvent
        where TCommand : struct, IEvent, INetworkTransactionCommand
    {
        /// <summary>Gets or sets the submitted command.</summary>
        public TCommand Command;
        /// <summary>Gets or sets the transaction identifier.</summary>
        public NetworkTransactionId TransactionId;
        /// <summary>Gets or sets the trusted command context.</summary>
        public NetworkCommandContext Context;
    }

    /// <summary>Reports a terminal transaction receipt.</summary>
    public struct NetworkTransactionResultEvent<TCommand> : IEvent
        where TCommand : struct, IEvent, INetworkTransactionCommand
    {
        /// <summary>Gets or sets the submitted command.</summary>
        public TCommand Command;
        /// <summary>Gets or sets the transaction identifier.</summary>
        public NetworkTransactionId TransactionId;
        /// <summary>Gets or sets the terminal status.</summary>
        public NetworkTransactionStatus Status;
        /// <summary>Gets or sets the trusted command context.</summary>
        public NetworkCommandContext Context;
    }

    /// <summary>Requests completion of a dispatched transaction by gameplay.</summary>
    public struct CompleteNetworkTransactionRequest : IEvent
    {
        /// <summary>Gets or sets the admitted peer that owns the transaction.</summary>
        public uint PeerId;
        /// <summary>Gets or sets the connection epoch that owns the transaction.</summary>
        public uint Epoch;
        /// <summary>Gets or sets the transaction identifier.</summary>
        public NetworkTransactionId TransactionId;
        /// <summary>Gets or sets the gameplay outcome.</summary>
        public NetworkTransactionStatus Status;
    }
}
