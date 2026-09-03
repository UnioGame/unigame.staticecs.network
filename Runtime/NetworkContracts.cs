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

        public ulong Value => _value;

        public bool Equals(NetworkTransactionId other) => _value == other._value;
        public override bool Equals(object obj) => obj is NetworkTransactionId other && Equals(other);
        public override int GetHashCode() => _value.GetHashCode();
        public int CompareTo(NetworkTransactionId other) => _value.CompareTo(other._value);
        public override string ToString() => _value.ToString();
        public static bool operator ==(NetworkTransactionId left, NetworkTransactionId right) => left.Equals(right);
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
        public NetworkEndpointAttribute(string name, Type worldType, NetworkRole role,
            params Type[] rootTypes)
        {
            Name = name ?? throw new ArgumentNullException(nameof(name));
            WorldType = worldType ?? throw new ArgumentNullException(nameof(worldType));
            Role = role;
            RootTypes = rootTypes ?? Array.Empty<Type>();
        }

        public string Name { get; }
        public Type WorldType { get; }
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
        public NetworkManifestRecordAttribute(uint typeId, NetworkSchemaKind kind, Type runtimeType, byte version = 0)
        { Id = typeId; Kind = kind; RuntimeType = runtimeType; Version = version; }
        public uint Id { get; }
        public NetworkSchemaKind Kind { get; }
        public Type RuntimeType { get; }
        public byte Version { get; }
    }

    /// <summary>Publishes one generated server command policy as assembly metadata.</summary>
    [AttributeUsage(AttributeTargets.Assembly, AllowMultiple = true)]
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    public sealed class NetworkCommandPolicyManifestRecordAttribute : Attribute
    {
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
        Client = 1,
        Server = 2
    }

    /// <summary>Stores server-assigned ownership. Client commands never write this value.</summary>
    public struct NetworkOwnerComponent : IComponent, INetworkType
    {
        public uint PeerId;

        public void Write<TWorld>(ref BinaryPackWriter writer, World<TWorld>.Entity self) where TWorld : struct, IWorldType => writer.WriteUint(PeerId);

        public void Read<TWorld>(ref BinaryPackReader reader, World<TWorld>.Entity self, byte version, bool disabled) where TWorld : struct, IWorldType
        {
            PeerId = reader.ReadUint();
        }
    }

    /// <summary>Links one local replica to its authoritative entity identity.</summary>
    public struct NetworkReplicaIdentityComponent : IComponent
    {
        public EntityGID AuthorityGid;

        public NetworkTypeId KindId;
    }

    /// <summary>Contains trusted command ordering and authorization data.</summary>
    public readonly struct NetworkCommandContext
    {
        public NetworkCommandContext(uint peerId, uint epoch, uint sequence, uint targetTick)
            : this(peerId, epoch, sequence, targetTick, NetworkCommandDelivery.Input, default) { }

        public NetworkCommandContext(uint peerId, uint epoch, uint sequence, uint targetTick,
            NetworkCommandDelivery delivery, NetworkTypeId typeId)
            : this(peerId, epoch, sequence, targetTick, delivery, typeId, default) { }

        public NetworkCommandContext(uint peerId, uint epoch, uint sequence, uint targetTick,
            NetworkCommandDelivery delivery, NetworkTypeId typeId,
            NetworkTransactionId transactionId)
        { PeerId = peerId; Epoch = epoch; Sequence = sequence; TargetTick = targetTick; Delivery = delivery; TypeId = typeId; TransactionId = transactionId; }
        public uint PeerId { get; }
        public uint Epoch { get; }
        public uint Sequence { get; }
        public uint TargetTick { get; }
        public NetworkCommandDelivery Delivery { get; }
        public NetworkTypeId TypeId { get; }
        /// <summary>Gets the transaction identity, or the default value for input commands.</summary>
        public NetworkTransactionId TransactionId { get; }
    }

    /// <summary>Reports an accepted typed command to server systems.</summary>
    public struct NetworkCommandAcceptedEvent<TCommand> : IEvent where TCommand : struct, IEvent, INetworkCommand
    {
        public TCommand Command;
        public NetworkCommandContext Context;
    }

    /// <summary>Reports a policy-rejected typed command to server systems.</summary>
    public struct NetworkCommandRejectedEvent<TCommand> : IEvent where TCommand : struct, IEvent, INetworkCommand
    {
        public TCommand Command;
        public NetworkCommandContext Context;
    }

    /// <summary>Reports a transaction submitted to the reliable command channel.</summary>
    public struct NetworkTransactionSubmittedEvent<TCommand> : IEvent
        where TCommand : struct, IEvent, INetworkTransactionCommand
    {
        public TCommand Command;
        public NetworkTransactionId TransactionId;
        public NetworkCommandContext Context;
    }

    /// <summary>Reports a terminal transaction receipt.</summary>
    public struct NetworkTransactionResultEvent<TCommand> : IEvent
        where TCommand : struct, IEvent, INetworkTransactionCommand
    {
        public TCommand Command;
        public NetworkTransactionId TransactionId;
        public NetworkTransactionStatus Status;
        public NetworkCommandContext Context;
    }

    /// <summary>Requests completion of a dispatched transaction by gameplay.</summary>
    public struct CompleteNetworkTransactionRequest : IEvent
    {
        public uint PeerId;
        public uint Epoch;
        public NetworkTransactionId TransactionId;
        public NetworkTransactionStatus Status;
    }
}
