using System;
using FFS.Libraries.StaticEcs;
using FFS.Libraries.StaticPack;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Marks a concrete Shared Static ECS wire type.</summary>
    public interface INetworkType { }

    /// <summary>Marks a concrete Shared Static ECS command event.</summary>
    public interface INetworkCommand { }

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
        public NetworkEndpointAttribute(string name, Type worldType, NetworkRole role)
        {
            Name = name ?? throw new ArgumentNullException(nameof(name));
            WorldType = worldType ?? throw new ArgumentNullException(nameof(worldType));
            Role = role;
        }

        /// <summary>Gets the generated endpoint identifier.</summary>
        public string Name { get; }
        /// <summary>Gets the closed Static ECS world type.</summary>
        public Type WorldType { get; }
        /// <summary>Gets the endpoint role.</summary>
        public NetworkRole Role { get; }
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

    /// <summary>Identifies an endpoint role.</summary>
    public enum NetworkRole : byte
    {
        /// <summary>Produces commands and applies authoritative snapshots.</summary>
        Client = 1,
        /// <summary>Authorizes commands and produces authoritative snapshots.</summary>
        Server = 2
    }

    /// <summary>Stores server-assigned ownership. Client input never writes this value.</summary>
    public struct NetworkOwnerComponent : IComponent
    {
        /// <summary>Gets or sets the trusted peer identifier.</summary>
        public uint PeerId;

        /// <summary>Writes trusted ownership through the normal Static ECS hook.</summary>
        public void Write<TWorld>(ref BinaryPackWriter writer, World<TWorld>.Entity self) where TWorld : struct, IWorldType => writer.WriteUint(PeerId);

        /// <summary>Reads trusted ownership through the normal Static ECS hook.</summary>
        public void Read<TWorld>(ref BinaryPackReader reader, World<TWorld>.Entity self, byte version, bool disabled) where TWorld : struct, IWorldType
        {
            PeerId = reader.ReadUint();
            self.Set(this);
        }
    }

    /// <summary>Contains trusted command ordering and authorization data.</summary>
    public readonly struct NetworkCommandContext
    {
        /// <summary>Creates a trusted command context.</summary>
        public NetworkCommandContext(uint peerId, uint epoch, uint sequence, uint targetTick)
        { PeerId = peerId; Epoch = epoch; Sequence = sequence; TargetTick = targetTick; }
        /// <summary>Gets the admitted peer.</summary>
        public uint PeerId { get; }
        /// <summary>Gets the admitted session epoch.</summary>
        public uint Epoch { get; }
        /// <summary>Gets the per-peer sequence.</summary>
        public uint Sequence { get; }
        /// <summary>Gets the authoritative target tick.</summary>
        public uint TargetTick { get; }
    }

    /// <summary>Reports an accepted typed command to server systems.</summary>
    public struct NetworkCommandAccepted<TCommand> : IEvent where TCommand : struct, IEvent, INetworkCommand
    {
        /// <summary>Gets or sets the decoded command.</summary>
        public TCommand Command;
        /// <summary>Gets or sets trusted context.</summary>
        public NetworkCommandContext Context;
    }

    /// <summary>Reports a policy-rejected typed command to server systems.</summary>
    public struct NetworkCommandRejected<TCommand> : IEvent where TCommand : struct, IEvent, INetworkCommand
    {
        /// <summary>Gets or sets the decoded command.</summary>
        public TCommand Command;
        /// <summary>Gets or sets trusted context.</summary>
        public NetworkCommandContext Context;
    }
}
