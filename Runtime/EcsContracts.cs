using FFS.Libraries.StaticEcs;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Provides trusted endpoint metadata to command authorization.</summary>
    public readonly struct CommandContext
    {
        /// <summary>Creates a trusted command context.</summary>
        public CommandContext(uint peerId, uint sequence, uint clientTick) { PeerId = peerId; Sequence = sequence; ClientTick = clientTick; }
        /// <summary>Gets the peer identity supplied by the transport endpoint.</summary>
        public uint PeerId { get; }
        /// <summary>Gets the ordered command sequence.</summary>
        public uint Sequence { get; }
        /// <summary>Gets the originating client tick.</summary>
        public uint ClientTick { get; }
    }

    /// <summary>Authorizes one typed command using trusted endpoint context.</summary>
    public interface ICommandAuthorizer<TWorld, TCommand> where TWorld : struct, IWorldType where TCommand : unmanaged
    {
        /// <summary>Returns whether the command may enter ECS staging.</summary>
        bool Authorize(in CommandContext context, in TCommand command);
    }

    /// <summary>Marks a command that targets a persistent entity identity.</summary>
    public interface ITargetCommand
    {
        /// <summary>Gets the target entity identity.</summary>
        EntityGID Target { get; }
    }

    /// <summary>Authorizes commands only when the trusted peer owns the target entity.</summary>
    public readonly struct OwnerAuthorizer<TWorld, TCommand> : ICommandAuthorizer<TWorld, TCommand>
        where TWorld : struct, IWorldType where TCommand : unmanaged, ITargetCommand
    {
        /// <inheritdoc />
        public bool Authorize(in CommandContext context, in TCommand command)
        {
            if (!command.Target.TryUnpack<TWorld>(out var entity) || !entity.Has<PeerOwnerComponent>()) return false;
            return entity.Read<PeerOwnerComponent>().PeerId == context.PeerId;
        }
    }

    /// <summary>Requests transmission of one typed local command.</summary>
    public struct SendCommandEvent<T> : IEvent where T : unmanaged
    {
        /// <summary>Gets or sets the command.</summary>
        public T Command;
        /// <summary>Gets or sets the originating client tick.</summary>
        public uint ClientTick;
    }

    /// <summary>Reports one accepted typed command.</summary>
    public struct CommandAcceptedEvent<T> : IEvent where T : unmanaged
    {
        /// <summary>Gets or sets the accepted command.</summary>
        public T Command;
        /// <summary>Gets or sets trusted command context.</summary>
        public CommandContext Context;
    }

    /// <summary>Reports one rejected typed command.</summary>
    public struct CommandRejectedEvent<T> : IEvent where T : unmanaged
    {
        /// <summary>Gets or sets the rejected command.</summary>
        public T Command;
        /// <summary>Gets or sets trusted command context.</summary>
        public CommandContext Context;
    }

    /// <summary>Marks an entity included in network replication.</summary>
    public struct ReplicatedTag : ITag { }

    /// <summary>Stores the trusted peer that owns an entity.</summary>
    public struct PeerOwnerComponent : IComponent
    {
        /// <summary>Gets or sets the owning peer identifier.</summary>
        public uint PeerId;
    }
}
