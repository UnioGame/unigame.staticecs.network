using System;
using FFS.Libraries.StaticEcs;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Describes the result of dispatching one staged typed command.</summary>
    public enum DispatchResult : byte
    {
        /// <summary>The command was authorized and delivered to a receiver.</summary>
        Accepted = 0,
        /// <summary>The command was rejected and the rejection was delivered to a receiver.</summary>
        Rejected = 1,
        /// <summary>The selected result event was registered but had no receiver.</summary>
        NoReceiver = 2,
        /// <summary>One or both required closed generic result event types were not registered.</summary>
        ConfigurationError = 3,
        /// <summary>The staged payload is not a command batch.</summary>
        WrongPayload = 4,
        /// <summary>The staged payload was validated by a different schema.</summary>
        SchemaMismatch = 5,
        /// <summary>The index, command binding, version, bounds, or encoded command is invalid.</summary>
        InvalidCommand = 6
    }

    /// <summary>Dispatches schema-bound commands into one Static ECS world through retained typed invokers.</summary>
    public sealed class CommandDispatcher<TWorld> where TWorld : struct, IWorldType
    {
        private readonly Schema _schema;

        /// <summary>Creates a dispatcher for an immutable network schema.</summary>
        public CommandDispatcher(Schema schema)
        {
            if (schema == null) throw new ArgumentNullException(nameof(schema));
            schema.EnsureWorld<TWorld>();
            if (World<TWorld>.Status != WorldStatus.Initialized)
                throw new InvalidOperationException($"World `{typeof(TWorld).FullName}` must be initialized before command dispatch is configured.");
            var entries = schema.RetainedEntries;
            for (var i = 0; i < entries.Length; i++)
            {
                var entry = entries[i];
                if (entry.Kind != SchemaKind.Command) continue;
                if (entry.CommandInvoker == null || !entry.CommandInvoker.HasRegisteredResultEvents)
                    throw new InvalidOperationException($"Command `{entry.RuntimeType.FullName}` requires both accepted and rejected result event registrations.");
            }
            _schema = schema;
        }

        /// <summary>Validates, authorizes, and emits one command using the trusted endpoint peer identity.</summary>
        public DispatchResult Dispatch(StagedPayload commands, int index, uint peerId)
        {
            if (commands == null || commands.Kind != PacketKind.CommandBatch) return DispatchResult.WrongPayload;
            if (commands.SchemaHash != _schema.Hash) return DispatchResult.SchemaMismatch;
            if (!commands.IsActive) return DispatchResult.InvalidCommand;
            var staged = commands.Commands;
            if ((uint)index >= (uint)staged.Length) return DispatchResult.InvalidCommand;
            ref readonly var command = ref staged[index];
            var payloadLength = commands.Payload.Length;
            if (!_schema.TryGet(command.TypeId, out var entry) || entry.Kind != SchemaKind.Command ||
                entry.Version != command.Version || entry.CommandInvoker == null || command.Sequence == 0 ||
                command.Offset < 0 || command.Length < 0 || command.Offset > payloadLength ||
                command.Length > payloadLength - command.Offset || command.Length > entry.MaxPayload)
                return DispatchResult.InvalidCommand;
            var payload = commands.GetPayload(command);
            var context = new CommandContext(peerId, command.Sequence, command.ClientTick);
            return entry.CommandInvoker.Dispatch(payload, in context);
        }
    }
}
