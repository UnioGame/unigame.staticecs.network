using System;
using FFS.Libraries.StaticEcs;

namespace UniGame.StaticEcs.Network
{
    internal interface ISchemaInvoker
    {
        Type RuntimeType { get; }
    }

    internal interface IEntityInvoker : ISchemaInvoker
    {
    }

    internal interface IEntityInvoker<TWorld> : IEntityInvoker where TWorld : struct, IWorldType
    {
        byte EntityTypeId { get; }
        bool Matches(World<TWorld>.Entity entity);
        World<TWorld>.Entity Create(EntityGID entity);
    }

    internal interface IRecordInvoker<TWorld> : IEntryCodec where TWorld : struct, IWorldType
    {
        CaptureRecordResult Capture(World<TWorld>.Entity entity, SchemaEntry entry, ref SnapshotWriter writer, ref CaptureContext context);
        ApplyResult Preflight(ReadOnlySpan<byte> payload, uint count, RecordFlags flags, ref ApplyContext context);
        void Apply(World<TWorld>.Entity entity, ReadOnlySpan<byte> payload, uint count);
        void Remove(World<TWorld>.Entity entity);
        void Normalize(World<TWorld>.Entity entity, RecordFlags flags);
    }

    internal interface IEntryCodec : ISchemaInvoker
    {
        bool Validate(ReadOnlySpan<byte> payload, uint count);
    }

    internal interface ICommandInvoker : IEntryCodec
    {
        Type AuthorizerType { get; }
        bool HasRegisteredResultEvents { get; }
        DispatchResult Dispatch(ReadOnlySpan<byte> payload, in CommandContext context);
    }

    internal interface ICommandInvoker<T> : ICommandInvoker where T : unmanaged
    {
        bool TryWrite(in T command, Span<byte> destination, out int written);
        bool TryAuthorize(ReadOnlySpan<byte> payload, in CommandContext context, out T command);
    }

    internal sealed class EntityInvoker<TWorld, TEntityType> : IEntityInvoker<TWorld>
        where TWorld : struct, IWorldType
        where TEntityType : unmanaged, IEntityType
    {
        public Type RuntimeType => typeof(TEntityType);
        public byte EntityTypeId => default(TEntityType).Id();

        public bool Matches(World<TWorld>.Entity entity) => entity.EntityType == EntityTypeId;

        public World<TWorld>.Entity Create(EntityGID entity) => World<TWorld>.NewEntityByGID<TEntityType>(entity);
    }

    internal sealed class ComponentInvoker<TWorld, T, TCodec> : IRecordInvoker<TWorld>
        where TWorld : struct, IWorldType
        where T : unmanaged, IComponent
        where TCodec : struct, ICodec<T>
    {
        private readonly bool _disableable = typeof(IDisableable).IsAssignableFrom(typeof(T));
        public Type RuntimeType => typeof(T);

        public bool Validate(ReadOnlySpan<byte> payload, uint count)
        {
            var codec = default(TCodec);
            return count == 1 && codec.TryRead(payload, out _, out var read) && read == payload.Length;
        }

        public CaptureRecordResult Capture(World<TWorld>.Entity entity, SchemaEntry entry, ref SnapshotWriter writer, ref CaptureContext context)
        {
            if (!entity.Has<T>()) return CaptureRecordResult.Absent;
            var flags = _disableable && World<TWorld>.Components<T>.Instance.HasDisabled(entity) ? RecordFlags.Disabled : 0;
            writer.BeginRecord(entry, 1, flags, out var lengthOffset, out var payloadOffset);
            var codec = default(TCodec);
            var maximum = (int)entry.MaxPayload;
            var destination = writer.Writable(maximum);
            if (!codec.TryWrite(in entity.Read<T>(), destination, out var written) || written < 0 || written > destination.Length)
                return !writer.Valid || destination.Length < maximum ? CaptureRecordResult.LimitExceeded : CaptureRecordResult.CodecFailed;
            if (!writer.Advance(written)) return CaptureRecordResult.LimitExceeded;
            writer.EndRecord(lengthOffset, payloadOffset);
            return writer.Valid ? CaptureRecordResult.Written : CaptureRecordResult.LimitExceeded;
        }

        public ApplyResult Preflight(ReadOnlySpan<byte> payload, uint count, RecordFlags flags, ref ApplyContext context) =>
            flags == RecordFlags.Disabled && !_disableable ? ApplyResult.InvalidEntity : ApplyResult.Success;

        public void Apply(World<TWorld>.Entity entity, ReadOnlySpan<byte> payload, uint count)
        {
            var codec = default(TCodec);
            if (!codec.TryRead(payload, out var value, out var read) || read != payload.Length) throw new InvalidOperationException("A preflighted component codec failed during apply.");
            entity.Set(value);
        }

        public void Remove(World<TWorld>.Entity entity) { if (entity.Has<T>()) entity.Delete<T>(); }

        public void Normalize(World<TWorld>.Entity entity, RecordFlags flags)
        {
            if (!_disableable) return;
            if (flags == RecordFlags.Disabled) World<TWorld>.Components<T>.Instance.Disable(entity);
            else World<TWorld>.Components<T>.Instance.Enable(entity);
        }
    }

    internal sealed class TagInvoker<TWorld, T> : IRecordInvoker<TWorld>
        where TWorld : struct, IWorldType
        where T : unmanaged, ITag
    {
        public Type RuntimeType => typeof(T);

        public bool Validate(ReadOnlySpan<byte> payload, uint count) => count == 0 && payload.IsEmpty;

        public CaptureRecordResult Capture(World<TWorld>.Entity entity, SchemaEntry entry, ref SnapshotWriter writer, ref CaptureContext context)
        {
            if (!entity.Has<T>()) return CaptureRecordResult.Absent;
            writer.BeginRecord(entry, 0, 0, out var lengthOffset, out var payloadOffset);
            writer.EndRecord(lengthOffset, payloadOffset);
            return writer.Valid ? CaptureRecordResult.Written : CaptureRecordResult.LimitExceeded;
        }

        public ApplyResult Preflight(ReadOnlySpan<byte> payload, uint count, RecordFlags flags, ref ApplyContext context) => ApplyResult.Success;
        public void Apply(World<TWorld>.Entity entity, ReadOnlySpan<byte> payload, uint count) => entity.Set<T>();
        public void Remove(World<TWorld>.Entity entity) { if (entity.Has<T>()) entity.Delete<T>(); }
        public void Normalize(World<TWorld>.Entity entity, RecordFlags flags) { }
    }

    internal sealed class LinkInvoker<TWorld, T> : IRecordInvoker<TWorld>
        where TWorld : struct, IWorldType
        where T : unmanaged, ILinkType
    {
        public Type RuntimeType => typeof(T);

        public bool Validate(ReadOnlySpan<byte> payload, uint count) => count == 1 && payload.Length == 8;

        public CaptureRecordResult Capture(World<TWorld>.Entity entity, SchemaEntry entry, ref SnapshotWriter writer, ref CaptureContext context)
        {
            if (!entity.Has<World<TWorld>.Link<T>>()) return CaptureRecordResult.Absent;
            if (World<TWorld>.Components<World<TWorld>.Link<T>>.Instance.HasDisabled(entity)) return CaptureRecordResult.DisabledUnsupported;
            var target = entity.Read<World<TWorld>.Link<T>>().Value;
            if (target.Version == 0 || !context.Contains(in target)) return CaptureRecordResult.MissingTarget;
            writer.BeginRecord(entry, 1, 0, out var lengthOffset, out var payloadOffset);
            writer.Entity(in target);
            writer.EndRecord(lengthOffset, payloadOffset);
            return writer.Valid ? CaptureRecordResult.Written : CaptureRecordResult.LimitExceeded;
        }

        public ApplyResult Preflight(ReadOnlySpan<byte> payload, uint count, RecordFlags flags, ref ApplyContext context)
        {
            if (!RecordInvokerUtility.TryReadEntity(payload, 0, out var target)) return ApplyResult.InvalidEntity;
            return context.Contains(in target) ? ApplyResult.Success : ApplyResult.MissingTarget;
        }

        public void Apply(World<TWorld>.Entity entity, ReadOnlySpan<byte> payload, uint count)
        {
            if (!RecordInvokerUtility.TryReadEntity(payload, 0, out var target)) throw new InvalidOperationException("A preflighted link failed during apply.");
            entity.Set(new World<TWorld>.Link<T>(target));
        }

        public void Remove(World<TWorld>.Entity entity) { if (entity.Has<World<TWorld>.Link<T>>()) entity.Delete<World<TWorld>.Link<T>>(); }
        public void Normalize(World<TWorld>.Entity entity, RecordFlags flags) => World<TWorld>.Components<World<TWorld>.Link<T>>.Instance.Enable(entity);
    }

    internal sealed class LinksInvoker<TWorld, T> : IRecordInvoker<TWorld>
        where TWorld : struct, IWorldType
        where T : unmanaged, ILinksType
    {
        public Type RuntimeType => typeof(T);

        public bool Validate(ReadOnlySpan<byte> payload, uint count) => payload.Length == count * 8L;

        public CaptureRecordResult Capture(World<TWorld>.Entity entity, SchemaEntry entry, ref SnapshotWriter writer, ref CaptureContext context)
        {
            if (!entity.Has<World<TWorld>.Links<T>>()) return CaptureRecordResult.Absent;
            if (World<TWorld>.Components<World<TWorld>.Links<T>>.Instance.HasDisabled(entity)) return CaptureRecordResult.DisabledUnsupported;
            var source = entity.Read<World<TWorld>.Links<T>>().AsReadOnlySpan;
            if (source.Length > entry.MaxCount || source.Length > context.LinkScratch.Length) return CaptureRecordResult.LimitExceeded;
            var targets = context.LinkScratch.Slice(0, source.Length);
            for (var i = 0; i < source.Length; i++) targets[i] = source[i].Value;
            ReplicationSort.Sort(targets);
            for (var i = 0; i < targets.Length; i++)
            {
                if (targets[i].Version == 0 || !context.Contains(in targets[i])) return CaptureRecordResult.MissingTarget;
                if (i > 0 && targets[i] == targets[i - 1]) return CaptureRecordResult.EntityConflict;
            }
            writer.BeginRecord(entry, (uint)targets.Length, 0, out var lengthOffset, out var payloadOffset);
            for (var i = 0; i < targets.Length; i++) writer.Entity(in targets[i]);
            writer.EndRecord(lengthOffset, payloadOffset);
            return writer.Valid ? CaptureRecordResult.Written : CaptureRecordResult.LimitExceeded;
        }

        public ApplyResult Preflight(ReadOnlySpan<byte> payload, uint count, RecordFlags flags, ref ApplyContext context)
        {
            for (var i = 0; i < count; i++)
            {
                if (!RecordInvokerUtility.TryReadEntity(payload, i * 8, out var target)) return ApplyResult.InvalidEntity;
                if (!context.Contains(in target)) return ApplyResult.MissingTarget;
            }
            return ApplyResult.Success;
        }

        public void Apply(World<TWorld>.Entity entity, ReadOnlySpan<byte> payload, uint count)
        {
            if (entity.Has<World<TWorld>.Links<T>>()) entity.Delete<World<TWorld>.Links<T>>();
            ref var links = ref entity.Add<World<TWorld>.Links<T>>();
            for (var i = 0; i < count; i++)
            {
                if (!RecordInvokerUtility.TryReadEntity(payload, i * 8, out var target)) throw new InvalidOperationException("A preflighted links record failed during apply.");
                links.Add(new World<TWorld>.Link<T>(target));
            }
        }

        public void Remove(World<TWorld>.Entity entity) { if (entity.Has<World<TWorld>.Links<T>>()) entity.Delete<World<TWorld>.Links<T>>(); }
        public void Normalize(World<TWorld>.Entity entity, RecordFlags flags) => World<TWorld>.Components<World<TWorld>.Links<T>>.Instance.Enable(entity);
    }

    internal sealed class MultiInvoker<TWorld, T, TCodec> : IRecordInvoker<TWorld>
        where TWorld : struct, IWorldType
        where T : unmanaged, IMultiComponent
        where TCodec : struct, ICodec<T>
    {
        private readonly uint _maxItemBytes;

        internal MultiInvoker(uint maxItemBytes) => _maxItemBytes = maxItemBytes;

        public Type RuntimeType => typeof(T);

        public bool Validate(ReadOnlySpan<byte> payload, uint count)
        {
            var offset = 0;
            var codec = default(TCodec);
            for (var i = 0; i < count; i++)
            {
                if (offset > payload.Length - 4) return false;
                var length = Hashing.Read32(payload, offset);
                offset += 4;
                if (length > _maxItemBytes || length > payload.Length - offset ||
                    !codec.TryRead(payload.Slice(offset, (int)length), out _, out var read) || read != length)
                    return false;
                offset += (int)length;
            }
            return offset == payload.Length;
        }

        public CaptureRecordResult Capture(World<TWorld>.Entity entity, SchemaEntry entry, ref SnapshotWriter writer, ref CaptureContext context)
        {
            if (!entity.Has<World<TWorld>.Multi<T>>()) return CaptureRecordResult.Absent;
            if (World<TWorld>.Components<World<TWorld>.Multi<T>>.Instance.HasDisabled(entity)) return CaptureRecordResult.DisabledUnsupported;
            var values = entity.Read<World<TWorld>.Multi<T>>().AsReadOnlySpan;
            if (values.Length > entry.MaxCount) return CaptureRecordResult.LimitExceeded;
            writer.BeginRecord(entry, (uint)values.Length, 0, out var recordLengthOffset, out var payloadOffset);
            var codec = default(TCodec);
            for (var i = 0; i < values.Length; i++)
            {
                if (writer.Position - payloadOffset > ProtocolLimits.MaxComponentBytes - 4) return CaptureRecordResult.LimitExceeded;
                var itemLengthOffset = writer.ReserveU32();
                var itemOffset = writer.Position;
                var remaining = ProtocolLimits.MaxComponentBytes - (itemOffset - payloadOffset);
                var maximum = (int)Math.Min(_maxItemBytes, (uint)remaining);
                var destination = writer.Writable(maximum);
                if (!codec.TryWrite(in values[i], destination, out var written) || written < 0 || written > destination.Length)
                    return !writer.Valid || destination.Length < maximum ? CaptureRecordResult.LimitExceeded : CaptureRecordResult.CodecFailed;
                if (!writer.Advance(written)) return CaptureRecordResult.LimitExceeded;
                writer.PatchU32(itemLengthOffset, (uint)written);
            }
            writer.EndRecord(recordLengthOffset, payloadOffset);
            return writer.Valid ? CaptureRecordResult.Written : CaptureRecordResult.LimitExceeded;
        }

        public ApplyResult Preflight(ReadOnlySpan<byte> payload, uint count, RecordFlags flags, ref ApplyContext context) => ApplyResult.Success;

        public void Apply(World<TWorld>.Entity entity, ReadOnlySpan<byte> payload, uint count)
        {
            if (entity.Has<World<TWorld>.Multi<T>>()) entity.Delete<World<TWorld>.Multi<T>>();
            ref var values = ref entity.Add<World<TWorld>.Multi<T>>();
            var codec = default(TCodec);
            var offset = 0;
            for (var i = 0; i < count; i++)
            {
                var length = (int)Hashing.Read32(payload, offset);
                offset += 4;
                if (!codec.TryRead(payload.Slice(offset, length), out var value, out var read) || read != length)
                    throw new InvalidOperationException("A preflighted multi codec failed during apply.");
                values.Add(value);
                offset += length;
            }
        }

        public void Remove(World<TWorld>.Entity entity) { if (entity.Has<World<TWorld>.Multi<T>>()) entity.Delete<World<TWorld>.Multi<T>>(); }
        public void Normalize(World<TWorld>.Entity entity, RecordFlags flags) => World<TWorld>.Components<World<TWorld>.Multi<T>>.Instance.Enable(entity);
    }

    internal static class RecordInvokerUtility
    {
        internal static bool TryReadEntity(ReadOnlySpan<byte> payload, int offset, out EntityGID entity)
        {
            entity = default;
            if (offset < 0 || offset > payload.Length - 8) return false;
            var version = (ushort)(payload[offset + 6] | payload[offset + 7] << 8);
            if (version == 0) return false;
            entity = new EntityGID(Hashing.Read32(payload, offset), version, (ushort)(payload[offset + 4] | payload[offset + 5] << 8));
            return true;
        }
    }

    internal sealed class CommandInvoker<TWorld, T, TCodec, TAuthorizer> : ICommandInvoker<T>
        where TWorld : struct, IWorldType
        where T : unmanaged
        where TCodec : struct, ICodec<T>
        where TAuthorizer : struct, ICommandAuthorizer<TWorld, T>
    {
        public Type RuntimeType => typeof(T);
        public Type AuthorizerType => typeof(TAuthorizer);
        public bool HasRegisteredResultEvents =>
            World<TWorld>.IsEventTypeRegistered<CommandAcceptedEvent<T>>() &&
            World<TWorld>.IsEventTypeRegistered<CommandRejectedEvent<T>>();

        public bool TryWrite(in T command, Span<byte> destination, out int written)
        {
            var codec = default(TCodec);
            return codec.TryWrite(in command, destination, out written);
        }

        public bool Validate(ReadOnlySpan<byte> payload, uint count)
        {
            var codec = default(TCodec);
            return count == 1 && codec.TryRead(payload, out _, out var read) && read == payload.Length;
        }

        public bool TryAuthorize(ReadOnlySpan<byte> payload, in CommandContext context, out T command)
        {
            var codec = default(TCodec);
            if (!codec.TryRead(payload, out command, out var read) || read != payload.Length) return false;
            var authorizer = default(TAuthorizer);
            return authorizer.Authorize(in context, in command);
        }

        public DispatchResult Dispatch(ReadOnlySpan<byte> payload, in CommandContext context)
        {
            var codec = default(TCodec);
            if (!codec.TryRead(payload, out var command, out var read) || read != payload.Length)
                return DispatchResult.InvalidCommand;
            if (!HasRegisteredResultEvents)
                return DispatchResult.ConfigurationError;

            var authorizer = default(TAuthorizer);
            var accepted = authorizer.Authorize(in context, in command);
            var sent = accepted
                ? World<TWorld>.SendEvent(new CommandAcceptedEvent<T> { Command = command, Context = context })
                : World<TWorld>.SendEvent(new CommandRejectedEvent<T> { Command = command, Context = context });
            if (!sent) return DispatchResult.NoReceiver;
            return accepted ? DispatchResult.Accepted : DispatchResult.Rejected;
        }
    }
}
