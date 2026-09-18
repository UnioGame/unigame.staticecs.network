using System;

namespace UniGame.StaticEcs.Network
{
    /// <summary>
    /// Optional capability (NCORE-14) for a replicated component type: encode a changed
    /// record's canonical payload bytes relative to the same record's baseline canonical
    /// payload bytes instead of sending the full new payload. A component opts in by
    /// implementing this interface alongside its existing Static ECS Write/Read hooks; no
    /// registration or source-generator change is required (see
    /// <see cref="NetworkCompilerSchemaFactory{TWorld}.Component{T}"/>).
    /// </summary>
    /// <remarks>
    /// Both methods operate purely on the bytes a schema's Write hook already produces (the
    /// record payload, i.e. everything after the typeId/kind/version/disabled/length header)
    /// -- they have no dependency on the component's CLR type or on <c>TWorld</c>. This lets
    /// the codec (which is itself type-agnostic, see <c>SnapshotDeltaCodec</c>) invoke a hook
    /// found by wire type id alone, without any generic dispatch on the hot path.
    /// </remarks>
    public interface INetworkComponentDelta
    {
        /// <summary>
        /// Attempts to write a value delta of <paramref name="targetPayload"/> relative to
        /// <paramref name="baselinePayload"/> into <paramref name="destination"/>.
        /// </summary>
        /// <returns>
        /// The number of bytes written into <paramref name="destination"/>, or a negative
        /// number when this hook cannot represent the change and the caller must fall back to
        /// sending the full payload. Returning a length not strictly smaller than
        /// <paramref name="targetPayload"/>.Length is treated the same as a fallback by the
        /// caller (it never spends the delta path to grow the wire size).
        /// </returns>
        int TryWriteValueDelta(ReadOnlySpan<byte> baselinePayload,
            ReadOnlySpan<byte> targetPayload, Span<byte> destination);

        /// <summary>
        /// Reconstructs the exact target payload bytes from baseline payload bytes and a delta
        /// previously produced by <see cref="TryWriteValueDelta"/>. Must reproduce the
        /// canonical bytes exactly (bit-for-bit) for every delta it accepts.
        /// </summary>
        /// <returns>
        /// <c>true</c> and the exact reconstructed byte count in <paramref name="written"/> on
        /// success; <c>false</c> on any malformed or inconsistent input (the caller treats this
        /// as a corrupt/incompatible delta and rejects the whole snapshot).
        /// </returns>
        bool TryReadValueDelta(ReadOnlySpan<byte> baselinePayload, ReadOnlySpan<byte> delta,
            Span<byte> targetPayload, out int written);
    }

    /// <summary>
    /// Immutable per-schema lookup from a generated wire type id to its optional value-delta
    /// hook (NCORE-14). Built once when a schema is frozen; never mutated afterward, so it is
    /// safe to share across the tick loop without locking.
    /// </summary>
    public sealed class NetworkComponentDeltaHooks
    {
        /// <summary>The lookup for a schema with no delta-capable component.</summary>
        public static readonly NetworkComponentDeltaHooks Empty =
            new NetworkComponentDeltaHooks(Array.Empty<uint>(),
                Array.Empty<INetworkComponentDelta>());

        private readonly uint[] _typeIds;
        private readonly INetworkComponentDelta[] _hooks;

        internal NetworkComponentDeltaHooks(uint[] typeIds, INetworkComponentDelta[] hooks)
        {
            _typeIds = typeIds;
            _hooks = hooks;
        }

        /// <summary>Finds the delta hook registered for a wire type id, if any.</summary>
        public bool TryGet(uint typeId, out INetworkComponentDelta hook)
        {
            var index = Array.BinarySearch(_typeIds, typeId);
            if (index >= 0)
            {
                hook = _hooks[index];
                return true;
            }
            hook = null;
            return false;
        }
    }
}
