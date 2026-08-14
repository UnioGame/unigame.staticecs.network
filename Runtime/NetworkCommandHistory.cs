namespace UniGame.StaticEcs.Network
{
    /// <summary>Stores one typed command together with its client sequence and target tick.</summary>
    public readonly struct NetworkCommandFrame<TCommand>
        where TCommand : struct, INetworkCommand
    {
        /// <summary>Creates an immutable command frame.</summary>
        public NetworkCommandFrame(uint tick, uint sequence, in TCommand command)
        {
            Tick = tick;
            Sequence = sequence;
            Command = command;
        }

        /// <summary>Gets the target authoritative tick.</summary>
        public uint Tick { get; }

        /// <summary>Gets the monotonic client command sequence.</summary>
        public uint Sequence { get; }

        /// <summary>Gets the typed command value.</summary>
        public TCommand Command { get; }
    }

}
