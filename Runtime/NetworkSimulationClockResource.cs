namespace UniGame.StaticEcs.Network
{
    using System;
    using FFS.Libraries.StaticEcs;

    /// <summary>Provides the shared authoritative and predicted simulation timing contract.</summary>
    public sealed class NetworkSimulationClockResource : IResource
    {
        /// <summary>Creates validated simulation timing settings.</summary>
        public NetworkSimulationClockResource(
            int ticksPerSecond,
            int predictionHistoryTicks = 64,
            int interpolationDelayTicks = 2,
            int commandRedundancy = 3,
            float maxResimulationMilliseconds = 2f)
            : this(new NetworkSimulationConfig
            {
                TicksPerSecond = ticksPerSecond,
                PredictionHistoryTicks = predictionHistoryTicks,
                InterpolationDelayTicks = interpolationDelayTicks,
                CommandRedundancy = commandRedundancy,
                MaxResimulationMilliseconds = maxResimulationMilliseconds
            })
        {
        }

        /// <summary>Creates the clock resource from one shared simulation configuration.</summary>
        public NetworkSimulationClockResource(in NetworkSimulationConfig config)
        {
            if (config.TicksPerSecond <= 0)
                throw new ArgumentException("Simulation config is not initialized.", nameof(config));
            Config = config;
            TicksPerSecond = config.TicksPerSecond;
            TickSeconds = 1f / config.TicksPerSecond;
            PredictionHistoryTicks = config.PredictionHistoryTicks;
            InterpolationDelayTicks = config.InterpolationDelayTicks;
            CommandRedundancy = config.CommandRedundancy;
            MaxResimulationMilliseconds = config.MaxResimulationMilliseconds;
            Fingerprint = CalculateFingerprint();
        }

        /// <summary>Gets the complete shared simulation configuration.</summary>
        public NetworkSimulationConfig Config { get; }

        /// <summary>Gets the fixed simulation frequency.</summary>
        public int TicksPerSecond { get; }

        /// <summary>Gets the duration of one simulation tick in seconds.</summary>
        public float TickSeconds { get; }

        /// <summary>Gets the number of prediction ticks retained by clients.</summary>
        public int PredictionHistoryTicks { get; }

        /// <summary>Gets the number of authoritative ticks presentation renders behind.</summary>
        public int InterpolationDelayTicks { get; }

        /// <summary>Gets the number of previous command ticks repeated in each send.</summary>
        public int CommandRedundancy { get; }

        /// <summary>Gets the maximum resimulation work allowed per rendered frame.</summary>
        public float MaxResimulationMilliseconds { get; }

        /// <summary>Gets the deterministic wire fingerprint of all simulation settings.</summary>
        public ulong Fingerprint { get; }

        private ulong CalculateFingerprint()
        {
            const ulong offset = 14695981039346656037UL;
            const ulong prime = 1099511628211UL;
            ulong value = offset;
            Add(ref value, TicksPerSecond, prime);
            Add(ref value, PredictionHistoryTicks, prime);
            Add(ref value, InterpolationDelayTicks, prime);
            Add(ref value, CommandRedundancy, prime);
            Add(ref value, BitConverter.SingleToInt32Bits(MaxResimulationMilliseconds), prime);
            return value;
        }

        private static void Add(ref ulong hash, int value, ulong prime)
        {
            hash ^= unchecked((uint)value);
            hash *= prime;
        }
    }
}
