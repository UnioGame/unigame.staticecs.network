namespace UniGame.StaticEcs.Network
{
    using System;
    using FFS.Libraries.StaticEcs;

    /// <summary>Provides the shared authoritative and predicted simulation timing contract.</summary>
    public sealed class NetworkSimulationConfigResource : IResource
    {
        /// <summary>Creates validated simulation timing settings.</summary>
        public NetworkSimulationConfigResource(
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
        public NetworkSimulationConfigResource(in NetworkSimulationConfig config)
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

        /// <summary>Complete shared simulation configuration.</summary>
        public NetworkSimulationConfig Config;

        /// <summary>Fixed simulation frequency.</summary>
        public int TicksPerSecond;

        /// <summary>Duration of one simulation tick in seconds.</summary>
        public float TickSeconds;

        /// <summary>Number of prediction ticks retained by clients.</summary>
        public int PredictionHistoryTicks;

        /// <summary>Number of authoritative ticks presentation renders behind.</summary>
        public int InterpolationDelayTicks;

        /// <summary>Number of previous command ticks repeated in each send.</summary>
        public int CommandRedundancy;

        /// <summary>Maximum resimulation work allowed per rendered frame.</summary>
        public float MaxResimulationMilliseconds;

        /// <summary>Deterministic wire fingerprint of all simulation settings.</summary>
        public ulong Fingerprint;

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
