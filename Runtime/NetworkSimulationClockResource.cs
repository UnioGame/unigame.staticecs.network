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
            int inputRedundancy = 3,
            float maxResimulationMilliseconds = 2f)
        {
            if (ticksPerSecond <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(ticksPerSecond));
            }

            if (predictionHistoryTicks <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(predictionHistoryTicks));
            }

            if (interpolationDelayTicks < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(interpolationDelayTicks));
            }

            if (inputRedundancy <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(inputRedundancy));
            }

            if (maxResimulationMilliseconds <= 0f)
            {
                throw new ArgumentOutOfRangeException(nameof(maxResimulationMilliseconds));
            }

            TicksPerSecond = ticksPerSecond;
            TickSeconds = 1f / ticksPerSecond;
            PredictionHistoryTicks = predictionHistoryTicks;
            InterpolationDelayTicks = interpolationDelayTicks;
            InputRedundancy = inputRedundancy;
            MaxResimulationMilliseconds = maxResimulationMilliseconds;
            Fingerprint = CalculateFingerprint();
        }

        /// <summary>Gets the fixed simulation frequency.</summary>
        public int TicksPerSecond { get; }

        /// <summary>Gets the duration of one simulation tick in seconds.</summary>
        public float TickSeconds { get; }

        /// <summary>Gets the number of prediction ticks retained by clients.</summary>
        public int PredictionHistoryTicks { get; }

        /// <summary>Gets the number of authoritative ticks presentation renders behind.</summary>
        public int InterpolationDelayTicks { get; }

        /// <summary>Gets the number of recent input frames repeated in each send.</summary>
        public int InputRedundancy { get; }

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
            Add(ref value, InputRedundancy, prime);
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
