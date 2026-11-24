namespace UniGame.StaticEcs.Network
{
    using System;

    /// <summary>
    /// Behaviour settings of <see cref="NetworkClient{TWorld}"/> that are not part of the wire
    /// format. A new instance holds the defaults; the client copies the values when it is created.
    /// </summary>
    public sealed class NetworkClientSettings
    {
        /// <summary>
        /// Seconds a snapshot split into chunks may stay incomplete before the client drops the
        /// received part and asks for a resync.
        /// </summary>
        public double SnapshotAssemblyTimeoutSeconds { get; set; } = 2d;

        /// <summary>
        /// Seconds an acknowledgement may wait for the transport to accept it before the client
        /// disconnects: the server cannot advance a delta baseline it was never told about.
        /// </summary>
        public double AcknowledgementSendTimeoutSeconds { get; set; } = 5d;

        /// <summary>Seconds between two clock probes of <see cref="NetworkClient{TWorld}.SynchronizeClock"/>.</summary>
        public double ClockProbeIntervalSeconds { get; set; } = 1d;

        /// <summary>
        /// Weight of a new round-trip sample in the smoothed round-trip time, in (0, 1]; 1 keeps
        /// only the last sample.
        /// </summary>
        public double RoundTripSmoothing { get; set; } = 0.2d;

        /// <summary>
        /// Own-entity states that arrived ahead of the snapshot of their tick and wait for it; the
        /// oldest is dropped when one more arrives.
        /// </summary>
        public int MaximumPendingOwnedEntityRefreshes { get; set; } = 16;

        /// <summary>Throws when a value is outside what the client can run with.</summary>
        public void Validate()
        {
            RequirePositive(SnapshotAssemblyTimeoutSeconds, nameof(SnapshotAssemblyTimeoutSeconds));
            RequirePositive(AcknowledgementSendTimeoutSeconds,
                nameof(AcknowledgementSendTimeoutSeconds));
            RequirePositive(ClockProbeIntervalSeconds, nameof(ClockProbeIntervalSeconds));
            RequirePositive(RoundTripSmoothing, nameof(RoundTripSmoothing));
            if (RoundTripSmoothing > 1d)
                throw new ArgumentOutOfRangeException(nameof(RoundTripSmoothing));
            if (MaximumPendingOwnedEntityRefreshes < 1)
                throw new ArgumentOutOfRangeException(nameof(MaximumPendingOwnedEntityRefreshes));
        }

        private static void RequirePositive(double value, string name)
        {
            // The negated comparison also rejects NaN.
            if (!(value > 0d) || double.IsInfinity(value))
                throw new ArgumentOutOfRangeException(name);
        }
    }
}
