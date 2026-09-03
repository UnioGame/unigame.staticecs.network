using System.Collections.Generic;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Identifies a simulated packet direction.</summary>
    public enum NetworkSimulationDirection : byte
    {
        ClientToServer,

        ServerToClient
    }

    /// <summary>Identifies the final simulator decision for one original packet.</summary>
    public enum NetworkSimulationDecisionKind : byte
    {
        Scheduled,

        Lost,

        Overflow,

        Disconnected,

        ReplayMismatch
    }

    /// <summary>Names the built-in simulator configurations.</summary>
    public enum NetworkSimulationPreset : byte
    {
        Immediate,

        Local,

        Unstable
    }

    /// <summary>Configures future packet decisions made by <see cref="NetworkSimulator"/>.</summary>
    public struct NetworkSimulationConfig
    {
        public const int MinimumCommandRedundancy = 1;
        public const int DefaultCommandRedundancy = 3;
        public const int MaximumCommandRedundancy = 32;

        public int TicksPerSecond;

        public int PredictionHistoryTicks;

        public int InterpolationDelayTicks;

        public int CommandRedundancy;

        public float MaxResimulationMilliseconds;

        public uint Seed;

        public int LatencyMilliseconds;

        public int JitterMilliseconds;

        public float LossProbability;

        public float DuplicateProbability;

        public float ReorderProbability;

        public long BandwidthBytesPerSecond;

        public int MaxQueuedPackets;

        public long MaxQueuedBytes;

        public int DecisionCapacity;
    }

    /// <summary>Contains counters for one simulated direction.</summary>
    public struct NetworkSimulationDirectionStats
    {
        public int QueuedPackets;

        public long QueuedBytes;

        public long ScheduledPackets;

        public long DeliveredPackets;

        public long LostPackets;

        public long OverflowPackets;

        public long DuplicatePackets;

        public long ReorderedPackets;
    }

    /// <summary>Provides an immutable-by-copy simulator status snapshot.</summary>
    public struct NetworkSimulationStats
    {
        public long TimeMilliseconds;

        public ulong Cycle;

        public ulong ConnectionGeneration;

        public bool Connected;

        public bool Paused;

        public bool Recording;

        public bool Replaying;

        public long ReplayErrors;

        public NetworkSimulationDirectionStats ClientToServer;

        public NetworkSimulationDirectionStats ServerToClient;
    }

    /// <summary>Describes one payload-free deterministic packet decision.</summary>
    public struct NetworkSimulationDecision
    {
        public long TimeMilliseconds;

        public NetworkSimulationDirection Direction;

        public ulong Ordinal;

        public int Bytes;

        public NetworkSimulationDecisionKind Kind;

        public long ScheduledMilliseconds;

        public bool Reordered;

        public bool Duplicated;
    }

    /// <summary>Controls and observes one deterministic simulated link.</summary>
    public interface INetworkSimulatorControl
    {
        /// <summary>Returns a copy of the active configuration.</summary>
        NetworkSimulationConfig CaptureConfig();

        /// <summary>Applies validated settings to future sends.</summary>
        void ApplyConfig(in NetworkSimulationConfig config);

        /// <summary>Returns a copy of current counters and state.</summary>
        NetworkSimulationStats CaptureStats();

        /// <summary>Returns a bounded copy of recent decisions.</summary>
        IReadOnlyList<NetworkSimulationDecision> CaptureDecisions();

        /// <summary>Advances the explicit monotonic simulator clock.</summary>
        void Advance(long elapsedMilliseconds);

        /// <summary>Connects an empty simulated link.</summary>
        void Connect();

        /// <summary>Disconnects and clears the simulated link.</summary>
        void Disconnect();

        /// <summary>Pauses or resumes packet delivery without stopping time.</summary>
        void SetPaused(bool paused);

        /// <summary>Clears queues, counters, time and retained decisions while preserving configuration.</summary>
        void Reset();

        void StartRecording();

        IReadOnlyList<NetworkSimulationDecision> StopRecording();

        /// <summary>Starts deterministic replay of supplied decisions.</summary>
        void StartReplay(IReadOnlyList<NetworkSimulationDecision> decisions);

        /// <summary>Stops replay and returns to generated decisions.</summary>
        void StopReplay();
    }

    /// <summary>Creates validated built-in simulator configurations.</summary>
    public static class NetworkSimulationPresets
    {
        public static NetworkSimulationConfig Create(NetworkSimulationPreset preset)
        {
            var config = new NetworkSimulationConfig
            {
                TicksPerSecond = 20,
                PredictionHistoryTicks = 64,
                InterpolationDelayTicks = 2,
                CommandRedundancy = NetworkSimulationConfig.DefaultCommandRedundancy,
                MaxResimulationMilliseconds = 2f,
                Seed = 1,
                MaxQueuedPackets = 1024,
                MaxQueuedBytes = 16 * 1024 * 1024,
                DecisionCapacity = 512
            };

            if (preset == NetworkSimulationPreset.Local)
            {
                config.LatencyMilliseconds = 20;
                config.JitterMilliseconds = 5;
            }
            else if (preset == NetworkSimulationPreset.Unstable)
            {
                config.LatencyMilliseconds = 100;
                config.JitterMilliseconds = 30;
                config.LossProbability = 0.05f;
                config.DuplicateProbability = 0.01f;
                config.ReorderProbability = 0.1f;
                config.BandwidthBytesPerSecond = 128 * 1024;
            }

            return config;
        }
    }
}
