using System.Collections.Generic;

namespace UniGame.StaticEcs.Network
{
    /// <summary>Identifies a simulated packet direction.</summary>
    public enum NetworkSimulationDirection : byte
    {
        /// <summary>Packets sent by the client endpoint.</summary>
        ClientToServer,

        /// <summary>Packets sent by the server endpoint.</summary>
        ServerToClient
    }

    /// <summary>Identifies the final simulator decision for one original packet.</summary>
    public enum NetworkSimulationDecisionKind : byte
    {
        /// <summary>The packet was scheduled for delivery.</summary>
        Scheduled,

        /// <summary>The packet was accepted and intentionally lost.</summary>
        Lost,

        /// <summary>The packet was rejected by queue bounds.</summary>
        Overflow,

        /// <summary>The packet was rejected because the link is disconnected.</summary>
        Disconnected,

        /// <summary>Replay input did not match the recorded decision.</summary>
        ReplayMismatch
    }

    /// <summary>Names the built-in simulator configurations.</summary>
    public enum NetworkSimulationPreset : byte
    {
        /// <summary>No delay, loss or bandwidth limit.</summary>
        Immediate,

        /// <summary>Low deterministic local-development delay.</summary>
        Local,

        /// <summary>Adverse deterministic development conditions.</summary>
        Unstable
    }

    /// <summary>Configures future packet decisions made by <see cref="NetworkSimulator"/>.</summary>
    public struct NetworkSimulationConfig
    {
        /// <summary>Authoritative gameplay ticks per second.</summary>
        public int TicksPerSecond;

        /// <summary>Number of client prediction ticks retained for reconciliation.</summary>
        public int PredictionHistoryTicks;

        /// <summary>Number of authoritative ticks presentation renders behind.</summary>
        public int InterpolationDelayTicks;

        /// <summary>Number of recent input frames repeated in one unreliable batch.</summary>
        public int InputRedundancy;

        /// <summary>Maximum prediction replay work per rendered frame.</summary>
        public float MaxResimulationMilliseconds;

        /// <summary>Deterministic random seed.</summary>
        public uint Seed;

        /// <summary>Base one-way delay in milliseconds.</summary>
        public int LatencyMilliseconds;

        /// <summary>Symmetric one-way delay variation in milliseconds.</summary>
        public int JitterMilliseconds;

        /// <summary>Probability that an accepted packet is lost.</summary>
        public float LossProbability;

        /// <summary>Probability that a scheduled packet receives one duplicate copy.</summary>
        public float DuplicateProbability;

        /// <summary>Probability that a packet receives priority over equal-due packets.</summary>
        public float ReorderProbability;

        /// <summary>Per-direction bandwidth in bytes per second, or zero for unlimited.</summary>
        public long BandwidthBytesPerSecond;

        /// <summary>Maximum scheduled and ready packets per direction.</summary>
        public int MaxQueuedPackets;

        /// <summary>Maximum scheduled and ready bytes per direction.</summary>
        public long MaxQueuedBytes;

        /// <summary>Maximum retained payload-free decisions.</summary>
        public int DecisionCapacity;
    }

    /// <summary>Contains counters for one simulated direction.</summary>
    public struct NetworkSimulationDirectionStats
    {
        /// <summary>Packets currently held by the direction.</summary>
        public int QueuedPackets;

        /// <summary>Bytes currently held by the direction.</summary>
        public long QueuedBytes;

        /// <summary>Packet copies accepted into the scheduled queue.</summary>
        public long ScheduledPackets;

        /// <summary>Packet copies moved to a receive queue.</summary>
        public long DeliveredPackets;

        /// <summary>Original packets intentionally lost.</summary>
        public long LostPackets;

        /// <summary>Packet copies rejected by queue limits.</summary>
        public long OverflowPackets;

        /// <summary>Duplicate packet copies accepted into the queue.</summary>
        public long DuplicatePackets;

        /// <summary>Packets assigned reorder priority.</summary>
        public long ReorderedPackets;
    }

    /// <summary>Provides an immutable-by-copy simulator status snapshot.</summary>
    public struct NetworkSimulationStats
    {
        /// <summary>Current explicit simulator time.</summary>
        public long TimeMilliseconds;

        /// <summary>Number of explicit advance calls.</summary>
        public ulong Cycle;

        /// <summary>Monotonic link connection generation.</summary>
        public ulong ConnectionGeneration;

        /// <summary>Whether the simulated link accepts traffic.</summary>
        public bool Connected;

        /// <summary>Whether delivery is paused while time continues advancing.</summary>
        public bool Paused;

        /// <summary>Whether decisions are being recorded.</summary>
        public bool Recording;

        /// <summary>Whether recorded decisions drive future sends.</summary>
        public bool Replaying;

        /// <summary>Replay validation failures.</summary>
        public long ReplayErrors;

        /// <summary>Client-to-server counters.</summary>
        public NetworkSimulationDirectionStats ClientToServer;

        /// <summary>Server-to-client counters.</summary>
        public NetworkSimulationDirectionStats ServerToClient;
    }

    /// <summary>Describes one payload-free deterministic packet decision.</summary>
    public struct NetworkSimulationDecision
    {
        /// <summary>Simulator time at which the send was evaluated.</summary>
        public long TimeMilliseconds;

        /// <summary>Packet direction.</summary>
        public NetworkSimulationDirection Direction;

        /// <summary>Direction-local original packet ordinal.</summary>
        public ulong Ordinal;

        /// <summary>Original packet byte length.</summary>
        public int Bytes;

        /// <summary>Final decision for the original packet.</summary>
        public NetworkSimulationDecisionKind Kind;

        /// <summary>Scheduled delivery time for the original packet.</summary>
        public long ScheduledMilliseconds;

        /// <summary>Whether equal-due reorder priority was assigned.</summary>
        public bool Reordered;

        /// <summary>Whether one duplicate copy was scheduled.</summary>
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

        /// <summary>Starts a new bounded decision recording.</summary>
        void StartRecording();

        /// <summary>Stops recording and returns its payload-free decisions.</summary>
        IReadOnlyList<NetworkSimulationDecision> StopRecording();

        /// <summary>Starts deterministic replay of supplied decisions.</summary>
        void StartReplay(IReadOnlyList<NetworkSimulationDecision> decisions);

        /// <summary>Stops replay and returns to generated decisions.</summary>
        void StopReplay();
    }

    /// <summary>Creates validated built-in simulator configurations.</summary>
    public static class NetworkSimulationPresets
    {
        /// <summary>Creates one built-in configuration.</summary>
        public static NetworkSimulationConfig Create(NetworkSimulationPreset preset)
        {
            var config = new NetworkSimulationConfig
            {
                TicksPerSecond = 20,
                PredictionHistoryTicks = 64,
                InterpolationDelayTicks = 2,
                InputRedundancy = 3,
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
