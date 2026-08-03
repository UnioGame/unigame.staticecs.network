namespace UniGame.StaticEcs.Network.Tests
{
    // Unity's pinned NUnit 3.5 predates NonParallelizableAttribute; pool-sensitive fixtures share this gate.
    internal static class PoolTestGate
    {
        internal static readonly object Sync = new();
    }
}
