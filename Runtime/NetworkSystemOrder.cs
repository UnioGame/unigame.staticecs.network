namespace UniGame.StaticEcs.Network
{
    /// <summary>Defines stable update ordering boundaries for network ECS systems.</summary>
    public static class NetworkSystemOrder
    {
        /// <summary>Starts or closes client connection lifecycle before packet receive.</summary>
        public const int ClientConnectionLifecycle = -32000;
        /// <summary>Receives and applies client packets.</summary>
        public const int ClientReceive = -31000;
        /// <summary>Projects client session and local ownership into ECS data.</summary>
        public const int ClientStateProjection = -30000;
        /// <summary>Runs feature-owned reconciliation after authoritative apply.</summary>
        public const int ClientReconciliation = -29000;
        /// <summary>Flushes gameplay commands after feature-owned command senders.</summary>
        public const int ClientCommandFlush = 1200;
        /// <summary>Sends clock synchronization after command flushing.</summary>
        public const int ClientClockSync = 1210;
        /// <summary>Receives server packets before the authoritative tick begins.</summary>
        public const int ServerReceive = -30000;
        /// <summary>Dispatches due commands before authority gameplay.</summary>
        public const int ServerCommandDispatch = -29000;
        /// <summary>Projects server connection state after command dispatch.</summary>
        public const int ServerStateProjection = -28000;
        /// <summary>Captures and sends authoritative state after gameplay.</summary>
        public const int ServerSnapshot = 30000;
    }
}
