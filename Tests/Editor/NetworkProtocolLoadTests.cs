namespace UniGame.StaticEcs.Network.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using FFS.Libraries.StaticEcs;
    using FFS.Libraries.StaticPack;
    using NUnit.Framework;

    /// <summary>Exercises deterministic multi-peer protocol traffic with real canonical ECS captures.</summary>
    [TestFixture]
    internal sealed class NetworkProtocolLoadTests
    {
        private static readonly NetworkBufferPool Buffers = new NetworkBufferPool(64L << 20);

        [TestCase(false, TestName = "Smoke_Immediate")]
        [TestCase(true, TestName = "Smoke_Adverse")]
        public void Smoke(bool adverse) => RunProfile("Smoke", 8, 250, 200, adverse);

        [Test, Explicit, Category("NetworkLoad")]
        public void Baseline() => RunProfile("Baseline", 32, 1_000, 400, true);

        [Test, Explicit, Category("NetworkLoad")]
        public void Capacity() => RunProfile("Capacity", 64, 2_000, 400, true);

        private static void RunProfile(string name, int peerCount, int actorCount,
            int ticks, bool adverse)
        {
            NetworkReplicator<LoadWorld> replicator = null;
            NetworkReplicator<LoadClientWorld> client = null;
            List<Peer> peers = null;
            try
            {
                CreateWorld();
                CreateClientWorld();
                var schema = Schema();
                var clientSchema = ClientSchema();
                Assert.That(clientSchema.Fingerprint,
                    Is.EqualTo(schema.Fingerprint));
                replicator = new NetworkReplicator<LoadWorld>(schema,
                    static (_, _) => true, new ScopeId(1));
                client = new NetworkReplicator<LoadClientWorld>(clientSchema,
                    new ScopeId(1));
                var actors = new World<LoadWorld>.Entity[actorCount];
                for (var index = 0; index < actors.Length; index++)
                {
                    actors[index] = World<LoadWorld>.NewEntity<LoadEntity>();
                    actors[index].Set(new LoadComponent { Value = index });
                }
                var allocationStart = GC.GetAllocatedBytesForCurrentThread();
                peers = CreatePeers(peerCount, adverse);
                var tickSamples = new long[ticks];
                var captureSamples = new long[ticks * peerCount];
                var applySamples = new long[ticks * peerCount];
                var captureIndex = 0;
                var applyIndex = 0;
                long bytes = 0;
                var expectedCommands = ticks;
                Handshake(peers, ref bytes);
                for (uint tick = 1; tick <= ticks; tick++)
                {
                    actors[(int)((tick - 1) % (uint)actors.Length)].Set(
                        new LoadComponent { Value = checked((int)tick) });
                    NetworkSnapshot capture = null;
                    try
                    {
                        Assert.That(replicator.Capture(tick, out capture),
                            Is.EqualTo(SnapshotCaptureResult.Success));
                        var tickStart = Stopwatch.GetTimestamp();
                        for (var peerIndex = 0; peerIndex < peers.Count; peerIndex++)
                        {
                            var peer = peers[peerIndex];
                            var command = CommandPayload(tick);
                            var commandPacket = Encode(PacketKind.CommandBatch,
                                PacketFlags.UnreliableSequenced, tick, tick, command);
                            bytes += commandPacket.Length;
                            Assert.That(peer.Simulator.Client.TrySend(commandPacket), Is.True);

                            var captureStart = Stopwatch.GetTimestamp();
                            var snapshot = EncodeSnapshot(
                                ++peer.SnapshotSequence, capture);
                            captureSamples[captureIndex] = ElapsedNanoseconds(captureStart);
                            bytes += snapshot.Length;
                            Assert.That(peer.Simulator.Server.TrySend(snapshot), Is.True);
                            captureIndex++;
                        }
                        AdvanceAndDrain(peers, 50, applySamples, ref applyIndex,
                            ref bytes, client, clientSchema.Fingerprint);
                        tickSamples[tick - 1] = ElapsedNanoseconds(tickStart);
                    }
                    finally
                    {
                        capture?.Dispose();
                    }
                }

                for (var index = 0; index < 12; index++)
                {
                    var tick = (uint)ticks;
                    foreach (var peer in peers)
                    {
                        var packet = Encode(PacketKind.CommandBatch,
                            PacketFlags.UnreliableSequenced, tick + (uint)index + 1,
                            tick, CommandPayload(tick));
                        bytes += packet.Length;
                        Assert.That(peer.Simulator.Client.TrySend(packet), Is.True);
                    }
                    AdvanceAndDrain(peers, 50, applySamples, ref applyIndex,
                        ref bytes, client, clientSchema.Fingerprint);
                }
                AdvanceAndDrain(peers, 500, applySamples, ref applyIndex,
                    ref bytes, client, clientSchema.Fingerprint);

                long maxQueuedPackets = 0;
                foreach (var peer in peers)
                {
                    Assert.That(peer.ProcessedCommands.Count,
                        adverse
                            ? Is.GreaterThanOrEqualTo(expectedCommands * 4 / 5)
                            : Is.EqualTo(expectedCommands),
                        "The adverse unreliable stream may lose ticks, but must remain bounded.");
                    Assert.That(peer.ProcessedCommands, Does.Contain((uint)ticks),
                        "Drain must converge on the latest held command state.");
                    Assert.That(peer.ProcessedCommands.Count,
                        Is.LessThanOrEqualTo(expectedCommands),
                        "Duplicated packets must not apply a command twice.");
                    Assert.That(peer.LastSnapshotTick, Is.EqualTo((uint)ticks));
                    Assert.That(peer.ProtocolErrors, Is.Zero);
                    var stats = peer.Simulator.CaptureStats();
                    maxQueuedPackets = Math.Max(maxQueuedPackets,
                        Math.Max(stats.ClientToServer.QueuedPackets,
                            stats.ServerToClient.QueuedPackets));
                    Assert.That(stats.ClientToServer.OverflowPackets, Is.Zero);
                    Assert.That(stats.ServerToClient.OverflowPackets, Is.Zero);
                    Assert.That(stats.ClientToServer.QueuedPackets, Is.Zero);
                    Assert.That(stats.ServerToClient.QueuedPackets, Is.Zero);
                }

                var allocated = GC.GetAllocatedBytesForCurrentThread() - allocationStart;
                TestContext.Progress.WriteLine(
                    $"NetworkLoad {name} adverse={adverse} peers={peerCount} actors={actorCount} " +
                    $"ticks={ticks} tick_p50_ns={Percentile(tickSamples, 0.50)} " +
                    $"tick_p95_ns={Percentile(tickSamples, 0.95)} " +
                    $"tick_p99_ns={Percentile(tickSamples, 0.99)} " +
                    $"tick_max_ns={Percentile(tickSamples, 1.00)} " +
                    $"capture_p95_ns={Percentile(captureSamples, 0.95)} " +
                    $"apply_p95_ns={Percentile(applySamples, 0.95)} " +
                    $"bytes_per_peer_tick={(double)bytes / peerCount / ticks:F1} " +
                    $"commands={expectedCommands * peerCount} max_queue={maxQueuedPackets} " +
                    $"protocol_errors=0 allocated_bytes={allocated}");
            }
            finally
            {
                if (peers != null)
                    foreach (var peer in peers)
                        peer.Simulator.Dispose();
                client?.ClearReplicas();
                client?.Dispose();
                replicator?.Dispose();
                if (World<LoadWorld>.Status == WorldStatus.Initialized)
                    World<LoadWorld>.Destroy();
                if (World<LoadClientWorld>.Status == WorldStatus.Initialized)
                    World<LoadClientWorld>.Destroy();
            }
        }

        private static List<Peer> CreatePeers(int count, bool adverse)
        {
            var peers = new List<Peer>(count);
            for (var index = 0; index < count; index++)
            {
                var config = NetworkSimulationPresets.Create(
                    adverse ? NetworkSimulationPreset.Unstable : NetworkSimulationPreset.Immediate);
                config.Seed = (uint)(7_919 + index * 17);
                if (adverse)
                {
                    config.LatencyMilliseconds = 100 + index % 101;
                    config.JitterMilliseconds = 50;
                    config.LossProbability = 0.1f;
                    config.DuplicateProbability = 0.05f;
                    config.ReorderProbability = 0.1f;
                    config.BandwidthBytesPerSecond = 8 * 1024 * 1024;
                }
                peers.Add(new Peer(new NetworkSimulator(
                    new ConnectionId((uint)(index + 1)), in config)));
            }
            return peers;
        }

        private static void Handshake(IReadOnlyList<Peer> peers, ref long bytes)
        {
            foreach (var peer in peers)
            {
                var hello = Encode(PacketKind.Hello, PacketFlags.ReliableOrdered, 1, 0,
                    ReadOnlySpan<byte>.Empty);
                bytes += hello.Length;
                Assert.That(peer.Simulator.Client.TrySend(hello), Is.True);
            }
            foreach (var peer in peers)
            {
                peer.Simulator.Advance(250);
                Assert.That(peer.Simulator.Server.TryReceive(out var hello), Is.True);
                Assert.That(NetworkPacket.TryDecode(hello, out var header, out _), Is.True);
                Assert.That(header.Kind, Is.EqualTo(PacketKind.Hello));
                hello.Dispose();
                var ready = Encode(PacketKind.Ready, PacketFlags.ReliableOrdered, 1, 0,
                    ReadOnlySpan<byte>.Empty);
                bytes += ready.Length;
                Assert.That(peer.Simulator.Server.TrySend(ready), Is.True);
                peer.Simulator.Advance(250);
                Assert.That(peer.Simulator.Client.TryReceive(out var response), Is.True);
                Assert.That(NetworkPacket.TryDecode(response, out header, out _), Is.True);
                Assert.That(header.Kind, Is.EqualTo(PacketKind.Ready));
                response.Dispose();
            }
        }

        private static void AdvanceAndDrain(IReadOnlyList<Peer> peers, int milliseconds,
            long[] applySamples, ref int applyIndex, ref long bytes,
            NetworkReplicator<LoadClientWorld> client,
            SchemaFingerprint fingerprint)
        {
            foreach (var peer in peers)
            {
                peer.Simulator.Advance(milliseconds);
                DrainServer(peer);
                while (peer.Simulator.Client.TryReceive(out var snapshotPacket))
                {
                    try
                    {
                        var applyStart = Stopwatch.GetTimestamp();
                        if (!NetworkPacket.TryDecode(snapshotPacket, out var header, out var payload) ||
                            header.Kind != PacketKind.SnapshotChunk ||
                            payload.Length < SnapshotChunkHeader.Size ||
                            !SnapshotChunkHeader.TryRead(payload.Span,
                                out var chunk) ||
                            chunk.PayloadKind != SnapshotPayloadKind.Keyframe ||
                            chunk.ChunkIndex != 0 || chunk.ChunkCount != 1 ||
                            chunk.SnapshotTick != header.ServerTick)
                        {
                            peer.ProtocolErrors++;
                            continue;
                        }
                        var canonical = payload.Span.Slice(
                            SnapshotChunkHeader.Size);
                        int entities;
                        int records;
                        if (chunk.TotalLength != (uint)canonical.Length ||
                            chunk.TotalHash != Hashing.XxHash64(canonical) ||
                            !SnapshotDeltaCodec.TryInspectCanonical(canonical,
                                out entities, out records))
                        {
                            peer.ProtocolErrors++;
                            continue;
                        }
                        var exact = snapshotPacket.RetainSlice(
                            PacketHeader.Size + SnapshotChunkHeader.Size,
                            canonical.Length);
                        var snapshot = client.CreateSnapshot(chunk.SnapshotTick,
                            fingerprint, new ScopeId(1), exact, entities, records);
                        var result = client.Stage(snapshot, out var staged);
                        if (result == SnapshotApplyResult.Success)
                        {
                            result = client.Apply(in staged);
                            staged.Dispose();
                        }
                        if (result != SnapshotApplyResult.Success)
                        {
                            snapshot.Dispose();
                            peer.ProtocolErrors++;
                            continue;
                        }
                        peer.LastSnapshotTick = chunk.SnapshotTick;
                        if (applyIndex < applySamples.Length)
                            applySamples[applyIndex++] = ElapsedNanoseconds(applyStart);
                        var ack = Encode(PacketKind.Ack, PacketFlags.ReliableOrdered,
                            ++peer.AckSequence, header.ServerTick, ReadOnlySpan<byte>.Empty);
                        bytes += ack.Length;
                        Assert.That(peer.Simulator.Client.TrySend(ack), Is.True);
                    }
                    finally
                    {
                        snapshotPacket.Dispose();
                    }
                }
                peer.Simulator.Advance(milliseconds);
                DrainServer(peer);
            }
        }

        private static void DrainServer(Peer peer)
        {
            while (peer.Simulator.Server.TryReceive(out var packet))
            {
                try
                {
                    if (!NetworkPacket.TryDecode(packet, out var header, out var payload))
                    {
                        peer.ProtocolErrors++;
                        continue;
                    }
                    if (header.Kind == PacketKind.Ack)
                        continue;
                    if (header.Kind != PacketKind.CommandBatch || payload.Length % 4 != 0)
                    {
                        peer.ProtocolErrors++;
                        continue;
                    }
                    var span = payload.Span;
                    for (var offset = 0; offset < span.Length; offset += 4)
                        peer.ProcessedCommands.Add(Read32(span, offset));
                }
                finally
                {
                    packet.Dispose();
                }
            }
        }

        private static byte[] CommandPayload(uint tick)
        {
            var count = (int)Math.Min(4u, tick);
            var payload = new byte[count * 4];
            for (var index = 0; index < count; index++)
                Write32(payload, index * 4, tick - (uint)index);
            return payload;
        }

        private static NetworkBufferLease EncodeSnapshot(uint sequence,
            NetworkSnapshot snapshot)
        {
            var payload = Buffers.Rent(checked(
                SnapshotChunkHeader.Size + snapshot.ByteLength));
            try
            {
                var chunk = new SnapshotChunkHeader
                {
                    PayloadKind = SnapshotPayloadKind.Keyframe,
                    SnapshotTick = snapshot.ServerTick,
                    BaselineTick = 0,
                    TotalLength = checked((uint)snapshot.ByteLength),
                    TotalHash = snapshot.PayloadHash,
                    ChunkIndex = 0,
                    ChunkCount = 1
                };
                Assert.That(chunk.TryWrite(payload.WritableSpan), Is.True);
                snapshot.Bytes.Span.CopyTo(payload.WritableSpan.Slice(
                    SnapshotChunkHeader.Size));
                return Encode(PacketKind.SnapshotChunk,
                    PacketFlags.ReliableOrdered, sequence,
                    snapshot.ServerTick, payload.Span,
                    snapshot.SchemaFingerprint);
            }
            finally
            {
                payload.Dispose();
            }
        }

        private static NetworkBufferLease Encode(PacketKind kind, PacketFlags flags,
            uint sequence,
            uint tick, ReadOnlySpan<byte> payload,
            SchemaFingerprint fingerprint = default)
        {
            var header = new PacketHeader
            {
                Kind = kind,
                Flags = flags,
                Compression = NetworkCompression.None,
                SessionEpoch = 1,
                PacketSequence = sequence,
                ServerTick = tick,
                SchemaFingerprint = fingerprint,
            };
            Assert.That(NetworkPacket.TryEncode(Buffers, header, payload,
                out var packet), Is.True);
            return packet;
        }

        private static long ElapsedNanoseconds(long start) =>
            (long)((Stopwatch.GetTimestamp() - start) *
                   (1_000_000_000d / Stopwatch.Frequency));

        private static long Percentile(long[] samples, double percentile)
        {
            var copy = (long[])samples.Clone();
            Array.Sort(copy);
            return copy[Math.Min(copy.Length - 1,
                (int)Math.Ceiling(percentile * copy.Length) - 1)];
        }

        private static uint Read32(ReadOnlySpan<byte> source, int offset) =>
            (uint)(source[offset] | source[offset + 1] << 8 |
                   source[offset + 2] << 16 | source[offset + 3] << 24);

        private static void Write32(Span<byte> destination, int offset, uint value)
        {
            destination[offset] = (byte)value;
            destination[offset + 1] = (byte)(value >> 8);
            destination[offset + 2] = (byte)(value >> 16);
            destination[offset + 3] = (byte)(value >> 24);
        }

        private static void CreateWorld()
        {
            World<LoadWorld>.Create(WorldConfig.Default());
            var types = World<LoadWorld>.Types();
            types.EntityType<LoadEntity>();
            types.Component<LoadComponent>();
            World<LoadWorld>.Initialize();
        }

        private static void CreateClientWorld()
        {
            World<LoadClientWorld>.Create(WorldConfig.Default());
            var types = World<LoadClientWorld>.Types();
            types.RegisterAll(typeof(NetworkOwnerComponent).Assembly);
            types.EntityType<LoadEntity>();
            types.Component<LoadComponent>();
            World<LoadClientWorld>.Initialize();
        }

        private static NetworkSchema<LoadWorld> Schema()
        {
            var factory = NetworkCompilerSupport.Create<LoadWorld>();
            factory.Entity<LoadEntity>(new NetworkTypeId(1));
            factory.DisableableComponent<LoadComponent>(new NetworkTypeId(2));
            return factory.Freeze();
        }

        private static NetworkSchema<LoadClientWorld> ClientSchema()
        {
            var factory = NetworkCompilerSupport.Create<LoadClientWorld>();
            factory.Entity<LoadEntity>(new NetworkTypeId(1));
            factory.DisableableComponent<LoadComponent>(new NetworkTypeId(2));
            return factory.Freeze();
        }

        private struct LoadWorld : IWorldType { }
        private struct LoadClientWorld : IWorldType { }
        internal struct LoadEntity : IEntityType, INetworkType
        {
            public byte Id() => 1;
        }
        internal struct LoadComponent : IComponent, IDisableable, INetworkType
        {
            public int Value;
            public void Write<TWorld>(ref BinaryPackWriter writer,
                World<TWorld>.Entity self) where TWorld : struct, IWorldType =>
                writer.WriteInt(Value);
            public void Read<TWorld>(ref BinaryPackReader reader,
                World<TWorld>.Entity self, byte version, bool disabled)
                where TWorld : struct, IWorldType => Value = reader.ReadInt();
        }

        private sealed class Peer
        {
            internal readonly NetworkSimulator Simulator;
            internal readonly HashSet<uint> ProcessedCommands = new HashSet<uint>();
            internal uint LastSnapshotTick;
            internal uint AckSequence;
            internal uint SnapshotSequence = 1;
            internal int ProtocolErrors;

            internal Peer(NetworkSimulator simulator) => Simulator = simulator;
        }
    }
}
