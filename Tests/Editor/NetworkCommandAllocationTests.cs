using System;
using System.Globalization;
using FFS.Libraries.StaticEcs;
using FFS.Libraries.StaticPack;
using NUnit.Framework;

namespace UniGame.StaticEcs.Network.Tests
{
    /// <summary>
    /// Editor-only, deterministic managed-allocation diagnostics for the hot server command path.
    /// <para>
    /// Every measurement warms the relevant JIT, closed generic, ArrayPool, buffer-pool and
    /// coordinator high-water state first, then snapshots
    /// <see cref="GC.GetAllocatedBytesForCurrentThread"/> immediately around one tight loop and
    /// prints one compact line after the window. Assertions only validate harness invariants,
    /// decoded values, exact consumption and lease ownership cleanup. They never assert an
    /// unknown current allocation slope as pass/fail.
    /// </para>
    /// </summary>
    [TestFixture]
    internal sealed class NetworkCommandAllocationTests
    {
        private const int ReadOperations = 4096;
        private const int ScratchOperations = 4096;
        private const int LeaseOperations = 4096;
        private const int CoordinatorOperations = 16;
        private const int CoordinatorWarmup = 16;

        [Test]
        public void CommandInvokerReadAllocationDistinguishesArrayPoolFromSerializer()
        {
            var buffer = new byte[64];
            const int offset = 7;
            const int length = sizeof(int);
            const int expected = unchecked((int)0x5A17C0DE);
            Assert.That(offset, Is.GreaterThan(0), "probe must exercise a non-zero input offset");
            Assert.That(offset + length, Is.LessThanOrEqualTo(buffer.Length));
            var writer = BinaryPackWriter.Create(buffer, (uint)offset);
            writer.WriteInt(expected);

            // Warm JIT, the closed generic instantiation, ArrayPool and BinaryPackReader.
            for (var i = 0; i < 32; i++)
                Assert.That(ReadInvokerProbe.ReadExposed(buffer, offset, length, 0).Value,
                    Is.EqualTo(expected));

            // Exact-consumption validation must reject a truncated payload.
            Assert.That(() => ReadInvokerProbe.ReadExposed(buffer, offset, length - 1, 0),
                Throws.Exception);

            long checksum = 0;
            var readStart = GC.GetAllocatedBytesForCurrentThread();
            for (var i = 0; i < ReadOperations; i++)
                checksum += ReadInvokerProbe.ReadExposed(buffer, offset, length, 0).Value;
            var readBytes = GC.GetAllocatedBytesForCurrentThread() - readStart;
            Assert.That(checksum, Is.EqualTo((long)expected * ReadOperations),
                "every protected Read must decode the exact payload at the non-zero offset");

            var scratch = new byte[64];
            checksum = 0;
            var scratchStart = GC.GetAllocatedBytesForCurrentThread();
            for (var i = 0; i < ScratchOperations; i++)
                checksum += ReadWithPreallocatedScratch(scratch, buffer, offset, length, 0).Value;
            var scratchBytes = GC.GetAllocatedBytesForCurrentThread() - scratchStart;
            Assert.That(checksum, Is.EqualTo((long)expected * ScratchOperations),
                "controlled scratch path must decode the same payload");

            Report("command.invoker_read_arraypool", ReadOperations, readBytes);
            Report("command.invoker_read_scratch", ScratchOperations, scratchBytes);
        }

        [Test]
        public void NetworkBufferLeaseRetainSliceAllocationIsMeasured()
        {
            using var pool = new NetworkBufferPool(1 << 16);
            var root = pool.Copy(new byte[64]);
            try
            {
                for (var i = 0; i < 32; i++)
                {
                    var warm = root.RetainSlice(8, 16);
                    Assert.That(warm.Length, Is.EqualTo(16));
                    warm.Dispose();
                }
                Assert.That(pool.CaptureDiagnostics().OutstandingLeases, Is.EqualTo(1));

                var start = GC.GetAllocatedBytesForCurrentThread();
                for (var i = 0; i < LeaseOperations; i++)
                {
                    var slice = root.RetainSlice(8, 16);
                    slice.Dispose();
                }
                var bytes = GC.GetAllocatedBytesForCurrentThread() - start;

                Assert.That(root.Length, Is.EqualTo(64));
                Assert.That(pool.CaptureDiagnostics().OutstandingLeases, Is.EqualTo(1));
                Report("buffer.retain_slice", LeaseOperations, bytes);
            }
            finally
            {
                root.Dispose();
            }
            Assert.That(pool.CaptureDiagnostics().OutstandingLeases, Is.Zero);
        }

        [Test]
        public void CoordinatorQueueHeapAllocationIsMeasuredAtHighWater()
        {
            var worldCreated = false;
            NetworkServerCoordinator<CoordinatorAllocationWorld> coordinator = null;
            var warmEnvelopes = new NetworkCommandEnvelope[CoordinatorWarmup];
            var warmTransferred = new bool[CoordinatorWarmup];
            var envelopes = new NetworkCommandEnvelope[CoordinatorOperations];
            var transferred = new bool[CoordinatorOperations];
            using var pool = new NetworkBufferPool(1 << 20);
            try
            {
                World<CoordinatorAllocationWorld>.Create(WorldConfig.Default());
                worldCreated = true;
                World<CoordinatorAllocationWorld>.Types()
                    .Event<NetworkCommandAcceptedEvent<AllocationCommand>>()
                    .Event<NetworkCommandRejectedEvent<AllocationCommand>>();
                World<CoordinatorAllocationWorld>.Initialize();

                var clientSchema = CoordinatorSchema(false);
                var serverSchema = CoordinatorSchema(true);
                var connection = new ConnectionId(1);
                var client = new NetworkSession<CoordinatorAllocationWorld>(connection,
                    NetworkRole.Client, clientSchema, pool);
                var server = new NetworkSession<CoordinatorAllocationWorld>(connection,
                    NetworkRole.Server, serverSchema, pool);
                Assert.That(client.Admit(serverSchema.Fingerprint, 1, 1, new ScopeId(1)),
                    Is.EqualTo(NetworkAdmissionResult.Accepted));
                Assert.That(server.Admit(clientSchema.Fingerprint, 1, 1, new ScopeId(1)),
                    Is.EqualTo(NetworkAdmissionResult.Accepted));

                coordinator = new NetworkServerCoordinator<CoordinatorAllocationWorld>();
                coordinator.Add(server);

                // Warm the heap to the measured high-water and populate the internal maps so the
                // measured pushes neither grow the backing array nor insert new dictionary keys.
                for (var i = 0; i < CoordinatorWarmup; i++)
                {
                    var tick = (uint)(i + 1);
                    Assert.That(client.CreateCommand(new AllocationCommand { Value = i }, tick,
                        out warmEnvelopes[i]), Is.EqualTo(NetworkCommandResult.Queued));
                    warmTransferred[i] = coordinator.Queue(warmEnvelopes[i], tick) ==
                        NetworkCommandResult.Queued;
                    Assert.That(warmTransferred[i], Is.True);
                }
                Assert.That(coordinator.Dispatch((uint)CoordinatorWarmup).Total,
                    Is.EqualTo(CoordinatorWarmup));
                Assert.That(coordinator.PendingCommandCount, Is.Zero);

                for (var i = 0; i < CoordinatorOperations; i++)
                    Assert.That(client.CreateCommand(new AllocationCommand { Value = 100 + i }, 1,
                        out envelopes[i]), Is.EqualTo(NetworkCommandResult.Queued));

                var start = GC.GetAllocatedBytesForCurrentThread();
                for (var i = 0; i < CoordinatorOperations; i++)
                    transferred[i] = coordinator.Queue(envelopes[i], 1) ==
                        NetworkCommandResult.Queued;
                var bytes = GC.GetAllocatedBytesForCurrentThread() - start;

                Assert.That(coordinator.PendingCommandCount,
                    Is.EqualTo(CoordinatorOperations));
                Assert.That(coordinator.PendingCommandsHighWater,
                    Is.GreaterThanOrEqualTo(CoordinatorOperations));
                Report("coordinator.queue_heap", CoordinatorOperations, bytes);
            }
            finally
            {
                try
                {
                    coordinator?.Clear();
                }
                finally
                {
                    for (var i = 0; i < warmTransferred.Length; i++)
                        if (!warmTransferred[i])
                            warmEnvelopes[i].Dispose();
                    for (var i = 0; i < transferred.Length; i++)
                        if (!transferred[i])
                            envelopes[i].Dispose();
                    if (worldCreated)
                        World<CoordinatorAllocationWorld>.Destroy();
                }
            }
            Assert.That(pool.CaptureDiagnostics().OutstandingLeases, Is.Zero);
        }

        private static AllocationCommand ReadWithPreallocatedScratch(byte[] scratch,
            byte[] buffer, int offset, int length, byte version)
        {
            Buffer.BlockCopy(buffer, offset, scratch, 0, length);
            var reader = new BinaryPackReader(scratch, (uint)length, 0);
            var value = default(AllocationCommand);
            value.Read(ref reader, version);
            if (reader.Position != (uint)length)
                throw new InvalidOperationException(
                    "Command hook did not consume the exact payload.");
            return value;
        }

        private static NetworkSchema<CoordinatorAllocationWorld> CoordinatorSchema(bool server)
        {
            var factory = NetworkCompilerSupport.Create<CoordinatorAllocationWorld>();
            if (server)
                factory.Command<AllocationCommand, RejectAllocationPolicy>(new NetworkTypeId(77));
            else
                factory.Command<AllocationCommand>(new NetworkTypeId(77));
            return factory.Freeze();
        }

        private static void Report(string name, int iterations, long bytes)
        {
            var perOperation = iterations == 0 ? 0d : (double)bytes / iterations;
            TestContext.Out.WriteLine(
                "alloc " + name +
                " iterations=" + iterations.ToString(CultureInfo.InvariantCulture) +
                " bytes=" + bytes.ToString(CultureInfo.InvariantCulture) +
                " bytesPerOp=" + perOperation.ToString("F4", CultureInfo.InvariantCulture));
        }

        internal sealed class ReadInvokerProbe :
            CommandNetworkInvoker<AllocationWorld, AllocationCommand>
        {
            public static AllocationCommand ReadExposed(byte[] buffer, int offset, int length,
                byte version) => Read(buffer, offset, length, version);
        }

        internal struct AllocationWorld : IWorldType { }

        private struct CoordinatorAllocationWorld : IWorldType { }

        internal struct AllocationCommand : IEvent, INetworkCommand
        {
            public int Value;

            public void Write(ref BinaryPackWriter writer) => writer.WriteInt(Value);

            public void Read(ref BinaryPackReader reader, byte version) =>
                Value = reader.ReadInt();
        }

        private struct RejectAllocationPolicy :
            INetworkCommandPolicy<CoordinatorAllocationWorld, AllocationCommand>
        {
            public bool Authorize(in NetworkCommandContext context,
                in AllocationCommand command) => false;
        }
    }
}
