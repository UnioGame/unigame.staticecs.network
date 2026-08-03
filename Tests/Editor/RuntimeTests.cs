using System;
using System.Threading;
using FFS.Libraries.StaticEcs;
using NUnit.Framework;

namespace UniGame.StaticEcs.Network.Tests
{
    public sealed class RuntimeTests
    {
        [SetUp]
        public void EnterLeaseTestLock() => Monitor.Enter(PoolTestGate.Sync);

        [TearDown]
        public void ExitLeaseTestLock() => Monitor.Exit(PoolTestGate.Sync);

        [Test]
        public void SchemaHashIsIndependentOfRegistrationOrderAndDuplicatesFail()
        {
            var a = new SchemaBuilder<TestWorld>().Tag<TestTag>(Id(2), 1).Component<TestComponent, IntCodec>(Id(1), 3, Codec(4), 4).Freeze();
            var b = new SchemaBuilder<TestWorld>().Component<TestComponent, IntCodec>(Id(1), 3, Codec(4), 4).Tag<TestTag>(Id(2), 1).Freeze();
            Assert.That(a.Hash, Is.EqualTo(b.Hash)); Assert.That(a.Entries[0].Kind, Is.EqualTo(SchemaKind.Component));
            var duplicate = new SchemaBuilder<TestWorld>().Tag<TestTag>(Id(1), 1);
            Assert.Throws<InvalidOperationException>(() => duplicate.Component<TestComponent, IntCodec>(Id(1), 1, Codec(2), 4));
        }

        [Test]
        public void SchemaRetainsEveryTypedInvokerAndEnforcesCollectionStorageLimits()
        {
            var schema = new SchemaBuilder<TestWorld>()
                .EntityKind<TestEntityType>(Id(20))
                .Component<TestComponent, IntCodec>(Id(1), 1, Codec(1), 4)
                .Tag<TestTag>(Id(2), 1)
                .Link<TestLink>(Id(3), 1)
                .Links<TestLinks>(Id(4), 1, 32768)
                .Multi<TestMulti, MultiIntCodec>(Id(5), 1, Codec(5), 32768, 4)
                .Command<TestCommand, TestCommandCodec, TestAuthorizer>(Id(10), 1, Codec(10), 4)
                .Freeze();

            foreach (var entry in schema.Entries)
            {
                Assert.That(entry.Invoker, Is.Not.Null);
                Assert.That(entry.Invoker.RuntimeType, Is.EqualTo(entry.RuntimeType));
            }
            Assert.Throws<ArgumentOutOfRangeException>(() => new SchemaBuilder<TestWorld>().Links<TestLinks>(Id(4), 1, 32769));
            Assert.Throws<ArgumentOutOfRangeException>(() => new SchemaBuilder<TestWorld>().Multi<TestMulti, MultiIntCodec>(Id(5), 1, Codec(5), 32769, 4));
            Assert.Throws<InvalidOperationException>(() => new SchemaBuilder<TestWorld>().Tag<ReplicatedTag>(Id(6), 1));
        }

        [Test]
        public void CodecReportsExactConsumptionAndBounds()
        {
            var codec = new IntCodec(); var bytes = new byte[4]; var value = 42;
            Assert.That(codec.TryWrite(in value, bytes, out var written), Is.True); Assert.That(written, Is.EqualTo(4));
            Assert.That(codec.TryRead(bytes, out int decoded, out var read), Is.True); Assert.That(decoded, Is.EqualTo(value)); Assert.That(read, Is.EqualTo(4));
            Assert.That(codec.TryRead(bytes.AsSpan(0, 3), out int _, out _), Is.False);
        }

        [Test]
        public void PacketLeaseIsAReadonlyValueHandleWithGenerationCheckedAliases()
        {
            Assert.That(typeof(PacketLease).IsValueType, Is.True);
            var missing = default(PacketLease);
            Assert.That(missing.IsValid, Is.False);
            Assert.Throws<InvalidOperationException>(() => { var _ = missing.Length; });
            Assert.Throws<InvalidOperationException>(() => { var _ = missing.Span; });
            Assert.Throws<InvalidOperationException>(() => { var _ = missing.CapacitySpan; });
            Assert.Throws<InvalidOperationException>(() => missing.SetLength(0));
            Assert.Throws<InvalidOperationException>(() => missing.Copy());
            Assert.Throws<InvalidOperationException>(() => missing.Dispose());
            Assert.Throws<InvalidOperationException>(() => { PacketLease.Transfer(ref missing); });

            var owner = Lease(7);
            var alias = owner;
            var transferred = PacketLease.Transfer(ref owner);
            Assert.That(owner.IsValid, Is.False);
            Assert.That(alias.IsValid, Is.False);
            Assert.Throws<InvalidOperationException>(() => alias.Dispose());
            Assert.That(transferred.Span[0], Is.EqualTo(7));

            var copy = transferred.Copy();
            transferred.Dispose();
            Assert.That(copy.IsValid, Is.True);
            Assert.That(copy.Span[0], Is.EqualTo(7));
            Assert.Throws<InvalidOperationException>(() => transferred.Dispose());
            copy.Dispose();
        }

        [Test]
        public void PacketLeaseGenerationExhaustionRetiresStateWithoutAliasRevival()
        {
            var owner = Lease(9);
            var originalAlias = owner;
            PacketLease.ForceGenerationForTests(ref owner, long.MaxValue - 1);
            Assert.That(originalAlias.IsValid, Is.False);

            var nearExhaustionAlias = owner;
            var exhausted = PacketLease.Transfer(ref owner);
            Assert.That(owner.IsValid, Is.False);
            Assert.That(nearExhaustionAlias.IsValid, Is.False);

            var exhaustedAlias = exhausted;
            var migrated = PacketLease.Transfer(ref exhausted);
            Assert.That(exhausted.IsValid, Is.False);
            Assert.That(exhaustedAlias.IsValid, Is.False);
            Assert.That(migrated.IsValid, Is.True);
            Assert.That(migrated.Span[0], Is.EqualTo(9));
            migrated.Dispose();

            var retireOnDispose = Lease(10);
            PacketLease.ForceGenerationForTests(ref retireOnDispose, long.MaxValue);
            var retiredAlias = retireOnDispose;
            retireOnDispose.Dispose();
            var recycled = Lease(11);
            Assert.That(retiredAlias.IsValid, Is.False);
            Assert.That(recycled.Span[0], Is.EqualTo(11));
            recycled.Dispose();
        }

        [Test]
        public void PacketLeasePoolAllocatesStatesOnlyForAConcurrencyHighWaterMark()
        {
            var pooled = PacketLease.PooledStateCountForTests;
            var leases = new PacketLease[pooled + 2];
            var allocations = PacketLease.StateAllocationCountForTests;
            for (var i = 0; i < leases.Length; i++) leases[i] = PacketLease.Rent(1);
            Assert.That(PacketLease.StateAllocationCountForTests - allocations, Is.EqualTo(2));
            for (var i = 0; i < leases.Length; i++) { leases[i].Dispose(); leases[i] = default; }

            allocations = PacketLease.StateAllocationCountForTests;
            for (var i = 0; i < leases.Length; i++) leases[i] = PacketLease.Rent(1);
            Assert.That(PacketLease.StateAllocationCountForTests, Is.EqualTo(allocations));
            for (var i = 0; i < leases.Length; i++) { leases[i].Dispose(); leases[i] = default; }
        }

        [Test]
        public void WarmedPacketLeaseRentDisposeLoopAllocatesNoManagedMemory()
        {
            const int capacity = 257;
            const int iterations = 4096;
            RentDisposeLoop(capacity, 1);
            var before = GC.GetAllocatedBytesForCurrentThread();
            for (var i = 0; i < iterations; i++)
            {
                var lease = PacketLease.Rent(capacity);
                lease.Dispose();
            }
            var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            Assert.That(allocated, Is.Zero);
        }

        [Test]
        public void WarmedPacketLeaseTransferDisposeLoopAllocatesNoManagedMemory()
        {
            const int capacity = 257;
            const int iterations = 4096;
            RentTransferDisposeLoop(capacity, 1);
            var before = GC.GetAllocatedBytesForCurrentThread();
            for (var i = 0; i < iterations; i++)
            {
                var lease = PacketLease.Rent(capacity);
                var transferred = PacketLease.Transfer(ref lease);
                transferred.Dispose();
            }
            var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            Assert.That(allocated, Is.Zero);
        }

        [Test]
        public void PacketLeaseSupportsSerializedCrossThreadOwnershipHandoff()
        {
            var owner = Lease(12);
            var staleAlias = owner;
            var handoff = PacketLease.Transfer(ref owner);
            Exception workerFailure = null;
            var worker = new Thread(() =>
            {
                try
                {
                    if (!handoff.IsValid || handoff.Span[0] != 12) throw new InvalidOperationException("Transferred lease was not visible to the worker.");
                    handoff.Span[0] = 13;
                }
                catch (Exception exception)
                {
                    workerFailure = exception;
                }
                finally
                {
                    if (handoff.IsValid) { handoff.Dispose(); handoff = default; }
                }
            });

            worker.Start();
            worker.Join();

            if (workerFailure != null) throw workerFailure;
            Assert.That(owner.IsValid, Is.False);
            Assert.That(staleAlias.IsValid, Is.False);
            Assert.That(handoff.IsValid, Is.False);
        }

        [Test]
        public void TransportStateAndErrorValuesAreStable()
        {
            Assert.That((int)TransportState.Connected, Is.Zero);
            Assert.That((int)TransportState.Faulted, Is.EqualTo(1));
            Assert.That((int)TransportState.Disposed, Is.EqualTo(2));
            Assert.That((int)TransportState.Closed, Is.EqualTo(3));
            Assert.That((byte)TransportError.None, Is.Zero);
            Assert.That((byte)TransportError.QueueOverflow, Is.EqualTo(1));
            Assert.That((byte)TransportError.RemoteClosed, Is.EqualTo(2));
            Assert.That((byte)TransportError.InvalidPacket, Is.EqualTo(3));
            Assert.That((byte)TransportError.Disposed, Is.EqualTo(4));

            MemoryTransport.CreatePair(1, out var left, out var right);
            AssertTransport(left, TransportState.Connected, TransportError.None);
            AssertTransport(right, TransportState.Connected, TransportError.None);
            left.Dispose();
            right.Dispose();
        }

        [Test]
        public void ReliableTransportTransfersOwnershipAndPreservesOrdering()
        {
            MemoryTransport.CreatePair(2, out var sender, out var receiver);
            var first = Lease(1);
            var second = Lease(2);
            Assert.That(sender.TrySend(Channel.ReliableOrdered, ref first), Is.True);
            Assert.That(first.IsValid, Is.False);
            Assert.That(sender.TrySend(Channel.ReliableOrdered, ref second), Is.True);
            Assert.That(receiver.TryReceive(out var channel, out var received), Is.True);
            Assert.That(channel, Is.EqualTo(Channel.ReliableOrdered));
            Assert.That(received.Span[0], Is.EqualTo(1));
            received.Dispose();
            Assert.That(receiver.TryReceive(out channel, out received), Is.True);
            Assert.That(channel, Is.EqualTo(Channel.ReliableOrdered));
            Assert.That(received.Span[0], Is.EqualTo(2));
            received.Dispose();
            sender.Dispose();
            receiver.Dispose();
        }

        [Test]
        public void DisposeClosesPeerDrainsBothQueuesAndIsIdempotent()
        {
            MemoryTransport.CreatePair(2, out var left, out var right);
            var toRight = Lease(1);
            var toRightAlias = toRight;
            var toLeft = HeaderPacket(PacketKind.FullSnapshot, 0, 1);
            var toLeftAlias = toLeft;
            Assert.That(left.TrySend(Channel.ReliableOrdered, ref toRight), Is.True);
            Assert.That(right.TrySend(Channel.UnreliableSequenced, ref toLeft), Is.True);

            left.Dispose();

            AssertTransport(left, TransportState.Disposed, TransportError.Disposed);
            AssertTransport(right, TransportState.Closed, TransportError.RemoteClosed);
            Assert.That(toRightAlias.IsValid, Is.False);
            Assert.That(toLeftAlias.IsValid, Is.False);
            AssertTerminalReceive(left);
            AssertTerminalReceive(right);

            var afterClose = Lease(3);
            var afterCloseAlias = afterClose;
            Assert.That(right.TrySend(Channel.ReliableOrdered, ref afterClose), Is.False);
            Assert.That(afterClose.IsValid, Is.False);
            Assert.That(afterCloseAlias.IsValid, Is.False);
            AssertTransport(right, TransportState.Closed, TransportError.RemoteClosed);

            left.Dispose();
            AssertTransport(left, TransportState.Disposed, TransportError.Disposed);
            right.Dispose();
            right.Dispose();
            AssertTransport(right, TransportState.Disposed, TransportError.Disposed);
            AssertTransport(left, TransportState.Disposed, TransportError.Disposed);
        }

        [Test]
        public void ReliableOverflowFaultsBothEndpointsAndDrainsBothQueues()
        {
            MemoryTransport.CreatePair(1, out var sender, out var receiver);
            var queuedAtReceiver = Lease(1);
            var receiverAlias = queuedAtReceiver;
            var queuedAtSender = HeaderPacket(PacketKind.FullSnapshot, 0, 1);
            var senderAlias = queuedAtSender;
            Assert.That(sender.TrySend(Channel.ReliableOrdered, ref queuedAtReceiver), Is.True);
            Assert.That(receiver.TrySend(Channel.UnreliableSequenced, ref queuedAtSender), Is.True);

            var trigger = Lease(2);
            var triggerAlias = trigger;
            Assert.That(sender.TrySend(Channel.ReliableOrdered, ref trigger), Is.False);

            Assert.That(trigger.IsValid, Is.False);
            Assert.That(triggerAlias.IsValid, Is.False);
            Assert.That(receiverAlias.IsValid, Is.False);
            Assert.That(senderAlias.IsValid, Is.False);
            AssertTransport(sender, TransportState.Faulted, TransportError.QueueOverflow);
            AssertTransport(receiver, TransportState.Faulted, TransportError.QueueOverflow);
            AssertTerminalReceive(sender);
            AssertTerminalReceive(receiver);

            var afterFault = Lease(3);
            var afterFaultAlias = afterFault;
            Assert.That(sender.TrySend(Channel.ReliableOrdered, ref afterFault), Is.False);
            Assert.That(afterFault.IsValid, Is.False);
            Assert.That(afterFaultAlias.IsValid, Is.False);
            AssertTransport(sender, TransportState.Faulted, TransportError.QueueOverflow);

            sender.Dispose();
            AssertTransport(sender, TransportState.Disposed, TransportError.Disposed);
            AssertTransport(receiver, TransportState.Faulted, TransportError.QueueOverflow);
            sender.Dispose();
            AssertTransport(sender, TransportState.Disposed, TransportError.Disposed);
            AssertTransport(receiver, TransportState.Faulted, TransportError.QueueOverflow);
            receiver.Dispose();
        }

        [Test]
        public void DisposedSendConsumesLeaseAndPreservesTerminalState()
        {
            MemoryTransport.CreatePair(1, out var sender, out var receiver);
            sender.Dispose();
            var packet = Lease(1);
            var alias = packet;

            Assert.That(sender.TrySend(Channel.ReliableOrdered, ref packet), Is.False);

            Assert.That(packet.IsValid, Is.False);
            Assert.That(alias.IsValid, Is.False);
            AssertTransport(sender, TransportState.Disposed, TransportError.Disposed);
            AssertTransport(receiver, TransportState.Closed, TransportError.RemoteClosed);
            receiver.Dispose();
        }

        [Test]
        public void InvalidLeaseThrowsBeforeStateOrChannelMutation()
        {
            MemoryTransport.CreatePair(1, out var sender, out var receiver);
            PacketLease missing = default;
            Assert.Throws<InvalidOperationException>(
                () => sender.TrySend((Channel)99, ref missing));
            AssertTransport(sender, TransportState.Connected, TransportError.None);
            AssertTransport(receiver, TransportState.Connected, TransportError.None);

            var returned = Lease(1);
            returned.Dispose();
            Assert.Throws<InvalidOperationException>(
                () => sender.TrySend(Channel.ReliableOrdered, ref returned));
            AssertTransport(sender, TransportState.Connected, TransportError.None);
            AssertTransport(receiver, TransportState.Connected, TransportError.None);

            sender.Dispose();
            receiver.Dispose();
        }

        [Test]
        public void HistoryEvictsWholeTicksByCountBytesAndOwnsLeases()
        {
            using var history = new TickHistory(2, 2, 4); Assert.That(history.Add(Record(1, 2)), Is.True); Assert.That(history.Add(Record(2, 2)), Is.True); Assert.That(history.Add(Record(3, 2)), Is.True);
            Assert.That(history.Count, Is.EqualTo(2)); Assert.That(history.TryGet(1, out _), Is.EqualTo(HistoryLookup.Evicted)); Assert.That(history.Reconcile(1, 0), Is.EqualTo(ReconcileResult.HistoryUnavailable));
            Assert.That(history.Reconcile(3, 3), Is.EqualTo(ReconcileResult.Match)); Assert.That(history.Reconcile(3, 4), Is.EqualTo(ReconcileResult.NeedsRollback));
            var oversized = Record(4, 5); Assert.That(history.Add(oversized), Is.False); Assert.That(history.TryGet(4, out _), Is.EqualTo(HistoryLookup.Evicted));
        }

        [Test]
        public void TickRecordPreflightsEverySourceBeforeTransferringOwnership()
        {
            var generated = Lease(1);
            var received = Lease(2);
            var postApply = Lease(3);
            var commands = new[] { Lease(4), default(PacketLease) };

            Assert.Throws<ArgumentException>(() =>
                new TickRecord(1, ref generated, ref received, ref postApply, 1, 2, 3, 4, 5, commands));

            Assert.That(generated.IsValid, Is.True);
            Assert.That(received.IsValid, Is.True);
            Assert.That(postApply.IsValid, Is.True);
            Assert.That(commands[0].IsValid, Is.True);
            generated.Dispose();
            received.Dispose();
            postApply.Dispose();
            commands[0].Dispose();

            generated = Lease(6);
            received = default;
            postApply = default;
            commands = new[] { generated };
            Assert.Throws<ArgumentException>(() =>
                new TickRecord(2, ref generated, ref received, ref postApply, 1, 2, 3, 4, 5, commands));
            Assert.That(generated.IsValid, Is.True);
            Assert.That(commands[0].IsValid, Is.True);
            generated.Dispose();
            Assert.That(commands[0].IsValid, Is.False);
        }

        [Test]
        public void TickRecordTransfersMutableSourcesAndExposesBorrowedAliases()
        {
            var generated = Lease(1);
            var received = Lease(2);
            var postApply = Lease(3);
            var commands = new[] { Lease(4), Lease(5) };
            var record = new TickRecord(1, ref generated, ref received, ref postApply, 1, 2, 3, 4, 5, commands.AsSpan());

            Assert.That(generated.IsValid, Is.False);
            Assert.That(received.IsValid, Is.False);
            Assert.That(postApply.IsValid, Is.False);
            Assert.That(commands[0].IsValid, Is.False);
            Assert.That(commands[1].IsValid, Is.False);
            Assert.That(record.Bytes, Is.EqualTo(5));

            var generatedBorrow = record.Generated;
            var commandBorrow = record.Commands[0];
            var retainedCopy = record.Generated.Copy();
            record.Dispose();
            record.Dispose();

            Assert.That(generatedBorrow.IsValid, Is.False);
            Assert.That(commandBorrow.IsValid, Is.False);
            Assert.That(retainedCopy.IsValid, Is.True);
            Assert.That(retainedCopy.Span[0], Is.EqualTo(1));
            retainedCopy.Dispose();
        }

        [Test]
        public void CommandStageRetainsAotAuthorizerAndUsesTrustedContext()
        {
            var schema = new SchemaBuilder<TestWorld>().Command<TestCommand, TestCommandCodec, TestAuthorizer>(Id(10), 1, Codec(10), 4).Freeze();
            Assert.That(schema.Entries[0].AuthorizerType, Is.EqualTo(typeof(TestAuthorizer)));
            var bytes = new byte[64]; var payload = new CommandBatchPayload { Commands = new[] { new CommandRecord { TypeId = Id(10), Version = 1, Sequence = 1, ClientTick = 4, Payload = BitConverter.GetBytes(42) } } };
            Assert.That(PayloadCodec.TryWrite(payload, bytes, out var length), Is.True);
            var header = Header(PacketKind.CommandBatch, PacketFlags.ReliableOrdered, 1, schema.Hash);
            Assert.That(PacketFraming.TryEncode(header, bytes.AsSpan(0, length), new NoOpTransform(), schema, out var packet), Is.True);
            Assert.That(PacketFraming.TryDecode(packet, new NoOpTransform(), schema, out _, out var staged), Is.True);
            Assert.That(staged.SchemaHash, Is.EqualTo(schema.Hash));
            var trusted = new CommandContext(7, 1, 4); Assert.That(schema.TryAuthorizeCommand(staged, 0, in trusted, out TestCommand command), Is.True); Assert.That(command.Value, Is.EqualTo(42));
            var untrusted = new CommandContext(8, 1, 4); Assert.That(schema.TryAuthorizeCommand(staged, 0, in untrusted, out command), Is.False);
            staged.Dispose(); packet.Dispose();
        }

        [Test]
        public void SchemaLessStageRetainsEmptySchemaIdentity()
        {
            var payload = PacketLease.Rent(1);
            payload.SetLength(0);
            Assert.That(PayloadStager.TryStage(PacketKind.Ack, ref payload, null, out var staged), Is.True);
            Assert.That(payload.IsValid, Is.False);
            Assert.That(staged.SchemaHash, Is.EqualTo(TypeId.Empty));
            staged.Dispose();
        }

        [Test]
        public void PayloadStagerConsumesValidInputsOnSuccessAndFailure()
        {
            var valid = PacketLease.Rent(1);
            valid.SetLength(0);
            var validAlias = valid;
            Assert.That(PayloadStager.TryStage(PacketKind.Ack, ref valid, null, out var staged), Is.True);
            Assert.That(valid.IsValid, Is.False);
            Assert.That(validAlias.IsValid, Is.False);
            Assert.That(staged.Payload.IsEmpty, Is.True);
            staged.Dispose();
            staged.Dispose();
            Assert.Throws<ObjectDisposedException>(() => { var _ = staged.Payload; });

            var invalid = Lease(1);
            var invalidAlias = invalid;
            Assert.That(PayloadStager.TryStage(PacketKind.Ack, ref invalid, null, out staged), Is.False);
            Assert.That(invalid.IsValid, Is.False);
            Assert.That(invalidAlias.IsValid, Is.False);
            Assert.That(staged, Is.Null);
        }

        [Test]
        public void CommandDispatcherEmitsAcceptedEventWithTrustedContext()
        {
            var schema = DispatchSchema();
            World<DispatchWorld>.Create(WorldConfig.Default());
            World<DispatchWorld>.Types().Event<CommandAcceptedEvent<DispatchCommand>>().Event<CommandRejectedEvent<DispatchCommand>>();
            World<DispatchWorld>.Initialize();
            var receiver = World<DispatchWorld>.RegisterEventReceiver<CommandAcceptedEvent<DispatchCommand>>();
            try
            {
                using var staged = StageDispatchCommand(schema, 11, 19, 42);
                Assert.That(new CommandDispatcher<DispatchWorld>(schema).Dispatch(staged, 0, 7), Is.EqualTo(DispatchResult.Accepted));
                var count = 0;
                foreach (var item in receiver)
                {
                    count++;
                    Assert.That(item.Value.Command.Value, Is.EqualTo(42));
                    Assert.That(item.Value.Context.PeerId, Is.EqualTo(7));
                    Assert.That(item.Value.Context.Sequence, Is.EqualTo(11));
                    Assert.That(item.Value.Context.ClientTick, Is.EqualTo(19));
                }
                Assert.That(count, Is.EqualTo(1));
            }
            finally
            {
                World<DispatchWorld>.DeleteEventReceiver(ref receiver);
                World<DispatchWorld>.Destroy();
            }
        }

        [Test]
        public void CommandDispatcherEmitsRejectedEvent()
        {
            var schema = DispatchSchema();
            World<DispatchWorld>.Create(WorldConfig.Default());
            World<DispatchWorld>.Types().Event<CommandAcceptedEvent<DispatchCommand>>().Event<CommandRejectedEvent<DispatchCommand>>();
            World<DispatchWorld>.Initialize();
            var receiver = World<DispatchWorld>.RegisterEventReceiver<CommandRejectedEvent<DispatchCommand>>();
            try
            {
                using var staged = StageDispatchCommand(schema, 12, 20, 42);
                Assert.That(new CommandDispatcher<DispatchWorld>(schema).Dispatch(staged, 0, 8), Is.EqualTo(DispatchResult.Rejected));
                var count = 0;
                foreach (var item in receiver)
                {
                    count++;
                    Assert.That(item.Value.Context.PeerId, Is.EqualTo(8));
                }
                Assert.That(count, Is.EqualTo(1));
            }
            finally
            {
                World<DispatchWorld>.DeleteEventReceiver(ref receiver);
                World<DispatchWorld>.Destroy();
            }
        }

        [Test]
        public void CommandDispatcherDistinguishesConfigurationAndReceiverFailures()
        {
            var schema = DispatchSchema();
            using var staged = StageDispatchCommand(schema, 1, 2, 42);

            World<DispatchWorld>.Create(WorldConfig.Default());
            World<DispatchWorld>.Types().Event<CommandAcceptedEvent<DispatchCommand>>();
            World<DispatchWorld>.Initialize();
            try
            {
                Assert.Throws<InvalidOperationException>(() => new CommandDispatcher<DispatchWorld>(schema));
            }
            finally
            {
                World<DispatchWorld>.Destroy();
            }

            World<DispatchWorld>.Create(WorldConfig.Default());
            World<DispatchWorld>.Types().Event<CommandAcceptedEvent<DispatchCommand>>().Event<CommandRejectedEvent<DispatchCommand>>();
            World<DispatchWorld>.Initialize();
            try
            {
                Assert.That(new CommandDispatcher<DispatchWorld>(schema).Dispatch(staged, 0, 7), Is.EqualTo(DispatchResult.NoReceiver));
            }
            finally
            {
                World<DispatchWorld>.Destroy();
            }
        }

        [Test]
        public void CommandDispatcherRejectsWrongPayloadSchemaAndIndexBeforeMutation()
        {
            var schema = DispatchSchema();
            World<DispatchWorld>.Create(WorldConfig.Default());
            World<DispatchWorld>.Types().Event<CommandAcceptedEvent<DispatchCommand>>().Event<CommandRejectedEvent<DispatchCommand>>();
            World<DispatchWorld>.Initialize();
            try
            {
                var dispatcher = new CommandDispatcher<DispatchWorld>(schema);
                using var staged = StageDispatchCommand(schema, 1, 2, 42);
                Assert.That(dispatcher.Dispatch(staged, -1, 7), Is.EqualTo(DispatchResult.InvalidCommand));
                Assert.That(dispatcher.Dispatch(staged, 1, 7), Is.EqualTo(DispatchResult.InvalidCommand));

                var other = new SchemaBuilder<DispatchWorld>()
                    .Command<DispatchCommand, DispatchCommandCodec, DispatchAuthorizer>(Id(31), 1, Codec(30), 4)
                    .Freeze();
                Assert.That(new CommandDispatcher<DispatchWorld>(other).Dispatch(staged, 0, 7), Is.EqualTo(DispatchResult.SchemaMismatch));

                var ackLease = PacketLease.Rent(1);
                ackLease.SetLength(0);
                Assert.That(PayloadStager.TryStage(PacketKind.Ack, ref ackLease, null, out var ack), Is.True);
                Assert.That(dispatcher.Dispatch(ack, 0, 7), Is.EqualTo(DispatchResult.WrongPayload));
                ack.Dispose();

                var disposed = StageDispatchCommand(schema, 1, 2, 42);
                disposed.Dispose();
                Assert.That(dispatcher.Dispatch(disposed, 0, 7), Is.EqualTo(DispatchResult.InvalidCommand));
            }
            finally
            {
                World<DispatchWorld>.Destroy();
            }
        }

        [Test]
        public void CommandDispatcherRequiresInitializedWorldAndRetainsLifecycleDefense()
        {
            var schema = DispatchSchema();
            Assert.Throws<InvalidOperationException>(() => new CommandDispatcher<DispatchWorld>(schema));

            World<DispatchWorld>.Create(WorldConfig.Default());
            World<DispatchWorld>.Types().Event<CommandAcceptedEvent<DispatchCommand>>().Event<CommandRejectedEvent<DispatchCommand>>();
            World<DispatchWorld>.Initialize();
            var dispatcher = new CommandDispatcher<DispatchWorld>(schema);
            World<DispatchWorld>.Destroy();

            using var staged = StageDispatchCommand(schema, 1, 2, 42);
            Assert.That(dispatcher.Dispatch(staged, 0, 7), Is.EqualTo(DispatchResult.ConfigurationError));
        }

        [Test]
        public void MemoryTransportBeginStepIsANoOpAcrossLifecycleStates()
        {
            MemoryTransport.CreatePair(2, out var sender, out var receiver);
            var packet = Lease(3);
            Assert.That(sender.TrySend(Channel.ReliableOrdered, ref packet), Is.True);
            ((ISteppedTransport)sender).BeginStep(1);
            ((ISteppedTransport)receiver).BeginStep(ulong.MaxValue);
            AssertTransport(sender, TransportState.Connected, TransportError.None);
            AssertTransport(receiver, TransportState.Connected, TransportError.None);
            Assert.That(receiver.TryReceive(out var channel, out var received), Is.True);
            Assert.That(channel, Is.EqualTo(Channel.ReliableOrdered));
            Assert.That(received.Span[0], Is.EqualTo(3));
            received.Dispose();

            sender.Dispose();
            ((ISteppedTransport)sender).BeginStep(2);
            ((ISteppedTransport)receiver).BeginStep(2);
            AssertTransport(sender, TransportState.Disposed, TransportError.Disposed);
            AssertTransport(receiver, TransportState.Closed, TransportError.RemoteClosed);
            receiver.Dispose();
        }

        [Test]
        public void MemoryTransportBeginStepPreservesReliableQueueOverflowFault()
        {
            MemoryTransport.CreatePair(1, out var sender, out var receiver);
            var queued = Lease(1);
            var queuedAlias = queued;
            Assert.That(sender.TrySend(Channel.ReliableOrdered, ref queued), Is.True);
            var overflow = Lease(2);
            Assert.That(sender.TrySend(Channel.ReliableOrdered, ref overflow), Is.False);
            Assert.That(overflow.IsValid, Is.False);
            Assert.That(queuedAlias.IsValid, Is.False);

            ((ISteppedTransport)sender).BeginStep(0);
            ((ISteppedTransport)receiver).BeginStep(ulong.MaxValue);

            AssertTransport(sender, TransportState.Faulted, TransportError.QueueOverflow);
            AssertTransport(receiver, TransportState.Faulted, TransportError.QueueOverflow);
            AssertTerminalReceive(sender);
            AssertTerminalReceive(receiver);
            sender.Dispose();
            receiver.Dispose();
        }

        [Test]
        public void CommandDispatcherChecksNullFirstAndRejectsMissingAcceptedEventWithoutActivity()
        {
            Assert.Throws<ArgumentNullException>(() => new CommandDispatcher<MissingAcceptedWorld>(null));
            MissingAcceptedCodec.ReadCalls = 0;
            MissingAcceptedCodec.WriteCalls = 0;
            MissingAcceptedAuthorizer.Calls = 0;
            var schema = new SchemaBuilder<MissingAcceptedWorld>()
                .Command<MissingAcceptedCommand, MissingAcceptedCodec, MissingAcceptedAuthorizer>(Id(41), 1, Codec(41), 4)
                .Freeze();

            World<MissingAcceptedWorld>.Create(WorldConfig.Default());
            try
            {
                World<MissingAcceptedWorld>.Types().Event<CommandRejectedEvent<MissingAcceptedCommand>>();
                World<MissingAcceptedWorld>.Initialize();
                Assert.Throws<InvalidOperationException>(() => new CommandDispatcher<MissingAcceptedWorld>(schema));
                Assert.That(MissingAcceptedCodec.ReadCalls, Is.Zero);
                Assert.That(MissingAcceptedCodec.WriteCalls, Is.Zero);
                Assert.That(MissingAcceptedAuthorizer.Calls, Is.Zero);
            }
            finally
            {
                World<MissingAcceptedWorld>.Destroy();
            }
        }

        [Test]
        public void CommandDispatcherRejectsSchemaFromAnotherWorldBeforeActivity()
        {
            CrossWorldAuthorizer.Calls = 0;
            OtherWorldAuthorizer.Calls = 0;
            var schema = new SchemaBuilder<SchemaOwnerWorld>()
                .Command<CrossWorldCommand, CrossWorldCommandCodec, CrossWorldAuthorizer>(Id(40), 1, Codec(40), 4)
                .Freeze();
            var equivalentOtherWorldSchema = new SchemaBuilder<WrongConsumerWorld>()
                .Command<CrossWorldCommand, CrossWorldCommandCodec, OtherWorldAuthorizer>(Id(40), 1, Codec(40), 4)
                .Freeze();

            Assert.Throws<InvalidOperationException>(() => new CommandDispatcher<WrongConsumerWorld>(schema));
            Assert.That(schema.Hash, Is.EqualTo(equivalentOtherWorldSchema.Hash));
            Assert.That(CrossWorldAuthorizer.Calls, Is.Zero);
            Assert.That(OtherWorldAuthorizer.Calls, Is.Zero);
            Assert.That(World<SchemaOwnerWorld>.Status, Is.EqualTo(WorldStatus.NotCreated));
            Assert.That(World<WrongConsumerWorld>.Status, Is.EqualTo(WorldStatus.NotCreated));
            Assert.That(World<SchemaOwnerWorld>.IsEventTypeRegistered<CommandAcceptedEvent<CrossWorldCommand>>(), Is.False);
            Assert.That(World<SchemaOwnerWorld>.IsEventTypeRegistered<CommandRejectedEvent<CrossWorldCommand>>(), Is.False);
            Assert.That(World<WrongConsumerWorld>.IsEventTypeRegistered<CommandAcceptedEvent<CrossWorldCommand>>(), Is.False);
            Assert.That(World<WrongConsumerWorld>.IsEventTypeRegistered<CommandRejectedEvent<CrossWorldCommand>>(), Is.False);
        }

        [Test]
        public void MalformedCommandCodecIsRejectedBeforeStagedPayloadEscapes()
        {
            var schema = new SchemaBuilder<TestWorld>().Command<TestCommand, TestCommandCodec, TestAuthorizer>(Id(10), 1, Codec(10), 4).Freeze();
            var bytes = new byte[64];
            var payload = new CommandBatchPayload
            {
                Commands = new[]
                {
                    new CommandRecord { TypeId = Id(10), Version = 1, Sequence = 1, ClientTick = 4, Payload = new byte[3] }
                }
            };
            Assert.That(PayloadCodec.TryWrite(payload, bytes, out var length), Is.True);
            var header = Header(PacketKind.CommandBatch, PacketFlags.ReliableOrdered, 1, schema.Hash);
            Assert.That(PacketFraming.TryEncode(header, bytes.AsSpan(0, length), new NoOpTransform(), schema, out _), Is.False);

            var direct = PacketLease.Rent(length);
            direct.SetLength(length);
            bytes.AsSpan(0, length).CopyTo(direct.Span);
            Assert.That(PayloadStager.TryStage(PacketKind.CommandBatch, ref direct, schema, out var directStage), Is.False);
            Assert.That(directStage, Is.Null);
            Assert.That(direct.IsValid, Is.False);

            header.WirePayloadLength = (uint)length;
            header.DecodedPayloadLength = (uint)length;
            // Frozen xxHash64 for this canonical malformed CommandBatch.
            header.PayloadHash = 5696635365932090410UL;
            var raw = PacketLease.Rent(PacketHeader.Size + length);
            raw.SetLength(PacketHeader.Size + length);
            Assert.That(header.TryWrite(raw.Span), Is.True);
            bytes.AsSpan(0, length).CopyTo(raw.Span.Slice(PacketHeader.Size));
            Assert.That(PacketFraming.TryDecode(raw, new NoOpTransform(), schema, out _, out var staged), Is.False);
            Assert.That(staged, Is.Null);
            raw.Dispose();
        }

        [Test]
        public void FramingReturnsLocalLeaseOwnershipWhenTransformsThrow()
        {
            var header = Header(PacketKind.Ack, PacketFlags.ReliableOrdered, 1, TypeId.Empty);
            var pooledBeforeEncode = PacketLease.PooledStateCountForTests;
            Assert.Throws<InvalidOperationException>(() =>
                PacketFraming.TryEncode(header, ReadOnlySpan<byte>.Empty, new ThrowingTransform(), out _));
            Assert.That(PacketLease.PooledStateCountForTests, Is.GreaterThanOrEqualTo(pooledBeforeEncode));

            Assert.That(PacketFraming.TryEncode(header, ReadOnlySpan<byte>.Empty, new NoOpTransform(), out var packet), Is.True);
            var packetAlias = packet;
            var pooledBeforeDecode = PacketLease.PooledStateCountForTests;
            Assert.Throws<InvalidOperationException>(() =>
                PacketFraming.TryDecode(in packet, new ThrowingTransform(), out _, out _));
            Assert.That(packet.IsValid, Is.True);
            Assert.That(packetAlias.IsValid, Is.True);
            Assert.That(PacketLease.PooledStateCountForTests, Is.GreaterThanOrEqualTo(pooledBeforeDecode));
            packet.Dispose();
        }

        [Test]
        public void SnapshotStageValidatesEveryRecordShapeAndCodec()
        {
            var schema = new SchemaBuilder<TestWorld>()
                .EntityKind<TestEntityType>(Id(20)).Component<TestComponent, IntCodec>(Id(1), 1, Codec(1), 4)
                .Tag<TestTag>(Id(2), 1).Link<TestLink>(Id(3), 1).Links<TestLinks>(Id(4), 1, 2)
                .Multi<TestMulti, MultiIntCodec>(Id(5), 1, Codec(5), 2, 4).Freeze();
            var link = EntityBytes(1); var links = new byte[16]; EntityBytes(1).CopyTo(links, 0); EntityBytes(2).CopyTo(links, 8);
            var multi = new byte[8]; BitConverter.GetBytes(4).CopyTo(multi, 0); BitConverter.GetBytes(9).CopyTo(multi, 4);
            var snapshot = new FullSnapshotPayload { Entities = new[] { new SnapshotEntity { Entity = new WireEntityId(1, 0, 1), KindId = Id(20), Records = new[] {
                new SnapshotRecord { TypeId = Id(1), Kind = RecordKind.Component, Version = 1, ElementCount = 1, Payload = BitConverter.GetBytes(6) },
                new SnapshotRecord { TypeId = Id(2), Kind = RecordKind.Tag, Version = 1, Payload = Array.Empty<byte>() },
                new SnapshotRecord { TypeId = Id(3), Kind = RecordKind.Link, Version = 1, ElementCount = 1, Payload = link },
                new SnapshotRecord { TypeId = Id(4), Kind = RecordKind.Links, Version = 1, ElementCount = 2, Payload = links },
                new SnapshotRecord { TypeId = Id(5), Kind = RecordKind.Multi, Version = 1, ElementCount = 1, Payload = multi }
            } } } };
            var bytes = new byte[512]; Assert.That(PayloadCodec.TryWrite(snapshot, bytes, out var length), Is.True); var header = Header(PacketKind.FullSnapshot, 0, 2, schema.Hash);
            Assert.That(PacketFraming.TryEncode(header, bytes.AsSpan(0, length), new NoOpTransform(), schema, out var packet), Is.True);
            Assert.That(PacketFraming.TryDecode(packet, new NoOpTransform(), schema, out _, out var staged), Is.True); Assert.That(staged.SchemaHash, Is.EqualTo(schema.Hash)); Assert.That(staged.Entities.Length, Is.EqualTo(1)); Assert.That(staged.Records.Length, Is.EqualTo(5)); staged.Dispose();
            var wrongSchema = new SchemaBuilder<TestWorld>().EntityKind<TestEntityType>(Id(20)).Freeze(); Assert.That(PacketFraming.TryDecode(packet, new NoOpTransform(), wrongSchema, out _, out _), Is.False);
            packet.Span[PacketHeader.Size] ^= 1; Assert.That(PacketFraming.TryDecode(packet, new NoOpTransform(), schema, out _, out _), Is.False); packet.Dispose();
            snapshot.Entities[0].Records[0] = new SnapshotRecord { TypeId = Id(1), Kind = RecordKind.Component, Version = 1, ElementCount = 1, Payload = new byte[3] };
            Assert.That(PayloadCodec.TryWrite(snapshot, bytes, out length), Is.True); Assert.That(PacketFraming.TryEncode(header, bytes.AsSpan(0, length), new NoOpTransform(), schema, out _), Is.False);
        }

        [Test]
        public void UnreliableSequencedKeepsLatestAndRejectsStaleWithoutFault()
        {
            MemoryTransport.CreatePair(2, out var sender, out var receiver);
            var reliable = Lease(9);
            Assert.That(sender.TrySend(Channel.ReliableOrdered, ref reliable), Is.True);
            var first = HeaderPacket(PacketKind.FullSnapshot, 0, 1);
            var firstAlias = first;
            Assert.That(sender.TrySend(Channel.UnreliableSequenced, ref first), Is.True);
            var latest = HeaderPacket(PacketKind.FullSnapshot, 0, 2);
            Assert.That(sender.TrySend(Channel.UnreliableSequenced, ref latest), Is.True);

            var equal = HeaderPacket(PacketKind.FullSnapshot, 0, 2);
            var equalAlias = equal;
            Assert.That(sender.TrySend(Channel.UnreliableSequenced, ref equal), Is.False);
            var stale = HeaderPacket(PacketKind.FullSnapshot, 0, 1);
            var staleAlias = stale;
            Assert.That(sender.TrySend(Channel.UnreliableSequenced, ref stale), Is.False);

            Assert.That(equalAlias.IsValid, Is.False);
            Assert.That(staleAlias.IsValid, Is.False);
            Assert.That(firstAlias.IsValid, Is.False);
            Assert.Throws<InvalidOperationException>(() => { var _ = firstAlias.Span; });
            AssertTransport(sender, TransportState.Connected, TransportError.None);
            AssertTransport(receiver, TransportState.Connected, TransportError.None);
            Assert.That(receiver.TryReceive(out var channel, out var received), Is.True);
            Assert.That(channel, Is.EqualTo(Channel.ReliableOrdered));
            Assert.That(received.Span[0], Is.EqualTo(9));
            received.Dispose();
            Assert.That(receiver.TryReceive(out channel, out received), Is.True);
            Assert.That(channel, Is.EqualTo(Channel.UnreliableSequenced));
            Assert.That(PacketHeader.TryRead(received.Span, out var header), Is.True);
            Assert.That(header.PacketSequence, Is.EqualTo(2));
            received.Dispose();
            sender.Dispose();
            receiver.Dispose();
        }

        [Test]
        public void UnreliableCapacityRejectionIsLossyAndPreservesReliableQueue()
        {
            MemoryTransport.CreatePair(1, out var sender, out var receiver);
            var reliable = Lease(7);
            Assert.That(sender.TrySend(Channel.ReliableOrdered, ref reliable), Is.True);
            var snapshot = HeaderPacket(PacketKind.FullSnapshot, 0, 1);
            var snapshotAlias = snapshot;

            Assert.That(sender.TrySend(Channel.UnreliableSequenced, ref snapshot), Is.False);

            Assert.That(snapshotAlias.IsValid, Is.False);
            AssertTransport(sender, TransportState.Connected, TransportError.None);
            AssertTransport(receiver, TransportState.Connected, TransportError.None);
            Assert.That(receiver.TryReceive(out var channel, out var received), Is.True);
            Assert.That(channel, Is.EqualTo(Channel.ReliableOrdered));
            Assert.That(received.Span[0], Is.EqualTo(7));
            received.Dispose();

            var retry = HeaderPacket(PacketKind.FullSnapshot, 0, 1);
            Assert.That(sender.TrySend(Channel.UnreliableSequenced, ref retry), Is.True);
            Assert.That(receiver.TryReceive(out channel, out received), Is.True);
            Assert.That(channel, Is.EqualTo(Channel.UnreliableSequenced));
            Assert.That(PacketHeader.TryRead(received.Span, out var header), Is.True);
            Assert.That(header.PacketSequence, Is.EqualTo(1));
            received.Dispose();
            sender.Dispose();
            receiver.Dispose();
        }

        [Test]
        public void InvalidUnreliablePacketsFaultSenderAndClosePeer()
        {
            var invalidPackets = new Func<PacketLease>[]
            {
                () => Lease(1),
                CorruptSnapshotPacket,
                InvalidSnapshotFlagsPacket,
                () => HeaderPacket(PacketKind.Ack, PacketFlags.ReliableOrdered, 1),
                () => HeaderPacket(PacketKind.FullSnapshot, 0, 0),
                InconsistentSnapshotLengthPacket
            };

            for (var i = 0; i < invalidPackets.Length; i++)
                AssertInvalidUnreliable(invalidPackets[i]());
        }

        [Test]
        public void UndefinedChannelFaultsSenderAndClosesPeer()
        {
            MemoryTransport.CreatePair(1, out var sender, out var receiver);
            var packet = Lease(1);
            var alias = packet;

            Assert.That(sender.TrySend((Channel)99, ref packet), Is.False);

            Assert.That(packet.IsValid, Is.False);
            Assert.That(alias.IsValid, Is.False);
            AssertTransport(sender, TransportState.Faulted, TransportError.InvalidPacket);
            AssertTransport(receiver, TransportState.Closed, TransportError.RemoteClosed);
            AssertTerminalReceive(sender);
            AssertTerminalReceive(receiver);
            sender.Dispose();
            receiver.Dispose();
        }

        [Test]
        public void LeaseRejectsDoubleReturnAndUseAfterTransfer()
        {
            var lease = Lease(1); lease.Dispose(); Assert.Throws<InvalidOperationException>(() => lease.Dispose()); Assert.Throws<InvalidOperationException>(() => { var _ = lease.Span; });
            MemoryTransport.CreatePair(1, out var sender, out var receiver); var sent = Lease(2); var alias = sent; sender.TrySend(Channel.ReliableOrdered, ref sent); Assert.That(sent.IsValid, Is.False); Assert.Throws<InvalidOperationException>(() => { var _ = alias.Span; }); receiver.TryReceive(out _, out var received); received.Dispose(); sender.Dispose(); receiver.Dispose();
        }

        private static TickRecord Record(uint tick, int bytes) { var lease = PacketLease.Rent(bytes); lease.SetLength(bytes); PacketLease received = default; PacketLease postApply = default; return new TickRecord(tick, ref lease, ref received, ref postApply, tick, tick, tick, 0, 0, Array.Empty<PacketLease>()); }
        private static PacketLease Lease(byte value) { var lease = PacketLease.Rent(1); lease.SetLength(1); lease.Span[0] = value; return lease; }
        private static Schema DispatchSchema() => new SchemaBuilder<DispatchWorld>()
            .Command<DispatchCommand, DispatchCommandCodec, DispatchAuthorizer>(Id(30), 1, Codec(30), 4)
            .Freeze();
        private static StagedPayload StageDispatchCommand(Schema schema, uint sequence, uint clientTick, int value)
        {
            var bytes = new byte[64];
            var payload = new CommandBatchPayload { Commands = new[] { new CommandRecord { TypeId = Id(30), Version = 1, Sequence = sequence, ClientTick = clientTick, Payload = BitConverter.GetBytes(value) } } };
            Assert.That(PayloadCodec.TryWrite(payload, bytes, out var length), Is.True);
            var lease = PacketLease.Rent(length); lease.SetLength(length); bytes.AsSpan(0, length).CopyTo(lease.Span);
            Assert.That(PayloadStager.TryStage(PacketKind.CommandBatch, ref lease, schema, out var staged), Is.True);
            Assert.That(staged.SchemaHash, Is.EqualTo(schema.Hash));
            return staged;
        }
        private static TypeId Id(int value) => new(new Guid(value, 0, 0, new byte[8]));
        private static CodecId Codec(int value) => new(new Guid(value, 0, 0, new byte[8]));
        private static PacketHeader Header(PacketKind kind, PacketFlags flags, uint sequence, TypeId schema) => new() { Kind = kind, Flags = flags, PacketSequence = sequence, BaselineTick = PacketHeader.NoneTick, SchemaHash = schema };
        private static PacketLease HeaderPacket(PacketKind kind, PacketFlags flags, uint sequence) { var lease = PacketLease.Rent(PacketHeader.Size); lease.SetLength(PacketHeader.Size); Header(kind, flags, sequence, TypeId.Empty).TryWrite(lease.Span); return lease; }
        private static PacketLease CorruptSnapshotPacket() { var lease = HeaderPacket(PacketKind.FullSnapshot, 0, 1); lease.Span[0] ^= 1; return lease; }
        private static PacketLease InvalidSnapshotFlagsPacket()
        {
            var lease = HeaderPacket(PacketKind.FullSnapshot, 0, 1);
            lease.Span[9] = (byte)PacketFlags.ReliableOrdered;
            lease.Span.Slice(64, 4).Clear();
            WriteUInt32(lease.Span, 64, Crc32(lease.Span));
            return lease;
        }
        private static PacketLease InconsistentSnapshotLengthPacket() { var lease = PacketLease.Rent(PacketHeader.Size); lease.SetLength(PacketHeader.Size); var header = Header(PacketKind.FullSnapshot, 0, 1, TypeId.Empty); header.WirePayloadLength = 1; header.DecodedPayloadLength = 1; Assert.That(header.TryWrite(lease.Span), Is.True); return lease; }
        private static void AssertInvalidUnreliable(PacketLease packet)
        {
            MemoryTransport.CreatePair(2, out var sender, out var receiver);
            var queuedAtReceiver = Lease(4);
            var queuedAtReceiverAlias = queuedAtReceiver;
            var queuedAtSender = Lease(5);
            var queuedAtSenderAlias = queuedAtSender;
            Assert.That(sender.TrySend(Channel.ReliableOrdered, ref queuedAtReceiver), Is.True);
            Assert.That(receiver.TrySend(Channel.ReliableOrdered, ref queuedAtSender), Is.True);
            var alias = packet;

            Assert.That(sender.TrySend(Channel.UnreliableSequenced, ref packet), Is.False);

            Assert.That(packet.IsValid, Is.False);
            Assert.That(alias.IsValid, Is.False);
            Assert.That(queuedAtReceiverAlias.IsValid, Is.False);
            Assert.That(queuedAtSenderAlias.IsValid, Is.False);
            AssertTransport(sender, TransportState.Faulted, TransportError.InvalidPacket);
            AssertTransport(receiver, TransportState.Closed, TransportError.RemoteClosed);
            AssertTerminalReceive(sender);
            AssertTerminalReceive(receiver);

            var afterClose = Lease(6);
            Assert.That(receiver.TrySend(Channel.ReliableOrdered, ref afterClose), Is.False);
            Assert.That(afterClose.IsValid, Is.False);
            AssertTransport(receiver, TransportState.Closed, TransportError.RemoteClosed);
            sender.Dispose();
            receiver.Dispose();
        }
        private static void AssertTransport(MemoryTransport transport, TransportState state, TransportError error) { Assert.That(transport.State, Is.EqualTo(state)); Assert.That(transport.Error, Is.EqualTo(error)); }
        private static void AssertTerminalReceive(MemoryTransport transport) { Assert.That(transport.TryReceive(out var channel, out var packet), Is.False); Assert.That(channel, Is.EqualTo(default(Channel))); Assert.That(packet.IsValid, Is.False); }
        private static void RentDisposeLoop(int capacity, int iterations)
        {
            for (var i = 0; i < iterations; i++)
            {
                var lease = PacketLease.Rent(capacity);
                lease.Dispose();
            }
        }
        private static void RentTransferDisposeLoop(int capacity, int iterations)
        {
            for (var i = 0; i < iterations; i++)
            {
                var lease = PacketLease.Rent(capacity);
                var transferred = PacketLease.Transfer(ref lease);
                transferred.Dispose();
            }
        }
        private static uint Crc32(ReadOnlySpan<byte> bytes)
        {
            var crc = uint.MaxValue;
            for (var i = 0; i < bytes.Length; i++)
            {
                crc ^= bytes[i];
                for (var bit = 0; bit < 8; bit++)
                    crc = (crc & 1) != 0 ? 0xedb88320U ^ crc >> 1 : crc >> 1;
            }
            return ~crc;
        }
        private static void WriteUInt32(Span<byte> bytes, int offset, uint value) { bytes[offset] = (byte)value; bytes[offset + 1] = (byte)(value >> 8); bytes[offset + 2] = (byte)(value >> 16); bytes[offset + 3] = (byte)(value >> 24); }
        private static byte[] EntityBytes(uint id) => new byte[] { (byte)id, (byte)(id >> 8), (byte)(id >> 16), (byte)(id >> 24), 0, 0, 1, 0 };
        private struct TestWorld : IWorldType { }
        private struct DispatchWorld : IWorldType { }
        private struct SchemaOwnerWorld : IWorldType { }
        private struct WrongConsumerWorld : IWorldType { }
        private struct MissingAcceptedWorld : IWorldType { }
        private struct TestEntityType : IEntityType { public byte Id() => 1; }
        private struct TestTag : ITag { }
        private struct TestLink : ILinkType { }
        private struct TestLinks : ILinksType { }
        private struct TestMulti : IMultiComponent { public int Value; }
        private struct TestComponent : IComponent { public int Value; }
        private struct TestCommand { public int Value; }
        private struct DispatchCommand { public int Value; }
        private struct CrossWorldCommand { public int Value; }
        private struct MissingAcceptedCommand { }
        private struct TestAuthorizer : ICommandAuthorizer<TestWorld, TestCommand> { public bool Authorize(in CommandContext context, in TestCommand command) => context.PeerId == 7 && command.Value == 42; }
        private struct DispatchAuthorizer : ICommandAuthorizer<DispatchWorld, DispatchCommand> { public bool Authorize(in CommandContext context, in DispatchCommand command) => context.PeerId == 7 && command.Value == 42; }
        private struct CrossWorldAuthorizer : ICommandAuthorizer<SchemaOwnerWorld, CrossWorldCommand>
        {
            internal static int Calls;
            public bool Authorize(in CommandContext context, in CrossWorldCommand command) { Calls++; return true; }
        }
        private struct OtherWorldAuthorizer : ICommandAuthorizer<WrongConsumerWorld, CrossWorldCommand>
        {
            internal static int Calls;
            public bool Authorize(in CommandContext context, in CrossWorldCommand command) { Calls++; return true; }
        }
        private struct MissingAcceptedAuthorizer : ICommandAuthorizer<MissingAcceptedWorld, MissingAcceptedCommand>
        {
            internal static int Calls;
            public bool Authorize(in CommandContext context, in MissingAcceptedCommand command) { Calls++; return true; }
        }
        private sealed class ThrowingTransform : IPayloadTransform
        {
            public byte Id => 0;
            public int MaxEncodedLength(int decodedLength) => decodedLength;
            public bool TryEncode(ReadOnlySpan<byte> decoded, Span<byte> destination, out int written) { written = 0; throw new InvalidOperationException("Encode failure."); }
            public bool TryDecode(ReadOnlySpan<byte> encoded, Span<byte> destination, out int written) { written = 0; throw new InvalidOperationException("Decode failure."); }
        }
        private struct TestCommandCodec : ICodec<TestCommand> { public bool TryWrite(in TestCommand value, Span<byte> destination, out int written) { var raw = value.Value; return new IntCodec().TryWrite(in raw, destination, out written); } public bool TryRead(ReadOnlySpan<byte> source, out TestCommand value, out int read) { var ok = new IntCodec().TryRead(source, out int raw, out read); value = new TestCommand { Value = raw }; return ok; } }
        private struct DispatchCommandCodec : ICodec<DispatchCommand> { public bool TryWrite(in DispatchCommand value, Span<byte> destination, out int written) { var raw = value.Value; return new IntCodec().TryWrite(in raw, destination, out written); } public bool TryRead(ReadOnlySpan<byte> source, out DispatchCommand value, out int read) { var ok = new IntCodec().TryRead(source, out int raw, out read); value = new DispatchCommand { Value = raw }; return ok; } }
        private struct CrossWorldCommandCodec : ICodec<CrossWorldCommand> { public bool TryWrite(in CrossWorldCommand value, Span<byte> destination, out int written) { var raw = value.Value; return new IntCodec().TryWrite(in raw, destination, out written); } public bool TryRead(ReadOnlySpan<byte> source, out CrossWorldCommand value, out int read) { var ok = new IntCodec().TryRead(source, out int raw, out read); value = new CrossWorldCommand { Value = raw }; return ok; } }
        private struct MissingAcceptedCodec : ICodec<MissingAcceptedCommand>
        {
            internal static int ReadCalls;
            internal static int WriteCalls;
            public bool TryWrite(in MissingAcceptedCommand value, Span<byte> destination, out int written) { WriteCalls++; written = 0; return false; }
            public bool TryRead(ReadOnlySpan<byte> source, out MissingAcceptedCommand value, out int read) { ReadCalls++; value = default; read = 0; return false; }
        }
        private struct MultiIntCodec : ICodec<TestMulti> { public bool TryWrite(in TestMulti value, Span<byte> destination, out int written) { var raw = value.Value; return new IntCodec().TryWrite(in raw, destination, out written); } public bool TryRead(ReadOnlySpan<byte> source, out TestMulti value, out int read) { var ok = new IntCodec().TryRead(source, out int raw, out read); value = new TestMulti { Value = raw }; return ok; } }
        private struct IntCodec : ICodec<TestComponent>, ICodec<int>
        {
            public bool TryWrite(in TestComponent value, Span<byte> destination, out int written) { var raw = value.Value; return TryWrite(in raw, destination, out written); }
            public bool TryRead(ReadOnlySpan<byte> source, out TestComponent value, out int read) { var ok = TryRead(source, out int raw, out read); value = new TestComponent { Value = raw }; return ok; }
            public bool TryWrite(in int value, Span<byte> destination, out int written) { if (destination.Length < 4) { written = 0; return false; } BitConverter.TryWriteBytes(destination, value); written = 4; return true; }
            public bool TryRead(ReadOnlySpan<byte> source, out int value, out int read) { if (source.Length != 4) { value = 0; read = 0; return false; } value = BitConverter.ToInt32(source); read = 4; return true; }
        }
    }
}
