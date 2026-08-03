using System;
using System.Diagnostics;
using System.Globalization;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using FFS.Libraries.StaticEcs;
using NUnit.Framework;

namespace UniGame.StaticEcs.Network.Tests
{
    public sealed class DiagnosticsTests
    {
        private const uint Chunk = 41;
        private const ushort Cluster = 6;
        private static readonly TypeId CommandId = new(new Guid(601, 0, 0, new byte[8]));
        private static readonly CodecId CommandCodecId = new(new Guid(602, 0, 0, new byte[8]));

        [SetUp]
        public void EnterPoolTestLock() => Monitor.Enter(PoolTestGate.Sync);

        [TearDown]
        public void ExitPoolTestLock() => Monitor.Exit(PoolTestGate.Sync);

        [Test]
        public void EventAndFingerprintAreValueOnly()
        {
            Assert.That(RuntimeHelpers.IsReferenceOrContainsReferences<SessionEvent>(), Is.False);
            var first = new TickFingerprint(0, 17, 23);
            var same = new TickFingerprint(0, 17, 23);
            Assert.That(first, Is.EqualTo(same));
            Assert.That(first == same, Is.True);
            Assert.That(first != new TickFingerprint(1, 17, 23), Is.True);
            Assert.That(first != new TickFingerprint(0, 18, 23), Is.True);
            Assert.That(first != new TickFingerprint(0, 17, 24), Is.True);
        }

        [Test]
        public void NdjsonGoldenIsInvariantPrivateAndLfOnly()
        {
            var previous = CultureInfo.CurrentCulture;
            CultureInfo.CurrentCulture = new CultureInfo("ar-SA");
            try
            {
                using var output = new MemoryStream();
                using (var log = new NdjsonLog(output, 2, 9, true))
                {
                    var value = Event(7, Stopwatch.Frequency, 0, SessionEventKind.Send,
                        SessionEventPhase.End, PacketKind.CommandBatch, Channel.ReliableOrdered,
                        0x0102030405060708UL);
                    log.Observe(in value);
                    log.Flush();
                }
                var text = Encoding.UTF8.GetString(output.ToArray());
                var expected = "{\"v\":1,\"source\":9,\"id\":7,\"step\":3,\"time_ns\":1000000000," +
                    "\"elapsed_ns\":0,\"role\":\"client\",\"kind\":\"send\",\"phase\":\"end\"," +
                    "\"state\":\"established\",\"error\":\"none\",\"packet\":\"command_batch\"," +
                    "\"channel\":\"reliable_ordered\",\"tick\":0,\"packet_sequence\":5," +
                    "\"wire_bytes\":91,\"decoded_bytes\":19,\"count\":2,\"code\":1," +
                    "\"reason\":0,\"hash\":\"0102030405060708\",\"success\":true,\"retry\":false}\n";
                Assert.That(text, Is.EqualTo(expected));
                Assert.That(text, Does.Not.Contain("nonce"));
                Assert.That(text, Does.Not.Contain("peer"));
                Assert.That(text, Does.Not.Contain("payload"));
                Assert.That(text, Does.Not.Contain("\r"));
            }
            finally { CultureInfo.CurrentCulture = previous; }
        }

        [Test]
        public void OverflowWritesRetainedPrefixThenOneGapThenLaterEvent()
        {
            using var output = new MemoryStream();
            using var log = new NdjsonLog(output, 2, 4, true);
            var one = Event(1); var two = Event(2); var three = Event(3); var four = Event(4);
            log.Observe(in one); log.Observe(in two); log.Observe(in three); log.Observe(in four);
            Assert.That(log.Pending, Is.EqualTo(2));
            Assert.That(log.Dropped, Is.EqualTo(2));
            log.Flush();
            var five = Event(5); log.Observe(in five); log.Flush();
            var lines = Encoding.UTF8.GetString(output.ToArray()).Split(new[] { '\n' }, StringSplitOptions.RemoveEmptyEntries);
            Assert.That(lines.Length, Is.EqualTo(4));
            Assert.That(lines[0], Does.Contain("\"id\":1,"));
            Assert.That(lines[1], Does.Contain("\"id\":2,"));
            Assert.That(lines[2], Is.EqualTo("{\"v\":1,\"source\":4,\"first_id\":3,\"last_id\":4,\"count\":2}"));
            Assert.That(lines[3], Does.Contain("\"id\":5,"));
        }

        [Test]
        public void StreamFailureIsTerminalAndClearsPendingWithoutThrowing()
        {
            using var stream = new ThrowingStream();
            var log = new NdjsonLog(stream, 2, leaveOpen: true);
            var one = Event(1); var two = Event(2);
            log.Observe(in one); log.Observe(in two);
            Assert.DoesNotThrow(log.Flush);
            Assert.That(log.Faulted, Is.True);
            Assert.That(log.Pending, Is.Zero);
            Assert.That(log.Dropped, Is.EqualTo(2));
            var three = Event(3); log.Observe(in three);
            Assert.That(log.Dropped, Is.EqualTo(3));
            Assert.DoesNotThrow(log.Flush);
            Assert.DoesNotThrow(log.Dispose);
        }

        [Test]
        public void DisposeIsIdempotentAndPostDisposeObserveCountsAsDropped()
        {
            var output = new MemoryStream();
            var log = new NdjsonLog(output, 1, leaveOpen: true);
            log.Dispose();
            log.Dispose();
            var value = Event(1);
            log.Observe(in value);
            Assert.That(log.Dropped, Is.EqualTo(1));
            Assert.DoesNotThrow(log.Flush);
        }

        [Test]
        public void ThrowingObserverCannotChangeHandshakeAndCountsEveryFailure()
        {
            CreateWorld<ClientWorld>(ChunkOwnerType.Other);
            CreateWorld<ServerWorld>(ChunkOwnerType.Self);
            MemoryTransport.CreatePair(8, out var clientTransport, out var serverTransport);
            using var client = new Session<ClientWorld>(ClientConfig(), EmptySchema<ClientWorld>(),
                clientTransport, new ThrowingObserver());
            using var server = new Session<ServerWorld>(ServerConfig(), EmptySchema<ServerWorld>(),
                serverTransport, new ThrowingObserver());
            try
            {
                PumpEstablished(client, server);
                Assert.That(client.Error, Is.EqualTo(SessionError.None));
                Assert.That(server.Error, Is.EqualTo(SessionError.None));
                Assert.That(client.Stats.ObserverErrors, Is.GreaterThan(0));
                Assert.That(server.Stats.ObserverErrors, Is.GreaterThan(0));
                Assert.That(client.Stats.Steps, Is.EqualTo(3));
                Assert.That(server.Stats.Steps, Is.EqualTo(3));
                Assert.That(client.Stats.SentPackets, Is.EqualTo(2));
                Assert.That(server.Stats.SentPackets, Is.EqualTo(2));
            }
            finally
            {
                DestroyWorld<ClientWorld>();
                DestroyWorld<ServerWorld>();
            }
        }

        [Test]
        public void StepExceptionStillPairsEventsWithRequestedStepAndFailure()
        {
            CreateWorld<ClientWorld>(ChunkOwnerType.Other);
            var observer = new RecordingObserver();
            using var session = new Session<ClientWorld>(ClientConfig(), EmptySchema<ClientWorld>(),
                new ThrowStepTransport(), observer);
            try
            {
                Assert.Throws<InvalidOperationException>(() => session.Step(17));
                Assert.That(observer.Events.Count, Is.EqualTo(2));
                Assert.That(observer.Events[0].Kind, Is.EqualTo(SessionEventKind.Step));
                Assert.That(observer.Events[0].Phase, Is.EqualTo(SessionEventPhase.Begin));
                Assert.That(observer.Events[0].Step, Is.EqualTo(17));
                Assert.That(observer.Events[1].Phase, Is.EqualTo(SessionEventPhase.End));
                Assert.That(observer.Events[1].Step, Is.EqualTo(17));
                Assert.That(observer.Events[1].Success, Is.False);
                Assert.That(observer.Events[1].Id, Is.EqualTo(observer.Events[0].Id + 1));
                Assert.That(session.Stats.Steps, Is.EqualTo(1));
            }
            finally { DestroyWorld<ClientWorld>(); }
        }

        [Test]
        public void CaptureApplyFingerprintsAndStatsUseCanonicalBytesAtTickZero()
        {
            CreateWorld<ClientWorld>(ChunkOwnerType.Other);
            CreateWorld<ServerWorld>(ChunkOwnerType.Self);
            MemoryTransport.CreatePair(8, out var clientTransport, out var serverTransport);
            var clientEvents = new RecordingObserver();
            var serverEvents = new RecordingObserver();
            using var client = new Session<ClientWorld>(ClientConfig(), EmptySchema<ClientWorld>(), clientTransport, clientEvents);
            using var server = new Session<ServerWorld>(ServerConfig(), EmptySchema<ServerWorld>(), serverTransport, serverEvents);
            try
            {
                PumpEstablished(client, server);
                Assert.That(server.Capture(0), Is.EqualTo(CaptureResult.Success));
                server.Step(3);
                client.Step(3);
                Assert.That(server.TryGetFingerprint(0, out var generated), Is.EqualTo(HistoryLookup.Found));
                Assert.That(client.TryGetFingerprint(0, out var received), Is.EqualTo(HistoryLookup.Found));
                Assert.That(generated, Is.EqualTo(received));
                Assert.That(generated.Tick, Is.Zero);
                Assert.That(generated.Bytes, Is.GreaterThan(0));
                Assert.That(server.Stats.SnapshotsCaptured, Is.EqualTo(1));
                Assert.That(client.Stats.SnapshotsApplied, Is.EqualTo(1));
                Assert.That(serverEvents.Events.Exists(value => value.Kind == SessionEventKind.Capture &&
                    value.Phase == SessionEventPhase.End && value.Success && value.Hash == generated.Hash), Is.True);
                Assert.That(clientEvents.Events.Exists(value => value.Kind == SessionEventKind.Apply &&
                    value.Phase == SessionEventPhase.End && value.Success && value.Hash == received.Hash), Is.True);
                Assert.That(server.TryGetFingerprint(1, out _), Is.EqualTo(HistoryLookup.NotYetSeen));
                for (uint tick = 1; tick <= TickHistory.DefaultTickCapacity; tick++)
                    Assert.That(server.Capture(tick), Is.EqualTo(CaptureResult.Success));
                Assert.That(server.TryGetFingerprint(0, out _), Is.EqualTo(HistoryLookup.Evicted));
                server.Dispose();
                client.Step(4);
                Assert.That(client.State, Is.EqualTo(SessionState.Faulted));
                Assert.That(client.TryGetFingerprint(0, out _), Is.EqualTo(HistoryLookup.Missing));
            }
            finally
            {
                DestroyWorld<ClientWorld>();
                DestroyWorld<ServerWorld>();
            }
        }

        [Test]
        public void LoggerFlushAndCloseFailuresAreTerminalAndNeverEscape()
        {
            var flushStream = new FailingFlushStream();
            var flushLog = new NdjsonLog(flushStream, 2, leaveOpen: true);
            var value = Event(1); flushLog.Observe(in value);
            Assert.DoesNotThrow(flushLog.Flush);
            Assert.That(flushLog.Faulted, Is.True);
            Assert.That(flushLog.Pending, Is.Zero);

            var closeLog = new NdjsonLog(new FailingCloseStream(), 1);
            Assert.DoesNotThrow(closeLog.Dispose);
            Assert.That(closeLog.Faulted, Is.True);

            var partial = new PartialWriteStream();
            var partialLog = new NdjsonLog(partial, 2, leaveOpen: true);
            var first = Event(2); var second = Event(3);
            partialLog.Observe(in first); partialLog.Observe(in second);
            Assert.DoesNotThrow(partialLog.Flush);
            Assert.That(partialLog.Faulted, Is.True);
            Assert.That(partialLog.Pending, Is.Zero);
            Assert.That(partialLog.Dropped, Is.EqualTo(2));
        }

        [Test]
        public void FalseSendRetriesFrozenIntentAndUpdatesRetryStatsAndPairs()
        {
            CreateWorld<ClientWorld>(ChunkOwnerType.Other);
            CreateWorld<ServerWorld>(ChunkOwnerType.Self);
            MemoryTransport.CreatePair(8, out var clientInner, out var serverTransport);
            var gated = new GateTransport(clientInner, 1);
            var observer = new RecordingObserver();
            using var client = new Session<ClientWorld>(ClientConfig(), EmptySchema<ClientWorld>(), gated, observer);
            using var server = new Session<ServerWorld>(ServerConfig(), EmptySchema<ServerWorld>(), serverTransport);
            try
            {
                client.Step(0); server.Step(0);
                client.Step(1); server.Step(1);
                client.Step(2); server.Step(2);
                client.Step(3); server.Step(3);
                Assert.That(client.State, Is.EqualTo(SessionState.Established));
                Assert.That(server.State, Is.EqualTo(SessionState.Established));
                Assert.That(client.Stats.SendRetries, Is.EqualTo(1));
                Assert.That(client.Stats.SentPackets, Is.EqualTo(2));
                var helloEnds = observer.Events.FindAll(item => item.Kind == SessionEventKind.Send &&
                    item.Phase == SessionEventPhase.End && item.Packet == PacketKind.Hello);
                Assert.That(helloEnds.Count, Is.EqualTo(2));
                Assert.That(helloEnds[0].Success, Is.False);
                Assert.That(helloEnds[0].Retry, Is.False);
                Assert.That(helloEnds[1].Success, Is.True);
                Assert.That(helloEnds[1].Retry, Is.True);
            }
            finally { DestroyWorld<ClientWorld>(); DestroyWorld<ServerWorld>(); }
        }

        [Test]
        public void ThrowingSendRetriesFrozenIntentAndPreservesOriginalException()
        {
            CreateWorld<ClientWorld>(ChunkOwnerType.Other);
            CreateWorld<ServerWorld>(ChunkOwnerType.Self);
            MemoryTransport.CreatePair(8, out var clientInner, out var serverTransport);
            var gated = new GateTransport(clientInner, 0, 1);
            var observer = new RecordingObserver();
            using var client = new Session<ClientWorld>(ClientConfig(), EmptySchema<ClientWorld>(), gated, observer);
            using var server = new Session<ServerWorld>(ServerConfig(), EmptySchema<ServerWorld>(), serverTransport);
            try
            {
                var error = Assert.Throws<InvalidOperationException>(() => client.Step(0));
                Assert.That(error.Message, Is.EqualTo("send"));
                server.Step(0);
                client.Step(1); server.Step(1);
                client.Step(2); server.Step(2);
                client.Step(3); server.Step(3);
                Assert.That(client.State, Is.EqualTo(SessionState.Established));
                Assert.That(client.Stats.SendRetries, Is.EqualTo(1));
                var sends = observer.Events.FindAll(item => item.Kind == SessionEventKind.Send &&
                    item.Phase == SessionEventPhase.End && item.Packet == PacketKind.Hello);
                Assert.That(sends.Count, Is.EqualTo(2));
                Assert.That(sends[0].Success, Is.False);
                Assert.That(sends[1].Success, Is.True);
                Assert.That(sends[1].Retry, Is.True);
            }
            finally { DestroyWorld<ClientWorld>(); DestroyWorld<ServerWorld>(); }
        }

        [Test]
        public void ReceiveExceptionPairsReceiveAndStepWithoutReplacingOriginal()
        {
            CreateWorld<ClientWorld>(ChunkOwnerType.Other);
            var observer = new RecordingObserver();
            using var session = new Session<ClientWorld>(ClientConfig(), EmptySchema<ClientWorld>(),
                new ThrowReceiveTransport(), observer);
            try
            {
                var error = Assert.Throws<InvalidOperationException>(() => session.Step(7));
                Assert.That(error.Message, Is.EqualTo("receive"));
                var receive = observer.Events.FindAll(item => item.Kind == SessionEventKind.Receive);
                var step = observer.Events.FindAll(item => item.Kind == SessionEventKind.Step);
                Assert.That(receive.Count, Is.EqualTo(2));
                Assert.That(receive[0].Phase, Is.EqualTo(SessionEventPhase.Begin));
                Assert.That(receive[1].Phase, Is.EqualTo(SessionEventPhase.End));
                Assert.That(receive[1].Success, Is.False);
                Assert.That(step.Count, Is.EqualTo(2));
                Assert.That(step[1].Success, Is.False);
            }
            finally { DestroyWorld<ClientWorld>(); }
        }

        [Test]
        public void SimultaneousCloseEmitsStateOnlyForClosingAndClosedTransitions()
        {
            CreateWorld<ClientWorld>(ChunkOwnerType.Other);
            CreateWorld<ServerWorld>(ChunkOwnerType.Self);
            MemoryTransport.CreatePair(8, out var clientTransport, out var serverTransport);
            var serverObserver = new RecordingObserver();
            using var client = new Session<ClientWorld>(ClientConfig(), EmptySchema<ClientWorld>(), clientTransport);
            using var server = new Session<ServerWorld>(ServerConfig(), EmptySchema<ServerWorld>(), serverTransport, serverObserver);
            try
            {
                PumpEstablished(client, server);
                client.Close();
                server.Close();
                var beforeExchange = serverObserver.Events.FindAll(item => item.Kind == SessionEventKind.State).Count;
                client.Step(3);
                server.Step(3);
                var afterExchange = serverObserver.Events.FindAll(item => item.Kind == SessionEventKind.State).Count;
                Assert.That(server.State, Is.EqualTo(SessionState.Closed));
                Assert.That(afterExchange, Is.EqualTo(beforeExchange + 1));
            }
            finally { DestroyWorld<ClientWorld>(); DestroyWorld<ServerWorld>(); }
        }

        [Test]
        public void AcceptedDispatchAndConsumedResyncAdvanceExactStatsAndPoints()
        {
            CreateWorld<ClientWorld>(ChunkOwnerType.Other, true);
            CreateWorld<ServerWorld>(ChunkOwnerType.Self, true);
            var receiver = World<ServerWorld>.RegisterEventReceiver<CommandAcceptedEvent<DiagCommand>>();
            MemoryTransport.CreatePair(8, out var clientTransport, out var serverTransport);
            var clientObserver = new RecordingObserver();
            var serverObserver = new RecordingObserver();
            using var client = new Session<ClientWorld>(ClientConfig(), CommandSchema<ClientWorld, ClientCommandAuthorizer>(), clientTransport, clientObserver);
            using var server = new Session<ServerWorld>(ServerConfig(), CommandSchema<ServerWorld, ServerCommandAuthorizer>(), serverTransport, serverObserver);
            try
            {
                PumpEstablished(client, server);
                var command = new DiagCommand { Value = 4 };
                Assert.That(client.Enqueue(in command, 2), Is.EqualTo(EnqueueResult.Queued));
                client.Step(3); server.Step(3); client.Step(4);
                Assert.That(client.Stats.CommandsQueued, Is.EqualTo(1));
                Assert.That(server.Stats.CommandsAccepted, Is.EqualTo(1));
                Assert.That(server.Stats.CommandsRejected, Is.Zero);
                Assert.That(serverObserver.Events.Exists(item => item.Kind == SessionEventKind.Dispatch &&
                    item.Phase == SessionEventPhase.End && item.Success && item.Code == (ushort)DispatchResult.Accepted), Is.True);

                var payload = PacketLease.Rent(8);
                PacketLease packet = default;
                try
                {
                    var request = new ResyncRequestPayload { Reason = ResyncReason.HashMismatch, LastAcceptedTick = PacketHeader.NoneTick };
                    Assert.That(PayloadCodec.TryWrite(request, payload.CapacitySpan.Slice(0, 8), out var written), Is.True);
                    payload.SetLength(written);
                    Assert.That(SessionProtocol.TryEncodeTransfer(PacketKind.ResyncRequest, Channel.ReliableOrdered,
                        server.Epoch, 4, PacketHeader.NoneTick, default, PacketHeader.NoneTick, 0,
                        payload.Span, null, out packet), Is.True);
                    Assert.That(clientTransport.TrySend(Channel.ReliableOrdered, ref packet), Is.True);
                    server.Step(4);
                }
                finally
                {
                    if (packet.IsValid) packet.Dispose();
                    if (payload.IsValid) payload.Dispose();
                }
                Assert.That(server.Stats.Resyncs, Is.EqualTo(1));
                Assert.That(serverObserver.Events.Exists(item => item.Kind == SessionEventKind.Resync &&
                    item.Phase == SessionEventPhase.Point && item.Reason == (ushort)ResyncReason.HashMismatch), Is.True);
            }
            finally
            {
                World<ServerWorld>.DeleteEventReceiver(ref receiver);
                DestroyWorld<ClientWorld>(); DestroyWorld<ServerWorld>();
            }
        }

        [Test]
        public void AuthorizationRejectedDispatchIsSuccessfulAndCountedSeparately()
        {
            CreateWorld<ClientWorld>(ChunkOwnerType.Other, true);
            CreateWorld<ServerWorld>(ChunkOwnerType.Self, true);
            var receiver = World<ServerWorld>.RegisterEventReceiver<CommandRejectedEvent<DiagCommand>>();
            MemoryTransport.CreatePair(8, out var clientTransport, out var serverTransport);
            var observer = new RecordingObserver();
            using var client = new Session<ClientWorld>(ClientConfig(), CommandSchema<ClientWorld, ClientCommandAuthorizer>(), clientTransport);
            using var server = new Session<ServerWorld>(ServerConfig(), CommandSchema<ServerWorld, RejectCommandAuthorizer>(), serverTransport, observer);
            try
            {
                PumpEstablished(client, server);
                var command = new DiagCommand { Value = 8 };
                client.Enqueue(in command, 3);
                client.Step(3); server.Step(3);
                Assert.That(server.Stats.CommandsAccepted, Is.Zero);
                Assert.That(server.Stats.CommandsRejected, Is.EqualTo(1));
                Assert.That(observer.Events.Exists(item => item.Kind == SessionEventKind.Dispatch &&
                    item.Phase == SessionEventPhase.End && item.Success && item.Code == (ushort)DispatchResult.Rejected), Is.True);
            }
            finally
            {
                World<ServerWorld>.DeleteEventReceiver(ref receiver);
                DestroyWorld<ClientWorld>(); DestroyWorld<ServerWorld>();
            }
        }

        [Test]
        public void DecodeFailurePairsFaultAndExactWireCountersWithThrowingObserver()
        {
            CreateWorld<ClientWorld>(ChunkOwnerType.Other);
            MemoryTransport.CreatePair(4, out var clientTransport, out var peer);
            var observer = new RecordingThrowingObserver();
            using var client = new Session<ClientWorld>(ClientConfig(), EmptySchema<ClientWorld>(), clientTransport, observer);
            try
            {
                var malformed = PacketLease.Rent(1); malformed.CapacitySpan[0] = 0xFF; malformed.SetLength(1);
                Assert.That(peer.TrySend(Channel.ReliableOrdered, ref malformed), Is.True);
                client.Step(0);
                Assert.That(client.State, Is.EqualTo(SessionState.Faulted));
                Assert.That(client.Stats.ReceivedPackets, Is.EqualTo(1));
                Assert.That(client.Stats.ReceivedBytes, Is.EqualTo(1));
                Assert.That(client.Stats.DecodedBytes, Is.Zero);
                Assert.That(client.Stats.Faults, Is.EqualTo(1));
                AssertPair(observer.Events, SessionEventKind.Decode, false);
                Assert.That(observer.Events.Exists(value => value.Kind == SessionEventKind.Fault), Is.True);
                Assert.That(client.Stats.ObserverErrors, Is.EqualTo((ulong)observer.Events.Count));
            }
            finally { peer.Dispose(); DestroyWorld<ClientWorld>(); }
        }

        [Test]
        public void DecodeExceptionPairsWithoutDecodedOrDispatchCounters()
        {
            CreateWorld<ClientWorld>(ChunkOwnerType.Other, true);
            CreateWorld<ServerWorld>(ChunkOwnerType.Self, true);
            var receiver = World<ServerWorld>.RegisterEventReceiver<CommandAcceptedEvent<DiagCommand>>();
            MemoryTransport.CreatePair(8, out var clientTransport, out var serverTransport);
            var observer = new RecordingThrowingObserver();
            using var client = new Session<ClientWorld>(ClientConfig(), CommandSchema<ClientWorld, DiagCommandCodec, ClientCommandAuthorizer>(), clientTransport);
            using var server = new Session<ServerWorld>(ServerConfig(), CommandSchema<ServerWorld, ModeCommandCodec, ServerCommandAuthorizer>(), serverTransport, observer);
            try
            {
                PumpEstablished(client, server);
                var before = server.Stats;
                var command = new DiagCommand { Value = 12 };
                Assert.That(client.Enqueue(in command, 2), Is.EqualTo(EnqueueResult.Queued));
                client.Step(3);
                ModeCommandCodec.ReadMode = 2;
                var error = Assert.Throws<InvalidOperationException>(() => server.Step(3));
                Assert.That(error.Message, Is.EqualTo("codec read"));
                Assert.That(server.Stats.ReceivedPackets, Is.EqualTo(before.ReceivedPackets + 1));
                Assert.That(server.Stats.ReceivedBytes, Is.EqualTo(before.ReceivedBytes +
                    (ulong)LastEnd(observer.Events, SessionEventKind.Receive).WireBytes));
                Assert.That(server.Stats.DecodedBytes, Is.EqualTo(before.DecodedBytes));
                Assert.That(server.Stats.CommandsAccepted, Is.EqualTo(before.CommandsAccepted));
                Assert.That(server.Stats.CommandsRejected, Is.EqualTo(before.CommandsRejected));
                Assert.That(server.Stats.Faults, Is.EqualTo(before.Faults + 1));
                AssertPair(observer.Events, SessionEventKind.Decode, false);
                Assert.That(server.Stats.ObserverErrors, Is.EqualTo((ulong)observer.Events.Count));
            }
            finally
            {
                ModeCommandCodec.ReadMode = 0;
                World<ServerWorld>.DeleteEventReceiver(ref receiver);
                DestroyWorld<ClientWorld>(); DestroyWorld<ServerWorld>();
            }
        }

        [TestCase(1, false)]
        [TestCase(2, true)]
        public void EncodeFailureAndExceptionPairWithoutAdvancingWireCounters(int readMode, bool throws)
        {
            CreateWorld<ClientWorld>(ChunkOwnerType.Other, true);
            CreateWorld<ServerWorld>(ChunkOwnerType.Self, true);
            MemoryTransport.CreatePair(8, out var clientTransport, out var serverTransport);
            var observer = new RecordingThrowingObserver();
            using var client = new Session<ClientWorld>(ClientConfig(), CommandSchema<ClientWorld, ModeCommandCodec, ClientCommandAuthorizer>(), clientTransport, observer);
            using var server = new Session<ServerWorld>(ServerConfig(), CommandSchema<ServerWorld, DiagCommandCodec, ServerCommandAuthorizer>(), serverTransport);
            try
            {
                PumpEstablished(client, server);
                var command = new DiagCommand { Value = 13 };
                Assert.That(client.Enqueue(in command, 2), Is.EqualTo(EnqueueResult.Queued));
                var before = client.Stats;
                ModeCommandCodec.ReadMode = readMode;
                if (throws)
                    Assert.That(Assert.Throws<InvalidOperationException>(() => client.Step(3)).Message, Is.EqualTo("codec read"));
                else
                    client.Step(3);
                Assert.That(client.Stats.SentPackets, Is.EqualTo(before.SentPackets));
                Assert.That(client.Stats.SentBytes, Is.EqualTo(before.SentBytes));
                AssertPair(observer.Events, SessionEventKind.Encode, false);
                Assert.That(client.Stats.Faults, Is.EqualTo(before.Faults + (throws ? 0UL : 1UL)));
                Assert.That(client.Stats.ObserverErrors, Is.EqualTo((ulong)observer.Events.Count));
            }
            finally { ModeCommandCodec.ReadMode = 0; DestroyWorld<ClientWorld>(); DestroyWorld<ServerWorld>(); }
        }

        [TestCase(1, false)]
        [TestCase(2, true)]
        public void CaptureFailureAndExceptionPairWithoutCommittingStats(int writeMode, bool throws)
        {
            CreateWorld<ClientWorld>(ChunkOwnerType.Other, replication: true);
            CreateWorld<ServerWorld>(ChunkOwnerType.Self, replication: true);
            var entity = World<ServerWorld>.NewEntityInChunk<DiagEntity>(Chunk);
            entity.Set<ReplicatedTag>(); entity.Set(new DiagValue { Value = 7 });
            MemoryTransport.CreatePair(8, out var clientTransport, out var serverTransport);
            var observer = new RecordingThrowingObserver();
            using var client = new Session<ClientWorld>(ClientConfig(), ReplicationSchema<ClientWorld>(), clientTransport);
            using var server = new Session<ServerWorld>(ServerConfig(), ReplicationSchema<ServerWorld>(), serverTransport, observer);
            try
            {
                PumpEstablished(client, server);
                DiagValueCodec.WriteMode = writeMode;
                if (throws)
                    Assert.That(Assert.Throws<InvalidOperationException>(() => server.Capture(0)).Message, Is.EqualTo("codec write"));
                else
                    Assert.That(server.Capture(0), Is.EqualTo(CaptureResult.CodecFailed));
                Assert.That(server.Stats.SnapshotsCaptured, Is.Zero);
                AssertPair(observer.Events, SessionEventKind.Capture, false);
                Assert.That(server.Stats.ObserverErrors, Is.EqualTo((ulong)observer.Events.Count));
            }
            finally { DiagValueCodec.WriteMode = 0; DestroyWorld<ClientWorld>(); DestroyWorld<ServerWorld>(); }
        }

        [Test]
        public void DispatchExceptionPairsAndPreservesCommandCounters()
        {
            CreateWorld<ClientWorld>(ChunkOwnerType.Other, true);
            CreateWorld<ServerWorld>(ChunkOwnerType.Self, true);
            var receiver = World<ServerWorld>.RegisterEventReceiver<CommandAcceptedEvent<DiagCommand>>();
            MemoryTransport.CreatePair(8, out var clientTransport, out var serverTransport);
            var observer = new RecordingThrowingObserver();
            using var client = new Session<ClientWorld>(ClientConfig(), CommandSchema<ClientWorld, DiagCommandCodec, ClientCommandAuthorizer>(), clientTransport);
            using var server = new Session<ServerWorld>(ServerConfig(), CommandSchema<ServerWorld, DiagCommandCodec, ModeCommandAuthorizer>(), serverTransport, observer);
            try
            {
                PumpEstablished(client, server);
                var command = new DiagCommand { Value = 14 }; client.Enqueue(in command, 2); client.Step(3); server.Step(3);
                Assert.That(server.Stats.CommandsAccepted, Is.EqualTo(1));
                var before = server.Stats;
                command.Value = 15; client.Enqueue(in command, 3); client.Step(4);
                ModeCommandAuthorizer.Throw = true;
                Assert.That(Assert.Throws<InvalidOperationException>(() => server.Step(4)).Message, Is.EqualTo("authorize"));
                Assert.That(server.Stats.CommandsAccepted, Is.EqualTo(before.CommandsAccepted));
                Assert.That(server.Stats.CommandsRejected, Is.EqualTo(before.CommandsRejected));
                Assert.That(server.Stats.DecodedBytes, Is.EqualTo(before.DecodedBytes +
                    (ulong)LastEnd(observer.Events, SessionEventKind.Decode).DecodedBytes));
                Assert.That(server.Stats.Faults, Is.EqualTo(before.Faults + 1));
                AssertPair(observer.Events, SessionEventKind.Dispatch, false);
                Assert.That(server.Stats.ObserverErrors, Is.EqualTo((ulong)observer.Events.Count));
            }
            finally
            {
                ModeCommandAuthorizer.Throw = false;
                World<ServerWorld>.DeleteEventReceiver(ref receiver);
                DestroyWorld<ClientWorld>(); DestroyWorld<ServerWorld>();
            }
        }

        [Test]
        public void ApplyConflictPairsAndQueuesExactlyOneClientResync()
        {
            CreateWorld<ClientWorld>(ChunkOwnerType.Other, replication: true);
            CreateWorld<ServerWorld>(ChunkOwnerType.Self, replication: true);
            var source = World<ServerWorld>.NewEntityInChunk<DiagEntity>(Chunk);
            source.Set<ReplicatedTag>(); source.Set(new DiagValue { Value = 9 });
            MemoryTransport.CreatePair(8, out var clientTransport, out var serverTransport);
            var observer = new RecordingThrowingObserver();
            using var client = new Session<ClientWorld>(ClientConfig(), ReplicationSchema<ClientWorld>(), clientTransport, observer);
            using var server = new Session<ServerWorld>(ServerConfig(), ReplicationSchema<ServerWorld>(), serverTransport);
            try
            {
                PumpEstablished(client, server);
                Assert.That(server.Capture(0), Is.EqualTo(CaptureResult.Success));
                server.Step(3); client.Step(3);
                Assert.That(source.GID.TryUnpack<ClientWorld>(out var replica), Is.True);
                replica.Delete<ReplicatedTag>();
                Assert.That(server.Capture(1), Is.EqualTo(CaptureResult.Success));
                server.Step(4); client.Step(4);
                Assert.That(client.Stats.SnapshotsApplied, Is.EqualTo(1));
                Assert.That(client.Stats.Resyncs, Is.EqualTo(1));
                AssertPair(observer.Events, SessionEventKind.Apply, false);
                Assert.That(observer.Events.FindAll(value => value.Kind == SessionEventKind.Resync).Count, Is.EqualTo(1));
                client.Step(5); server.Step(5); client.Step(6);
                Assert.That(client.Stats.Resyncs, Is.EqualTo(1));
                Assert.That(server.Stats.Resyncs, Is.EqualTo(1));
                Assert.That(client.Stats.ObserverErrors, Is.EqualTo((ulong)observer.Events.Count));
            }
            finally { DestroyWorld<ClientWorld>(); DestroyWorld<ServerWorld>(); }
        }

        [Test]
        public void ApplyExceptionPairsFaultsAndPreservesApplyCounter()
        {
            CreateWorld<ClientWorld>(ChunkOwnerType.Other, replication: true);
            CreateWorld<ServerWorld>(ChunkOwnerType.Self, replication: true);
            var source = World<ServerWorld>.NewEntityInChunk<DiagEntity>(Chunk);
            source.Set<ReplicatedTag>(); source.Set(new DiagValue { Value = 10 });
            MemoryTransport.CreatePair(8, out var clientTransport, out var serverTransport);
            var observer = new RecordingThrowingObserver();
            using var client = new Session<ClientWorld>(ClientConfig(), ReplicationSchema<ClientWorld>(), clientTransport, observer);
            using var server = new Session<ServerWorld>(ServerConfig(), ReplicationSchema<ServerWorld>(), serverTransport);
            try
            {
                PumpEstablished(client, server);
                Assert.That(server.Capture(0), Is.EqualTo(CaptureResult.Success));
                server.Step(3);
                var before = client.Stats;
                DiagValueCodec.Reads = 0;
                DiagValueCodec.ThrowOnReadCall = 2;
                Assert.That(Assert.Throws<InvalidOperationException>(() => client.Step(3)).Message, Is.EqualTo("value read"));
                Assert.That(client.Stats.SnapshotsApplied, Is.EqualTo(before.SnapshotsApplied));
                Assert.That(client.Stats.ReceivedPackets, Is.EqualTo(before.ReceivedPackets + 1));
                Assert.That(client.Stats.ReceivedBytes, Is.EqualTo(before.ReceivedBytes +
                    (ulong)LastEnd(observer.Events, SessionEventKind.Receive).WireBytes));
                Assert.That(client.Stats.DecodedBytes, Is.EqualTo(before.DecodedBytes +
                    (ulong)LastEnd(observer.Events, SessionEventKind.Decode).DecodedBytes));
                Assert.That(client.Stats.Faults, Is.EqualTo(before.Faults + 1));
                AssertPair(observer.Events, SessionEventKind.Apply, false);
                Assert.That(client.Stats.ObserverErrors, Is.EqualTo((ulong)observer.Events.Count));
            }
            finally
            {
                DiagValueCodec.Reads = 0;
                DiagValueCodec.ThrowOnReadCall = 0;
                DestroyWorld<ClientWorld>(); DestroyWorld<ServerWorld>();
            }
        }

        private static SessionEvent Event(ulong id, long timestamp = 0, long elapsed = 0,
            SessionEventKind kind = SessionEventKind.Step, SessionEventPhase phase = SessionEventPhase.Point,
            PacketKind packet = (PacketKind)0, Channel channel = default, ulong hash = 0) =>
            new(id, 3, timestamp, elapsed, 0, 5, 91, 19, 2, 1, 0, hash,
                SessionRole.Client, kind, phase, SessionState.Established, SessionError.None,
                packet, channel, true, false);

        private sealed class ThrowingStream : MemoryStream
        {
            public override void Write(byte[] buffer, int offset, int count) => throw new IOException("write");
        }

        private sealed class FailingFlushStream : MemoryStream
        {
            public override void Flush() => throw new IOException("flush");
        }

        private sealed class FailingCloseStream : MemoryStream
        {
            protected override void Dispose(bool disposing) => throw new IOException("close");
        }

        private sealed class PartialWriteStream : MemoryStream
        {
            public override void Write(byte[] buffer, int offset, int count)
            {
                base.Write(buffer, offset, Math.Min(3, count));
                throw new IOException("partial");
            }
        }

        private static void PumpEstablished(Session<ClientWorld> client, Session<ServerWorld> server)
        {
            for (ulong step = 0; step < 3; step++) { client.Step(step); server.Step(step); }
            Assert.That(client.State, Is.EqualTo(SessionState.Established));
            Assert.That(server.State, Is.EqualTo(SessionState.Established));
        }

        private static void CreateWorld<TWorld>(ChunkOwnerType owner, bool command = false, bool replication = false) where TWorld : struct, IWorldType
        {
            World<TWorld>.Create(WorldConfig.Default());
            World<TWorld>.Types().Tag<ReplicatedTag>();
            if (replication) World<TWorld>.Types().EntityType<DiagEntity>().Component<DiagValue>();
            if (command)
                World<TWorld>.Types().Event<CommandAcceptedEvent<DiagCommand>>()
                    .Event<CommandRejectedEvent<DiagCommand>>();
            World<TWorld>.Initialize();
            World<TWorld>.RegisterCluster(Cluster);
            World<TWorld>.RegisterChunk(Chunk, owner, Cluster);
        }

        private static void DestroyWorld<TWorld>() where TWorld : struct, IWorldType
        {
            if (World<TWorld>.Status != WorldStatus.NotCreated) World<TWorld>.Destroy();
        }

        private static SessionConfig ClientConfig() => SessionConfig.Client(51, 20, 40);
        private static SessionConfig ServerConfig() => SessionConfig.Server(7, 9, 53, 30,
            new[] { new ChunkMapping { Chunk = Chunk, Cluster = Cluster, Role = 1 } });
        private static Schema EmptySchema<TWorld>() where TWorld : struct, IWorldType =>
            new SchemaBuilder<TWorld>().Freeze();

        private static Schema CommandSchema<TWorld, TAuthorizer>()
            where TWorld : struct, IWorldType
            where TAuthorizer : struct, ICommandAuthorizer<TWorld, DiagCommand> =>
            new SchemaBuilder<TWorld>().Command<DiagCommand, DiagCommandCodec, TAuthorizer>(
                CommandId, 1, CommandCodecId, 4).Freeze();

        private static Schema CommandSchema<TWorld, TCodec, TAuthorizer>()
            where TWorld : struct, IWorldType
            where TCodec : struct, ICodec<DiagCommand>
            where TAuthorizer : struct, ICommandAuthorizer<TWorld, DiagCommand> =>
            new SchemaBuilder<TWorld>().Command<DiagCommand, TCodec, TAuthorizer>(
                CommandId, 1, CommandCodecId, 4).Freeze();

        private static Schema ReplicationSchema<TWorld>() where TWorld : struct, IWorldType =>
            new SchemaBuilder<TWorld>()
                .EntityKind<DiagEntity>(new TypeId(new Guid(603, 0, 0, new byte[8])))
                .Component<DiagValue, DiagValueCodec>(new TypeId(new Guid(604, 0, 0, new byte[8])),
                    1, new CodecId(new Guid(605, 0, 0, new byte[8])), 4)
                .Freeze();

        private static void AssertPair(List<SessionEvent> events, SessionEventKind kind, bool success)
        {
            var pair = events.FindAll(value => value.Kind == kind);
            Assert.That(pair.Count, Is.GreaterThanOrEqualTo(2), kind.ToString());
            Assert.That(pair[pair.Count - 2].Phase, Is.EqualTo(SessionEventPhase.Begin));
            Assert.That(pair[pair.Count - 1].Phase, Is.EqualTo(SessionEventPhase.End));
            Assert.That(pair[pair.Count - 1].Success, Is.EqualTo(success));
        }

        private static SessionEvent LastEnd(List<SessionEvent> events, SessionEventKind kind) =>
            events.FindLast(value => value.Kind == kind && value.Phase == SessionEventPhase.End);

        private sealed class RecordingObserver : ISessionObserver
        {
            internal readonly List<SessionEvent> Events = new();
            public void Observe(in SessionEvent value) => Events.Add(value);
        }

        private sealed class ThrowingObserver : ISessionObserver
        {
            public void Observe(in SessionEvent value) => throw new InvalidOperationException("observer");
        }

        private sealed class RecordingThrowingObserver : ISessionObserver
        {
            internal readonly List<SessionEvent> Events = new();
            public void Observe(in SessionEvent value)
            {
                Events.Add(value);
                throw new InvalidOperationException("observer");
            }
        }

        private sealed class ThrowStepTransport : ITransport, ISteppedTransport
        {
            public TransportState State { get; private set; } = TransportState.Connected;
            public TransportError Error { get; private set; } = TransportError.None;
            public void BeginStep(ulong stepIndex) => throw new InvalidOperationException("step");
            public bool TrySend(Channel channel, ref PacketLease packet) => false;
            public bool TryReceive(out Channel channel, out PacketLease packet) { channel = default; packet = default; return false; }
            public void Dispose() { State = TransportState.Disposed; Error = TransportError.Disposed; }
        }

        private sealed class ThrowReceiveTransport : ITransport, ISteppedTransport
        {
            public TransportState State { get; private set; } = TransportState.Connected;
            public TransportError Error { get; private set; } = TransportError.None;
            public void BeginStep(ulong stepIndex) { }
            public bool TrySend(Channel channel, ref PacketLease packet) => false;
            public bool TryReceive(out Channel channel, out PacketLease packet) { channel = default; packet = default; throw new InvalidOperationException("receive"); }
            public void Dispose() { State = TransportState.Disposed; Error = TransportError.Disposed; }
        }

        private sealed class GateTransport : ITransport, ISteppedTransport
        {
            private readonly ITransport _inner;
            private readonly ISteppedTransport _stepped;
            private int _rejects;
            private int _throws;
            internal GateTransport(ITransport inner, int rejects, int throws = 0) { _inner = inner; _stepped = (ISteppedTransport)inner; _rejects = rejects; _throws = throws; }
            public TransportState State => _inner.State;
            public TransportError Error => _inner.Error;
            public void BeginStep(ulong stepIndex) => _stepped.BeginStep(stepIndex);
            public bool TrySend(Channel channel, ref PacketLease packet) { if (_throws > 0) { _throws--; throw new InvalidOperationException("send"); } if (_rejects > 0) { _rejects--; return false; } return _inner.TrySend(channel, ref packet); }
            public bool TryReceive(out Channel channel, out PacketLease packet) => _inner.TryReceive(out channel, out packet);
            public void Dispose() => _inner.Dispose();
        }

        private struct DiagCommand { public int Value; }
        private struct DiagCommandCodec : ICodec<DiagCommand>
        {
            public bool TryWrite(in DiagCommand value, Span<byte> destination, out int written)
            {
                if (destination.Length < 4) { written = 0; return false; }
                BitConverter.TryWriteBytes(destination, value.Value); written = 4; return true;
            }
            public bool TryRead(ReadOnlySpan<byte> source, out DiagCommand value, out int read)
            {
                if (source.Length != 4) { value = default; read = 0; return false; }
                value = new DiagCommand { Value = BitConverter.ToInt32(source) }; read = 4; return true;
            }
        }
        private struct ModeCommandCodec : ICodec<DiagCommand>
        {
            internal static int ReadMode;
            public bool TryWrite(in DiagCommand value, Span<byte> destination, out int written) =>
                new DiagCommandCodec().TryWrite(in value, destination, out written);
            public bool TryRead(ReadOnlySpan<byte> source, out DiagCommand value, out int read)
            {
                if (ReadMode == 2) throw new InvalidOperationException("codec read");
                if (ReadMode == 1) { value = default; read = 0; return false; }
                return new DiagCommandCodec().TryRead(source, out value, out read);
            }
        }
        private struct ClientCommandAuthorizer : ICommandAuthorizer<ClientWorld, DiagCommand>
        { public bool Authorize(in CommandContext context, in DiagCommand command) => true; }
        private struct ServerCommandAuthorizer : ICommandAuthorizer<ServerWorld, DiagCommand>
        { public bool Authorize(in CommandContext context, in DiagCommand command) => true; }
        private struct RejectCommandAuthorizer : ICommandAuthorizer<ServerWorld, DiagCommand>
        { public bool Authorize(in CommandContext context, in DiagCommand command) => false; }
        private struct ModeCommandAuthorizer : ICommandAuthorizer<ServerWorld, DiagCommand>
        {
            internal static bool Throw;
            public bool Authorize(in CommandContext context, in DiagCommand command) =>
                Throw ? throw new InvalidOperationException("authorize") : true;
        }

        private struct DiagEntity : IEntityType { public byte Id() => 17; }
        private struct DiagValue : IComponent
        {
            public int Value;
        }
        private struct DiagValueCodec : ICodec<DiagValue>
        {
            internal static int WriteMode;
            internal static int Reads;
            internal static int ThrowOnReadCall;
            public bool TryWrite(in DiagValue value, Span<byte> destination, out int written)
            {
                if (WriteMode == 2) throw new InvalidOperationException("codec write");
                if (WriteMode == 1 || destination.Length < 4) { written = 0; return false; }
                BitConverter.TryWriteBytes(destination, value.Value); written = 4; return true;
            }
            public bool TryRead(ReadOnlySpan<byte> source, out DiagValue value, out int read)
            {
                if (++Reads == ThrowOnReadCall) throw new InvalidOperationException("value read");
                if (source.Length != 4) { value = default; read = 0; return false; }
                value = new DiagValue { Value = BitConverter.ToInt32(source) }; read = 4; return true;
            }
        }

        private struct ClientWorld : IWorldType { }
        private struct ServerWorld : IWorldType { }
    }
}
