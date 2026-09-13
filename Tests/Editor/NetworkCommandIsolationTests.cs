using System;
using System.Collections.Generic;
using FFS.Libraries.StaticEcs;
using FFS.Libraries.StaticPack;
using NUnit.Framework;

namespace UniGame.StaticEcs.Network.Tests
{
    public sealed class NetworkCommandIsolationTests
    {
        [Test]
        public void MalformedCommandDoesNotBlockOtherPeersOrNextTick()
        {
            World<IsolationWorld>.Create(WorldConfig.Default());
            World<IsolationWorld>.Types()
                .Event<IsolationCommand>()
                .Event<NetworkCommandAcceptedEvent<IsolationCommand>>()
                .Event<NetworkCommandRejectedEvent<IsolationCommand>>();
            World<IsolationWorld>.Initialize();
            var receiver = World<IsolationWorld>
                .RegisterEventReceiver<NetworkCommandAcceptedEvent<IsolationCommand>>();
            try
            {
                using var pool = new NetworkBufferPool(1 << 20);
                var clientSchema = Schema(false);
                var serverSchema = Schema(true);
                var clientA = new NetworkSession<IsolationWorld>(
                    new ConnectionId(1), NetworkRole.Client, clientSchema, pool);
                var clientB = new NetworkSession<IsolationWorld>(
                    new ConnectionId(2), NetworkRole.Client, clientSchema, pool);
                var serverA = new NetworkSession<IsolationWorld>(
                    new ConnectionId(1), NetworkRole.Server, serverSchema, pool);
                var serverB = new NetworkSession<IsolationWorld>(
                    new ConnectionId(2), NetworkRole.Server, serverSchema, pool);
                Admit(clientA, serverSchema, 1);
                Admit(serverA, clientSchema, 1);
                Admit(clientB, serverSchema, 2);
                Admit(serverB, clientSchema, 2);

                clientA.CreateCommand(new IsolationCommand { Value = -1 }, 1,
                    out var malformed);
                clientB.CreateCommand(new IsolationCommand { Value = 2 }, 1,
                    out var firstValid);
                clientB.CreateCommand(new IsolationCommand { Value = 3 }, 2,
                    out var nextTick);
                var coordinator = new NetworkServerCoordinator<IsolationWorld>();
                coordinator.Add(serverA);
                coordinator.Add(serverB);
                Assert.That(coordinator.Queue(malformed, 1), Is.EqualTo(NetworkCommandResult.Queued));
                Assert.That(coordinator.Queue(firstValid, 1), Is.EqualTo(NetworkCommandResult.Queued));
                Assert.That(coordinator.Queue(nextTick, 1), Is.EqualTo(NetworkCommandResult.Queued));

                var firstDispatch = coordinator.Dispatch(1);
                Assert.That(firstDispatch.Total, Is.EqualTo(2));
                Assert.That(firstDispatch.Accepted, Is.EqualTo(1));
                Assert.That(coordinator.PendingCommandCount, Is.EqualTo(1));
                Assert.That(pool.CaptureDiagnostics().OutstandingLeases, Is.EqualTo(1));

                var secondDispatch = coordinator.Dispatch(2);
                Assert.That(secondDispatch.Total, Is.EqualTo(1));
                Assert.That(secondDispatch.Accepted, Is.EqualTo(1));
                Assert.That(coordinator.PendingCommandCount, Is.Zero);
                Assert.That(pool.CaptureDiagnostics().OutstandingLeases, Is.Zero);

                var values = AcceptedValues(receiver);
                Assert.That(values, Is.EqualTo(new[] { 2, 3 }));
            }
            finally
            {
                World<IsolationWorld>.DeleteEventReceiver(ref receiver);
                World<IsolationWorld>.Destroy();
            }
        }

        [Test]
        public void PolicyExceptionRemovesConsumedPrefixAndCurrentCommand()
        {
            World<IsolationWorld>.Create(WorldConfig.Default());
            World<IsolationWorld>.Types()
                .Event<IsolationCommand>()
                .Event<NetworkCommandAcceptedEvent<IsolationCommand>>()
                .Event<NetworkCommandRejectedEvent<IsolationCommand>>();
            World<IsolationWorld>.Initialize();
            var receiver = World<IsolationWorld>
                .RegisterEventReceiver<NetworkCommandAcceptedEvent<IsolationCommand>>();
            try
            {
                using var pool = new NetworkBufferPool(1 << 20);
                var clientSchema = Schema(false);
                var serverSchema = Schema(true);
                var client = new NetworkSession<IsolationWorld>(
                    new ConnectionId(3), NetworkRole.Client, clientSchema, pool);
                var server = new NetworkSession<IsolationWorld>(
                    new ConnectionId(3), NetworkRole.Server, serverSchema, pool);
                Admit(client, serverSchema, 3);
                Admit(server, clientSchema, 3);

                client.CreateCommand(new IsolationCommand { Value = 1 }, 1,
                    out var accepted);
                client.CreateCommand(new IsolationCommand { Value = 99 }, 1,
                    out var throwing);
                client.CreateCommand(new IsolationCommand { Value = 3 }, 2,
                    out var nextTick);
                var coordinator = new NetworkServerCoordinator<IsolationWorld>();
                coordinator.Add(server);
                Assert.That(coordinator.Queue(accepted, 1), Is.EqualTo(NetworkCommandResult.Queued));
                Assert.That(coordinator.Queue(throwing, 1), Is.EqualTo(NetworkCommandResult.Queued));
                Assert.That(coordinator.Queue(nextTick, 1), Is.EqualTo(NetworkCommandResult.Queued));

                Assert.Throws<InvalidOperationException>(() => coordinator.Dispatch(1));
                Assert.That(coordinator.PendingCommandCount, Is.EqualTo(1));
                Assert.That(pool.CaptureDiagnostics().OutstandingLeases, Is.EqualTo(1));

                var secondDispatch = coordinator.Dispatch(2);
                Assert.That(secondDispatch.Total, Is.EqualTo(1));
                Assert.That(secondDispatch.Accepted, Is.EqualTo(1));
                Assert.That(coordinator.PendingCommandCount, Is.Zero);
                Assert.That(pool.CaptureDiagnostics().OutstandingLeases, Is.Zero);
                Assert.That(AcceptedValues(receiver), Is.EqualTo(new[] { 1, 3 }));
            }
            finally
            {
                World<IsolationWorld>.DeleteEventReceiver(ref receiver);
                World<IsolationWorld>.Destroy();
            }
        }

        [Test]
        public void HeapDispatchesPermutedCommandsInCanonicalTargetPeerSequenceOrder()
        {
            World<IsolationWorld>.Create(WorldConfig.Default());
            World<IsolationWorld>.Types()
                .Event<IsolationCommand>()
                .Event<NetworkCommandAcceptedEvent<IsolationCommand>>()
                .Event<NetworkCommandRejectedEvent<IsolationCommand>>();
            World<IsolationWorld>.Initialize();
            var receiver = World<IsolationWorld>
                .RegisterEventReceiver<NetworkCommandAcceptedEvent<IsolationCommand>>();
            try
            {
                using var pool = new NetworkBufferPool(1 << 20);
                var clientSchema = Schema(false);
                var serverSchema = Schema(true);
                var client1 = new NetworkSession<IsolationWorld>(
                    new ConnectionId(11), NetworkRole.Client, clientSchema, pool);
                var client2 = new NetworkSession<IsolationWorld>(
                    new ConnectionId(12), NetworkRole.Client, clientSchema, pool);
                var client3 = new NetworkSession<IsolationWorld>(
                    new ConnectionId(13), NetworkRole.Client, clientSchema, pool);
                var server1 = new NetworkSession<IsolationWorld>(
                    new ConnectionId(11), NetworkRole.Server, serverSchema, pool);
                var server2 = new NetworkSession<IsolationWorld>(
                    new ConnectionId(12), NetworkRole.Server, serverSchema, pool);
                var server3 = new NetworkSession<IsolationWorld>(
                    new ConnectionId(13), NetworkRole.Server, serverSchema, pool);
                Admit(client1, serverSchema, 1); Admit(server1, clientSchema, 1);
                Admit(client2, serverSchema, 2); Admit(server2, clientSchema, 2);
                Admit(client3, serverSchema, 3); Admit(server3, clientSchema, 3);

                client1.CreateCommand(new IsolationCommand { Value = 101 }, 5, out var p1a);
                client1.CreateCommand(new IsolationCommand { Value = 102 }, 4, out var p1b);
                client1.CreateCommand(new IsolationCommand { Value = 103 }, 6, out var p1c);
                client2.CreateCommand(new IsolationCommand { Value = 201 }, 4, out var p2a);
                client2.CreateCommand(new IsolationCommand { Value = 202 }, 5, out var p2b);
                client3.CreateCommand(new IsolationCommand { Value = 301 }, 3, out var p3a);

                var coordinator = new NetworkServerCoordinator<IsolationWorld>();
                coordinator.Add(server1);
                coordinator.Add(server2);
                coordinator.Add(server3);

                Assert.That(coordinator.Queue(p2a, 5), Is.EqualTo(NetworkCommandResult.Queued));
                Assert.That(coordinator.Queue(p3a, 5), Is.EqualTo(NetworkCommandResult.Queued));
                Assert.That(coordinator.Queue(p1a, 5), Is.EqualTo(NetworkCommandResult.Queued));
                Assert.That(coordinator.Queue(p2b, 5), Is.EqualTo(NetworkCommandResult.Queued));
                Assert.That(coordinator.Queue(p1b, 5), Is.EqualTo(NetworkCommandResult.Queued));
                Assert.That(coordinator.Queue(p1c, 5), Is.EqualTo(NetworkCommandResult.Queued));

                var first = coordinator.Dispatch(5);
                Assert.That(first.Total, Is.EqualTo(5));
                Assert.That(first.Accepted, Is.EqualTo(5));
                Assert.That(coordinator.PendingCommandCount, Is.EqualTo(1));
                Assert.That(AcceptedValues(receiver),
                    Is.EqualTo(new[] { 301, 102, 201, 101, 202 }));

                var second = coordinator.Dispatch(6);
                Assert.That(second.Total, Is.EqualTo(1));
                Assert.That(coordinator.PendingCommandCount, Is.Zero);
                Assert.That(AcceptedValues(receiver),
                    Is.EqualTo(new[] { 103 }));
                Assert.That(pool.CaptureDiagnostics().OutstandingLeases, Is.Zero);
            }
            finally
            {
                World<IsolationWorld>.DeleteEventReceiver(ref receiver);
                World<IsolationWorld>.Destroy();
            }
        }

        [Test]
        public void HeapDispatchExceptionPopsThrowingCommandAndRetainsOtherPeers()
        {
            World<IsolationWorld>.Create(WorldConfig.Default());
            World<IsolationWorld>.Types()
                .Event<IsolationCommand>()
                .Event<NetworkCommandAcceptedEvent<IsolationCommand>>()
                .Event<NetworkCommandRejectedEvent<IsolationCommand>>();
            World<IsolationWorld>.Initialize();
            var receiver = World<IsolationWorld>
                .RegisterEventReceiver<NetworkCommandAcceptedEvent<IsolationCommand>>();
            try
            {
                using var pool = new NetworkBufferPool(1 << 20);
                var clientSchema = Schema(false);
                var serverSchema = Schema(true);
                var client1 = new NetworkSession<IsolationWorld>(
                    new ConnectionId(21), NetworkRole.Client, clientSchema, pool);
                var client2 = new NetworkSession<IsolationWorld>(
                    new ConnectionId(22), NetworkRole.Client, clientSchema, pool);
                var server1 = new NetworkSession<IsolationWorld>(
                    new ConnectionId(21), NetworkRole.Server, serverSchema, pool);
                var server2 = new NetworkSession<IsolationWorld>(
                    new ConnectionId(22), NetworkRole.Server, serverSchema, pool);
                Admit(client1, serverSchema, 1); Admit(server1, clientSchema, 1);
                Admit(client2, serverSchema, 2); Admit(server2, clientSchema, 2);

                client1.CreateCommand(new IsolationCommand { Value = 1 }, 1, out var accepted);
                client1.CreateCommand(new IsolationCommand { Value = 99 }, 1, out var throwing);
                client1.CreateCommand(new IsolationCommand { Value = 3 }, 2, out var nextTick);
                client2.CreateCommand(new IsolationCommand { Value = 2 }, 1, out var otherPeer);

                var coordinator = new NetworkServerCoordinator<IsolationWorld>();
                coordinator.Add(server1);
                coordinator.Add(server2);
                Assert.That(coordinator.Queue(accepted, 1), Is.EqualTo(NetworkCommandResult.Queued));
                Assert.That(coordinator.Queue(throwing, 1), Is.EqualTo(NetworkCommandResult.Queued));
                Assert.That(coordinator.Queue(otherPeer, 1), Is.EqualTo(NetworkCommandResult.Queued));
                Assert.That(coordinator.Queue(nextTick, 1), Is.EqualTo(NetworkCommandResult.Queued));

                Assert.Throws<InvalidOperationException>(() => coordinator.Dispatch(1));
                Assert.That(coordinator.PendingCommandCount, Is.EqualTo(2));
                Assert.That(pool.CaptureDiagnostics().OutstandingLeases, Is.EqualTo(2));

                Assert.That(coordinator.Dispatch(1).Total, Is.EqualTo(1));
                Assert.That(coordinator.PendingCommandCount, Is.EqualTo(1));
                Assert.That(coordinator.Dispatch(2).Total, Is.EqualTo(1));
                Assert.That(coordinator.PendingCommandCount, Is.Zero);
                Assert.That(pool.CaptureDiagnostics().OutstandingLeases, Is.Zero);
                Assert.That(AcceptedValues(receiver), Is.EqualTo(new[] { 1, 2, 3 }));
            }
            finally
            {
                World<IsolationWorld>.DeleteEventReceiver(ref receiver);
                World<IsolationWorld>.Destroy();
            }
        }

        [Test]
        public void RemoveConnectionUnqueuesOwnedCommandsAndRestoresHeapOrder()
        {
            World<IsolationWorld>.Create(WorldConfig.Default());
            World<IsolationWorld>.Types()
                .Event<IsolationCommand>()
                .Event<NetworkCommandAcceptedEvent<IsolationCommand>>()
                .Event<NetworkCommandRejectedEvent<IsolationCommand>>();
            World<IsolationWorld>.Initialize();
            var receiver = World<IsolationWorld>
                .RegisterEventReceiver<NetworkCommandAcceptedEvent<IsolationCommand>>();
            try
            {
                using var pool = new NetworkBufferPool(1 << 20);
                var clientSchema = Schema(false);
                var serverSchema = Schema(true);
                var client1 = new NetworkSession<IsolationWorld>(
                    new ConnectionId(31), NetworkRole.Client, clientSchema, pool);
                var client2 = new NetworkSession<IsolationWorld>(
                    new ConnectionId(32), NetworkRole.Client, clientSchema, pool);
                var server1 = new NetworkSession<IsolationWorld>(
                    new ConnectionId(31), NetworkRole.Server, serverSchema, pool);
                var server2 = new NetworkSession<IsolationWorld>(
                    new ConnectionId(32), NetworkRole.Server, serverSchema, pool);
                Admit(client1, serverSchema, 1); Admit(server1, clientSchema, 1);
                Admit(client2, serverSchema, 2); Admit(server2, clientSchema, 2);

                client1.CreateCommand(new IsolationCommand { Value = 11 }, 5, out var p1a);
                client1.CreateCommand(new IsolationCommand { Value = 12 }, 4, out var p1b);
                client2.CreateCommand(new IsolationCommand { Value = 21 }, 6, out var p2a);
                client2.CreateCommand(new IsolationCommand { Value = 22 }, 3, out var p2b);

                var coordinator = new NetworkServerCoordinator<IsolationWorld>();
                coordinator.Add(server1);
                coordinator.Add(server2);
                Assert.That(coordinator.Queue(p1a, 5), Is.EqualTo(NetworkCommandResult.Queued));
                Assert.That(coordinator.Queue(p1b, 5), Is.EqualTo(NetworkCommandResult.Queued));
                Assert.That(coordinator.Queue(p2a, 5), Is.EqualTo(NetworkCommandResult.Queued));
                Assert.That(coordinator.Queue(p2b, 5), Is.EqualTo(NetworkCommandResult.Queued));
                Assert.That(coordinator.PendingCommandCount, Is.EqualTo(4));
                Assert.That(coordinator.PendingCommandBytes, Is.EqualTo(16));

                Assert.That(coordinator.Remove(new ConnectionId(32)), Is.True);
                Assert.That(coordinator.PendingCommandCount, Is.EqualTo(2));
                Assert.That(coordinator.PendingCommandBytes, Is.EqualTo(8));
                Assert.That(pool.CaptureDiagnostics().OutstandingLeases, Is.EqualTo(2));

                var summary = coordinator.Dispatch(5);
                Assert.That(summary.Total, Is.EqualTo(2));
                Assert.That(coordinator.PendingCommandCount, Is.Zero);
                Assert.That(pool.CaptureDiagnostics().OutstandingLeases, Is.Zero);
                Assert.That(AcceptedValues(receiver), Is.EqualTo(new[] { 12, 11 }));
            }
            finally
            {
                World<IsolationWorld>.DeleteEventReceiver(ref receiver);
                World<IsolationWorld>.Destroy();
            }
        }

        [Test]
        public void ClearDisposesQueuedCommandsAndHeapRemainsReusable()
        {
            World<IsolationWorld>.Create(WorldConfig.Default());
            World<IsolationWorld>.Types()
                .Event<IsolationCommand>()
                .Event<NetworkCommandAcceptedEvent<IsolationCommand>>()
                .Event<NetworkCommandRejectedEvent<IsolationCommand>>();
            World<IsolationWorld>.Initialize();
            var receiver = World<IsolationWorld>
                .RegisterEventReceiver<NetworkCommandAcceptedEvent<IsolationCommand>>();
            try
            {
                using var pool = new NetworkBufferPool(1 << 20);
                var clientSchema = Schema(false);
                var serverSchema = Schema(true);
                var client1 = new NetworkSession<IsolationWorld>(
                    new ConnectionId(41), NetworkRole.Client, clientSchema, pool);
                var client2 = new NetworkSession<IsolationWorld>(
                    new ConnectionId(42), NetworkRole.Client, clientSchema, pool);
                var server1 = new NetworkSession<IsolationWorld>(
                    new ConnectionId(41), NetworkRole.Server, serverSchema, pool);
                var server2 = new NetworkSession<IsolationWorld>(
                    new ConnectionId(42), NetworkRole.Server, serverSchema, pool);
                Admit(client1, serverSchema, 1); Admit(server1, clientSchema, 1);
                Admit(client2, serverSchema, 2); Admit(server2, clientSchema, 2);

                client1.CreateCommand(new IsolationCommand { Value = 7 }, 1, out var a);
                client2.CreateCommand(new IsolationCommand { Value = 8 }, 1, out var b);
                var coordinator = new NetworkServerCoordinator<IsolationWorld>();
                coordinator.Add(server1);
                coordinator.Add(server2);
                Assert.That(coordinator.Queue(a, 1), Is.EqualTo(NetworkCommandResult.Queued));
                Assert.That(coordinator.Queue(b, 1), Is.EqualTo(NetworkCommandResult.Queued));
                Assert.That(coordinator.PendingCommandCount, Is.EqualTo(2));
                Assert.That(coordinator.PendingCommandBytes, Is.EqualTo(8));

                coordinator.Clear();
                Assert.That(coordinator.PendingCommandCount, Is.Zero);
                Assert.That(coordinator.PendingCommandBytes, Is.Zero);
                Assert.That(pool.CaptureDiagnostics().OutstandingLeases, Is.Zero);
                Assert.That(coordinator.Dispatch(1).Total, Is.Zero);

                coordinator.Add(server1);
                coordinator.Add(server2);
                client1.CreateCommand(new IsolationCommand { Value = 9 }, 2, out var reused);
                Assert.That(coordinator.Queue(reused, 2), Is.EqualTo(NetworkCommandResult.Queued));
                Assert.That(coordinator.Dispatch(2).Total, Is.EqualTo(1));
                Assert.That(coordinator.PendingCommandCount, Is.Zero);
                Assert.That(pool.CaptureDiagnostics().OutstandingLeases, Is.Zero);
                Assert.That(AcceptedValues(receiver), Is.EqualTo(new[] { 9 }));
            }
            finally
            {
                World<IsolationWorld>.DeleteEventReceiver(ref receiver);
                World<IsolationWorld>.Destroy();
            }
        }

        [Test]
        public void RepeatedHeapCyclesReuseCapacityWithinLimitsAndHighWater()
        {
            World<IsolationWorld>.Create(WorldConfig.Default());
            World<IsolationWorld>.Types()
                .Event<IsolationCommand>()
                .Event<NetworkCommandAcceptedEvent<IsolationCommand>>()
                .Event<NetworkCommandRejectedEvent<IsolationCommand>>();
            World<IsolationWorld>.Initialize();
            try
            {
                using var pool = new NetworkBufferPool(1 << 20);
                var clientSchema = Schema(false);
                var serverSchema = Schema(true);
                var client1 = new NetworkSession<IsolationWorld>(
                    new ConnectionId(51), NetworkRole.Client, clientSchema, pool);
                var client2 = new NetworkSession<IsolationWorld>(
                    new ConnectionId(52), NetworkRole.Client, clientSchema, pool);
                var server1 = new NetworkSession<IsolationWorld>(
                    new ConnectionId(51), NetworkRole.Server, serverSchema, pool);
                var server2 = new NetworkSession<IsolationWorld>(
                    new ConnectionId(52), NetworkRole.Server, serverSchema, pool);
                Admit(client1, serverSchema, 1); Admit(server1, clientSchema, 1);
                Admit(client2, serverSchema, 2); Admit(server2, clientSchema, 2);

                var coordinator = new NetworkServerCoordinator<IsolationWorld>(
                    maxPendingCommandsPerPeer: 3);
                coordinator.Add(server1);
                coordinator.Add(server2);
                var dispatched = 0;
                for (uint cycle = 1; cycle <= 64; cycle++)
                {
                    client1.CreateCommand(new IsolationCommand { Value = 1000 + (int)cycle },
                        cycle, out var first);
                    client2.CreateCommand(new IsolationCommand { Value = 2000 + (int)cycle },
                        cycle, out var second);
                    Assert.That(coordinator.Queue(first, cycle), Is.EqualTo(NetworkCommandResult.Queued));
                    Assert.That(coordinator.Queue(second, cycle), Is.EqualTo(NetworkCommandResult.Queued));
                    Assert.That(coordinator.PendingCommandCount, Is.EqualTo(2));
                    dispatched += coordinator.Dispatch(cycle).Total;
                    Assert.That(coordinator.PendingCommandCount, Is.Zero);
                    Assert.That(coordinator.PendingCommandBytes, Is.Zero);
                }
                Assert.That(dispatched, Is.EqualTo(128));
                Assert.That(coordinator.PendingCommandsHighWater, Is.EqualTo(2));
                Assert.That(coordinator.PendingCommandBytesHighWater, Is.EqualTo(8));
                Assert.That(pool.CaptureDiagnostics().OutstandingLeases, Is.Zero);
            }
            finally
            {
                World<IsolationWorld>.Destroy();
            }
        }

        [Test]
        public void QueueEnforcesCommandAndByteLimitsBeforeHeapInsertion()
        {
            World<IsolationWorld>.Create(WorldConfig.Default());
            World<IsolationWorld>.Types()
                .Event<IsolationCommand>()
                .Event<NetworkCommandAcceptedEvent<IsolationCommand>>()
                .Event<NetworkCommandRejectedEvent<IsolationCommand>>();
            World<IsolationWorld>.Initialize();
            try
            {
                using var pool = new NetworkBufferPool(1 << 20);
                var clientSchema = Schema(false);
                var serverSchema = Schema(true);

                var commandClient = new NetworkSession<IsolationWorld>(
                    new ConnectionId(61), NetworkRole.Client, clientSchema, pool);
                var commandServer = new NetworkSession<IsolationWorld>(
                    new ConnectionId(61), NetworkRole.Server, serverSchema, pool);
                Admit(commandClient, serverSchema, 1); Admit(commandServer, clientSchema, 1);
                commandClient.CreateCommand(new IsolationCommand { Value = 1 }, 1, out var a);
                commandClient.CreateCommand(new IsolationCommand { Value = 2 }, 1, out var b);
                commandClient.CreateCommand(new IsolationCommand { Value = 3 }, 1, out var c);
                var commandLimited = new NetworkServerCoordinator<IsolationWorld>(
                    maxPendingCommandsPerPeer: 2);
                commandLimited.Add(commandServer);
                Assert.That(commandLimited.Queue(a, 1), Is.EqualTo(NetworkCommandResult.Queued));
                Assert.That(commandLimited.Queue(b, 1), Is.EqualTo(NetworkCommandResult.Queued));
                Assert.That(commandLimited.Queue(c, 1), Is.EqualTo(NetworkCommandResult.LimitExceeded));
                Assert.That(commandLimited.PendingCommandCount, Is.EqualTo(2));
                Assert.That(commandLimited.PendingCommandsHighWater, Is.EqualTo(2));
                c.Dispose();

                var byteClient = new NetworkSession<IsolationWorld>(
                    new ConnectionId(62), NetworkRole.Client, clientSchema, pool);
                var byteServer = new NetworkSession<IsolationWorld>(
                    new ConnectionId(62), NetworkRole.Server, serverSchema, pool);
                Admit(byteClient, serverSchema, 1); Admit(byteServer, clientSchema, 1);
                byteClient.CreateCommand(new IsolationCommand { Value = 4 }, 1, out var d);
                byteClient.CreateCommand(new IsolationCommand { Value = 5 }, 1, out var e);
                var byteLimited = new NetworkServerCoordinator<IsolationWorld>(
                    maxPendingBytesPerPeer: 6);
                byteLimited.Add(byteServer);
                Assert.That(byteLimited.Queue(d, 1), Is.EqualTo(NetworkCommandResult.Queued));
                Assert.That(byteLimited.Queue(e, 1), Is.EqualTo(NetworkCommandResult.LimitExceeded));
                Assert.That(byteLimited.PendingCommandCount, Is.EqualTo(1));
                Assert.That(byteLimited.PendingCommandBytes, Is.EqualTo(4));
                e.Dispose();

                commandLimited.Clear();
                byteLimited.Clear();
                Assert.That(pool.CaptureDiagnostics().OutstandingLeases, Is.Zero);
            }
            finally
            {
                World<IsolationWorld>.Destroy();
            }
        }

        private static NetworkSchema<IsolationWorld> Schema(bool server)
        {
            var factory = NetworkCompilerSupport.Create<IsolationWorld>();
            if (server)
                factory.Command<IsolationCommand, IsolationPolicy>(
                    new NetworkTypeId(77));
            else
                factory.Command<IsolationCommand>(new NetworkTypeId(77));
            return factory.Freeze();
        }

        private static void Admit(NetworkSession<IsolationWorld> session,
            NetworkSchema<IsolationWorld> remoteSchema, uint peer)
        {
            Assert.That(session.Admit(remoteSchema.Fingerprint, peer, 1,
                new ScopeId(1)), Is.EqualTo(NetworkAdmissionResult.Accepted));
        }

        private static List<int> AcceptedValues(
            EventReceiver<IsolationWorld, NetworkCommandAcceptedEvent<IsolationCommand>> receiver)
        {
            var values = new List<int>();
            foreach (World<IsolationWorld>.Event<NetworkCommandAcceptedEvent<IsolationCommand>> item in receiver)
                values.Add(item.Value.Command.Value);
            return values;
        }

        private struct IsolationWorld : IWorldType
        {
        }

        internal struct IsolationCommand : IEvent, INetworkCommand
        {
            public int Value;

            public void Write(ref BinaryPackWriter writer) => writer.WriteInt(Value);

            public void Read(ref BinaryPackReader reader, byte version)
            {
                Value = reader.ReadInt();
                if (Value == -1)
                    throw new InvalidOperationException("Malformed command payload.");
            }
        }

        private struct IsolationPolicy : INetworkCommandPolicy<IsolationWorld, IsolationCommand>
        {
            public bool Authorize(in NetworkCommandContext context,
                in IsolationCommand command)
            {
                if (command.Value == 99)
                    throw new InvalidOperationException("Gameplay policy failed.");
                return true;
            }
        }
    }
}
