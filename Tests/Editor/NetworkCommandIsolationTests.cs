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

        private struct IsolationCommand : IEvent, INetworkCommand
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
