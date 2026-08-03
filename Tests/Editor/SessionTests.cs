using System;
using System.Collections.Generic;
using System.Threading;
using FFS.Libraries.StaticEcs;
using NUnit.Framework;

namespace UniGame.StaticEcs.Network.Tests
{
    public sealed class SessionTests
    {
        private const uint Chunk = 19;
        private const uint SecondChunk = 20;
        private const ushort Cluster = 4;
        private static readonly TypeId DifferentId = new(new Guid(91, 0, 0, new byte[8]));

        [SetUp]
        public void EnterPoolTestLock() => Monitor.Enter(PoolTestGate.Sync);

        [TearDown]
        public void ExitPoolTestLock() => Monitor.Exit(PoolTestGate.Sync);

        [Test]
        public void PublicEnumValuesAndConfigBoundsAreFrozen()
        {
            Assert.That((byte)SessionRole.Client, Is.EqualTo(0));
            Assert.That((byte)SessionRole.Server, Is.EqualTo(1));
            Assert.That((byte)SessionState.Handshaking, Is.EqualTo(0));
            Assert.That((byte)SessionState.Established, Is.EqualTo(1));
            Assert.That((byte)SessionState.Closing, Is.EqualTo(2));
            Assert.That((byte)SessionState.Closed, Is.EqualTo(3));
            Assert.That((byte)SessionState.Faulted, Is.EqualTo(4));
            Assert.That((byte)SessionState.Disposed, Is.EqualTo(5));
            Assert.That((byte)SessionError.None, Is.EqualTo(0));
            Assert.That((byte)SessionError.Protocol, Is.EqualTo(1));
            Assert.That((byte)SessionError.Schema, Is.EqualTo(2));
            Assert.That((byte)SessionError.Limits, Is.EqualTo(3));
            Assert.That((byte)SessionError.Topology, Is.EqualTo(4));
            Assert.That((byte)SessionError.Epoch, Is.EqualTo(5));
            Assert.That((byte)SessionError.Sequence, Is.EqualTo(6));
            Assert.That((byte)SessionError.Transport, Is.EqualTo(7));
            Assert.That((byte)StepResult.None, Is.EqualTo(0));
            Assert.That((byte)StepResult.Received, Is.EqualTo(1));
            Assert.That((byte)StepResult.Sent, Is.EqualTo(2));
            Assert.That((byte)StepResult.StateChanged, Is.EqualTo(4));

            Assert.Throws<ArgumentOutOfRangeException>(() => SessionConfig.Client(0, 1, 1));
            Assert.Throws<ArgumentOutOfRangeException>(() => SessionConfig.Client(1, 0, 1));
            Assert.Throws<ArgumentOutOfRangeException>(() => SessionConfig.Client(1, 2, 1));
            Assert.Throws<ArgumentOutOfRangeException>(() => SessionConfig.Client(1, 1, 1, 23));
            Assert.Throws<ArgumentOutOfRangeException>(() => SessionConfig.Client(1, 1, 1, maxDecodedBytes: 23));
            Assert.Throws<ArgumentOutOfRangeException>(() => SessionConfig.Server(0, 1, 1, 1, Mapping()));
            Assert.Throws<ArgumentOutOfRangeException>(() => SessionConfig.Server(1, 0, 1, 1, Mapping()));
            Assert.Throws<ArgumentOutOfRangeException>(() => SessionConfig.Server(1, 1, 0, 1, Mapping()));
            Assert.Throws<ArgumentOutOfRangeException>(() => SessionConfig.Server(1, 1, 1, 0, Mapping()));
            Assert.Throws<ArgumentOutOfRangeException>(() => SessionConfig.Server(1, 1, 1, 1, ReadOnlySpan<ChunkMapping>.Empty));
            Assert.Throws<ArgumentException>(() => SessionConfig.Server(1, 1, 1, 1,
                new[] { Map(Chunk, Cluster), Map(Chunk, Cluster) }));
            Assert.Throws<ArgumentException>(() => SessionConfig.Server(1, 1, 1, 1,
                new[] { new ChunkMapping { Chunk = Chunk, Cluster = Cluster, Role = 2 } }));

            var source = new[] { Map(SecondChunk, Cluster), Map(Chunk, Cluster) };
            var config = SessionConfig.Server(7, 9, 11, 30, source);
            source[0] = Map(99, 99);
            Assert.That(config.Role, Is.EqualTo(SessionRole.Server));
            Assert.That(config.Chunks[0].Chunk, Is.EqualTo(Chunk));
            Assert.That(config.Chunks[1].Chunk, Is.EqualTo(SecondChunk));
        }

        [Test]
        public void ConstructorValidationOrderingAndOwnershipAreExact()
        {
            CreateWorld<ServerWorld>(ChunkOwnerType.Self);
            CreateWorld<ClientWorld>(ChunkOwnerType.Other);
            try
            {
                var schema = Schema<ServerWorld>();
                var config = ServerConfig();
                var transport = TestTransport.Unpaired();
                Assert.Throws<ArgumentNullException>(() => new Session<ServerWorld>(null, null, null));
                Assert.Throws<ArgumentNullException>(() => new Session<ServerWorld>(config, null, null));
                Assert.Throws<ArgumentNullException>(() => new Session<ServerWorld>(config, schema, null));
                Assert.Throws<InvalidOperationException>(() => new Session<ServerWorld>(config, Schema<ClientWorld>(), transport));
                Assert.That(transport.DisposeCount, Is.Zero);

                var nonStepped = new NonSteppedTransport();
                Assert.Throws<ArgumentException>(() => new Session<ServerWorld>(config, schema, nonStepped));
                Assert.That(nonStepped.DisposeCount, Is.Zero);

                transport.ForceTerminal(TransportState.Faulted, TransportError.InvalidPacket);
                Assert.Throws<InvalidOperationException>(() => new Session<ServerWorld>(config, schema, transport));
                Assert.That(transport.DisposeCount, Is.Zero);

                var owned = TestTransport.Unpaired();
                using (var session = new Session<ServerWorld>(config, schema, owned))
                {
                    Assert.That(session.HasScope, Is.True);
                    Assert.That(session.HasReplicator, Is.True);
                    Assert.That(session.State, Is.EqualTo(SessionState.Handshaking));
                }
                Assert.That(owned.DisposeCount, Is.EqualTo(1));
            }
            finally
            {
                DestroyWorld<ServerWorld>();
                DestroyWorld<ClientWorld>();
            }
        }

        [Test]
        public void ConstructorRejectsMissingTagAndAuthorityTopologyWithoutTakingTransport()
        {
            World<MissingTagWorld>.Create(WorldConfig.Default());
            World<MissingTagWorld>.Initialize();
            World<MissingTagWorld>.RegisterCluster(Cluster);
            World<MissingTagWorld>.RegisterChunk(Chunk, ChunkOwnerType.Self, Cluster);
            var missingTagTransport = TestTransport.Unpaired();
            try
            {
                Assert.Throws<InvalidOperationException>(() => new Session<MissingTagWorld>(
                    ServerConfig(), Schema<MissingTagWorld>(), missingTagTransport));
                Assert.That(missingTagTransport.DisposeCount, Is.Zero);
            }
            finally
            {
                DestroyWorld<MissingTagWorld>();
            }

            CreateWorld<InvalidAuthorityWorld>(ChunkOwnerType.Other);
            var topologyTransport = TestTransport.Unpaired();
            try
            {
                Assert.Throws<InvalidOperationException>(() => new Session<InvalidAuthorityWorld>(
                    ServerConfig(), Schema<InvalidAuthorityWorld>(), topologyTransport));
                Assert.That(topologyTransport.DisposeCount, Is.Zero);
            }
            finally
            {
                DestroyWorld<InvalidAuthorityWorld>();
            }
        }

        [Test]
        public void AcceptedHandshakeIsExactlyFourPacketsAcrossThreePumpIterations()
        {
            CreateWorld<ServerWorld>(ChunkOwnerType.Self);
            CreateWorld<ClientWorld>(ChunkOwnerType.Other);
            TestTransport.CreatePair(1, out var clientTransport, out var serverTransport);
            using var client = new Session<ClientWorld>(ClientConfig(), Schema<ClientWorld>(), clientTransport);
            using var server = new Session<ServerWorld>(ServerConfig(), Schema<ServerWorld>(), serverTransport);
            try
            {
                Assert.That(client.Step(0), Is.EqualTo(StepResult.Sent));
                Assert.That(server.Step(0), Is.EqualTo(StepResult.Received | StepResult.Sent));
                Assert.That(client.Step(1), Is.EqualTo(StepResult.Received));
                Assert.That(server.Step(1), Is.EqualTo(StepResult.Sent));
                Assert.That(client.Step(2), Is.EqualTo(StepResult.Received | StepResult.Sent | StepResult.StateChanged));
                Assert.That(server.Step(2), Is.EqualTo(StepResult.Received | StepResult.StateChanged));

                AssertEstablished(client);
                AssertEstablished(server);
                Assert.That(clientTransport.SendAttempts.Count, Is.EqualTo(2));
                Assert.That(serverTransport.SendAttempts.Count, Is.EqualTo(2));
                AssertPacket(clientTransport.SendAttempts[0], PacketKind.Hello, 1, 0);
                AssertPacket(serverTransport.SendAttempts[0], PacketKind.Hello, 1, 0);
                AssertPacket(serverTransport.SendAttempts[1], PacketKind.HelloAck, 2, 7);
                AssertPacket(clientTransport.SendAttempts[1], PacketKind.Ack, 2, 7);
                Assert.That(serverTransport.SendAttempts[0].StepIndex, Is.EqualTo(0));
                Assert.That(serverTransport.SendAttempts[1].StepIndex, Is.EqualTo(1));

                Assert.That(PayloadCodec.TryReadHello(Payload(clientTransport.SendAttempts[0].Bytes), out var clientHello), Is.True);
                Assert.That(clientHello.MaxWireBytes, Is.EqualTo(ProtocolLimits.MaxWirePayloadBytes));
                Assert.That(clientHello.MaxDecodedBytes, Is.EqualTo(ProtocolLimits.MaxDecodedPayloadBytes));
                Assert.That(PayloadCodec.TryReadHello(Payload(serverTransport.SendAttempts[0].Bytes), out var serverHello), Is.True);
                Assert.That(serverHello.Nonce, Is.EqualTo(33));
                Assert.That(serverHello.MinTickRate, Is.EqualTo(30));
                Assert.That(serverHello.MaxTickRate, Is.EqualTo(30));
            }
            finally
            {
                DestroyWorld<ServerWorld>();
                DestroyWorld<ClientWorld>();
            }
        }

        [Test]
        public void FalseFinalAckRetryIsByteIdenticalAndOwnsCollaboratorsBeforeEstablishment()
        {
            CreateWorld<ServerWorld>(ChunkOwnerType.Self);
            CreateWorld<ClientWorld>(ChunkOwnerType.Other);
            TestTransport.CreatePair(2, out var clientTransport, out var serverTransport);
            using var client = new Session<ClientWorld>(ClientConfig(), Schema<ClientWorld>(), clientTransport);
            using var server = new Session<ServerWorld>(ServerConfig(), Schema<ServerWorld>(), serverTransport);
            try
            {
                client.Step(0); server.Step(0);
                client.Step(1); server.Step(1);
                clientTransport.ReturnFalseNextSend = true;
                Assert.That(client.Step(2), Is.EqualTo(StepResult.Received));
                Assert.That(client.State, Is.EqualTo(SessionState.Handshaking));
                Assert.That(client.Result, Is.Null);
                Assert.That(client.HasScope, Is.True);
                Assert.That(client.HasReplicator, Is.True);
                var failedBytes = clientTransport.SendAttempts[1].Bytes;

                Assert.That(client.Step(3), Is.EqualTo(StepResult.Sent | StepResult.StateChanged));
                CollectionAssert.AreEqual(failedBytes, clientTransport.SendAttempts[2].Bytes);
                AssertPacket(clientTransport.SendAttempts[2], PacketKind.Ack, 2, 7);
                Assert.That(server.Step(3), Is.EqualTo(StepResult.Received | StepResult.StateChanged));
                AssertEstablished(client);
                AssertEstablished(server);
                Assert.Throws<ArgumentOutOfRangeException>(() => client.Step(3));
                CollectionAssert.AreEqual(new ulong[] { 0, 1, 2, 3 }, clientTransport.BeginSteps);
            }
            finally
            {
                DestroyWorld<ServerWorld>();
                DestroyWorld<ClientWorld>();
            }
        }

        [Test]
        public void SchemaRejectionUsesSafeClosingBarrierAndPublishesAtEndpointSpecificTimes()
        {
            CreateWorld<ServerWorld>(ChunkOwnerType.Self);
            CreateWorld<ClientWorld>(ChunkOwnerType.Other);
            TestTransport.CreatePair(1, out var clientTransport, out var serverTransport);
            using var client = new Session<ClientWorld>(ClientConfig(), DifferentSchema<ClientWorld>(), clientTransport);
            using var server = new Session<ServerWorld>(ServerConfig(), Schema<ServerWorld>(), serverTransport);
            try
            {
                client.Step(0); server.Step(0);
                client.Step(1); server.Step(1);
                Assert.That(server.State, Is.EqualTo(SessionState.Closing));
                Assert.That(server.Result, Is.Null);
                Assert.That(server.Reason, Is.Null);
                Assert.That(client.Step(2), Is.EqualTo(StepResult.Received | StepResult.StateChanged));
                Assert.That(client.State, Is.EqualTo(SessionState.Closed));
                Assert.That(client.Result, Is.EqualTo(ConnectResult.SchemaMismatch));
                Assert.That(client.Error, Is.EqualTo(SessionError.None));
                Assert.That(client.Reason, Is.Null);
                Assert.That(server.Step(2), Is.EqualTo(StepResult.None));
                client.Dispose();
                Assert.That(server.Step(3), Is.EqualTo(StepResult.StateChanged));
                Assert.That(server.State, Is.EqualTo(SessionState.Closed));
                Assert.That(server.Result, Is.EqualTo(ConnectResult.SchemaMismatch));
                Assert.That(server.Error, Is.EqualTo(SessionError.None));
                Assert.That(server.Reason, Is.Null);
                Assert.That(server.HasScope, Is.False);
                Assert.That(server.HasReplicator, Is.False);
            }
            finally
            {
                DestroyWorld<ServerWorld>();
                DestroyWorld<ClientWorld>();
            }
        }

        [Test]
        public void AsymmetricAcceptedMapSizeAndTickAdmissionUseExplicitRejections()
        {
            AssertRejection(SessionConfig.Client(21, 20, 40, 24, 24), Schema<ClientWorld>(), ConnectResult.LimitsRejected);
            AssertRejection(SessionConfig.Client(21, 10, 20), Schema<ClientWorld>(), ConnectResult.TickRateUnsupported);
        }

        [Test]
        public void OccupiedReplicaTopologyRejectsAcceptedMapBeforeFinalAckWithoutMutation()
        {
            CreateWorld<ServerWorld>(ChunkOwnerType.Self);
            CreateWorld<ClientWorld>(ChunkOwnerType.Other, occupied: true);
            var before = World<ClientWorld>.CalculateEntitiesCount();
            TestTransport.CreatePair(2, out var clientTransport, out var serverTransport);
            using var client = new Session<ClientWorld>(ClientConfig(), Schema<ClientWorld>(), clientTransport);
            using var server = new Session<ServerWorld>(ServerConfig(), Schema<ServerWorld>(), serverTransport);
            try
            {
                client.Step(0); server.Step(0);
                client.Step(1); server.Step(1);
                Assert.That(client.Step(2), Is.EqualTo(StepResult.Received | StepResult.StateChanged));
                Assert.That(client.State, Is.EqualTo(SessionState.Faulted));
                Assert.That(client.Error, Is.EqualTo(SessionError.Topology));
                Assert.That(client.Result, Is.EqualTo(ConnectResult.ChunkMapRejected));
                Assert.That(client.Reason, Is.Null);
                Assert.That(clientTransport.SendAttempts.Count, Is.EqualTo(1));
                Assert.That(client.HasScope, Is.False);
                Assert.That(client.HasReplicator, Is.False);
                Assert.That(World<ClientWorld>.CalculateEntitiesCount(), Is.EqualTo(before));
            }
            finally
            {
                DestroyWorld<ServerWorld>();
                DestroyWorld<ClientWorld>();
            }
        }

        [Test]
        public void AuthorityAndClientScopeDriftFaultAtRetryAndEstablishedSeams()
        {
            CreateWorld<ServerWorld>(ChunkOwnerType.Self);
            CreateWorld<ClientWorld>(ChunkOwnerType.Other);
            TestTransport.CreatePair(2, out var clientTransport, out var serverTransport);
            using var client = new Session<ClientWorld>(ClientConfig(), Schema<ClientWorld>(), clientTransport);
            using var server = new Session<ServerWorld>(ServerConfig(), Schema<ServerWorld>(), serverTransport);
            try
            {
                client.Step(0); server.Step(0);
                World<ServerWorld>.ChangeChunkOwner(Chunk, ChunkOwnerType.Other);
                Assert.That(server.Step(1), Is.EqualTo(StepResult.StateChanged));
                AssertTopologyFault(server);
                Assert.That(serverTransport.SendAttempts.Count, Is.EqualTo(1));
            }
            finally
            {
                DestroyWorld<ServerWorld>();
                DestroyWorld<ClientWorld>();
            }

            CreateWorld<ServerWorld>(ChunkOwnerType.Self);
            CreateWorld<ClientWorld>(ChunkOwnerType.Other);
            TestTransport.CreatePair(2, out clientTransport, out serverTransport);
            using var retryClient = new Session<ClientWorld>(ClientConfig(), Schema<ClientWorld>(), clientTransport);
            using var retryServer = new Session<ServerWorld>(ServerConfig(), Schema<ServerWorld>(), serverTransport);
            try
            {
                retryClient.Step(0); retryServer.Step(0);
                retryClient.Step(1); retryServer.Step(1);
                clientTransport.ReturnFalseNextSend = true;
                retryClient.Step(2);
                World<ClientWorld>.ChangeChunkOwner(Chunk, ChunkOwnerType.Self);
                Assert.That(retryClient.Step(3), Is.EqualTo(StepResult.StateChanged));
                AssertTopologyFault(retryClient);
                Assert.That(retryClient.Result, Is.EqualTo(ConnectResult.ChunkMapRejected));
                Assert.That(retryClient.HasScope, Is.False);
            }
            finally
            {
                DestroyWorld<ServerWorld>();
                DestroyWorld<ClientWorld>();
            }
        }

        [Test]
        public void ScopeLifecycleIsRevalidatedAtAuthorityRetryFinalAckAndBothEstablishedSteps()
        {
            CreateWorld<ServerWorld>(ChunkOwnerType.Self);
            CreateWorld<ClientWorld>(ChunkOwnerType.Other);
            TestTransport.CreatePair(2, out var clientTransport, out var serverTransport);
            using (var client = new Session<ClientWorld>(ClientConfig(), Schema<ClientWorld>(), clientTransport))
            using (var server = new Session<ServerWorld>(ServerConfig(), Schema<ServerWorld>(), serverTransport))
            {
                client.Step(0); server.Step(0); client.Step(1);
                serverTransport.ReturnFalseNextSend = true;
                Assert.That(server.Step(1), Is.EqualTo(StepResult.None));
                World<ServerWorld>.ChangeChunkOwner(Chunk, ChunkOwnerType.Other);
                Assert.That(server.Step(2), Is.EqualTo(StepResult.StateChanged));
                AssertTopologyFault(server);
                Assert.That(serverTransport.SendAttempts.Count, Is.EqualTo(2));
            }
            DestroyWorld<ServerWorld>();
            DestroyWorld<ClientWorld>();

            CreateWorld<ServerWorld>(ChunkOwnerType.Self);
            CreateWorld<ClientWorld>(ChunkOwnerType.Other);
            TestTransport.CreatePair(2, out clientTransport, out serverTransport);
            using (var client = new Session<ClientWorld>(ClientConfig(), Schema<ClientWorld>(), clientTransport))
            using (var server = new Session<ServerWorld>(ServerConfig(), Schema<ServerWorld>(), serverTransport))
            {
                client.Step(0); server.Step(0);
                client.Step(1); server.Step(1);
                client.Step(2);
                World<ServerWorld>.ChangeChunkOwner(Chunk, ChunkOwnerType.Other);
                Assert.That(server.Step(2), Is.EqualTo(StepResult.Received | StepResult.StateChanged));
                AssertTopologyFault(server);
                Assert.That(server.Result, Is.Null);
            }
            DestroyWorld<ServerWorld>();
            DestroyWorld<ClientWorld>();

            CreateWorld<ServerWorld>(ChunkOwnerType.Self);
            CreateWorld<ClientWorld>(ChunkOwnerType.Other);
            TestTransport.CreatePair(2, out clientTransport, out serverTransport);
            using (var client = new Session<ClientWorld>(ClientConfig(), Schema<ClientWorld>(), clientTransport))
            using (var server = new Session<ServerWorld>(ServerConfig(), Schema<ServerWorld>(), serverTransport))
            {
                PumpEstablished(client, server);
                DestroyWorld<ClientWorld>();
                Assert.That(client.Step(3), Is.EqualTo(StepResult.StateChanged));
                AssertTopologyFault(client);
                Assert.That(client.Result, Is.EqualTo(ConnectResult.Accepted));
            }
            DestroyWorld<ServerWorld>();
            DestroyWorld<ClientWorld>();

            CreateWorld<ServerWorld>(ChunkOwnerType.Self);
            CreateWorld<ClientWorld>(ChunkOwnerType.Other);
            TestTransport.CreatePair(2, out clientTransport, out serverTransport);
            using (var client = new Session<ClientWorld>(ClientConfig(), Schema<ClientWorld>(), clientTransport))
            using (var server = new Session<ServerWorld>(ServerConfig(), Schema<ServerWorld>(), serverTransport))
            {
                PumpEstablished(client, server);
                World<ServerWorld>.ChangeChunkOwner(Chunk, ChunkOwnerType.Other);
                Assert.That(server.Step(3), Is.EqualTo(StepResult.StateChanged));
                AssertTopologyFault(server);
                Assert.That(server.Result, Is.EqualTo(ConnectResult.Accepted));
            }
            DestroyWorld<ServerWorld>();
            DestroyWorld<ClientWorld>();
        }

        [Test]
        public void NonCanonicalAcceptedWireMapIsProtocolChunkMapFailure()
        {
            CreateWorld<ClientWorld>(ChunkOwnerType.Other, includeSecond: true);
            TestTransport.CreatePair(4, out var clientTransport, out var peer);
            using var client = new Session<ClientWorld>(ClientConfig(), Schema<ClientWorld>(), clientTransport);
            try
            {
                client.Step(0);
                var serverHello = HelloPacket(1, Schema<ClientWorld>().Hash, 0, new HelloPayload
                {
                    Nonce = 33,
                    MinTickRate = 30,
                    MaxTickRate = 30,
                    MaxWireBytes = ProtocolLimits.MaxWirePayloadBytes,
                    MaxDecodedBytes = ProtocolLimits.MaxDecodedPayloadBytes,
                    Capabilities = 0
                });
                clientTransport.Inject(Channel.ReliableOrdered, ref serverHello);
                Assert.That(client.Step(1), Is.EqualTo(StepResult.Received));

                var ack = HelloAckPacket(2, Schema<ClientWorld>().Hash, 7, ConnectResult.Accepted, 30, 9, 33,
                    new[] { Map(SecondChunk, Cluster), Map(Chunk, Cluster) });
                clientTransport.Inject(Channel.ReliableOrdered, ref ack);
                Assert.That(client.Step(2), Is.EqualTo(StepResult.Received | StepResult.StateChanged));
                Assert.That(client.State, Is.EqualTo(SessionState.Faulted));
                Assert.That(client.Error, Is.EqualTo(SessionError.Protocol));
                Assert.That(client.Result, Is.EqualTo(ConnectResult.ChunkMapRejected));
                Assert.That(client.Reason, Is.EqualTo(DisconnectReason.ProtocolViolation));
                Assert.That(client.HasScope, Is.False);
                Assert.That(clientTransport.SendAttempts.Count, Is.EqualTo(1));
            }
            finally
            {
                peer.Dispose();
                DestroyWorld<ClientWorld>();
            }
        }

        [Test]
        public void LocalWireAndDecodedBoundsAreIndependentBeforeDecode()
        {
            var wirePayload = new byte[25];
            ClientHelloBytes().CopyTo(wirePayload, 0);
            AssertLocalBound(wirePayload, wireLength: 25, decodedLength: 24, proveNoDecodeRent: false);
            AssertLocalBound(ClientHelloBytes(), wireLength: 24, decodedLength: 25, proveNoDecodeRent: true);
        }

        [Test]
        public void StepMonotonicityAndTransportExceptionsConsumeIndicesWithoutSyntheticState()
        {
            CreateWorld<ClientWorld>(ChunkOwnerType.Other);
            TestTransport.CreatePair(2, out var transport, out var peer);
            using var session = new Session<ClientWorld>(ClientConfig(), Schema<ClientWorld>(), transport);
            try
            {
                transport.ThrowBeginNext = true;
                Assert.Throws<TransportTestException>(() => session.Step(0));
                Assert.Throws<ArgumentOutOfRangeException>(() => session.Step(0));
                Assert.That(transport.BeginSteps.Count, Is.EqualTo(1));
                Assert.That(session.State, Is.EqualTo(SessionState.Handshaking));
                Assert.That(session.Result, Is.Null);

                transport.ThrowSendNext = true;
                Assert.Throws<TransportTestException>(() => session.Step(1));
                Assert.That(session.State, Is.EqualTo(SessionState.Handshaking));
                Assert.That(session.Result, Is.Null);
                var thrownBytes = transport.SendAttempts[0].Bytes;
                Assert.That(session.Step(2), Is.EqualTo(StepResult.Sent));
                CollectionAssert.AreEqual(thrownBytes, transport.SendAttempts[1].Bytes);
                AssertPacket(transport.SendAttempts[1], PacketKind.Hello, 1, 0);

                transport.ThrowReceiveNext = true;
                Assert.Throws<TransportTestException>(() => session.Step(3));
                Assert.Throws<ArgumentOutOfRangeException>(() => session.Step(3));
                Assert.That(session.State, Is.EqualTo(SessionState.Handshaking));
            }
            finally
            {
                peer.Dispose();
                DestroyWorld<ClientWorld>();
            }
        }

        [Test]
        public void SequenceDomainsRetainFourHighWatersAndNeverWrap()
        {
            var domains = new SequenceDomains
            {
                ReliableTransmit = uint.MaxValue,
                ReliableReceive = uint.MaxValue,
                UnreliableTransmit = 17,
                UnreliableReceive = 19
            };

            Assert.That(domains.TryNextReliableTransmit(out var transmit), Is.False);
            Assert.That(transmit, Is.Zero);
            Assert.That(domains.IsNextReliableReceive(0), Is.False);
            Assert.That(domains.ReliableTransmit, Is.EqualTo(uint.MaxValue));
            Assert.That(domains.ReliableReceive, Is.EqualTo(uint.MaxValue));
            Assert.That(domains.UnreliableTransmit, Is.EqualTo(17));
            Assert.That(domains.UnreliableReceive, Is.EqualTo(19));
        }

        [Test]
        public void SessionTransmitExhaustionRunsAfterBeginWithoutEncodeRentOrSend()
        {
            CreateWorld<ClientWorld>(ChunkOwnerType.Other);
            var transport = TestTransport.Unpaired();
            using var session = new Session<ClientWorld>(ClientConfig(), Schema<ClientWorld>(), transport);
            var held = new List<PacketLease>();
            try
            {
                session.SetReliableTransmitHighWaterForTests(uint.MaxValue);
                while (PacketLease.PooledStateCountForTests > 0) held.Add(PacketLease.Rent(1));
                var allocations = PacketLease.StateAllocationCountForTests;

                Assert.That(session.Step(0), Is.EqualTo(StepResult.StateChanged));
                Assert.That(transport.BeginSteps, Is.EqualTo(new ulong[] { 0 }));
                Assert.That(transport.SendAttempts, Is.Empty);
                Assert.That(PacketLease.StateAllocationCountForTests, Is.EqualTo(allocations));
                Assert.That(session.State, Is.EqualTo(SessionState.Faulted));
                Assert.That(session.Error, Is.EqualTo(SessionError.Sequence));
                Assert.That(session.Reason, Is.EqualTo(DisconnectReason.SequenceExhausted));
                Assert.Throws<InvalidOperationException>(() => session.SetReliableTransmitHighWaterForTests(0));
            }
            finally
            {
                for (var i = 0; i < held.Count; i++) held[i].Dispose();
                DestroyWorld<ClientWorld>();
            }
        }

        [Test]
        public void CachedControlTransformAndWarmedEncodingDoNotAllocatePerPacket()
        {
            var transform = SessionProtocol.ControlTransform;
            Assert.That(transform, Is.SameAs(SessionProtocol.ControlTransform));
            var hello = new HelloPayload
            {
                Nonce = 21,
                MinTickRate = 20,
                MaxTickRate = 40,
                MaxWireBytes = ProtocolLimits.MaxWirePayloadBytes,
                MaxDecodedBytes = ProtocolLimits.MaxDecodedPayloadBytes
            };
            var pending = PendingControl.HelloPacket(TypeId.Empty, in hello);
            for (var i = 0; i < 32; i++)
            {
                Assert.That(SessionProtocol.TryEncode(in pending, 1, out var warm), Is.True);
                warm.Dispose();
            }

            var before = GC.GetAllocatedBytesForCurrentThread();
            for (var i = 0; i < 256; i++)
            {
                if (!SessionProtocol.TryEncode(in pending, 1, out var packet)) Assert.Fail();
                packet.Dispose();
            }
            var after = GC.GetAllocatedBytesForCurrentThread();
            Assert.That(after, Is.EqualTo(before));
        }

        [Test]
        public void MemoryTransportCapacityOneProvesAcceptedPumpAndRejectedDeliveryBarrier()
        {
            CreateWorld<ServerWorld>(ChunkOwnerType.Self);
            CreateWorld<ClientWorld>(ChunkOwnerType.Other);
            MemoryTransport.CreatePair(1, out var clientTransport, out var serverTransport);
            using (var client = new Session<ClientWorld>(ClientConfig(), Schema<ClientWorld>(), clientTransport))
            using (var server = new Session<ServerWorld>(ServerConfig(), Schema<ServerWorld>(), serverTransport))
            {
                PumpEstablished(client, server);
            }
            DestroyWorld<ServerWorld>();
            DestroyWorld<ClientWorld>();

            CreateWorld<ServerWorld>(ChunkOwnerType.Self);
            CreateWorld<ClientWorld>(ChunkOwnerType.Other);
            MemoryTransport.CreatePair(1, out clientTransport, out serverTransport);
            var rejectedClient = new Session<ClientWorld>(ClientConfig(), DifferentSchema<ClientWorld>(), clientTransport);
            using var rejectedServer = new Session<ServerWorld>(ServerConfig(), Schema<ServerWorld>(), serverTransport);
            try
            {
                rejectedClient.Step(0); rejectedServer.Step(0);
                rejectedClient.Step(1); rejectedServer.Step(1);
                Assert.That(rejectedServer.State, Is.EqualTo(SessionState.Closing));
                Assert.That(rejectedServer.Result, Is.Null);
                Assert.That(rejectedClient.Step(2), Is.EqualTo(StepResult.Received | StepResult.StateChanged));
                Assert.That(rejectedClient.State, Is.EqualTo(SessionState.Closed));
                Assert.That(rejectedClient.Result, Is.EqualTo(ConnectResult.SchemaMismatch));
                rejectedClient.Dispose();
                Assert.That(rejectedServer.Step(2), Is.EqualTo(StepResult.StateChanged));
                Assert.That(rejectedServer.State, Is.EqualTo(SessionState.Closed));
                Assert.That(rejectedServer.Result, Is.EqualTo(ConnectResult.SchemaMismatch));
                Assert.That(rejectedServer.Error, Is.EqualTo(SessionError.None));
                Assert.That(rejectedServer.Reason, Is.Null);
            }
            finally
            {
                rejectedClient.Dispose();
                DestroyWorld<ServerWorld>();
                DestroyWorld<ClientWorld>();
            }
        }

        [Test]
        public void TransportThrowsAfterOwnershipTransferOrInboundAssignmentAreCleanedExactly()
        {
            CreateWorld<ClientWorld>(ChunkOwnerType.Other);
            TestTransport.CreatePair(2, out var clientTransport, out var peer);
            using (var client = new Session<ClientWorld>(ClientConfig(), Schema<ClientWorld>(), clientTransport))
            {
                clientTransport.TransferThenThrowSendNext = true;
                Assert.Throws<TransportTestException>(() => client.Step(0));
                Assert.That(clientTransport.LastSendAlias.IsValid, Is.False);
                Assert.Throws<ArgumentOutOfRangeException>(() => client.Step(0));
                Assert.That(client.Step(1), Is.EqualTo(StepResult.Sent));
                CollectionAssert.AreEqual(clientTransport.SendAttempts[0].Bytes, clientTransport.SendAttempts[1].Bytes);

                var hello = HelloPacket(1, Schema<ClientWorld>().Hash, 0, ServerHello());
                clientTransport.Inject(Channel.ReliableOrdered, ref hello);
                clientTransport.AssignThenThrowReceiveNext = true;
                Assert.Throws<TransportTestException>(() => client.Step(2));
                Assert.That(clientTransport.LastReceiveAlias.IsValid, Is.False);
                Assert.Throws<ArgumentOutOfRangeException>(() => client.Step(2));
            }
            peer.Dispose();
            DestroyWorld<ClientWorld>();

            CreateWorld<ServerWorld>(ChunkOwnerType.Self);
            TestTransport.CreatePair(2, out var serverTransport, out peer);
            using (var server = new Session<ServerWorld>(ServerConfig(), Schema<ServerWorld>(), serverTransport))
            {
                var hello = HelloPacket(1, Schema<ServerWorld>().Hash, 0, ClientHello());
                serverTransport.Inject(Channel.ReliableOrdered, ref hello);
                serverTransport.ThrowSendNext = true;
                Assert.Throws<TransportTestException>(() => server.Step(0));
                Assert.That(serverTransport.LastSendAlias.IsValid, Is.False);
                Assert.That(server.State, Is.EqualTo(SessionState.Handshaking));
                Assert.That(server.Result, Is.Null);
                Assert.Throws<ArgumentOutOfRangeException>(() => server.Step(0));
                Assert.That(server.Step(1), Is.EqualTo(StepResult.Sent));
                CollectionAssert.AreEqual(serverTransport.SendAttempts[0].Bytes, serverTransport.SendAttempts[1].Bytes);
                AssertPacket(serverTransport.SendAttempts[1], PacketKind.Hello, 1, 0);
            }
            peer.Dispose();
            DestroyWorld<ServerWorld>();
        }

        [Test]
        public void ServerHelloSemanticMatrixIsPhaseExact()
        {
            var hello = ServerHello();
            hello.Nonce = 0;
            AssertServerHelloFailure(hello, SessionError.Protocol, DisconnectReason.ProtocolViolation);
            hello = ServerHello(); hello.MinTickRate = 0; hello.MaxTickRate = 0;
            AssertServerHelloFailure(hello, SessionError.Protocol, DisconnectReason.ProtocolViolation);
            hello = ServerHello(); hello.MaxTickRate = 31;
            AssertServerHelloFailure(hello, SessionError.Protocol, DisconnectReason.ProtocolViolation);
            hello = ServerHello(); hello.MaxWireBytes = 23;
            AssertServerHelloFailure(hello, SessionError.Limits, DisconnectReason.LimitsExceeded);
            hello = ServerHello(); hello.MaxDecodedBytes = 23;
            AssertServerHelloFailure(hello, SessionError.Limits, DisconnectReason.LimitsExceeded);
            hello = ServerHello(); hello.Capabilities = 1;
            AssertServerHelloFailure(hello, SessionError.Limits, DisconnectReason.LimitsExceeded);

            CreateWorld<ClientWorld>(ChunkOwnerType.Other);
            TestTransport.CreatePair(2, out var transport, out var peer);
            using var client = new Session<ClientWorld>(ClientConfig(), Schema<ClientWorld>(), transport);
            try
            {
                client.Step(0);
                hello = ServerHello(); hello.MinTickRate = 50; hello.MaxTickRate = 50;
                var packet = HelloPacket(1, Schema<ClientWorld>().Hash, 0, hello);
                transport.Inject(Channel.ReliableOrdered, ref packet);
                Assert.That(client.Step(1), Is.EqualTo(StepResult.Received));
                Assert.That(client.State, Is.EqualTo(SessionState.Handshaking));
                Assert.That(client.Result, Is.Null);
                var rejected = HelloAckPacket(2, Schema<ClientWorld>().Hash, 0,
                    ConnectResult.TickRateUnsupported, 0, 0, 33, Array.Empty<ChunkMapping>());
                transport.Inject(Channel.ReliableOrdered, ref rejected);
                Assert.That(client.Step(2), Is.EqualTo(StepResult.Received | StepResult.StateChanged));
                Assert.That(client.State, Is.EqualTo(SessionState.Closed));
                Assert.That(client.Result, Is.EqualTo(ConnectResult.TickRateUnsupported));
            }
            finally
            {
                peer.Dispose();
                DestroyWorld<ClientWorld>();
            }
        }

        [Test]
        public void ClientHelloServerMatrixHasExactReplyAndPrecedence()
        {
            var hello = ClientHello();
            hello.Nonce = 0;
            AssertMalformedClientHello(hello);
            hello = ClientHello(); hello.MinTickRate = 0;
            AssertMalformedClientHello(hello);
            hello = ClientHello(); hello.MinTickRate = 0; hello.MaxTickRate = 0;
            AssertMalformedClientHello(hello);
            hello = ClientHello(); hello.MinTickRate = 40; hello.MaxTickRate = 20;
            AssertMalformedClientHello(hello);

            hello = ClientHello(); hello.MaxWireBytes = 23;
            AssertServerRejection(hello, Schema<ServerWorld>().Hash, ConnectResult.LimitsRejected);
            hello = ClientHello(); hello.MaxDecodedBytes = 23;
            AssertServerRejection(hello, Schema<ServerWorld>().Hash, ConnectResult.LimitsRejected);
            hello = ClientHello(); hello.Capabilities = 1;
            AssertServerRejection(hello, Schema<ServerWorld>().Hash, ConnectResult.LimitsRejected);

            hello = ClientHello();
            hello.MinTickRate = 40; hello.MaxTickRate = 50; hello.MaxWireBytes = 23;
            AssertServerRejection(hello, DifferentSchema<ServerWorld>().Hash, ConnectResult.SchemaMismatch);
            AssertServerRejection(hello, Schema<ServerWorld>().Hash, ConnectResult.TickRateUnsupported);
            hello = ClientHello(); hello.MaxWireBytes = 23; hello.Capabilities = 1;
            AssertServerRejection(hello, Schema<ServerWorld>().Hash, ConnectResult.LimitsRejected);
        }

        [Test]
        public void HelloAckSemanticMatrixBindsStoredSchemaNonceScalarsMapAndPrecedence()
        {
            var local = Schema<ClientWorld>().Hash;
            var different = DifferentSchema<ClientWorld>().Hash;
            AssertHelloAckFailure(local, ServerHello(), different, 7, ConnectResult.Accepted, 30, 9, 33,
                Mapping(), SessionError.Protocol, DisconnectReason.ProtocolViolation, null);
            AssertHelloAckFailure(local, ServerHello(), local, 7, ConnectResult.Accepted, 30, 9, 34,
                Mapping(), SessionError.Protocol, DisconnectReason.ProtocolViolation, null);
            AssertHelloAckFailure(local, ServerHello(), local, 0, ConnectResult.Accepted, 30, 9, 33,
                Mapping(), SessionError.Protocol, DisconnectReason.ProtocolViolation, null);
            AssertHelloAckFailure(local, ServerHello(), local, 7, ConnectResult.Accepted, 0, 9, 33,
                Mapping(), SessionError.Protocol, DisconnectReason.ProtocolViolation, null);
            AssertHelloAckFailure(local, ServerHello(), local, 7, ConnectResult.Accepted, 31, 9, 33,
                Mapping(), SessionError.Protocol, DisconnectReason.ProtocolViolation, null);
            AssertHelloAckFailure(local, ServerHello(), local, 7, ConnectResult.Accepted, 30, 0, 33,
                Mapping(), SessionError.Protocol, DisconnectReason.ProtocolViolation, null);
            AssertHelloAckFailure(local, ServerHello(), local, 7, ConnectResult.Accepted, 30, 9, 33,
                Array.Empty<ChunkMapping>(), SessionError.Protocol, DisconnectReason.ProtocolViolation,
                ConnectResult.ChunkMapRejected);
            AssertHelloAckFailure(local, ServerHello(), local, 7, ConnectResult.Accepted, 30, 9, 33,
                new[] { Map(Chunk, Cluster), Map(Chunk, Cluster) },
                SessionError.Protocol, DisconnectReason.ProtocolViolation, ConnectResult.ChunkMapRejected);
            AssertHelloAckFailure(local, ServerHello(), local, 7, ConnectResult.LimitsRejected, 0, 0, 33,
                Array.Empty<ChunkMapping>(), SessionError.Protocol, DisconnectReason.ProtocolViolation, null);
            AssertHelloAckFailure(local, ServerHello(), local, 0, ConnectResult.LimitsRejected, 30, 0, 33,
                Array.Empty<ChunkMapping>(), SessionError.Protocol, DisconnectReason.ProtocolViolation, null);
            AssertHelloAckFailure(local, ServerHello(), local, 0, ConnectResult.LimitsRejected, 0, 9, 33,
                Array.Empty<ChunkMapping>(), SessionError.Protocol, DisconnectReason.ProtocolViolation, null);
            AssertHelloAckFailure(local, ServerHello(), local, 0, ConnectResult.LimitsRejected, 0, 0, 33,
                Mapping(), SessionError.Protocol, DisconnectReason.ProtocolViolation, null);
            AssertHelloAckFailure(local, ServerHello(), local, 0, ConnectResult.LimitsRejected, 0, 0, 34,
                Array.Empty<ChunkMapping>(), SessionError.Protocol, DisconnectReason.ProtocolViolation, null);
            AssertHelloAckFailure(local, ServerHello(), local, 0, ConnectResult.ProtocolVersionMismatch, 0, 0, 33,
                Array.Empty<ChunkMapping>(), SessionError.Protocol, DisconnectReason.ProtocolViolation, null);

            var outside = ServerHello(); outside.MinTickRate = 50; outside.MaxTickRate = 50;
            AssertHelloAckFailure(different, outside, different, 0, ConnectResult.TickRateUnsupported, 0, 0, 33,
                Array.Empty<ChunkMapping>(), SessionError.Protocol, DisconnectReason.ProtocolViolation, null);
            AssertHelloAckFailure(different, ServerHello(), different, 7, ConnectResult.Accepted, 30, 9, 33,
                Mapping(), SessionError.Schema, DisconnectReason.SchemaMismatch, ConnectResult.SchemaMismatch);
        }

        [Test]
        public void FinalAckSemanticMatrixClassifiesSchemaEpochAndPayloadIndependently()
        {
            AssertFinalAckFailure(DifferentSchema<ServerWorld>().Hash, 7, Array.Empty<byte>(),
                SessionError.Schema, DisconnectReason.SchemaMismatch);
            AssertFinalAckFailure(Schema<ServerWorld>().Hash, 8, Array.Empty<byte>(),
                SessionError.Epoch, DisconnectReason.UnexpectedEpoch);
            AssertFinalAckFailure(Schema<ServerWorld>().Hash, 7, new byte[] { 1 },
                SessionError.Protocol, DisconnectReason.ProtocolViolation);
        }

        [Test]
        public void HeaderChannelSequenceAndPhaseMutationsFaultWithoutReply()
        {
            AssertClientHelloMutation(Channel.UnreliableSequenced, 1, 0, PacketHeader.NoneTick,
                PacketHeader.NoneTick, 0, SessionError.Protocol);
            AssertClientHelloMutation(Channel.ReliableOrdered, 2, 0, PacketHeader.NoneTick,
                PacketHeader.NoneTick, 0, SessionError.Protocol);
            AssertClientHelloMutation(Channel.ReliableOrdered, 1, 1, PacketHeader.NoneTick,
                PacketHeader.NoneTick, 0, SessionError.Protocol);
            AssertClientHelloMutation(Channel.ReliableOrdered, 1, 0, 0,
                PacketHeader.NoneTick, 0, SessionError.Protocol);
            AssertClientHelloMutation(Channel.ReliableOrdered, 1, 0, PacketHeader.NoneTick,
                0, 0, SessionError.Protocol);
            AssertClientHelloMutation(Channel.ReliableOrdered, 1, 0, PacketHeader.NoneTick,
                PacketHeader.NoneTick, 1, SessionError.Protocol);
        }

        [Test]
        public void RawHeaderFieldsAndLengthsFaultIndependently()
        {
            var hello = ServerHelloBytes();
            var hash = TestWire.XxHash64(hello);
            AssertRawClientFault(PacketKind.Hello, (PacketFlags)0, 0, PacketHeader.NoneTick,
                24, 24, hash, hello, Channel.ReliableOrdered);
            AssertRawClientFault(PacketKind.Hello, PacketFlags.ReliableOrdered, 1, PacketHeader.NoneTick,
                24, 24, hash, hello, Channel.ReliableOrdered);
            AssertRawClientFault(PacketKind.Hello, PacketFlags.ReliableOrdered, 0, 0,
                24, 24, hash, hello, Channel.ReliableOrdered);
            AssertRawClientFault((PacketKind)0, PacketFlags.ReliableOrdered, 0, PacketHeader.NoneTick,
                24, 24, hash, hello, Channel.ReliableOrdered);
            AssertRawClientFault(PacketKind.Ack, PacketFlags.ReliableOrdered, 0, PacketHeader.NoneTick,
                0, 0, TestWire.XxHash64(ReadOnlySpan<byte>.Empty), Array.Empty<byte>(), Channel.ReliableOrdered);
            AssertRawClientFault(PacketKind.Hello, PacketFlags.ReliableOrdered, 0, PacketHeader.NoneTick,
                23, 24, hash, hello, Channel.ReliableOrdered);
            AssertRawClientFault(PacketKind.Hello, PacketFlags.ReliableOrdered, 0, PacketHeader.NoneTick,
                24, 23, hash, hello, Channel.ReliableOrdered);
            AssertRawClientFault(PacketKind.Hello, PacketFlags.ReliableOrdered, 0, PacketHeader.NoneTick,
                24, 24, hash ^ 1UL, hello, Channel.ReliableOrdered);
            AssertRawClientFault(PacketKind.Hello, PacketFlags.ReliableOrdered, 0, PacketHeader.NoneTick,
                24, 24, hash, hello, Channel.UnreliableSequenced);
        }

        [Test]
        public void DirectionAndStateRejectUnexpectedKindsWithoutReply()
        {
            CreateWorld<ClientWorld>(ChunkOwnerType.Other);
            TestTransport.CreatePair(3, out var clientTransport, out var peer);
            using (var client = new Session<ClientWorld>(ClientConfig(), Schema<ClientWorld>(), clientTransport))
            {
                client.Step(0);
                var hello = HelloPacket(1, Schema<ClientWorld>().Hash, 0, ServerHello());
                clientTransport.Inject(Channel.ReliableOrdered, ref hello);
                Assert.That(client.Step(1), Is.EqualTo(StepResult.Received));
                hello = HelloPacket(2, Schema<ClientWorld>().Hash, 0, ServerHello());
                clientTransport.Inject(Channel.ReliableOrdered, ref hello);
                Assert.That(client.Step(2), Is.EqualTo(StepResult.Received | StepResult.StateChanged));
                AssertProtocolFault(client);
                Assert.That(clientTransport.SendAttempts.Count, Is.EqualTo(1));
            }
            peer.Dispose();
            DestroyWorld<ClientWorld>();

            CreateWorld<ServerWorld>(ChunkOwnerType.Self);
            TestTransport.CreatePair(3, out var serverTransport, out peer);
            using (var server = new Session<ServerWorld>(ServerConfig(), Schema<ServerWorld>(), serverTransport))
            {
                var ack = HelloAckPacket(1, Schema<ServerWorld>().Hash, 0, ConnectResult.LimitsRejected,
                    0, 0, 33, Array.Empty<ChunkMapping>());
                serverTransport.Inject(Channel.ReliableOrdered, ref ack);
                Assert.That(server.Step(0), Is.EqualTo(StepResult.Received | StepResult.StateChanged));
                AssertProtocolFault(server);
                Assert.That(serverTransport.SendAttempts, Is.Empty);
            }
            peer.Dispose();
            DestroyWorld<ServerWorld>();

            CreateWorld<ServerWorld>(ChunkOwnerType.Self);
            TestTransport.CreatePair(3, out serverTransport, out peer);
            using (var server = new Session<ServerWorld>(ServerConfig(), Schema<ServerWorld>(), serverTransport))
            {
                var hello = HelloPacket(1, Schema<ServerWorld>().Hash, 0, ClientHello());
                serverTransport.Inject(Channel.ReliableOrdered, ref hello);
                server.Step(0);
                server.Step(1);
                hello = HelloPacket(2, Schema<ServerWorld>().Hash, 0, ClientHello());
                serverTransport.Inject(Channel.ReliableOrdered, ref hello);
                Assert.That(server.Step(2), Is.EqualTo(StepResult.Received | StepResult.StateChanged));
                AssertProtocolFault(server);
                Assert.That(serverTransport.SendAttempts.Count, Is.EqualTo(2));
            }
            peer.Dispose();
            DestroyWorld<ServerWorld>();
        }

        [Test]
        public void RequestedCloseCoversInitiatorEchoSimultaneousAndHandshakePaths()
        {
            CreateWorld<ServerWorld>(ChunkOwnerType.Self);
            CreateWorld<ClientWorld>(ChunkOwnerType.Other);
            TestTransport.CreatePair(2, out var clientTransport, out var serverTransport);
            using var client = new Session<ClientWorld>(ClientConfig(), Schema<ClientWorld>(), clientTransport);
            using var server = new Session<ServerWorld>(ServerConfig(), Schema<ServerWorld>(), serverTransport);
            try
            {
                PumpEstablished(client, server);
                client.Close();
                Assert.That(client.State, Is.EqualTo(SessionState.Closing));
                Assert.That(client.Step(3), Is.EqualTo(StepResult.Sent));
                Assert.That(server.Step(3), Is.EqualTo(StepResult.Received | StepResult.Sent | StepResult.StateChanged));
                Assert.That(server.State, Is.EqualTo(SessionState.Closed));
                Assert.That(server.Reason, Is.EqualTo(DisconnectReason.Requested));
                Assert.That(client.Step(4), Is.EqualTo(StepResult.Received | StepResult.StateChanged));
                Assert.That(client.State, Is.EqualTo(SessionState.Closed));
                Assert.That(client.Reason, Is.EqualTo(DisconnectReason.Requested));
                Assert.That(client.HasScope, Is.False);
                Assert.That(server.HasReplicator, Is.False);
            }
            finally
            {
                DestroyWorld<ServerWorld>();
                DestroyWorld<ClientWorld>();
            }

            CreateWorld<ServerWorld>(ChunkOwnerType.Self);
            CreateWorld<ClientWorld>(ChunkOwnerType.Other);
            TestTransport.CreatePair(2, out clientTransport, out serverTransport);
            using var simultaneousClient = new Session<ClientWorld>(ClientConfig(), Schema<ClientWorld>(), clientTransport);
            using var simultaneousServer = new Session<ServerWorld>(ServerConfig(), Schema<ServerWorld>(), serverTransport);
            try
            {
                PumpEstablished(simultaneousClient, simultaneousServer);
                simultaneousClient.Close();
                simultaneousServer.Close();
                simultaneousClient.Step(3);
                simultaneousServer.Step(3);
                simultaneousClient.Step(4);
                Assert.That(simultaneousClient.State, Is.EqualTo(SessionState.Closed));
                Assert.That(simultaneousServer.State, Is.EqualTo(SessionState.Closed));
                Assert.That(simultaneousClient.Reason, Is.EqualTo(DisconnectReason.Requested));
                Assert.That(simultaneousServer.Reason, Is.EqualTo(DisconnectReason.Requested));
            }
            finally
            {
                DestroyWorld<ServerWorld>();
                DestroyWorld<ClientWorld>();
            }

            CreateWorld<ClientWorld>(ChunkOwnerType.Other);
            var handshakeTransport = TestTransport.Unpaired();
            var handshake = new Session<ClientWorld>(ClientConfig(), Schema<ClientWorld>(), handshakeTransport);
            try
            {
                handshake.Close();
                handshake.Close();
                Assert.That(handshake.State, Is.EqualTo(SessionState.Closed));
                Assert.That(handshake.Reason, Is.EqualTo(DisconnectReason.Requested));
                Assert.That(handshakeTransport.SendAttempts.Count, Is.Zero);
                Assert.That(handshake.Step(0), Is.EqualTo(StepResult.None));
                Assert.That(handshakeTransport.BeginSteps.Count, Is.Zero);
            }
            finally
            {
                handshake.Dispose();
                DestroyWorld<ClientWorld>();
            }
        }

        [Test]
        public void ConnectedFalseRequestedSendRetriesSameBytesAndRemoteCloseIsCleanOnlyAfterCommit()
        {
            CreateWorld<ServerWorld>(ChunkOwnerType.Self);
            CreateWorld<ClientWorld>(ChunkOwnerType.Other);
            TestTransport.CreatePair(2, out var clientTransport, out var serverTransport);
            using var client = new Session<ClientWorld>(ClientConfig(), Schema<ClientWorld>(), clientTransport);
            using var server = new Session<ServerWorld>(ServerConfig(), Schema<ServerWorld>(), serverTransport);
            try
            {
                PumpEstablished(client, server);
                client.Close();
                clientTransport.ReturnFalseNextSend = true;
                Assert.That(client.Step(3), Is.EqualTo(StepResult.None));
                var failed = clientTransport.SendAttempts[2].Bytes;
                Assert.That(client.Step(4), Is.EqualTo(StepResult.Sent));
                CollectionAssert.AreEqual(failed, clientTransport.SendAttempts[3].Bytes);
                AssertPacket(clientTransport.SendAttempts[3], PacketKind.Disconnect, 3, 7);
                server.Dispose();
                Assert.That(client.Step(5), Is.EqualTo(StepResult.StateChanged));
                Assert.That(client.State, Is.EqualTo(SessionState.Closed));
                Assert.That(client.Reason, Is.EqualTo(DisconnectReason.Requested));
            }
            finally
            {
                DestroyWorld<ServerWorld>();
                DestroyWorld<ClientWorld>();
            }
        }

        [Test]
        public void TransportTerminalStatesMapToFrozenSessionFailures()
        {
            AssertTransportTerminal(TransportState.Faulted, TransportError.QueueOverflow,
                SessionError.Limits, DisconnectReason.LimitsExceeded);
            AssertTransportTerminal(TransportState.Faulted, TransportError.InvalidPacket,
                SessionError.Protocol, DisconnectReason.ProtocolViolation);
            AssertTransportTerminal(TransportState.Closed, TransportError.RemoteClosed,
                SessionError.Transport, DisconnectReason.TransportClosed);
            AssertTransportTerminal(TransportState.Disposed, TransportError.Disposed,
                SessionError.Transport, DisconnectReason.TransportClosed);
        }

        [Test]
        public void EstablishedDisconnectMatrixAndReservedGameplayKindsAreTerminal()
        {
            AssertReceivedDisconnect(SessionRole.Client, DisconnectReason.ServerShutdown,
                SessionState.Closed, SessionError.None, DisconnectReason.ServerShutdown);
            AssertReceivedDisconnect(SessionRole.Server, DisconnectReason.ServerShutdown,
                SessionState.Faulted, SessionError.Protocol, DisconnectReason.ProtocolViolation);
            AssertReceivedDisconnect(SessionRole.Client, DisconnectReason.ProtocolViolation,
                SessionState.Faulted, SessionError.Protocol, DisconnectReason.ProtocolViolation);
            AssertReceivedDisconnect(SessionRole.Client, DisconnectReason.SchemaMismatch,
                SessionState.Faulted, SessionError.Schema, DisconnectReason.SchemaMismatch);
            AssertReceivedDisconnect(SessionRole.Client, DisconnectReason.LimitsExceeded,
                SessionState.Faulted, SessionError.Limits, DisconnectReason.LimitsExceeded);
            AssertReceivedDisconnect(SessionRole.Client, DisconnectReason.UnexpectedEpoch,
                SessionState.Faulted, SessionError.Epoch, DisconnectReason.UnexpectedEpoch);
            AssertReceivedDisconnect(SessionRole.Client, DisconnectReason.SequenceExhausted,
                SessionState.Faulted, SessionError.Sequence, DisconnectReason.SequenceExhausted);
            AssertReceivedDisconnect(SessionRole.Client, DisconnectReason.TransportClosed,
                SessionState.Faulted, SessionError.Transport, DisconnectReason.TransportClosed);
            AssertEstablishedReserved(PacketKind.CommandBatch);
            AssertEstablishedReserved(PacketKind.FullSnapshot);
            AssertEstablishedReserved(PacketKind.Ack);
            AssertEstablishedReserved(PacketKind.ResyncRequest);
            AssertEstablishedDisconnectFailure(DifferentSchema<ClientWorld>().Hash, 7,
                SessionError.Schema, DisconnectReason.SchemaMismatch);
            AssertEstablishedDisconnectFailure(Schema<ClientWorld>().Hash, 8,
                SessionError.Epoch, DisconnectReason.UnexpectedEpoch);
        }

        [Test]
        public void ImmediateDisposeIsIdempotentOwnsTransportAndSendsNothing()
        {
            CreateWorld<ClientWorld>(ChunkOwnerType.Other);
            var transport = TestTransport.Unpaired();
            var session = new Session<ClientWorld>(ClientConfig(), Schema<ClientWorld>(), transport);
            try
            {
                session.Dispose();
                session.Dispose();
                session.Close();
                Assert.That(session.State, Is.EqualTo(SessionState.Disposed));
                Assert.That(transport.DisposeCount, Is.EqualTo(1));
                Assert.That(transport.SendAttempts.Count, Is.Zero);
                Assert.Throws<ObjectDisposedException>(() => session.Step(0));
            }
            finally
            {
                DestroyWorld<ClientWorld>();
            }
        }

        private static void AssertLocalBound(
            byte[] payload,
            uint wireLength,
            uint decodedLength,
            bool proveNoDecodeRent)
        {
            CreateWorld<MinimumClientWorld>(ChunkOwnerType.Other);
            TestTransport.CreatePair(2, out var transport, out var peer);
            using var session = new Session<MinimumClientWorld>(
                SessionConfig.Client(21, 20, 40, 24, 24), Schema<MinimumClientWorld>(), transport);
            var held = new List<PacketLease>();
            try
            {
                session.Step(0);
                var packet = RawHeaderPacket(
                    PacketKind.Hello, PacketFlags.ReliableOrdered, 0, 0, 1,
                    PacketHeader.NoneTick, PacketHeader.NoneTick, PacketHeader.NoneTick,
                    wireLength, decodedLength, Schema<MinimumClientWorld>().Hash,
                    TestWire.XxHash64(payload), 0, payload);
                Assert.That(HeaderCrcMatches(packet.Span), Is.True);
                Assert.That(PacketHeader.TryRead(packet.Span, out _), Is.True);
                transport.Inject(Channel.ReliableOrdered, ref packet);
                if (proveNoDecodeRent)
                    while (PacketLease.PooledStateCountForTests > 0) held.Add(PacketLease.Rent(1));
                var allocations = PacketLease.StateAllocationCountForTests;

                Assert.That(session.Step(1), Is.EqualTo(StepResult.Received | StepResult.StateChanged));
                if (proveNoDecodeRent)
                    Assert.That(PacketLease.StateAllocationCountForTests, Is.EqualTo(allocations));
                Assert.That(session.State, Is.EqualTo(SessionState.Faulted));
                Assert.That(session.Error, Is.EqualTo(SessionError.Limits));
                Assert.That(session.Reason, Is.EqualTo(DisconnectReason.LimitsExceeded));
            }
            finally
            {
                for (var i = 0; i < held.Count; i++) held[i].Dispose();
                peer.Dispose();
                DestroyWorld<MinimumClientWorld>();
            }
        }

        private static void AssertMalformedClientHello(HelloPayload hello)
        {
            CreateWorld<ServerWorld>(ChunkOwnerType.Self);
            TestTransport.CreatePair(2, out var transport, out var peer);
            using var server = new Session<ServerWorld>(ServerConfig(), Schema<ServerWorld>(), transport);
            try
            {
                var payload = HelloBytes(hello);
                var packet = RawHeaderPacket(
                    PacketKind.Hello, PacketFlags.ReliableOrdered, 0, 0, 1,
                    PacketHeader.NoneTick, PacketHeader.NoneTick, PacketHeader.NoneTick,
                    24, 24, Schema<ServerWorld>().Hash, TestWire.XxHash64(payload), 0, payload);
                Assert.That(HeaderCrcMatches(packet.Span), Is.True);
                transport.Inject(Channel.ReliableOrdered, ref packet);

                Assert.That(server.Step(0), Is.EqualTo(StepResult.Received | StepResult.StateChanged));
                AssertProtocolFault(server);
                Assert.That(server.Result, Is.Null);
                Assert.That(transport.SendAttempts, Is.Empty);
            }
            finally
            {
                peer.Dispose();
                DestroyWorld<ServerWorld>();
            }
        }

        private static void AssertServerRejection(
            HelloPayload hello,
            TypeId clientSchema,
            ConnectResult expected)
        {
            CreateWorld<ServerWorld>(ChunkOwnerType.Self);
            TestTransport.CreatePair(3, out var transport, out var peer);
            using var server = new Session<ServerWorld>(ServerConfig(), Schema<ServerWorld>(), transport);
            try
            {
                var payload = HelloBytes(hello);
                var packet = RawHeaderPacket(
                    PacketKind.Hello, PacketFlags.ReliableOrdered, 0, 0, 1,
                    PacketHeader.NoneTick, PacketHeader.NoneTick, PacketHeader.NoneTick,
                    24, 24, clientSchema, TestWire.XxHash64(payload), 0, payload);
                Assert.That(HeaderCrcMatches(packet.Span), Is.True);
                Assert.That(PacketHeader.TryRead(packet.Span, out _), Is.True);
                transport.Inject(Channel.ReliableOrdered, ref packet);

                Assert.That(server.Step(0), Is.EqualTo(StepResult.Received | StepResult.Sent));
                Assert.That(server.State, Is.EqualTo(SessionState.Handshaking));
                Assert.That(server.Result, Is.Null);
                Assert.That(transport.SendAttempts.Count, Is.EqualTo(1));
                AssertPacket(transport.SendAttempts[0], PacketKind.Hello, 1, 0);

                Assert.That(server.Step(1), Is.EqualTo(StepResult.Sent | StepResult.StateChanged));
                Assert.That(server.State, Is.EqualTo(SessionState.Closing));
                Assert.That(server.Result, Is.Null);
                Assert.That(transport.SendAttempts.Count, Is.EqualTo(2));
                AssertPacket(transport.SendAttempts[1], PacketKind.HelloAck, 2, 0);
                Assert.That(PayloadCodec.TryReadHelloAck(Payload(transport.SendAttempts[1].Bytes), out var ack), Is.True);
                Assert.That(ack.Result, Is.EqualTo(expected));
                Assert.That(ack.TickRate, Is.Zero);
                Assert.That(ack.PeerId, Is.Zero);
                Assert.That(ack.ServerNonce, Is.EqualTo(33));
                Assert.That(ack.Chunks, Is.Empty);
            }
            finally
            {
                peer.Dispose();
                DestroyWorld<ServerWorld>();
            }
        }

        private static void AssertRawClientFault(
            PacketKind kind,
            PacketFlags flags,
            byte transformId,
            uint baselineTick,
            uint wireLength,
            uint decodedLength,
            ulong payloadHash,
            byte[] payload,
            Channel channel)
        {
            CreateWorld<ClientWorld>(ChunkOwnerType.Other);
            TestTransport.CreatePair(2, out var transport, out var peer);
            using var client = new Session<ClientWorld>(ClientConfig(), Schema<ClientWorld>(), transport);
            try
            {
                client.Step(0);
                var packet = RawHeaderPacket(
                    kind, flags, transformId, 0, 1,
                    PacketHeader.NoneTick, baselineTick, PacketHeader.NoneTick,
                    wireLength, decodedLength, Schema<ClientWorld>().Hash, payloadHash, 0, payload);
                Assert.That(HeaderCrcMatches(packet.Span), Is.True);
                transport.Inject(channel, ref packet);

                Assert.That(client.Step(1), Is.EqualTo(StepResult.Received | StepResult.StateChanged));
                AssertProtocolFault(client);
                Assert.That(client.Result, Is.Null);
                Assert.That(transport.SendAttempts.Count, Is.EqualTo(1));
            }
            finally
            {
                peer.Dispose();
                DestroyWorld<ClientWorld>();
            }
        }

        private static void AssertEstablishedReserved(PacketKind kind)
        {
            CreateWorld<ServerWorld>(ChunkOwnerType.Self);
            CreateWorld<ClientWorld>(ChunkOwnerType.Other);
            TestTransport.CreatePair(3, out var clientTransport, out var serverTransport);
            using var client = new Session<ClientWorld>(ClientConfig(), Schema<ClientWorld>(), clientTransport);
            using var server = new Session<ServerWorld>(ServerConfig(), Schema<ServerWorld>(), serverTransport);
            try
            {
                PumpEstablished(client, server);
                var packet = ReservedPacket(kind, Schema<ClientWorld>(), out var channel);
                clientTransport.Inject(channel, ref packet);
                Assert.That(client.Step(3), Is.EqualTo(StepResult.Received | StepResult.StateChanged));
                AssertProtocolFault(client);
                Assert.That(client.Result, Is.EqualTo(ConnectResult.Accepted));
                Assert.That(client.HasScope, Is.False);
                Assert.That(clientTransport.SendAttempts.Count, Is.EqualTo(2));
            }
            finally
            {
                DestroyWorld<ServerWorld>();
                DestroyWorld<ClientWorld>();
            }
        }

        private static void AssertEstablishedDisconnectFailure(
            TypeId schema,
            uint epoch,
            SessionError expectedError,
            DisconnectReason expectedReason)
        {
            CreateWorld<ServerWorld>(ChunkOwnerType.Self);
            CreateWorld<ClientWorld>(ChunkOwnerType.Other);
            TestTransport.CreatePair(3, out var clientTransport, out var serverTransport);
            using var client = new Session<ClientWorld>(ClientConfig(), Schema<ClientWorld>(), clientTransport);
            using var server = new Session<ServerWorld>(ServerConfig(), Schema<ServerWorld>(), serverTransport);
            try
            {
                PumpEstablished(client, server);
                var packet = DisconnectPacket(3, schema, epoch, DisconnectReason.ProtocolViolation);
                clientTransport.Inject(Channel.ReliableOrdered, ref packet);
                Assert.That(client.Step(3), Is.EqualTo(StepResult.Received | StepResult.StateChanged));
                Assert.That(client.State, Is.EqualTo(SessionState.Faulted));
                Assert.That(client.Error, Is.EqualTo(expectedError));
                Assert.That(client.Reason, Is.EqualTo(expectedReason));
                Assert.That(client.Result, Is.EqualTo(ConnectResult.Accepted));
                Assert.That(clientTransport.SendAttempts.Count, Is.EqualTo(2));
            }
            finally
            {
                DestroyWorld<ServerWorld>();
                DestroyWorld<ClientWorld>();
            }
        }

        private static void AssertProtocolFault<TWorld>(Session<TWorld> session)
            where TWorld : struct, IWorldType
        {
            Assert.That(session.State, Is.EqualTo(SessionState.Faulted));
            Assert.That(session.Error, Is.EqualTo(SessionError.Protocol));
            Assert.That(session.Reason, Is.EqualTo(DisconnectReason.ProtocolViolation));
        }

        private static void AssertRejection(SessionConfig clientConfig, Schema clientSchema, ConnectResult expected)
        {
            CreateWorld<ServerWorld>(ChunkOwnerType.Self);
            CreateWorld<ClientWorld>(ChunkOwnerType.Other);
            TestTransport.CreatePair(2, out var clientTransport, out var serverTransport);
            using var client = new Session<ClientWorld>(clientConfig, clientSchema, clientTransport);
            using var server = new Session<ServerWorld>(ServerConfig(), Schema<ServerWorld>(), serverTransport);
            try
            {
                client.Step(0); server.Step(0);
                client.Step(1); server.Step(1);
                Assert.That(server.State, Is.EqualTo(SessionState.Closing));
                Assert.That(server.Result, Is.Null);
                client.Step(2);
                Assert.That(client.State, Is.EqualTo(SessionState.Closed));
                Assert.That(client.Result, Is.EqualTo(expected));
                client.Dispose();
                server.Step(2);
                Assert.That(server.State, Is.EqualTo(SessionState.Closed));
                Assert.That(server.Result, Is.EqualTo(expected));
            }
            finally
            {
                DestroyWorld<ServerWorld>();
                DestroyWorld<ClientWorld>();
            }
        }

        private static void AssertServerHelloFailure(
            HelloPayload hello,
            SessionError expectedError,
            DisconnectReason expectedReason)
        {
            CreateWorld<ClientWorld>(ChunkOwnerType.Other);
            TestTransport.CreatePair(2, out var transport, out var peer);
            using var client = new Session<ClientWorld>(ClientConfig(), Schema<ClientWorld>(), transport);
            try
            {
                client.Step(0);
                var packet = HelloPacket(1, Schema<ClientWorld>().Hash, 0, hello);
                transport.Inject(Channel.ReliableOrdered, ref packet);
                Assert.That(client.Step(1), Is.EqualTo(StepResult.Received | StepResult.StateChanged));
                Assert.That(client.State, Is.EqualTo(SessionState.Faulted));
                Assert.That(client.Error, Is.EqualTo(expectedError));
                Assert.That(client.Reason, Is.EqualTo(expectedReason));
                Assert.That(client.Result, Is.Null);
            }
            finally
            {
                peer.Dispose();
                DestroyWorld<ClientWorld>();
            }
        }

        private static void AssertHelloAckFailure(
            TypeId serverHelloSchema,
            HelloPayload serverHello,
            TypeId ackSchema,
            uint epoch,
            ConnectResult result,
            ushort tick,
            uint peerId,
            ulong nonce,
            ChunkMapping[] chunks,
            SessionError expectedError,
            DisconnectReason expectedReason,
            ConnectResult? expectedResult)
        {
            CreateWorld<ClientWorld>(ChunkOwnerType.Other);
            TestTransport.CreatePair(2, out var transport, out var peer);
            using var client = new Session<ClientWorld>(ClientConfig(), Schema<ClientWorld>(), transport);
            try
            {
                client.Step(0);
                var hello = HelloPacket(1, serverHelloSchema, 0, serverHello);
                transport.Inject(Channel.ReliableOrdered, ref hello);
                Assert.That(client.Step(1), Is.EqualTo(StepResult.Received));
                var ack = HelloAckPacket(2, ackSchema, epoch, result, tick, peerId, nonce, chunks);
                transport.Inject(Channel.ReliableOrdered, ref ack);
                Assert.That(client.Step(2), Is.EqualTo(StepResult.Received | StepResult.StateChanged));
                Assert.That(client.State, Is.EqualTo(SessionState.Faulted));
                Assert.That(client.Error, Is.EqualTo(expectedError));
                Assert.That(client.Reason, Is.EqualTo(expectedReason));
                Assert.That(client.Result, Is.EqualTo(expectedResult));
                Assert.That(client.HasScope, Is.False);
                Assert.That(client.HasReplicator, Is.False);
            }
            finally
            {
                peer.Dispose();
                DestroyWorld<ClientWorld>();
            }
        }

        private static void AssertFinalAckFailure(
            TypeId schema,
            uint epoch,
            byte[] payload,
            SessionError expectedError,
            DisconnectReason expectedReason)
        {
            CreateWorld<ServerWorld>(ChunkOwnerType.Self);
            TestTransport.CreatePair(4, out var transport, out var peer);
            using var server = new Session<ServerWorld>(ServerConfig(), Schema<ServerWorld>(), transport);
            try
            {
                var hello = HelloPacket(1, Schema<ServerWorld>().Hash, 0, ClientHello());
                transport.Inject(Channel.ReliableOrdered, ref hello);
                Assert.That(server.Step(0), Is.EqualTo(StepResult.Received | StepResult.Sent));
                Assert.That(server.Step(1), Is.EqualTo(StepResult.Sent));
                var ack = RawControlPacket(PacketKind.Ack, 2, epoch, schema, payload);
                transport.Inject(Channel.ReliableOrdered, ref ack);
                Assert.That(server.Step(2), Is.EqualTo(StepResult.Received | StepResult.StateChanged));
                Assert.That(server.State, Is.EqualTo(SessionState.Faulted));
                Assert.That(server.Error, Is.EqualTo(expectedError));
                Assert.That(server.Reason, Is.EqualTo(expectedReason));
                Assert.That(server.Result, Is.Null);
                Assert.That(server.HasScope, Is.False);
                Assert.That(server.HasReplicator, Is.False);
            }
            finally
            {
                peer.Dispose();
                DestroyWorld<ServerWorld>();
            }
        }

        private static void AssertClientHelloMutation(
            Channel channel,
            uint sequence,
            uint epoch,
            uint serverTick,
            uint acknowledgedSnapshotTick,
            uint acknowledgedCommandSequence,
            SessionError expected)
        {
            CreateWorld<ClientWorld>(ChunkOwnerType.Other);
            TestTransport.CreatePair(2, out var transport, out var peer);
            using var session = new Session<ClientWorld>(ClientConfig(), Schema<ClientWorld>(), transport);
            try
            {
                session.Step(0);
                var payload = new HelloPayload
                {
                    Nonce = 33,
                    MinTickRate = 30,
                    MaxTickRate = 30,
                    MaxWireBytes = ProtocolLimits.MaxWirePayloadBytes,
                    MaxDecodedBytes = ProtocolLimits.MaxDecodedPayloadBytes
                };
                Span<byte> bytes = stackalloc byte[24];
                Assert.That(PayloadCodec.TryWrite(payload, bytes, out var written), Is.True);
                var header = ControlHeader(PacketKind.Hello, sequence, epoch, Schema<ClientWorld>().Hash);
                header.ServerTick = serverTick;
                header.AcknowledgedSnapshotTick = acknowledgedSnapshotTick;
                header.AcknowledgedCommandSequence = acknowledgedCommandSequence;
                Assert.That(PacketFraming.TryEncode(header, bytes.Slice(0, written), new NoOpTransform(), out var packet), Is.True);
                transport.Inject(channel, ref packet);

                Assert.That(session.Step(1), Is.EqualTo(StepResult.Received | StepResult.StateChanged));
                Assert.That(session.State, Is.EqualTo(SessionState.Faulted));
                Assert.That(session.Error, Is.EqualTo(expected));
                Assert.That(session.Reason, Is.EqualTo(DisconnectReason.ProtocolViolation));
                Assert.That(transport.SendAttempts.Count, Is.EqualTo(1));
            }
            finally
            {
                peer.Dispose();
                DestroyWorld<ClientWorld>();
            }
        }

        private static void AssertTransportTerminal(
            TransportState state,
            TransportError transportError,
            SessionError expectedError,
            DisconnectReason expectedReason)
        {
            CreateWorld<ClientWorld>(ChunkOwnerType.Other);
            var transport = TestTransport.Unpaired();
            using var session = new Session<ClientWorld>(ClientConfig(), Schema<ClientWorld>(), transport);
            try
            {
                transport.ForceTerminal(state, transportError);
                Assert.That(session.Step(0), Is.EqualTo(StepResult.StateChanged));
                Assert.That(session.State, Is.EqualTo(SessionState.Faulted));
                Assert.That(session.Error, Is.EqualTo(expectedError));
                Assert.That(session.Reason, Is.EqualTo(expectedReason));
                Assert.That(session.Result, Is.Null);
            }
            finally
            {
                DestroyWorld<ClientWorld>();
            }
        }

        private static void AssertReceivedDisconnect(
            SessionRole targetRole,
            DisconnectReason wireReason,
            SessionState expectedState,
            SessionError expectedError,
            DisconnectReason expectedReason)
        {
            CreateWorld<ServerWorld>(ChunkOwnerType.Self);
            CreateWorld<ClientWorld>(ChunkOwnerType.Other);
            TestTransport.CreatePair(3, out var clientTransport, out var serverTransport);
            using var client = new Session<ClientWorld>(ClientConfig(), Schema<ClientWorld>(), clientTransport);
            using var server = new Session<ServerWorld>(ServerConfig(), Schema<ServerWorld>(), serverTransport);
            try
            {
                PumpEstablished(client, server);
                var packet = DisconnectPacket(3, Schema<ClientWorld>().Hash, 7, wireReason);
                var targetTransport = targetRole == SessionRole.Client ? clientTransport : serverTransport;
                targetTransport.Inject(Channel.ReliableOrdered, ref packet);
                var target = targetRole == SessionRole.Client ? (ISessionView) new ClientView(client) : new ServerView(server);
                var step = targetRole == SessionRole.Client ? client.Step(3) : server.Step(3);
                Assert.That(step, Is.EqualTo(StepResult.Received | StepResult.StateChanged));
                Assert.That(target.State, Is.EqualTo(expectedState));
                Assert.That(target.Error, Is.EqualTo(expectedError));
                Assert.That(target.Reason, Is.EqualTo(expectedReason));
            }
            finally
            {
                DestroyWorld<ServerWorld>();
                DestroyWorld<ClientWorld>();
            }
        }

        private static void PumpEstablished(Session<ClientWorld> client, Session<ServerWorld> server)
        {
            for (ulong step = 0; step < 3; step++)
            {
                client.Step(step);
                server.Step(step);
            }
            AssertEstablished(client);
            AssertEstablished(server);
        }

        private static void AssertEstablished<TWorld>(Session<TWorld> session) where TWorld : struct, IWorldType
        {
            Assert.That(session.State, Is.EqualTo(SessionState.Established));
            Assert.That(session.Error, Is.EqualTo(SessionError.None));
            Assert.That(session.Result, Is.EqualTo(ConnectResult.Accepted));
            Assert.That(session.Reason, Is.Null);
            Assert.That(session.Epoch, Is.EqualTo(7));
            Assert.That(session.PeerId, Is.EqualTo(9));
            Assert.That(session.TickRate, Is.EqualTo(30));
            Assert.That(session.HasScope, Is.True);
            Assert.That(session.HasReplicator, Is.True);
        }

        private static void AssertTopologyFault<TWorld>(Session<TWorld> session) where TWorld : struct, IWorldType
        {
            Assert.That(session.State, Is.EqualTo(SessionState.Faulted));
            Assert.That(session.Error, Is.EqualTo(SessionError.Topology));
            Assert.That(session.Reason, Is.Null);
            Assert.That(session.HasScope, Is.False);
            Assert.That(session.HasReplicator, Is.False);
        }

        private static void AssertPacket(SendAttempt attempt, PacketKind kind, uint sequence, uint epoch)
        {
            Assert.That(attempt.Channel, Is.EqualTo(Channel.ReliableOrdered));
            Assert.That(PacketHeader.TryRead(attempt.Bytes, out var header), Is.True);
            Assert.That(header.Kind, Is.EqualTo(kind));
            Assert.That(header.PacketSequence, Is.EqualTo(sequence));
            Assert.That(header.SessionEpoch, Is.EqualTo(epoch));
            Assert.That(header.Flags, Is.EqualTo(PacketFlags.ReliableOrdered));
            Assert.That(header.ServerTick, Is.EqualTo(PacketHeader.NoneTick));
            Assert.That(header.BaselineTick, Is.EqualTo(PacketHeader.NoneTick));
            Assert.That(header.AcknowledgedSnapshotTick, Is.EqualTo(PacketHeader.NoneTick));
            Assert.That(header.AcknowledgedCommandSequence, Is.Zero);
        }

        private static ReadOnlySpan<byte> Payload(byte[] packet) => packet.AsSpan(PacketHeader.Size);

        private static byte[] ClientHelloBytes() => HelloBytes(ClientHello());
        private static byte[] ServerHelloBytes() => HelloBytes(ServerHello());

        private static byte[] HelloBytes(HelloPayload hello)
        {
            var payload = new byte[24];
            TestWire.Write64(payload, 0, hello.Nonce);
            TestWire.Write16(payload, 8, hello.MinTickRate);
            TestWire.Write16(payload, 10, hello.MaxTickRate);
            TestWire.Write32(payload, 12, hello.MaxWireBytes);
            TestWire.Write32(payload, 16, hello.MaxDecodedBytes);
            TestWire.Write32(payload, 20, hello.Capabilities);
            return payload;
        }

        private static PacketLease RawHeaderPacket(
            PacketKind kind,
            PacketFlags flags,
            byte transformId,
            uint epoch,
            uint sequence,
            uint serverTick,
            uint baselineTick,
            uint acknowledgedSnapshotTick,
            uint wireLength,
            uint decodedLength,
            TypeId schema,
            ulong payloadHash,
            uint acknowledgedCommandSequence,
            byte[] payload)
        {
            var packet = PacketLease.Rent(PacketHeader.Size + payload.Length);
            packet.SetLength(PacketHeader.Size + payload.Length);
            var bytes = packet.Span;
            bytes.Clear();
            payload.CopyTo(bytes.Slice(PacketHeader.Size));
            TestWire.Write32(bytes, 0, 0x53434553);
            TestWire.Write16(bytes, 4, PacketHeader.Version);
            TestWire.Write16(bytes, 6, PacketHeader.Size);
            bytes[8] = (byte)kind;
            bytes[9] = (byte)flags;
            bytes[10] = transformId;
            TestWire.Write32(bytes, 12, epoch);
            TestWire.Write32(bytes, 16, sequence);
            TestWire.Write32(bytes, 20, serverTick);
            TestWire.Write32(bytes, 24, baselineTick);
            TestWire.Write32(bytes, 28, acknowledgedSnapshotTick);
            TestWire.Write32(bytes, 32, wireLength);
            TestWire.Write32(bytes, 36, decodedLength);
            schema.WriteBytes(bytes.Slice(40, 16));
            TestWire.Write64(bytes, 56, payloadHash);
            TestWire.Write32(bytes, 68, acknowledgedCommandSequence);
            TestWire.Write32(bytes, 64, TestWire.Crc32(bytes.Slice(0, PacketHeader.Size)));
            return packet;
        }

        private static bool HeaderCrcMatches(ReadOnlySpan<byte> packet)
        {
            if (packet.Length < PacketHeader.Size) return false;
            Span<byte> header = stackalloc byte[PacketHeader.Size];
            packet.Slice(0, PacketHeader.Size).CopyTo(header);
            var expected = TestWire.Read32(header, 64);
            header.Slice(64, 4).Clear();
            return TestWire.Crc32(header) == expected;
        }

        private static PacketLease ReservedPacket(PacketKind kind, Schema schema, out Channel channel)
        {
            byte[] payload;
            var header = ControlHeader(kind, 3, 7, schema.Hash);
            switch (kind)
            {
                case PacketKind.CommandBatch:
                    payload = new byte[4];
                    channel = Channel.ReliableOrdered;
                    break;
                case PacketKind.FullSnapshot:
                    payload = new byte[4];
                    header.Flags = 0;
                    channel = Channel.UnreliableSequenced;
                    break;
                case PacketKind.Ack:
                    payload = Array.Empty<byte>();
                    channel = Channel.ReliableOrdered;
                    break;
                case PacketKind.ResyncRequest:
                    payload = new byte[8];
                    if (!PayloadCodec.TryWrite(new ResyncRequestPayload
                        {
                            Reason = ResyncReason.HashMismatch,
                            LastAcceptedTick = 1
                        }, payload, out var written) || written != payload.Length)
                        throw new InvalidOperationException();
                    channel = Channel.ReliableOrdered;
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(kind));
            }

            var encoded = kind == PacketKind.CommandBatch || kind == PacketKind.FullSnapshot
                ? PacketFraming.TryEncode(header, payload, new NoOpTransform(), schema, out var packet)
                : PacketFraming.TryEncode(header, payload, new NoOpTransform(), out packet);
            if (!encoded) throw new InvalidOperationException();
            StagedPayload staged = null;
            try
            {
                var valid = kind == PacketKind.CommandBatch || kind == PacketKind.FullSnapshot
                    ? PacketFraming.TryDecode(in packet, new NoOpTransform(), schema, out _, out staged)
                    : PacketFraming.TryDecode(in packet, new NoOpTransform(), out _, out staged);
                if (!valid) throw new InvalidOperationException();
            }
            finally
            {
                staged?.Dispose();
            }
            return packet;
        }

        private static PacketLease HelloPacket(uint sequence, TypeId schema, uint epoch, HelloPayload payload)
        {
            Span<byte> bytes = stackalloc byte[24];
            if (!PayloadCodec.TryWrite(payload, bytes, out var written)) throw new InvalidOperationException();
            if (!PacketFraming.TryEncode(ControlHeader(PacketKind.Hello, sequence, epoch, schema),
                    bytes.Slice(0, written), new NoOpTransform(), out var packet)) throw new InvalidOperationException();
            return packet;
        }

        private static PacketLease HelloAckPacket(
            uint sequence,
            TypeId schema,
            uint epoch,
            ConnectResult result,
            ushort tick,
            uint peer,
            ulong nonce,
            ChunkMapping[] chunks)
        {
            var payload = new HelloAckPayload
            {
                Result = result,
                TickRate = tick,
                PeerId = peer,
                ServerNonce = nonce,
                Chunks = chunks
            };
            var bytes = new byte[20 + chunks.Length * 8];
            if (!PayloadCodec.TryWrite(payload, bytes, out var written)) throw new InvalidOperationException();
            if (!PacketFraming.TryEncode(ControlHeader(PacketKind.HelloAck, sequence, epoch, schema),
                    bytes.AsSpan(0, written), new NoOpTransform(), out var packet)) throw new InvalidOperationException();
            return packet;
        }

        private static PacketLease DisconnectPacket(uint sequence, TypeId schema, uint epoch, DisconnectReason reason)
        {
            Span<byte> bytes = stackalloc byte[4];
            if (!PayloadCodec.TryWrite(new DisconnectPayload { Reason = reason }, bytes, out var written))
                throw new InvalidOperationException();
            if (!PacketFraming.TryEncode(ControlHeader(PacketKind.Disconnect, sequence, epoch, schema),
                    bytes.Slice(0, written), new NoOpTransform(), out var packet)) throw new InvalidOperationException();
            return packet;
        }

        private static PacketLease RawControlPacket(PacketKind kind, uint sequence, uint epoch, TypeId schema, byte[] payload)
        {
            var packet = PacketLease.Rent(PacketHeader.Size + payload.Length);
            packet.SetLength(PacketHeader.Size + payload.Length);
            payload.CopyTo(packet.Span.Slice(PacketHeader.Size));
            var header = ControlHeader(kind, sequence, epoch, schema);
            header.WirePayloadLength = (uint)payload.Length;
            header.DecodedPayloadLength = (uint)payload.Length;
            header.PayloadHash = TestWire.XxHash64(payload);
            if (!header.TryWrite(packet.Span)) throw new InvalidOperationException();
            return packet;
        }

        private static PacketHeader ControlHeader(PacketKind kind, uint sequence, uint epoch, TypeId schema) => new()
        {
            Kind = kind,
            Flags = PacketFlags.ReliableOrdered,
            SessionEpoch = epoch,
            PacketSequence = sequence,
            ServerTick = PacketHeader.NoneTick,
            BaselineTick = PacketHeader.NoneTick,
            AcknowledgedSnapshotTick = PacketHeader.NoneTick,
            SchemaHash = schema,
            AcknowledgedCommandSequence = 0
        };

        private static void CreateWorld<TWorld>(
            ChunkOwnerType owner,
            bool occupied = false,
            bool includeSecond = false)
            where TWorld : struct, IWorldType
        {
            World<TWorld>.Create(WorldConfig.Default());
            World<TWorld>.Types().Tag<ReplicatedTag>().EntityType<TestEntity>();
            World<TWorld>.Initialize();
            World<TWorld>.RegisterCluster(Cluster);
            World<TWorld>.RegisterChunk(Chunk, owner, Cluster);
            if (includeSecond) World<TWorld>.RegisterChunk(SecondChunk, owner, Cluster);
            if (occupied)
            {
                var id = (Chunk << Const.ENTITIES_IN_CHUNK_SHIFT) + 1;
                World<TWorld>.NewEntityByGID<TestEntity>(new EntityGID(id, 1, Cluster));
            }
        }

        private static void DestroyWorld<TWorld>() where TWorld : struct, IWorldType
        {
            if (World<TWorld>.Status != WorldStatus.NotCreated) World<TWorld>.Destroy();
        }

        private static SessionConfig ClientConfig() => SessionConfig.Client(21, 20, 40);
        private static SessionConfig ServerConfig() => SessionConfig.Server(7, 9, 33, 30, Mapping());
        private static HelloPayload ClientHello() => new()
        {
            Nonce = 21,
            MinTickRate = 20,
            MaxTickRate = 40,
            MaxWireBytes = ProtocolLimits.MaxWirePayloadBytes,
            MaxDecodedBytes = ProtocolLimits.MaxDecodedPayloadBytes,
            Capabilities = 0
        };
        private static HelloPayload ServerHello() => new()
        {
            Nonce = 33,
            MinTickRate = 30,
            MaxTickRate = 30,
            MaxWireBytes = ProtocolLimits.MaxWirePayloadBytes,
            MaxDecodedBytes = ProtocolLimits.MaxDecodedPayloadBytes,
            Capabilities = 0
        };
        private static ChunkMapping[] Mapping() => new[] { Map(Chunk, Cluster) };
        private static ChunkMapping Map(uint chunk, ushort cluster) => new() { Chunk = chunk, Cluster = cluster, Role = 1 };
        private static Schema Schema<TWorld>() where TWorld : struct, IWorldType => new SchemaBuilder<TWorld>().Freeze();
        private static Schema DifferentSchema<TWorld>() where TWorld : struct, IWorldType =>
            new SchemaBuilder<TWorld>().Tag<DifferentTag>(DifferentId, 1).Freeze();

        private static class TestWire
        {
            private const ulong Prime1 = 11400714785074694791UL;
            private const ulong Prime2 = 14029467366897019727UL;
            private const ulong Prime3 = 1609587929392839161UL;
            private const ulong Prime4 = 9650029242287828579UL;
            private const ulong Prime5 = 2870177450012600261UL;

            internal static uint Crc32(ReadOnlySpan<byte> data)
            {
                var crc = uint.MaxValue;
                for (var i = 0; i < data.Length; i++)
                {
                    crc ^= data[i];
                    for (var bit = 0; bit < 8; bit++)
                        crc = (crc & 1) != 0 ? 0xedb88320U ^ crc >> 1 : crc >> 1;
                }
                return ~crc;
            }

            internal static ulong XxHash64(ReadOnlySpan<byte> data)
            {
                unchecked
                {
                    var index = 0;
                    ulong hash;
                    if (data.Length >= 32)
                    {
                        var v1 = Prime1 + Prime2;
                        var v2 = Prime2;
                        var v3 = 0UL;
                        var v4 = 0UL - Prime1;
                        var limit = data.Length - 32;
                        do
                        {
                            v1 = Round(v1, Read64(data, index)); index += 8;
                            v2 = Round(v2, Read64(data, index)); index += 8;
                            v3 = Round(v3, Read64(data, index)); index += 8;
                            v4 = Round(v4, Read64(data, index)); index += 8;
                        } while (index <= limit);
                        hash = Rotate(v1, 1) + Rotate(v2, 7) + Rotate(v3, 12) + Rotate(v4, 18);
                        hash = Merge(hash, v1);
                        hash = Merge(hash, v2);
                        hash = Merge(hash, v3);
                        hash = Merge(hash, v4);
                    }
                    else
                    {
                        hash = Prime5;
                    }

                    hash += (ulong)data.Length;
                    while (index <= data.Length - 8)
                    {
                        hash ^= Round(0, Read64(data, index));
                        hash = Rotate(hash, 27) * Prime1 + Prime4;
                        index += 8;
                    }
                    if (index <= data.Length - 4)
                    {
                        hash ^= Read32(data, index) * Prime1;
                        hash = Rotate(hash, 23) * Prime2 + Prime3;
                        index += 4;
                    }
                    while (index < data.Length)
                    {
                        hash ^= data[index] * Prime5;
                        hash = Rotate(hash, 11) * Prime1;
                        index++;
                    }
                    hash ^= hash >> 33;
                    hash *= Prime2;
                    hash ^= hash >> 29;
                    hash *= Prime3;
                    hash ^= hash >> 32;
                    return hash;
                }
            }

            internal static uint Read32(ReadOnlySpan<byte> data, int offset) =>
                data[offset] | (uint)data[offset + 1] << 8 |
                (uint)data[offset + 2] << 16 | (uint)data[offset + 3] << 24;

            internal static void Write16(Span<byte> data, int offset, ushort value)
            {
                data[offset] = (byte)value;
                data[offset + 1] = (byte)(value >> 8);
            }

            internal static void Write32(Span<byte> data, int offset, uint value)
            {
                data[offset] = (byte)value;
                data[offset + 1] = (byte)(value >> 8);
                data[offset + 2] = (byte)(value >> 16);
                data[offset + 3] = (byte)(value >> 24);
            }

            internal static void Write64(Span<byte> data, int offset, ulong value)
            {
                Write32(data, offset, (uint)value);
                Write32(data, offset + 4, (uint)(value >> 32));
            }

            private static ulong Read64(ReadOnlySpan<byte> data, int offset) =>
                Read32(data, offset) | (ulong)Read32(data, offset + 4) << 32;

            private static ulong Round(ulong value, ulong input)
            {
                unchecked
                {
                    value += input * Prime2;
                    return Rotate(value, 31) * Prime1;
                }
            }

            private static ulong Merge(ulong hash, ulong value)
            {
                unchecked
                {
                    return (hash ^ Round(0, value)) * Prime1 + Prime4;
                }
            }

            private static ulong Rotate(ulong value, int count) =>
                value << count | value >> (64 - count);
        }

        private struct ServerWorld : IWorldType { }
        private struct ClientWorld : IWorldType { }
        private struct MinimumClientWorld : IWorldType { }
        private struct MissingTagWorld : IWorldType { }
        private struct InvalidAuthorityWorld : IWorldType { }
        private struct TestEntity : IEntityType { public byte Id() => 1; }
        private struct DifferentTag : ITag { }

        private interface ISessionView
        {
            SessionState State { get; }
            SessionError Error { get; }
            DisconnectReason? Reason { get; }
        }

        private readonly struct ClientView : ISessionView
        {
            private readonly Session<ClientWorld> _session;
            internal ClientView(Session<ClientWorld> session) => _session = session;
            public SessionState State => _session.State;
            public SessionError Error => _session.Error;
            public DisconnectReason? Reason => _session.Reason;
        }

        private readonly struct ServerView : ISessionView
        {
            private readonly Session<ServerWorld> _session;
            internal ServerView(Session<ServerWorld> session) => _session = session;
            public SessionState State => _session.State;
            public SessionError Error => _session.Error;
            public DisconnectReason? Reason => _session.Reason;
        }

        private sealed class TransportTestException : Exception { }

        private sealed class NonSteppedTransport : ITransport
        {
            internal int DisposeCount;
            public TransportState State => TransportState.Connected;
            public TransportError Error => TransportError.None;
            public bool TrySend(Channel channel, ref PacketLease packet) { packet.Dispose(); packet = default; return false; }
            public bool TryReceive(out Channel channel, out PacketLease packet) { channel = default; packet = default; return false; }
            public void Dispose() => DisposeCount++;
        }

        private readonly struct SendAttempt
        {
            internal SendAttempt(Channel channel, byte[] bytes, ulong stepIndex)
            {
                Channel = channel;
                Bytes = bytes;
                StepIndex = stepIndex;
            }
            internal Channel Channel { get; }
            internal byte[] Bytes { get; }
            internal ulong StepIndex { get; }
        }

        private sealed class TestTransport : ITransport, ISteppedTransport
        {
            private readonly Queue<Item> _incoming = new();
            private readonly int _capacity;
            private TestTransport _peer;
            private ulong _currentStep;

            private TestTransport(int capacity)
            {
                _capacity = capacity;
                State = TransportState.Connected;
                Error = TransportError.None;
            }

            internal readonly List<ulong> BeginSteps = new();
            internal readonly List<SendAttempt> SendAttempts = new();
            internal bool ReturnFalseNextSend;
            internal bool ThrowBeginNext;
            internal bool ThrowReceiveNext;
            internal bool ThrowSendNext;
            internal bool TransferThenThrowSendNext;
            internal bool AssignThenThrowReceiveNext;
            internal PacketLease LastSendAlias;
            internal PacketLease LastReceiveAlias;
            internal int DisposeCount;

            public TransportState State { get; private set; }
            public TransportError Error { get; private set; }

            internal static TestTransport Unpaired() => new(4);

            internal static void CreatePair(int capacity, out TestTransport left, out TestTransport right)
            {
                left = new TestTransport(capacity);
                right = new TestTransport(capacity);
                left._peer = right;
                right._peer = left;
            }

            public void BeginStep(ulong stepIndex)
            {
                BeginSteps.Add(stepIndex);
                _currentStep = stepIndex;
                if (!ThrowBeginNext) return;
                ThrowBeginNext = false;
                throw new TransportTestException();
            }

            public bool TrySend(Channel channel, ref PacketLease packet)
            {
                SendAttempts.Add(new SendAttempt(channel, packet.Span.ToArray(), _currentStep));
                LastSendAlias = packet;
                if (TransferThenThrowSendNext)
                {
                    TransferThenThrowSendNext = false;
                    var transferred = PacketLease.Transfer(ref packet);
                    LastSendAlias = transferred;
                    transferred.Dispose();
                    throw new TransportTestException();
                }
                if (ThrowSendNext)
                {
                    ThrowSendNext = false;
                    throw new TransportTestException();
                }

                var owned = PacketLease.Transfer(ref packet);
                if (ReturnFalseNextSend)
                {
                    ReturnFalseNextSend = false;
                    owned.Dispose();
                    return false;
                }
                if (State != TransportState.Connected || _peer == null || _peer.State != TransportState.Connected)
                {
                    owned.Dispose();
                    return false;
                }
                if (_peer._incoming.Count >= _peer._capacity)
                {
                    owned.Dispose();
                    ForcePairFault(TransportError.QueueOverflow);
                    return false;
                }
                _peer._incoming.Enqueue(new Item(channel, ref owned));
                return true;
            }

            public bool TryReceive(out Channel channel, out PacketLease packet)
            {
                if (ThrowReceiveNext)
                {
                    ThrowReceiveNext = false;
                    throw new TransportTestException();
                }
                channel = default;
                packet = default;
                if (State != TransportState.Connected || _incoming.Count == 0) return false;
                var item = _incoming.Dequeue();
                channel = item.Channel;
                packet = item.Take();
                LastReceiveAlias = packet;
                if (AssignThenThrowReceiveNext)
                {
                    AssignThenThrowReceiveNext = false;
                    throw new TransportTestException();
                }
                return true;
            }

            public void Dispose()
            {
                if (State == TransportState.Disposed) return;
                DisposeCount++;
                var peer = _peer;
                _peer = null;
                if (peer != null) peer._peer = null;
                Drain();
                State = TransportState.Disposed;
                Error = TransportError.Disposed;
                if (peer != null && peer.State == TransportState.Connected)
                {
                    peer.Drain();
                    peer.State = TransportState.Closed;
                    peer.Error = TransportError.RemoteClosed;
                }
            }

            internal void Inject(Channel channel, ref PacketLease packet)
            {
                var owned = PacketLease.Transfer(ref packet);
                _incoming.Enqueue(new Item(channel, ref owned));
            }

            internal void ForceTerminal(TransportState state, TransportError error)
            {
                Drain();
                State = state;
                Error = error;
            }

            private void ForcePairFault(TransportError error)
            {
                ForceTerminal(TransportState.Faulted, error);
                _peer?.ForceTerminal(TransportState.Faulted, error);
            }

            private void Drain()
            {
                while (_incoming.Count > 0) _incoming.Dequeue().Dispose();
            }

            private sealed class Item : IDisposable
            {
                private PacketLease _packet;
                internal Item(Channel channel, ref PacketLease packet)
                {
                    Channel = channel;
                    _packet = PacketLease.Transfer(ref packet);
                }
                internal Channel Channel { get; }
                internal PacketLease Take() => PacketLease.Transfer(ref _packet);
                public void Dispose()
                {
                    if (!_packet.IsValid) return;
                    _packet.Dispose();
                    _packet = default;
                }
            }
        }
    }
}
