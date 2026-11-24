namespace UniGame.StaticEcs.Network.Tests
{
    using FFS.Libraries.StaticEcs;
    using NUnit.Framework;

    public sealed partial class NetworkV7Tests
    {
        [Test]
        public void SuppressedAcknowledgementsApplySnapshotsWithoutSendingAck()
        {
            CreateReplicationWorld<AuthorityWorld>(true);
            CreateReplicationWorld<ClientAWorld>(false);
            try
            {
                MemoryNetworkTransport.CreatePair(new ConnectionId(97),
                    out var clientTransport, out var serverTransport);
                using (clientTransport)
                using (serverTransport)
                {
                    var observer = new TraceCollector();
                    var server = new NetworkServer<AuthorityWorld>(
                        Schema<AuthorityWorld>(true), static (_, _) => true);
                    server.AddConnection(serverTransport, 7, 15, new ScopeId(1));
                    var client = new NetworkClient<ClientAWorld>(clientTransport,
                        Schema<ClientAWorld>(false), new ScopeId(1), observer);
                    Assert.That(client.BeginHandshake(), Is.True);
                    server.Receive();
                    var entity = World<AuthorityWorld>.NewEntity<TestEntity>();
                    entity.Set(new TestComponent { Value = 1 });
                    server.Tick(_ => { });
                    client.Process();
                    server.Receive();
                    Assert.That(client.AcknowledgedSnapshotTick, Is.EqualTo(1));
                    Assert.That(observer.Count(NetworkPhase.Send, NetworkPacketKind.Ack),
                        Is.EqualTo(1));
                    Assert.That(serverTransport.TryReceive(out _), Is.False);

                    client.SuppressAcknowledgements = true;
                    entity.Set(new TestComponent { Value = 2 });
                    server.Tick(_ => { });
                    client.Process();

                    Assert.That(client.AcknowledgedSnapshotTick, Is.EqualTo(2),
                        "the snapshot is still applied");
                    Assert.That(observer.Count(NetworkPhase.Send, NetworkPacketKind.Ack),
                        Is.EqualTo(1), "no acknowledgement leaves a draining client");
                    Assert.That(serverTransport.TryReceive(out _), Is.False,
                        "nothing is queued towards the server");
                    Assert.That(client.CaptureConnection().Ticks.AcknowledgedSnapshotTick,
                        Is.EqualTo(2));
                    Assert.That(server.ServerTick, Is.EqualTo(2));
                    client.Process();
                    Assert.That(serverTransport.TryReceive(out _), Is.False,
                        "no retry is scheduled either");
                }
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
                World<ClientAWorld>.Destroy();
            }
        }
    }
}
