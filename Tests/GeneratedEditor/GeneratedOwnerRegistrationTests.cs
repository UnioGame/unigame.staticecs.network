using FFS.Libraries.StaticEcs;
using NUnit.Framework;
using UniGame.StaticEcs.Network.GeneratedTests.Shared;

[assembly: UniGame.StaticEcs.Network.NetworkEndpoint("OwnerAuthority", typeof(UniGame.StaticEcs.Network.GeneratedTests.OwnerAuthorityWorld), UniGame.StaticEcs.Network.NetworkRole.Server)]
[assembly: UniGame.StaticEcs.Network.NetworkEndpoint("OwnerReplica", typeof(UniGame.StaticEcs.Network.GeneratedTests.OwnerReplicaWorld), UniGame.StaticEcs.Network.NetworkRole.Client)]

namespace UniGame.StaticEcs.Network.GeneratedTests
{
    /// <summary>
    /// Identifies the authority world used by the generated owner-replication fixture.
    /// </summary>
    public struct OwnerAuthorityWorld : IWorldType { }

    /// <summary>
    /// Identifies the replica world used by the generated owner-replication fixture.
    /// </summary>
    public struct OwnerReplicaWorld : IWorldType { }

    internal sealed class GeneratedOwnerRegistrationTests
    {
        [Test]
        public void DocumentedRegistrationPathReplicatesOwnerWithGeneratedSchemas()
        {
            try
            {
                CreateAuthorityWorld();
                CreateReplicaWorld();
                var authoritySchema = global::GeneratedOwnerAuthorityNetwork.CreateSchema();
                var replicaSchema = global::GeneratedOwnerReplicaNetwork.CreateSchema();
                Assert.That(replicaSchema.Fingerprint, Is.EqualTo(authoritySchema.Fingerprint));

                var source = World<OwnerAuthorityWorld>.NewEntity<GeneratedOwnerEntity>();
                source.Set(new NetworkOwnerComponent { PeerId = 42 });
                var capture = new NetworkReplicator<OwnerAuthorityWorld>(authoritySchema, (scope, entity) => true, new ScopeId(1));
                Assert.That(capture.Capture(1, out var snapshot), Is.EqualTo(SnapshotCaptureResult.Success));

                var apply = new NetworkReplicator<OwnerReplicaWorld>(replicaSchema, new ScopeId(1));
                Assert.That(apply.Stage(snapshot, out var staged), Is.EqualTo(SnapshotApplyResult.Success));
                Assert.That(apply.Apply(staged), Is.EqualTo(SnapshotApplyResult.Success));
                Assert.That(source.GID.TryUnpack<OwnerReplicaWorld>(out var replica), Is.True);
                Assert.That(replica.Read<NetworkOwnerComponent>().PeerId, Is.EqualTo(42));
            }
            finally
            {
                if (World<OwnerAuthorityWorld>.Status != WorldStatus.NotCreated) World<OwnerAuthorityWorld>.Destroy();
                if (World<OwnerReplicaWorld>.Status != WorldStatus.NotCreated) World<OwnerReplicaWorld>.Destroy();
            }
        }

        private static void CreateAuthorityWorld()
        {
            World<OwnerAuthorityWorld>.Create(WorldConfig.Default());
            var types = World<OwnerAuthorityWorld>.Types();
            types.RegisterAll(typeof(NetworkOwnerComponent).Assembly);
            types.RegisterAll(typeof(GeneratedOwnerEntity).Assembly);
            global::GeneratedOwnerAuthorityNetwork.RegisterTypes(types);
            World<OwnerAuthorityWorld>.Initialize();
        }

        private static void CreateReplicaWorld()
        {
            World<OwnerReplicaWorld>.Create(WorldConfig.Default());
            var types = World<OwnerReplicaWorld>.Types();
            types.RegisterAll(typeof(NetworkOwnerComponent).Assembly);
            types.RegisterAll(typeof(GeneratedOwnerEntity).Assembly);
            global::GeneratedOwnerReplicaNetwork.RegisterTypes(types);
            World<OwnerReplicaWorld>.Initialize();
        }
    }
}
