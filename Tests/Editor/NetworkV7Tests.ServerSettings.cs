using System;
using NUnit.Framework;

namespace UniGame.StaticEcs.Network.Tests
{
    /// <summary>Covers <see cref="NetworkServerSettings"/>: validation and the values the server reads from it.</summary>
    public sealed partial class NetworkV7Tests
    {
        [Test]
        public void ServerSettingsRejectValuesTheServerCannotRunWith()
        {
            Assert.DoesNotThrow(() => new NetworkServerSettings().Validate());
            AssertInvalid(new NetworkServerSettings { MinimumHistoryTicksPerScope = 0 });
            AssertInvalid(new NetworkServerSettings { MinimumHistoryBytesPerScope = 0 });
            AssertInvalid(new NetworkServerSettings { MaximumPendingCommandsPerPeer = 0 });
            AssertInvalid(new NetworkServerSettings { MaximumPendingCommandBytesPerPeer = 0 });
            AssertInvalid(new NetworkServerSettings { CommandPastWindowTicks = -1 });
            AssertInvalid(new NetworkServerSettings { CommandFutureWindowTicks = -1 });
            AssertInvalid(new NetworkServerSettings { RecoveryRetryTicks = -1 });
            AssertInvalid(new NetworkServerSettings { OwnerStateIntervalTicks = 0 });
            AssertInvalid(new NetworkServerSettings { CellCacheStaleTicks = 0 });
            AssertInvalid(new NetworkServerSettings { UnreliableDeltaChunks = -1 });
            AssertInvalid(new NetworkServerSettings
            {
                UnreliableDeltaChunks = NetworkServerSettings.MaximumUnreliableDeltaChunks + 1,
            });
            AssertInvalid(new NetworkServerSettings { UnreliableDeltaLostTicks = 0 });
            Assert.Throws<ArgumentOutOfRangeException>(() =>
                new NetworkServer<TestWorld>(Schema<TestWorld>(true), static (_, _) => true,
                    settings: new NetworkServerSettings { OwnerStateIntervalTicks = 0 }));
        }

        [Test]
        public void HistoryFloorPerScopeComesFromTheSettings()
        {
            var schema = Schema<TestWorld>(true);
            var coordinator = new NetworkServerCoordinator<TestWorld>(64, 1_000_000,
                new NetworkServerSettings { MinimumHistoryBytesPerScope = 700_000 });
            var scopeA = new ScopeId(1);
            var scopeB = new ScopeId(2);
            coordinator.StoreCapture(scopeA, new NetworkSnapshot(1, schema.Fingerprint, scopeA,
                Lease(new byte[] { 1 }), 0, 0));
            var sessionB = new NetworkSession<TestWorld>(new ConnectionId(2), NetworkRole.Server,
                schema);
            sessionB.Admit(schema.Fingerprint, 2, 20, scopeB);
            coordinator.Add(sessionB);
            coordinator.StoreCapture(scopeB, new NetworkSnapshot(1, schema.Fingerprint, scopeB,
                Lease(new byte[] { 2 }), 0, 0));

            Assert.That(coordinator.History(scopeA).MaxBytes, Is.EqualTo(700_000),
                "Half of the shared bytes is below the floor.");
            Assert.That(coordinator.History(scopeB).MaxBytes, Is.EqualTo(700_000));
            coordinator.Clear();
        }

        private static void AssertInvalid(NetworkServerSettings settings) =>
            Assert.Throws<ArgumentOutOfRangeException>(settings.Validate);
    }
}
