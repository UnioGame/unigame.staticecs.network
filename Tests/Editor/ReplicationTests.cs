using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using FFS.Libraries.StaticEcs;
using NUnit.Framework;

namespace UniGame.StaticEcs.Network.Tests
{
    public sealed class ReplicationTests
    {
        private const uint Chunk = 9;
        private const ushort Cluster = 3;

        [SetUp]
        public void EnterPoolTestLock() => Monitor.Enter(PoolTestGate.Sync);

        [TearDown]
        public void ExitPoolTestLock() => Monitor.Exit(PoolTestGate.Sync);

        [Test]
        public void CaptureIsCanonicalAndApplyReplacesCompleteReplicaState()
        {
            CreateWorld<AuthorityWorld>(ChunkOwnerType.Self);
            CreateWorld<ReplicaWorld>(ChunkOwnerType.Other);
            try
            {
                var authoritySchema = Schema<AuthorityWorld>();
                var replicaSchema = Schema<ReplicaWorld>();
                var map = Mapping();
                using var authorityScope = new ReplicaScope<AuthorityWorld>(ScopeRole.Authority, map);
                using var replicaScope = new ReplicaScope<ReplicaWorld>(ScopeRole.Replica, map);
                using var authority = new Replicator<AuthorityWorld>(authoritySchema, authorityScope);
                using var replica = new Replicator<ReplicaWorld>(replicaSchema, replicaScope);

                var target = World<AuthorityWorld>.NewEntityInChunk<NetEntity>(Chunk);
                target.Set<ReplicatedTag>();
                target.Set(new Value { Number = 17 });
                World<AuthorityWorld>.Components<Value>.Instance.Disable(target);
                target.Set<StateTag>();
                ref var values = ref target.Add<World<AuthorityWorld>.Multi<Item>>();
                values.Add(new Item { Number = 2 });
                values.Add(new Item { Number = 1 });

                var source = World<AuthorityWorld>.NewEntityInChunk<NetEntity>(Chunk);
                source.Set<ReplicatedTag>();
                source.Set(new Value { Number = 42 });
                source.Set(new World<AuthorityWorld>.Link<ParentLink>(target));
                ref var links = ref source.Add<World<AuthorityWorld>.Links<TargetLinks>>();
                links.Add(target);
                source.Disable();

                Assert.That(authority.Capture(out var first), Is.EqualTo(CaptureResult.Success));
                Assert.That(authority.Capture(out var second), Is.EqualTo(CaptureResult.Success));
                CollectionAssert.AreEqual(first.Span.ToArray(), second.Span.ToArray());
                second.Dispose();
                Assert.That(PayloadStager.TryStage(PacketKind.FullSnapshot, ref first, replicaSchema, out var staged), Is.True);
                using (staged)
                {
                    Assert.That(replica.Apply(staged), Is.EqualTo(ApplyResult.Success));
                }

                Assert.That(source.GID.TryUnpack<ReplicaWorld>(out var replicaSource), Is.True);
                Assert.That(replicaSource.Read<Value>().Number, Is.EqualTo(42));
                Assert.That(replicaSource.IsDisabled, Is.True);
                Assert.That(replicaSource.Read<World<ReplicaWorld>.Link<ParentLink>>().Value, Is.EqualTo(target.GID));
                World<ReplicaWorld>.Components<World<ReplicaWorld>.Link<ParentLink>>.Instance.Disable(replicaSource);
                Assert.That(World<ReplicaWorld>.Components<World<ReplicaWorld>.Link<ParentLink>>.Instance.HasDisabled(replicaSource), Is.True);
                Assert.That(target.GID.TryUnpack<ReplicaWorld>(out var replicaTarget), Is.True);
                Assert.That(replicaTarget.Has<StateTag>(), Is.True);
                Assert.That(World<ReplicaWorld>.Components<Value>.Instance.HasDisabled(replicaTarget), Is.True);
                Assert.That(replicaTarget.Read<World<ReplicaWorld>.Multi<Item>>().AsReadOnlySpan[0].Number, Is.EqualTo(2));

                Assert.That(authority.Capture(out var normalize), Is.EqualTo(CaptureResult.Success));
                Assert.That(PayloadStager.TryStage(PacketKind.FullSnapshot, ref normalize, replicaSchema, out staged), Is.True);
                using (staged) Assert.That(replica.Apply(staged), Is.EqualTo(ApplyResult.Success));
                Assert.That(World<ReplicaWorld>.Components<World<ReplicaWorld>.Link<ParentLink>>.Instance.HasDisabled(replicaSource), Is.False);

                source.Delete<Value>();
                source.Delete<World<AuthorityWorld>.Link<ParentLink>>();
                source.Delete<World<AuthorityWorld>.Links<TargetLinks>>();
                target.Delete<StateTag>();
                target.Delete<Value>();
                target.Delete<World<AuthorityWorld>.Multi<Item>>();
                Assert.That(authority.Capture(out var removal), Is.EqualTo(CaptureResult.Success));
                Assert.That(PayloadStager.TryStage(PacketKind.FullSnapshot, ref removal, replicaSchema, out staged), Is.True);
                using (staged) Assert.That(replica.Apply(staged), Is.EqualTo(ApplyResult.Success));
                Assert.That(replicaSource.Has<Value>(), Is.False);
                Assert.That(replicaSource.Has<World<ReplicaWorld>.Link<ParentLink>>(), Is.False);
                Assert.That(replicaSource.Has<World<ReplicaWorld>.Links<TargetLinks>>(), Is.False);
                Assert.That(replicaTarget.Has<StateTag>(), Is.False);
                Assert.That(replicaTarget.Has<Value>(), Is.False);
                Assert.That(replicaTarget.Has<World<ReplicaWorld>.Multi<Item>>(), Is.False);

                var sourceGid = source.GID;
                source.Destroy();
                Assert.That(authority.Capture(out var despawn), Is.EqualTo(CaptureResult.Success));
                Assert.That(PayloadStager.TryStage(PacketKind.FullSnapshot, ref despawn, replicaSchema, out staged), Is.True);
                using (staged) Assert.That(replica.Apply(staged), Is.EqualTo(ApplyResult.Success));
                Assert.That(sourceGid.TryUnpack<ReplicaWorld>(out _), Is.False);
                Assert.That(target.GID.TryUnpack<ReplicaWorld>(out _), Is.True);
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
                World<ReplicaWorld>.Destroy();
            }
        }

        [Test]
        public void CaptureMatchesGoldenBytesAcrossEntityInsertionOrders()
        {
            CreateWorld<AuthorityWorld>(ChunkOwnerType.Self);
            CreateWorld<AlternateAuthorityWorld>(ChunkOwnerType.Self);
            try
            {
                var firstSchema = Schema<AuthorityWorld>();
                var secondSchema = Schema<AlternateAuthorityWorld>();
                var firstLow = World<AuthorityWorld>.NewEntityInChunk<NetEntity>(Chunk);
                var firstHigh = World<AuthorityWorld>.NewEntityInChunk<NetEntity>(Chunk);
                SetValueEntity(firstHigh, 20);
                SetValueEntity(firstLow, 10);
                var secondLow = World<AlternateAuthorityWorld>.NewEntityInChunk<NetEntity>(Chunk);
                var secondHigh = World<AlternateAuthorityWorld>.NewEntityInChunk<NetEntity>(Chunk);
                SetValueEntity(secondLow, 10);
                SetValueEntity(secondHigh, 20);
                var low = firstLow.GID;
                var high = firstHigh.GID;
                Assert.That(secondLow.GID, Is.EqualTo(low));
                Assert.That(secondHigh.GID, Is.EqualTo(high));
                using var firstScope = new ReplicaScope<AuthorityWorld>(ScopeRole.Authority, Mapping());
                using var secondScope = new ReplicaScope<AlternateAuthorityWorld>(ScopeRole.Authority, Mapping());
                using var first = new Replicator<AuthorityWorld>(firstSchema, firstScope);
                using var second = new Replicator<AlternateAuthorityWorld>(secondSchema, secondScope);

                Assert.That(first.Capture(out var firstBytes), Is.EqualTo(CaptureResult.Success));
                Assert.That(second.Capture(out var secondBytes), Is.EqualTo(CaptureResult.Success));
                var expected = Snapshot(
                    ValueSnapshot(low, 10),
                    ValueSnapshot(high, 20));
                var golden = new byte[256];
                Assert.That(PayloadCodec.TryWrite(expected, golden, out var goldenLength), Is.True);
                CollectionAssert.AreEqual(golden.AsSpan(0, goldenLength).ToArray(), firstBytes.Span.ToArray());
                CollectionAssert.AreEqual(firstBytes.Span.ToArray(), secondBytes.Span.ToArray());
                firstBytes.Dispose();
                secondBytes.Dispose();
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
                World<AlternateAuthorityWorld>.Destroy();
            }
        }

        [Test]
        public void WarmedRepresentativeCaptureAllocatesNoManagedMemory()
        {
            const int iterations = 128;
            CreateWorld<AllocationWorld>(ChunkOwnerType.Self);
            try
            {
                var target = World<AllocationWorld>.NewEntityInChunk<NetEntity>(Chunk);
                target.Set<ReplicatedTag>();
                target.Set(new Value { Number = 17 });
                target.Set<StateTag>();
                ref var values = ref target.Add<World<AllocationWorld>.Multi<Item>>();
                values.Add(new Item { Number = 2 });
                values.Add(new Item { Number = 1 });

                var source = World<AllocationWorld>.NewEntityInChunk<NetEntity>(Chunk);
                source.Set<ReplicatedTag>();
                source.Set(new Value { Number = 42 });
                source.Set(new World<AllocationWorld>.Link<ParentLink>(target));
                ref var links = ref source.Add<World<AllocationWorld>.Links<TargetLinks>>();
                links.Add(target);

                using var scope = new ReplicaScope<AllocationWorld>(ScopeRole.Authority, Mapping());
                using var replicator = new Replicator<AllocationWorld>(Schema<AllocationWorld>(), scope);
                Assert.That(replicator.Capture(out var warm), Is.EqualTo(CaptureResult.Success));
                Assert.That(warm.IsValid, Is.True);
                Assert.That(warm.Length, Is.GreaterThan(0));
                warm.Dispose();

                var resultTotal = 0;
                var validTotal = 0;
                var lengthTotal = 0;
                var before = GC.GetAllocatedBytesForCurrentThread();
                for (var i = 0; i < iterations; i++)
                {
                    var result = replicator.Capture(out var payload);
                    resultTotal += (int)result;
                    if (!payload.IsValid) continue;
                    validTotal++;
                    lengthTotal += payload.Length;
                    payload.Dispose();
                }
                var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

                Assert.That(resultTotal, Is.Zero);
                Assert.That(validTotal, Is.EqualTo(iterations));
                Assert.That(lengthTotal, Is.GreaterThan(iterations));
                Assert.That(allocated, Is.Zero);
            }
            finally
            {
                World<AllocationWorld>.Destroy();
            }
        }

        [Test]
        public void CaptureRejectsRelationOutsideSameSnapshotWithoutLeakingLease()
        {
            CreateWorld<AuthorityWorld>(ChunkOwnerType.Self);
            try
            {
                var map = Mapping();
                using var scope = new ReplicaScope<AuthorityWorld>(ScopeRole.Authority, map);
                using var replicator = new Replicator<AuthorityWorld>(Schema<AuthorityWorld>(), scope);
                var target = World<AuthorityWorld>.NewEntityInChunk<NetEntity>(Chunk);
                var source = World<AuthorityWorld>.NewEntityInChunk<NetEntity>(Chunk);
                source.Set<ReplicatedTag>();
                source.Set(new World<AuthorityWorld>.Link<ParentLink>(target));

                Assert.That(replicator.Capture(out var payload), Is.EqualTo(CaptureResult.MissingTarget));
                Assert.That(payload.IsValid, Is.False);
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
            }
        }

        [Test]
        public void CaptureRejectsDisabledRelationStorageInVersionOne()
        {
            CreateWorld<AuthorityWorld>(ChunkOwnerType.Self);
            try
            {
                var map = Mapping();
                using var scope = new ReplicaScope<AuthorityWorld>(ScopeRole.Authority, map);
                using var replicator = new Replicator<AuthorityWorld>(Schema<AuthorityWorld>(), scope);
                var target = World<AuthorityWorld>.NewEntityInChunk<NetEntity>(Chunk);
                target.Set<ReplicatedTag>();
                var source = World<AuthorityWorld>.NewEntityInChunk<NetEntity>(Chunk);
                source.Set<ReplicatedTag>();
                source.Set(new World<AuthorityWorld>.Link<ParentLink>(target));
                World<AuthorityWorld>.Components<World<AuthorityWorld>.Link<ParentLink>>.Instance.Disable(source);

                Assert.That(replicator.Capture(out var payload), Is.EqualTo(CaptureResult.DisabledUnsupported));
                Assert.That(payload.IsValid, Is.False);

                World<AuthorityWorld>.Components<World<AuthorityWorld>.Link<ParentLink>>.Instance.Enable(source);
                source.Delete<World<AuthorityWorld>.Link<ParentLink>>();
                ref var links = ref source.Add<World<AuthorityWorld>.Links<TargetLinks>>();
                links.Add(target);
                World<AuthorityWorld>.Components<World<AuthorityWorld>.Links<TargetLinks>>.Instance.Disable(source);
                Assert.That(replicator.Capture(out payload), Is.EqualTo(CaptureResult.DisabledUnsupported));
                Assert.That(payload.IsValid, Is.False);

                source.Delete<World<AuthorityWorld>.Links<TargetLinks>>();
                ref var values = ref source.Add<World<AuthorityWorld>.Multi<Item>>();
                values.Add(new Item { Number = 1 });
                World<AuthorityWorld>.Components<World<AuthorityWorld>.Multi<Item>>.Instance.Disable(source);
                Assert.That(replicator.Capture(out payload), Is.EqualTo(CaptureResult.DisabledUnsupported));
                Assert.That(payload.IsValid, Is.False);
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
            }
        }

        [Test]
        public void CaptureCodecFailureAndExceptionReturnDefaultAndReusePooledLeaseState()
        {
            CreateWorld<CaptureFailureWorld>(ChunkOwnerType.Self);
            try
            {
                var entity = World<CaptureFailureWorld>.NewEntityInChunk<NetEntity>(Chunk);
                entity.Set<ReplicatedTag>();
                entity.Set(new Value { Number = 42 });
                using var scope = new ReplicaScope<CaptureFailureWorld>(ScopeRole.Authority, Mapping());
                using var replicator = new Replicator<CaptureFailureWorld>(Schema<CaptureFailureWorld>(), scope);

                Assert.That(replicator.Capture(out var warm), Is.EqualTo(CaptureResult.Success));
                warm.Dispose();
                var stateAllocations = PacketLease.StateAllocationCountForTests;

                ValueCodec.FailWrites = true;
                Assert.That(replicator.Capture(out var failed), Is.EqualTo(CaptureResult.CodecFailed));
                Assert.That(failed.IsValid, Is.False);
                ValueCodec.FailWrites = false;
                Assert.That(replicator.Capture(out var afterFailure), Is.EqualTo(CaptureResult.Success));
                Assert.That(afterFailure.IsValid, Is.True);
                afterFailure.Dispose();
                Assert.That(PacketLease.StateAllocationCountForTests, Is.EqualTo(stateAllocations));

                var thrown = default(PacketLease);
                ValueCodec.ThrowOnWrite = true;
                Assert.Throws<InvalidOperationException>(() => replicator.Capture(out thrown));
                Assert.That(thrown.IsValid, Is.False);
                ValueCodec.ThrowOnWrite = false;
                Assert.That(replicator.Capture(out var afterException), Is.EqualTo(CaptureResult.Success));
                Assert.That(afterException.IsValid, Is.True);
                afterException.Dispose();
                Assert.That(PacketLease.StateAllocationCountForTests, Is.EqualTo(stateAllocations));
            }
            finally
            {
                ValueCodec.FailWrites = false;
                ValueCodec.ThrowOnWrite = false;
                World<CaptureFailureWorld>.Destroy();
            }
        }

        [Test]
        public void FfsTagStorageCannotRepresentDisabledTagState()
        {
            CreateWorld<AuthorityWorld>(ChunkOwnerType.Self);
            try
            {
                var entity = World<AuthorityWorld>.NewEntityInChunk<NetEntity>(Chunk);
                entity.Set<StateTag>();
                Assert.Catch<Exception>(() => World<AuthorityWorld>.Components<StateTag>.Instance.Disable(entity));
                Assert.That(entity.Has<StateTag>(), Is.True);
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
            }
        }

        [Test]
        public void RolesAndForeignReplicaOccupantsFailBeforeMutation()
        {
            CreateWorld<AuthorityWorld>(ChunkOwnerType.Self);
            CreateWorld<ReplicaWorld>(ChunkOwnerType.Other);
            try
            {
                var map = Mapping();
                using var authorityScope = new ReplicaScope<AuthorityWorld>(ScopeRole.Authority, map);
                using var replicaScope = new ReplicaScope<ReplicaWorld>(ScopeRole.Replica, map);
                var authoritySchema = Schema<AuthorityWorld>();
                var replicaSchema = Schema<ReplicaWorld>();
                using var authority = new Replicator<AuthorityWorld>(authoritySchema, authorityScope);
                using var replica = new Replicator<ReplicaWorld>(replicaSchema, replicaScope);
                Assert.That(replica.Capture(out var wrongRolePayload), Is.EqualTo(CaptureResult.WrongRole));
                Assert.That(wrongRolePayload.IsValid, Is.False);
                SeedRichReplica(replica, replicaSchema);

                var source = World<AuthorityWorld>.NewEntityInChunk<NetEntity>(Chunk);
                source.Set<ReplicatedTag>();
                Assert.That(authority.Capture(out var payload), Is.EqualTo(CaptureResult.Success));
                Assert.That(PayloadStager.TryStage(PacketKind.FullSnapshot, ref payload, replicaSchema, out var staged), Is.True);
                var foreignGid = new EntityGID((Chunk << Const.ENTITIES_IN_CHUNK_SHIFT) + 100, 1, Cluster);
                var foreign = World<ReplicaWorld>.NewEntityByGID<NetEntity>(foreignGid);
                foreign.Set(new Value { Number = 99 });

                using (staged) AssertApplyFailure(replica, staged, ApplyResult.EntityConflict);
                Assert.That(foreign.Read<Value>().Number, Is.EqualTo(99));
                Assert.That(source.GID.TryUnpack<ReplicaWorld>(out _), Is.False);
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
                World<ReplicaWorld>.Destroy();
            }
        }

        [Test]
        public void InvalidTopologyMappingsAreRejectedWithoutTopologyMutation()
        {
            CreateWorld<AuthorityWorld>(ChunkOwnerType.Self);
            try
            {
                World<AuthorityWorld>.RegisterChunk(Chunk + 1, ChunkOwnerType.Other, Cluster);
                using var missing = new ReplicaScope<AuthorityWorld>(ScopeRole.Authority,
                    new[] { new ChunkMapping { Chunk = 77, Cluster = Cluster, Role = 1 } });
                using var wrongCluster = new ReplicaScope<AuthorityWorld>(ScopeRole.Authority,
                    new[] { new ChunkMapping { Chunk = Chunk, Cluster = (ushort)(Cluster + 1), Role = 1 } });
                using var wrongOwner = new ReplicaScope<AuthorityWorld>(ScopeRole.Authority,
                    new[] { new ChunkMapping { Chunk = Chunk + 1, Cluster = Cluster, Role = 1 } });
                using var duplicate = new ReplicaScope<AuthorityWorld>(ScopeRole.Authority,
                    new[] { new ChunkMapping { Chunk = Chunk, Cluster = Cluster, Role = 1 }, new ChunkMapping { Chunk = Chunk, Cluster = Cluster, Role = 1 } });
                using var a = new Replicator<AuthorityWorld>(Schema<AuthorityWorld>(), missing);
                using var b = new Replicator<AuthorityWorld>(Schema<AuthorityWorld>(), wrongCluster);
                using var c = new Replicator<AuthorityWorld>(Schema<AuthorityWorld>(), wrongOwner);
                using var d = new Replicator<AuthorityWorld>(Schema<AuthorityWorld>(), duplicate);

                Assert.That(a.Capture(out var missingPayload), Is.EqualTo(CaptureResult.ScopeInvalid));
                Assert.That(missingPayload.IsValid, Is.False);
                Assert.That(b.Capture(out var clusterPayload), Is.EqualTo(CaptureResult.ScopeInvalid));
                Assert.That(clusterPayload.IsValid, Is.False);
                Assert.That(c.Capture(out var ownerPayload), Is.EqualTo(CaptureResult.ScopeInvalid));
                Assert.That(ownerPayload.IsValid, Is.False);
                Assert.That(d.Capture(out var duplicatePayload), Is.EqualTo(CaptureResult.ScopeInvalid));
                Assert.That(duplicatePayload.IsValid, Is.False);
                Assert.That(World<AuthorityWorld>.GetChunkOwner(Chunk), Is.EqualTo(ChunkOwnerType.Self));
                Assert.That(World<AuthorityWorld>.GetChunkClusterId(Chunk), Is.EqualTo(Cluster));

                using var driftScope = new ReplicaScope<AuthorityWorld>(ScopeRole.Authority, Mapping());
                using var drift = new Replicator<AuthorityWorld>(Schema<AuthorityWorld>(), driftScope);
                World<AuthorityWorld>.ChangeChunkOwner(Chunk, ChunkOwnerType.Other);
                Assert.That(drift.Capture(out var driftPayload), Is.EqualTo(CaptureResult.ScopeInvalid));
                Assert.That(driftPayload.IsValid, Is.False);
                Assert.That(World<AuthorityWorld>.GetChunkOwner(Chunk), Is.EqualTo(ChunkOwnerType.Other));
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
            }
        }

        [Test]
        public void ApplyRejectsIdentityAndSegmentConflictsThenReplacesLedgerOwnedGeneration()
        {
            CreateWorld<ReplicaWorld>(ChunkOwnerType.Other);
            try
            {
                var schema = Schema<ReplicaWorld>();
                var map = Mapping();
                using var scope = new ReplicaScope<ReplicaWorld>(ScopeRole.Replica, map);
                using var replica = new Replicator<ReplicaWorld>(schema, scope);
                var id = Chunk << Const.ENTITIES_IN_CHUNK_SHIFT;
                SeedRichReplica(replica, schema);

                using (var zero = Stage(schema, Snapshot(new WireEntityId(id, Cluster, 0), Id(1))))
                    AssertApplyFailure(replica, zero, ApplyResult.InvalidEntity);
                using (var duplicate = Stage(schema, Snapshot(
                           new SnapshotEntity { Entity = new WireEntityId(id, Cluster, 1), KindId = Id(1) },
                           new SnapshotEntity { Entity = new WireEntityId(id, Cluster, 2), KindId = Id(1) })))
                    AssertApplyFailure(replica, duplicate, ApplyResult.InvalidEntity);
                using (var segment = Stage(schema, Snapshot(
                           new SnapshotEntity { Entity = new WireEntityId(id, Cluster, 1), KindId = Id(1) },
                           new SnapshotEntity { Entity = new WireEntityId(id + 1, Cluster, 1), KindId = Id(7) })))
                    AssertApplyFailure(replica, segment, ApplyResult.InvalidEntity);
                Assert.That(new EntityGID(id, 1, Cluster).TryUnpack<ReplicaWorld>(out _), Is.False);

                using (var first = Stage(schema, Snapshot(new WireEntityId(id, Cluster, 1), Id(1))))
                    Assert.That(replica.Apply(first), Is.EqualTo(ApplyResult.Success));
                using (var replacement = Stage(schema, Snapshot(new WireEntityId(id, Cluster, 2), Id(1))))
                    Assert.That(replica.Apply(replacement), Is.EqualTo(ApplyResult.Success));
                Assert.That(new EntityGID(id, 1, Cluster).TryUnpack<ReplicaWorld>(out _), Is.False);
                Assert.That(new EntityGID(id, 2, Cluster).TryUnpack<ReplicaWorld>(out var current), Is.True);
                Assert.That(current.Has<ReplicatedTag>(), Is.True);
            }
            finally
            {
                World<ReplicaWorld>.Destroy();
            }
        }

        [Test]
        public void ApplySupportsOutgoingSegmentTransitionAndChangedKindGenerationReplacement()
        {
            CreateWorld<ReplicaWorld>(ChunkOwnerType.Other);
            try
            {
                var schema = Schema<ReplicaWorld>();
                using var scope = new ReplicaScope<ReplicaWorld>(ScopeRole.Replica, Mapping());
                using var replica = new Replicator<ReplicaWorld>(schema, scope);
                SeedRichReplica(replica, schema);
                var id = Chunk << Const.ENTITIES_IN_CHUNK_SHIFT;
                using (var initial = Stage(schema, Snapshot(
                           new SnapshotEntity { Entity = new WireEntityId(id, Cluster, 1), KindId = Id(1) },
                           new SnapshotEntity { Entity = new WireEntityId(id + 1, Cluster, 1), KindId = Id(1) })))
                    Assert.That(replica.Apply(initial), Is.EqualTo(ApplyResult.Success));

                using (var transition = Stage(schema, Snapshot(new WireEntityId(id + 2, Cluster, 1), Id(7))))
                    Assert.That(replica.Apply(transition), Is.EqualTo(ApplyResult.Success));
                Assert.That(new EntityGID(id, 1, Cluster).TryUnpack<ReplicaWorld>(out _), Is.False);
                Assert.That(new EntityGID(id + 1, 1, Cluster).TryUnpack<ReplicaWorld>(out _), Is.False);
                Assert.That(new EntityGID(id + 2, 1, Cluster).TryUnpack<ReplicaWorld>(out var changedSegment), Is.True);
                Assert.That(changedSegment.EntityType, Is.EqualTo(default(OtherEntity).Id()));

                using (var replacement = Stage(schema, Snapshot(new WireEntityId(id + 2, Cluster, 2), Id(1))))
                    Assert.That(replica.Apply(replacement), Is.EqualTo(ApplyResult.Success));
                Assert.That(new EntityGID(id + 2, 1, Cluster).TryUnpack<ReplicaWorld>(out _), Is.False);
                Assert.That(new EntityGID(id + 2, 2, Cluster).TryUnpack<ReplicaWorld>(out var changedKind), Is.True);
                Assert.That(changedKind.EntityType, Is.EqualTo(default(NetEntity).Id()));

                var rich = RichSnapshot().Entities;
                var successfulEntities = new SnapshotEntity[rich.Length + 1];
                successfulEntities[0] = new SnapshotEntity
                {
                    Entity = new WireEntityId(id + 2, Cluster, 2),
                    KindId = Id(1),
                    Records = new[] { ComponentRecord(91) }
                };
                Array.Copy(rich, 0, successfulEntities, 1, rich.Length);
                using (var successful = Stage(schema, Snapshot(successfulEntities)))
                    Assert.That(replica.Apply(successful), Is.EqualTo(ApplyResult.Success));

                Assert.That(new EntityGID(id + 2, 2, Cluster).TryUnpack<ReplicaWorld>(out var survivor), Is.True);
                Assert.That(survivor.Read<Value>().Number, Is.EqualTo(91));
                var richGids = RichGids();
                Assert.That(richGids[0].TryUnpack<ReplicaWorld>(out var first), Is.True);
                Assert.That(richGids[1].TryUnpack<ReplicaWorld>(out var second), Is.True);
                Assert.That(richGids[2].TryUnpack<ReplicaWorld>(out var source), Is.True);
                Assert.That(first.Has<StateTag>(), Is.True);
                Assert.That(first.Read<Value>().Number, Is.EqualTo(17));
                Assert.That(World<ReplicaWorld>.Components<Value>.Instance.HasDisabled(first), Is.True);
                var multi = first.Read<World<ReplicaWorld>.Multi<Item>>().AsReadOnlySpan;
                Assert.That(multi.Length, Is.EqualTo(2));
                Assert.That(multi[0].Number, Is.EqualTo(2));
                Assert.That(multi[1].Number, Is.EqualTo(1));
                Assert.That(second.Read<Value>().Number, Is.EqualTo(23));
                Assert.That(source.IsDisabled, Is.True);
                Assert.That(source.Read<Value>().Number, Is.EqualTo(42));
                Assert.That(source.Read<World<ReplicaWorld>.Link<ParentLink>>().Value, Is.EqualTo(richGids[1]));
                var links = source.Read<World<ReplicaWorld>.Links<TargetLinks>>().AsReadOnlySpan;
                Assert.That(links.Length, Is.EqualTo(2));
                Assert.That(links[0].Value, Is.EqualTo(richGids[0]));
                Assert.That(links[1].Value, Is.EqualTo(richGids[1]));
                Assert.That(World<ReplicaWorld>.GetChunkOwner(Chunk), Is.EqualTo(ChunkOwnerType.Other));
                Assert.That(World<ReplicaWorld>.GetChunkClusterId(Chunk), Is.EqualTo(Cluster));

                var conflictEntities = new SnapshotEntity[rich.Length + 2];
                conflictEntities[0] = successfulEntities[0];
                conflictEntities[1] = new SnapshotEntity { Entity = new WireEntityId(id + 3, Cluster, 1), KindId = Id(7) };
                Array.Copy(rich, 0, conflictEntities, 2, rich.Length);
                var before = Fingerprint<ReplicaWorld>();
                using var survivorConflict = Stage(schema, Snapshot(conflictEntities));
                Assert.That(replica.Apply(survivorConflict), Is.EqualTo(ApplyResult.InvalidEntity));
                Assert.That(Fingerprint<ReplicaWorld>(), Is.EqualTo(before));
            }
            finally
            {
                World<ReplicaWorld>.Destroy();
            }
        }

        [Test]
        public void ApplyMissingRelationTargetPreservesCompleteWorldFingerprint()
        {
            CreateWorld<ReplicaWorld>(ChunkOwnerType.Other);
            try
            {
                var schema = Schema<ReplicaWorld>();
                using var scope = new ReplicaScope<ReplicaWorld>(ScopeRole.Replica, Mapping());
                using var replica = new Replicator<ReplicaWorld>(schema, scope);
                SeedRichReplica(replica, schema);
                var id = Chunk << Const.ENTITIES_IN_CHUNK_SHIFT;
                var missing = new EntityGID(id + 10, 1, Cluster);
                var record = new SnapshotRecord { TypeId = Id(4), Kind = RecordKind.Link, Version = 1, ElementCount = 1, Payload = EntityBytes(missing) };
                var source = new SnapshotEntity { Entity = new WireEntityId(id, Cluster, 1), KindId = Id(1), Records = new[] { record } };
                using var staged = Stage(schema, Snapshot(source));
                AssertApplyFailure(replica, staged, ApplyResult.MissingTarget);
            }
            finally
            {
                World<ReplicaWorld>.Destroy();
            }
        }

        [Test]
        public void ApplyRolePayloadAndTopologyFailuresPreserveCompleteWorldFingerprint()
        {
            CreateWorld<AuthorityWorld>(ChunkOwnerType.Self);
            CreateWorld<ReplicaWorld>(ChunkOwnerType.Other);
            try
            {
                var authoritySchema = Schema<AuthorityWorld>();
                var replicaSchema = Schema<ReplicaWorld>();
                using var authorityScope = new ReplicaScope<AuthorityWorld>(ScopeRole.Authority, Mapping());
                using var replicaScope = new ReplicaScope<ReplicaWorld>(ScopeRole.Replica, Mapping());
                using var authority = new Replicator<AuthorityWorld>(authoritySchema, authorityScope);
                using var replica = new Replicator<ReplicaWorld>(replicaSchema, replicaScope);
                SeedRichAuthority<AuthorityWorld>();
                SeedRichReplica(replica, replicaSchema);
                using var authoritySnapshot = Stage(authoritySchema, Snapshot());
                AssertApplyFailure(authority, authoritySnapshot, ApplyResult.WrongRole);

                var ackLease = PacketLease.Rent(1);
                ackLease.SetLength(0);
                Assert.That(PayloadStager.TryStage(PacketKind.Ack, ref ackLease, null, out var ack), Is.True);
                using (ack) AssertApplyFailure(replica, ack, ApplyResult.WrongPayload);

                using (var oversized = OversizedStage(replicaSchema))
                    AssertApplyFailure(replica, oversized, ApplyResult.LimitExceeded);

                using var replicaSnapshot = Stage(replicaSchema, Snapshot());
                World<ReplicaWorld>.ChangeChunkOwner(Chunk, ChunkOwnerType.Self);
                AssertApplyFailure(replica, replicaSnapshot, ApplyResult.ScopeInvalid);
            }
            finally
            {
                World<AuthorityWorld>.Destroy();
                World<ReplicaWorld>.Destroy();
            }
        }

        [Test]
        public void ApplyRejectsSchemaMismatchAndPropagatesCodecExceptionWithoutRollbackPromise()
        {
            CreateWorld<ReplicaWorld>(ChunkOwnerType.Other);
            try
            {
                var schema = Schema<ReplicaWorld>();
                var map = Mapping();
                using var scope = new ReplicaScope<ReplicaWorld>(ScopeRole.Replica, map);
                using var replica = new Replicator<ReplicaWorld>(schema, scope);
                var id = Chunk << Const.ENTITIES_IN_CHUNK_SHIFT;
                SeedRichReplica(replica, schema);
                var entity = new SnapshotEntity
                {
                    Entity = new WireEntityId(id, Cluster, 1),
                    KindId = Id(1),
                    Records = new[] { new SnapshotRecord { TypeId = Id(2), Kind = RecordKind.Component, Version = 1, ElementCount = 1, Payload = BitConverter.GetBytes(7) } }
                };
                using var staged = Stage(schema, new FullSnapshotPayload { Entities = new[] { entity } });
                var otherSchema = new SchemaBuilder<ReplicaWorld>().EntityKind<NetEntity>(Id(1)).EntityKind<OtherEntity>(Id(7)).Freeze();
                using var mismatched = new Replicator<ReplicaWorld>(otherSchema, scope);
                AssertApplyFailure(mismatched, staged, ApplyResult.SchemaMismatch);

                ValueCodec.Reads = 0;
                ValueCodec.ThrowOnReadCall = 2;
                try
                {
                    Assert.Throws<InvalidOperationException>(() => replica.Apply(staged));
                }
                finally
                {
                    ValueCodec.ThrowOnReadCall = 0;
                    ValueCodec.Reads = 0;
                }
                Assert.That(new EntityGID(id, 1, Cluster).TryUnpack<ReplicaWorld>(out var partial), Is.True,
                    "Typed apply exceptions propagate after the documented mutation boundary; rollback is not promised.");
                Assert.That(partial.Has<ReplicatedTag>(), Is.True);
            }
            finally
            {
                ValueCodec.ThrowOnReadCall = 0;
                World<ReplicaWorld>.Destroy();
            }
        }

        [Test]
        public void ApplyPropagatesComponentOnAddHookAndLeavesDocumentedPartialState()
        {
            CreateWorld<HookWorld>(ChunkOwnerType.Other);
            try
            {
                var schema = HookSchema<HookWorld>();
                using var scope = new ReplicaScope<HookWorld>(ScopeRole.Replica, Mapping());
                using var replica = new Replicator<HookWorld>(schema, scope);
                var id = Chunk << Const.ENTITIES_IN_CHUNK_SHIFT;
                var record = new SnapshotRecord
                {
                    TypeId = Id(8),
                    Kind = RecordKind.Component,
                    Version = 1,
                    ElementCount = 1,
                    Payload = BitConverter.GetBytes(99)
                };
                var snapshot = Snapshot(new SnapshotEntity
                {
                    Entity = new WireEntityId(id, Cluster, 1),
                    KindId = Id(1),
                    Records = new[] { record }
                });
                using var staged = Stage(schema, snapshot);

                HookComponent.ThrowOnAdd = true;
                try
                {
                    Assert.Throws<InvalidOperationException>(() => replica.Apply(staged));
                }
                finally
                {
                    HookComponent.ThrowOnAdd = false;
                }

                var gid = new EntityGID(id, 1, Cluster);
                Assert.That(gid.TryUnpack<HookWorld>(out var partial), Is.True,
                    "Lifecycle hooks propagate after mutation begins; replication does not promise rollback.");
                Assert.That(partial.Has<ReplicatedTag>(), Is.True);
                Assert.That(partial.Has<HookComponent>(), Is.True);
                Assert.That(partial.Read<HookComponent>().Number, Is.EqualTo(99));
            }
            finally
            {
                HookComponent.ThrowOnAdd = false;
                World<HookWorld>.Destroy();
            }
        }

        private static void CreateWorld<TWorld>(ChunkOwnerType owner) where TWorld : struct, IWorldType
        {
            World<TWorld>.Create(WorldConfig.Default());
            World<TWorld>.Types().EntityType<NetEntity>().Tag<ReplicatedTag>().Tag<StateTag>()
                .EntityType<OtherEntity>().Component<Value>().Component<HookComponent>()
                .Link<ParentLink>().Links<TargetLinks>().Multi<Item>();
            World<TWorld>.Initialize();
            World<TWorld>.RegisterCluster(Cluster);
            World<TWorld>.RegisterChunk(Chunk, owner, Cluster);
        }

        private static Schema Schema<TWorld>() where TWorld : struct, IWorldType => new SchemaBuilder<TWorld>()
            .EntityKind<NetEntity>(Id(1))
            .EntityKind<OtherEntity>(Id(7))
            .Component<Value, ValueCodec>(Id(2), 1, Codec(2), 4)
            .Tag<StateTag>(Id(3), 1)
            .Link<ParentLink>(Id(4), 1)
            .Links<TargetLinks>(Id(5), 1, 8)
            .Multi<Item, ItemCodec>(Id(6), 1, Codec(6), 8, 4)
            .Freeze();

        private static Schema HookSchema<TWorld>() where TWorld : struct, IWorldType => new SchemaBuilder<TWorld>()
            .EntityKind<NetEntity>(Id(1))
            .Component<HookComponent, HookCodec>(Id(8), 1, Codec(8), 4)
            .Freeze();

        private static void SeedRichReplica<TWorld>(Replicator<TWorld> replica, Schema schema)
            where TWorld : struct, IWorldType
        {
            using var staged = Stage(schema, RichSnapshot());
            Assert.That(replica.Apply(staged), Is.EqualTo(ApplyResult.Success));
            var gids = RichGids();
            Assert.That(gids[0].TryUnpack<TWorld>(out var first), Is.True);
            Assert.That(gids[1].TryUnpack<TWorld>(out var second), Is.True);
            Assert.That(gids[2].TryUnpack<TWorld>(out var source), Is.True);
            World<TWorld>.Components<World<TWorld>.Multi<Item>>.Instance.Disable(first);
            World<TWorld>.Components<World<TWorld>.Link<ParentLink>>.Instance.Disable(source);
            World<TWorld>.Components<World<TWorld>.Links<TargetLinks>>.Instance.Disable(source);

            Assert.That(first.Has<ReplicatedTag>(), Is.True);
            Assert.That(first.Has<StateTag>(), Is.True);
            Assert.That(first.Read<Value>().Number, Is.EqualTo(17));
            Assert.That(World<TWorld>.Components<Value>.Instance.HasDisabled(first), Is.True);
            var multi = first.Read<World<TWorld>.Multi<Item>>().AsReadOnlySpan;
            Assert.That(multi.Length, Is.EqualTo(2));
            Assert.That(multi[0].Number, Is.EqualTo(2));
            Assert.That(multi[1].Number, Is.EqualTo(1));
            Assert.That(World<TWorld>.Components<World<TWorld>.Multi<Item>>.Instance.HasDisabled(first), Is.True);
            Assert.That(second.Read<Value>().Number, Is.EqualTo(23));
            Assert.That(source.IsDisabled, Is.True);
            Assert.That(source.Read<World<TWorld>.Link<ParentLink>>().Value, Is.EqualTo(gids[1]));
            Assert.That(World<TWorld>.Components<World<TWorld>.Link<ParentLink>>.Instance.HasDisabled(source), Is.True);
            var links = source.Read<World<TWorld>.Links<TargetLinks>>().AsReadOnlySpan;
            Assert.That(links.Length, Is.EqualTo(2));
            Assert.That(links[0].Value, Is.EqualTo(gids[0]));
            Assert.That(links[1].Value, Is.EqualTo(gids[1]));
            Assert.That(World<TWorld>.Components<World<TWorld>.Links<TargetLinks>>.Instance.HasDisabled(source), Is.True);
        }

        private static void SeedRichAuthority<TWorld>() where TWorld : struct, IWorldType
        {
            var first = World<TWorld>.NewEntityInChunk<NetEntity>(Chunk);
            first.Set<ReplicatedTag>();
            first.Set(new Value { Number = 17 });
            World<TWorld>.Components<Value>.Instance.Disable(first);
            first.Set<StateTag>();
            ref var values = ref first.Add<World<TWorld>.Multi<Item>>();
            values.Add(new Item { Number = 2 });
            values.Add(new Item { Number = 1 });
            World<TWorld>.Components<World<TWorld>.Multi<Item>>.Instance.Disable(first);

            var second = World<TWorld>.NewEntityInChunk<NetEntity>(Chunk);
            second.Set<ReplicatedTag>();
            second.Set(new Value { Number = 23 });

            var source = World<TWorld>.NewEntityInChunk<NetEntity>(Chunk);
            source.Set<ReplicatedTag>();
            source.Set(new Value { Number = 42 });
            source.Set(new World<TWorld>.Link<ParentLink>(second));
            ref var links = ref source.Add<World<TWorld>.Links<TargetLinks>>();
            links.Add(first);
            links.Add(second);
            World<TWorld>.Components<World<TWorld>.Link<ParentLink>>.Instance.Disable(source);
            World<TWorld>.Components<World<TWorld>.Links<TargetLinks>>.Instance.Disable(source);
            source.Disable();
        }

        private static FullSnapshotPayload RichSnapshot()
        {
            var gids = RichGids();
            return Snapshot(
                new SnapshotEntity
                {
                    Entity = Wire(gids[0]),
                    KindId = Id(1),
                    Records = new[]
                    {
                        ComponentRecord(17, RecordFlags.Disabled),
                        TagRecord(),
                        MultiRecord(2, 1)
                    }
                },
                new SnapshotEntity
                {
                    Entity = Wire(gids[1]),
                    KindId = Id(1),
                    Records = new[] { ComponentRecord(23) }
                },
                new SnapshotEntity
                {
                    Entity = Wire(gids[2]),
                    KindId = Id(1),
                    Flags = EntityFlags.Disabled,
                    Records = new[]
                    {
                        ComponentRecord(42),
                        LinkRecord(gids[1]),
                        LinksRecord(gids[0], gids[1])
                    }
                });
        }

        private static EntityGID[] RichGids()
        {
            var id = (Chunk << Const.ENTITIES_IN_CHUNK_SHIFT) + 64;
            return new[]
            {
                new EntityGID(id, 1, Cluster),
                new EntityGID(id + 1, 1, Cluster),
                new EntityGID(id + 2, 1, Cluster)
            };
        }

        private static SnapshotRecord ComponentRecord(int value, RecordFlags flags = 0) => new()
        {
            TypeId = Id(2), Kind = RecordKind.Component, Flags = flags, Version = 1, ElementCount = 1,
            Payload = BitConverter.GetBytes(value)
        };

        private static SnapshotRecord TagRecord() => new()
        {
            TypeId = Id(3), Kind = RecordKind.Tag, Version = 1, ElementCount = 0, Payload = Array.Empty<byte>()
        };

        private static SnapshotRecord LinkRecord(EntityGID target) => new()
        {
            TypeId = Id(4), Kind = RecordKind.Link, Version = 1, ElementCount = 1, Payload = EntityBytes(target)
        };

        private static SnapshotRecord LinksRecord(params EntityGID[] targets)
        {
            var payload = new byte[targets.Length * 8];
            for (var i = 0; i < targets.Length; i++) EntityBytes(targets[i]).CopyTo(payload, i * 8);
            return new SnapshotRecord
            {
                TypeId = Id(5), Kind = RecordKind.Links, Version = 1, ElementCount = (uint)targets.Length, Payload = payload
            };
        }

        private static SnapshotRecord MultiRecord(params int[] values)
        {
            var payload = new byte[values.Length * 8];
            for (var i = 0; i < values.Length; i++)
            {
                BitConverter.TryWriteBytes(payload.AsSpan(i * 8, 4), 4);
                BitConverter.TryWriteBytes(payload.AsSpan(i * 8 + 4, 4), values[i]);
            }
            return new SnapshotRecord
            {
                TypeId = Id(6), Kind = RecordKind.Multi, Version = 1, ElementCount = (uint)values.Length, Payload = payload
            };
        }

        private static WireEntityId Wire(EntityGID entity) => new(entity.Id, entity.ClusterId, entity.Version);

        private static ChunkMapping[] Mapping() => new[] { new ChunkMapping { Chunk = Chunk, Cluster = Cluster, Role = 1 } };
        private static SnapshotEntity ValueSnapshot(EntityGID gid, int value) => new()
        {
            Entity = new WireEntityId(gid.Id, gid.ClusterId, gid.Version),
            KindId = Id(1),
            Records = new[] { new SnapshotRecord { TypeId = Id(2), Kind = RecordKind.Component, Version = 1, ElementCount = 1, Payload = BitConverter.GetBytes(value) } }
        };
        private static void SetValueEntity<TWorld>(World<TWorld>.Entity entity, int value) where TWorld : struct, IWorldType
        {
            entity.Set<ReplicatedTag>();
            entity.Set(new Value { Number = value });
        }
        private static FullSnapshotPayload Snapshot(WireEntityId entity, TypeId kind) => Snapshot(new SnapshotEntity { Entity = entity, KindId = kind });
        private static FullSnapshotPayload Snapshot(params SnapshotEntity[] entities) => new() { Entities = entities };
        private static StagedPayload Stage(Schema schema, FullSnapshotPayload snapshot)
        {
            var bytes = new byte[1024];
            Assert.That(PayloadCodec.TryWrite(snapshot, bytes, out var length), Is.True);
            var lease = PacketLease.Rent(length);
            try
            {
                lease.SetLength(length);
                bytes.AsSpan(0, length).CopyTo(lease.Span);
                Assert.That(PayloadStager.TryStage(PacketKind.FullSnapshot, ref lease, schema, out var staged), Is.True);
                return staged;
            }
            finally
            {
                if (lease.IsValid)
                {
                    lease.Dispose();
                    lease = default;
                }
            }
        }

        private static StagedPayload OversizedStage(Schema schema)
        {
            var lease = PacketLease.Rent(1);
            lease.SetLength(0);
            var staged = new StagedPayload(PacketKind.FullSnapshot, ref lease);
            try
            {
                var entities = ArrayPool<StagedEntity>.Shared.Rent(ProtocolLimits.MaxEntities + 1);
                staged.SetSnapshot(entities, ProtocolLimits.MaxEntities + 1, null, 0);
                staged.BindSchema(schema.Hash);
                return staged;
            }
            catch
            {
                staged.Dispose();
                throw;
            }
        }

        private static byte[] EntityBytes(EntityGID entity)
        {
            var bytes = new byte[8];
            BitConverter.TryWriteBytes(bytes.AsSpan(0, 4), entity.Id);
            BitConverter.TryWriteBytes(bytes.AsSpan(4, 2), entity.ClusterId);
            BitConverter.TryWriteBytes(bytes.AsSpan(6, 2), entity.Version);
            return bytes;
        }
        private static void AssertApplyFailure<TWorld>(Replicator<TWorld> replicator, StagedPayload staged, ApplyResult expected)
            where TWorld : struct, IWorldType
        {
            var before = Fingerprint<TWorld>();
            Assert.That(replicator.Apply(staged), Is.EqualTo(expected));
            Assert.That(Fingerprint<TWorld>(), Is.EqualTo(before));
        }
        private static string Fingerprint<TWorld>() where TWorld : struct, IWorldType
        {
            var values = new List<string>();
            foreach (var entity in World<TWorld>.Query().Entities(EntityStatusType.Any))
            {
                var fingerprint = new StringBuilder();
                fingerprint.Append(entity.GID.Raw).Append(':').Append(entity.EntityType)
                    .Append(":entityDisabled=").Append(entity.IsDisabled)
                    .Append(":replicated=").Append(entity.Has<ReplicatedTag>())
                    .Append(":tag=").Append(entity.Has<StateTag>());

                if (entity.Has<Value>())
                    fingerprint.Append(":value=").Append(entity.Read<Value>().Number)
                        .Append(":valueDisabled=").Append(World<TWorld>.Components<Value>.Instance.HasDisabled(entity));
                else fingerprint.Append(":value=absent");

                if (entity.Has<World<TWorld>.Link<ParentLink>>())
                    fingerprint.Append(":link=").Append(entity.Read<World<TWorld>.Link<ParentLink>>().Value.Raw)
                        .Append(":linkDisabled=").Append(World<TWorld>.Components<World<TWorld>.Link<ParentLink>>.Instance.HasDisabled(entity));
                else fingerprint.Append(":link=absent");

                if (entity.Has<World<TWorld>.Links<TargetLinks>>())
                {
                    var links = entity.Read<World<TWorld>.Links<TargetLinks>>().AsReadOnlySpan;
                    fingerprint.Append(":linksDisabled=").Append(World<TWorld>.Components<World<TWorld>.Links<TargetLinks>>.Instance.HasDisabled(entity))
                        .Append(":links=[");
                    for (var i = 0; i < links.Length; i++)
                    {
                        if (i > 0) fingerprint.Append(',');
                        fingerprint.Append(links[i].Value.Raw);
                    }
                    fingerprint.Append(']');
                }
                else fingerprint.Append(":links=absent");

                if (entity.Has<World<TWorld>.Multi<Item>>())
                {
                    var multi = entity.Read<World<TWorld>.Multi<Item>>().AsReadOnlySpan;
                    fingerprint.Append(":multiDisabled=").Append(World<TWorld>.Components<World<TWorld>.Multi<Item>>.Instance.HasDisabled(entity))
                        .Append(":multi=[");
                    for (var i = 0; i < multi.Length; i++)
                    {
                        if (i > 0) fingerprint.Append(',');
                        fingerprint.Append(multi[i].Number);
                    }
                    fingerprint.Append(']');
                }
                else fingerprint.Append(":multi=absent");

                values.Add(fingerprint.ToString());
            }
            values.Sort(StringComparer.Ordinal);
            return $"chunk={Chunk}:owner={World<TWorld>.GetChunkOwner(Chunk)}:cluster={World<TWorld>.GetChunkClusterId(Chunk)}|{string.Join("|", values)}";
        }
        private static TypeId Id(int value) => new(new Guid(value, 0, 0, new byte[8]));
        private static CodecId Codec(int value) => new(new Guid(value, 1, 0, new byte[8]));

        private struct AuthorityWorld : IWorldType { }
        private struct AlternateAuthorityWorld : IWorldType { }
        private struct AllocationWorld : IWorldType { }
        private struct CaptureFailureWorld : IWorldType { }
        private struct HookWorld : IWorldType { }
        private struct ReplicaWorld : IWorldType { }
        private struct NetEntity : IEntityType { public byte Id() => 11; }
        private struct OtherEntity : IEntityType { public byte Id() => 12; }
        private struct StateTag : ITag, IDisableable { }
        private struct ParentLink : ILinkType { }
        private struct TargetLinks : ILinksType { }
        private struct Value : IComponent, IDisableable { public int Number; }
        private struct HookComponent : IComponent
        {
            internal static bool ThrowOnAdd;
            public int Number;
            public void OnAdd<TWorld>(World<TWorld>.Entity self) where TWorld : struct, IWorldType
            {
                if (ThrowOnAdd) throw new InvalidOperationException("component OnAdd hook");
            }
        }
        private struct Item : IMultiComponent { public int Number; }
        private struct ValueCodec : ICodec<Value>
        {
            internal static int Reads;
            internal static int ThrowOnReadCall;
            internal static bool FailWrites;
            internal static bool ThrowOnWrite;
            public bool TryWrite(in Value value, Span<byte> destination, out int written)
            {
                if (ThrowOnWrite) throw new InvalidOperationException("codec write hook");
                if (FailWrites || destination.Length < 4) { written = 0; return false; }
                BitConverter.TryWriteBytes(destination, value.Number); written = 4; return true;
            }
            public bool TryRead(ReadOnlySpan<byte> source, out Value value, out int read) { if (++Reads == ThrowOnReadCall) throw new InvalidOperationException("codec hook"); if (source.Length != 4) { value = default; read = 0; return false; } value = new Value { Number = BitConverter.ToInt32(source) }; read = 4; return true; }
        }
        private struct HookCodec : ICodec<HookComponent>
        {
            public bool TryWrite(in HookComponent value, Span<byte> destination, out int written) { if (destination.Length < 4) { written = 0; return false; } BitConverter.TryWriteBytes(destination, value.Number); written = 4; return true; }
            public bool TryRead(ReadOnlySpan<byte> source, out HookComponent value, out int read) { if (source.Length != 4) { value = default; read = 0; return false; } value = new HookComponent { Number = BitConverter.ToInt32(source) }; read = 4; return true; }
        }
        private struct ItemCodec : ICodec<Item>
        {
            public bool TryWrite(in Item value, Span<byte> destination, out int written) { if (destination.Length < 4) { written = 0; return false; } BitConverter.TryWriteBytes(destination, value.Number); written = 4; return true; }
            public bool TryRead(ReadOnlySpan<byte> source, out Item value, out int read) { if (source.Length != 4) { value = default; read = 0; return false; } value = new Item { Number = BitConverter.ToInt32(source) }; read = 4; return true; }
        }
    }
}
