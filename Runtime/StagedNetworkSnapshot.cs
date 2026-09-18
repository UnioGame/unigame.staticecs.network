namespace UniGame.StaticEcs.Network
{
    using System;
    using System.Buffers;
    using System.Collections.Generic;
    using FFS.Libraries.StaticEcs;
    using FFS.Libraries.StaticPack;

    /// <summary>Contains validated pooled metadata; applying it is the first ECS mutation.</summary>
    public struct StagedNetworkSnapshot : IDisposable
    {
        internal object Owner;
        internal NetworkSnapshot Snapshot;
        internal StagedEntity[] Entities;
        internal StagedRecord[] Records;
        internal int EntityCount;
        internal int RecordCount;

        /// <summary>Gets authoritative simulation time.</summary>
        public uint ServerTick => Snapshot?.ServerTick ?? 0;

        internal SchemaFingerprint Fingerprint => Snapshot?.SchemaFingerprint ?? default;
        internal ScopeId Scope => Snapshot?.Scope ?? default;

        /// <inheritdoc />
        public void Dispose()
        {
            if (Entities != null)
            {
                Array.Clear(Entities, 0, EntityCount);
                ArrayPool<StagedEntity>.Shared.Return(Entities);
            }
            if (Records != null)
            {
                Array.Clear(Records, 0, RecordCount);
                ArrayPool<StagedRecord>.Shared.Return(Records);
            }
            Owner = null;
            Snapshot = null;
            Entities = null;
            Records = null;
            EntityCount = 0;
            RecordCount = 0;
        }
    }

    internal struct StagedEntity
    {
        internal EntityGID Gid;
        internal NetworkSchemaEntry Kind;
        internal bool Disabled;
        internal int RecordStart;
        internal int RecordCount;
        // Byte range (relative to the owning NetworkSnapshot's bytes, same base as
        // StagedRecord.Offset) spanning every one of this entity's record headers and
        // payloads contiguously. Apply() uses it to detect an entity whose replicated
        // state is byte-identical to what it last applied, without decoding records
        // that did not change; see NetworkReplicaEntry.AppliedBytes.
        internal int ByteOffset;
        internal int ByteLength;
    }

    internal struct StagedRecord
    {
        internal NetworkSchemaEntry Entry;
        internal int Offset;
        internal int Length;
        internal bool Disabled;
    }

    internal readonly struct NetworkReplicaEntry
    {
        internal readonly EntityGID LocalGid;
        internal readonly NetworkTypeId KindId;
        internal readonly bool Disabled;
        // Retained view (via NetworkSnapshot.RetainBytes) over this entity's exact
        // record bytes as of the last successful apply. Independently ref-counted
        // from the owning NetworkSnapshot/History, so it stays valid after the
        // snapshot that produced it is evicted. Owned by the replicas dictionary
        // entry that holds it; every path that removes or replaces an entry must
        // dispose it exactly once (see NetworkReplicator.Apply/ClearReplicas).
        internal readonly NetworkBufferLease AppliedBytes;

        internal NetworkReplicaEntry(EntityGID localGid, NetworkTypeId kindId,
            bool disabled, NetworkBufferLease appliedBytes)
        {
            LocalGid = localGid;
            KindId = kindId;
            Disabled = disabled;
            AppliedBytes = appliedBytes;
        }
    }
}
