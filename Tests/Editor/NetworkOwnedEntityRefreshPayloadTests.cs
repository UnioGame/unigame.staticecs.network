using System;
using System.Collections.Generic;
using NUnit.Framework;

namespace UniGame.StaticEcs.Network.Tests
{
    /// <summary>
    /// Covers the record patch of an <c>OwnedEntityRefresh</c> packet on hand-built entity blocks:
    /// it carries only the records that differ and rebuilds the current block exactly.
    /// </summary>
    [TestFixture]
    public sealed class NetworkOwnedEntityRefreshPayloadTests
    {
        private const ulong Gid = 0x0102030405060708UL;
        private const uint Kind = 77;

        [Test]
        public void PatchCarriesOnlyTheChangedRecordsAndRebuildsTheCurrentBlock()
        {
            var held = Block(Gid, Kind, false, (10, Bytes(1, 2, 3, 4)), (20, Bytes(5, 6)), (30, Bytes(7)));
            var fresh = Block(Gid, Kind, false, (10, Bytes(1, 2, 3, 4)), (20, Bytes(5, 9)), (30, Bytes(7)));
            var patch = new byte[OwnedEntityRefreshPayload.MaximumPatchBytes(fresh)];

            Assert.That(OwnedEntityRefreshPayload.TryWritePatch(held, fresh, patch, out var patchLength),
                Is.True);
            // Entity id, flag and count (11), then one record index (2) with its record (11 + 2).
            Assert.That(patchLength, Is.EqualTo(11 + 2 + 11 + 2));
            Assert.That(OwnedEntityRefreshPayload.PatchEntity(patch.AsSpan(0, patchLength)),
                Is.EqualTo(Gid));

            var rebuilt = new byte[held.Length + patchLength];
            Assert.That(OwnedEntityRefreshPayload.TryApplyPatch(held, patch.AsSpan(0, patchLength),
                rebuilt, out var written, out var records), Is.True);
            Assert.That(rebuilt.AsSpan(0, written).ToArray(), Is.EqualTo(fresh));
            Assert.That(records, Is.EqualTo(3));
        }

        [Test]
        public void PatchFollowsARecordThatChangesItsLengthAndTheDisabledFlag()
        {
            var held = Block(Gid, Kind, false, (10, Bytes(1)), (20, Bytes(5, 6)));
            var fresh = Block(Gid, Kind, true, (10, Bytes(1, 2, 3)), (20, Bytes()));
            var patch = new byte[OwnedEntityRefreshPayload.MaximumPatchBytes(fresh)];
            Assert.That(OwnedEntityRefreshPayload.TryWritePatch(held, fresh, patch, out var patchLength),
                Is.True);

            var rebuilt = new byte[held.Length + patchLength];
            Assert.That(OwnedEntityRefreshPayload.TryApplyPatch(held, patch.AsSpan(0, patchLength),
                rebuilt, out var written, out _), Is.True);
            Assert.That(rebuilt.AsSpan(0, written).ToArray(), Is.EqualTo(fresh));
        }

        [Test]
        public void EqualBlocksGiveAnEmptyPatch()
        {
            var block = Block(Gid, Kind, false, (10, Bytes(1, 2)));
            var patch = new byte[OwnedEntityRefreshPayload.MaximumPatchBytes(block)];

            Assert.That(OwnedEntityRefreshPayload.TryWritePatch(block, block, patch, out var patchLength),
                Is.True);
            Assert.That(patchLength, Is.EqualTo(11));
        }

        [Test]
        public void BlocksWithADifferentRecordSequenceHaveNoPatch()
        {
            var held = Block(Gid, Kind, false, (10, Bytes(1)), (20, Bytes(2)));
            var patch = new byte[256];

            Assert.That(OwnedEntityRefreshPayload.TryWritePatch(held,
                Block(Gid, Kind, false, (10, Bytes(1))), patch, out _), Is.False, "record removed");
            Assert.That(OwnedEntityRefreshPayload.TryWritePatch(held,
                Block(Gid, Kind, false, (10, Bytes(1)), (25, Bytes(2))), patch, out _), Is.False,
                "record type replaced");
            Assert.That(OwnedEntityRefreshPayload.TryWritePatch(held,
                Block(Gid, Kind + 1, false, (10, Bytes(1)), (20, Bytes(2))), patch, out _), Is.False,
                "entity kind changed");
            Assert.That(OwnedEntityRefreshPayload.TryWritePatch(held,
                Block(Gid + 1, Kind, false, (10, Bytes(1)), (20, Bytes(2))), patch, out _), Is.False,
                "another entity");
            Assert.That(OwnedEntityRefreshPayload.TryWritePatch(held,
                Block(Gid, Kind, false, (10, Bytes(9)), (20, Bytes(2))), new byte[12], out _), Is.False,
                "target too small");
        }

        [Test]
        public void MalformedPatchIsRejected()
        {
            var held = Block(Gid, Kind, false, (10, Bytes(1, 2)), (20, Bytes(3)));
            var fresh = Block(Gid, Kind, false, (10, Bytes(1, 2)), (20, Bytes(4)));
            var buffer = new byte[OwnedEntityRefreshPayload.MaximumPatchBytes(fresh)];
            Assert.That(OwnedEntityRefreshPayload.TryWritePatch(held, fresh, buffer, out var length),
                Is.True);
            var patch = buffer.AsSpan(0, length).ToArray();
            var rebuilt = new byte[held.Length + patch.Length];

            Assert.That(Apply(held, patch.AsSpan(0, patch.Length - 1).ToArray(), rebuilt), Is.False,
                "truncated record");
            Assert.That(Apply(held, Concat(patch, 0), rebuilt), Is.False, "trailing byte");
            Assert.That(Apply(Block(Gid + 1, Kind, false, (10, Bytes(1, 2)), (20, Bytes(3))), patch,
                rebuilt), Is.False, "block of another entity");
            Assert.That(Apply(Block(Gid, Kind, false, (10, Bytes(1, 2)), (21, Bytes(3))), patch,
                rebuilt), Is.False, "record type differs from the held block");

            var beyond = (byte[])patch.Clone();
            beyond[11] = 2;
            Assert.That(Apply(held, beyond, rebuilt), Is.False, "record index past the block");
            var overCount = (byte[])patch.Clone();
            overCount[9] = 3;
            Assert.That(Apply(held, overCount, rebuilt), Is.False, "more changes than records");
            Assert.That(Apply(held, patch, new byte[held.Length - 1]), Is.False, "target too small");
            Assert.That(Apply(held, patch, rebuilt), Is.True, "the untouched patch still applies");
        }

        private static bool Apply(byte[] held, byte[] patch, byte[] target) =>
            OwnedEntityRefreshPayload.TryApplyPatch(held, patch, target, out _, out _);

        private static byte[] Bytes(params byte[] values) => values;

        private static byte[] Concat(byte[] bytes, byte tail)
        {
            var result = new byte[bytes.Length + 1];
            bytes.CopyTo(result, 0);
            result[bytes.Length] = tail;
            return result;
        }

        // The canonical entity block: id, kind, disabled flag, record count, then each record as
        // type id, schema kind, version, disabled flag, payload length, payload.
        private static byte[] Block(ulong gid, uint kind, bool disabled,
            params (uint Type, byte[] Payload)[] records)
        {
            var bytes = new List<byte>();
            bytes.AddRange(BitConverter.GetBytes(gid));
            bytes.AddRange(BitConverter.GetBytes(kind));
            bytes.Add(disabled ? (byte)1 : (byte)0);
            bytes.AddRange(BitConverter.GetBytes((ushort)records.Length));
            foreach (var record in records)
            {
                bytes.AddRange(BitConverter.GetBytes(record.Type));
                bytes.Add(1);
                bytes.Add(1);
                bytes.Add(0);
                bytes.AddRange(BitConverter.GetBytes((uint)record.Payload.Length));
                bytes.AddRange(record.Payload);
            }
            return bytes.ToArray();
        }
    }
}
