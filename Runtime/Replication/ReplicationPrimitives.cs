using System;
using FFS.Libraries.StaticEcs;

namespace UniGame.StaticEcs.Network
{
    internal enum CaptureRecordResult : byte
    {
        Absent,
        Written,
        EntityConflict,
        DisabledUnsupported,
        MissingTarget,
        LimitExceeded,
        CodecFailed
    }

    internal ref struct CaptureContext
    {
        internal ReadOnlySpan<EntityGID> Entities;
        internal Span<EntityGID> LinkScratch;

        internal bool Contains(in EntityGID entity) => ReplicationSort.Contains(Entities, in entity);
    }

    internal readonly ref struct ApplyContext
    {
        internal readonly ReadOnlySpan<EntityGID> Entities;

        internal ApplyContext(ReadOnlySpan<EntityGID> entities) => Entities = entities;

        internal bool Contains(in EntityGID entity) => ReplicationSort.Contains(Entities, in entity);
    }

    internal ref struct SnapshotWriter
    {
        private Span<byte> _destination;
        internal int Position { get; private set; }
        internal int Remaining => Valid ? _destination.Length - Position : 0;
        internal bool Valid { get; private set; }

        internal SnapshotWriter(Span<byte> destination) { _destination = destination; Position = 0; Valid = true; }

        internal void U8(byte value) { if (!Take(1, out var offset)) return; _destination[offset] = value; }
        internal void U16(ushort value) { if (!Take(2, out var offset)) return; Hashing.Write16(_destination, offset, value); }
        internal void U32(uint value) { if (!Take(4, out var offset)) return; Hashing.Write32(_destination, offset, value); }
        internal void Id(TypeId value) { if (!Take(16, out var offset)) return; value.WriteBytes(_destination.Slice(offset, 16)); }
        internal void Entity(in EntityGID value) { U32(value.Id); U16(value.ClusterId); U16(value.Version); }
        internal int ReserveU16() { var offset = Position; U16(0); return offset; }
        internal int ReserveU32() { var offset = Position; U32(0); return offset; }
        internal void PatchU16(int offset, ushort value) { if ((uint)offset > (uint)(Position - 2)) { Valid = false; return; } Hashing.Write16(_destination, offset, value); }
        internal void PatchU32(int offset, uint value) { if ((uint)offset > (uint)(Position - 4)) { Valid = false; return; } Hashing.Write32(_destination, offset, value); }

        internal Span<byte> Writable(int maximum)
        {
            if (!Valid || maximum < 0) return Span<byte>.Empty;
            return _destination.Slice(Position, Math.Min(maximum, _destination.Length - Position));
        }

        internal bool Advance(int count)
        {
            if (!Take(count, out _)) return false;
            return true;
        }

        internal void BeginRecord(SchemaEntry entry, uint count, RecordFlags flags, out int lengthOffset, out int payloadOffset)
        {
            Id(entry.TypeId);
            U8((byte)entry.Kind);
            U8((byte)flags);
            U16(entry.Version);
            U32(count);
            lengthOffset = ReserveU32();
            payloadOffset = Position;
        }

        internal void EndRecord(int lengthOffset, int payloadOffset) => PatchU32(lengthOffset, (uint)(Position - payloadOffset));

        private bool Take(int count, out int offset)
        {
            offset = Position;
            if (!Valid || count < 0 || count > _destination.Length - Position) { Valid = false; return false; }
            Position += count;
            return true;
        }
    }

    internal static class ReplicationSort
    {
        internal static int Compare(in EntityGID left, in EntityGID right)
        {
            var cluster = left.ClusterId.CompareTo(right.ClusterId);
            if (cluster != 0) return cluster;
            var id = left.Id.CompareTo(right.Id);
            return id != 0 ? id : left.Version.CompareTo(right.Version);
        }

        internal static bool Contains(ReadOnlySpan<EntityGID> values, in EntityGID value)
        {
            var low = 0;
            var high = values.Length - 1;
            while (low <= high)
            {
                var middle = low + ((high - low) >> 1);
                var comparison = Compare(in values[middle], in value);
                if (comparison == 0) return true;
                if (comparison < 0) low = middle + 1; else high = middle - 1;
            }
            return false;
        }

        internal static void Sort(Span<EntityGID> values)
        {
            if (values.Length < 2) return;
            QuickSort(values, 0, values.Length - 1);
        }

        internal static void Sort(Span<uint> values)
        {
            if (values.Length < 2) return;
            QuickSort(values, 0, values.Length - 1);
        }

        private static void QuickSort(Span<EntityGID> values, int left, int right)
        {
            while (left < right)
            {
                var i = left;
                var j = right;
                var pivot = values[left + ((right - left) >> 1)];
                while (i <= j)
                {
                    while (Compare(in values[i], in pivot) < 0) i++;
                    while (Compare(in values[j], in pivot) > 0) j--;
                    if (i <= j) { var value = values[i]; values[i++] = values[j]; values[j--] = value; }
                }
                if (j - left < right - i) { if (left < j) QuickSort(values, left, j); left = i; }
                else { if (i < right) QuickSort(values, i, right); right = j; }
            }
        }

        private static void QuickSort(Span<uint> values, int left, int right)
        {
            while (left < right)
            {
                var i = left;
                var j = right;
                var pivot = values[left + ((right - left) >> 1)];
                while (i <= j)
                {
                    while (values[i] < pivot) i++;
                    while (values[j] > pivot) j--;
                    if (i <= j) { var value = values[i]; values[i++] = values[j]; values[j--] = value; }
                }
                if (j - left < right - i) { if (left < j) QuickSort(values, left, j); left = i; }
                else { if (i < right) QuickSort(values, i, right); right = j; }
            }
        }
    }
}
