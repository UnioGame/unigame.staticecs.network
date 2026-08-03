using System;

namespace UniGame.StaticEcs.Network
{
    internal static class Hashing
    {
        private const ulong Prime1 = 11400714785074694791UL;
        private const ulong Prime2 = 14029467366897019727UL;
        private const ulong Prime3 = 1609587929392839161UL;
        private const ulong Prime4 = 9650029242287828579UL;
        private const ulong Prime5 = 2870177450012600261UL;
        private static readonly uint[] CrcTable = CreateCrcTable();

        internal static uint Crc32(ReadOnlySpan<byte> data)
        {
            var crc = uint.MaxValue;
            for (var i = 0; i < data.Length; i++) crc = (crc >> 8) ^ CrcTable[(crc ^ data[i]) & 0xff];
            return ~crc;
        }

        internal static ulong XxHash64(ReadOnlySpan<byte> data)
        {
            var index = 0;
            ulong hash;
            if (data.Length >= 32)
            {
                var v1 = unchecked(Prime1 + Prime2); var v2 = Prime2; var v3 = 0UL; var v4 = unchecked(0UL - Prime1);
                var limit = data.Length - 32;
                do
                {
                    v1 = Round(v1, Read64(data, index)); index += 8;
                    v2 = Round(v2, Read64(data, index)); index += 8;
                    v3 = Round(v3, Read64(data, index)); index += 8;
                    v4 = Round(v4, Read64(data, index)); index += 8;
                } while (index <= limit);
                hash = Rotate(v1, 1) + Rotate(v2, 7) + Rotate(v3, 12) + Rotate(v4, 18);
                hash = Merge(hash, v1); hash = Merge(hash, v2); hash = Merge(hash, v3); hash = Merge(hash, v4);
            }
            else hash = Prime5;
            hash += (ulong)data.Length;
            while (index <= data.Length - 8) { var value = Round(0, Read64(data, index)); hash ^= value; hash = Rotate(hash, 27) * Prime1 + Prime4; index += 8; }
            if (index <= data.Length - 4) { hash ^= Read32(data, index) * Prime1; hash = Rotate(hash, 23) * Prime2 + Prime3; index += 4; }
            while (index < data.Length) { hash ^= data[index] * Prime5; hash = Rotate(hash, 11) * Prime1; index++; }
            hash ^= hash >> 33; hash *= Prime2; hash ^= hash >> 29; hash *= Prime3; hash ^= hash >> 32;
            return hash;
        }

        internal static uint Read32(ReadOnlySpan<byte> data, int offset) =>
            (uint)(data[offset] | data[offset + 1] << 8 | data[offset + 2] << 16 | data[offset + 3] << 24);
        internal static ulong Read64(ReadOnlySpan<byte> data, int offset) =>
            Read32(data, offset) | (ulong)Read32(data, offset + 4) << 32;
        internal static void Write16(Span<byte> data, int offset, ushort value) { data[offset] = (byte)value; data[offset + 1] = (byte)(value >> 8); }
        internal static void Write32(Span<byte> data, int offset, uint value) { data[offset] = (byte)value; data[offset + 1] = (byte)(value >> 8); data[offset + 2] = (byte)(value >> 16); data[offset + 3] = (byte)(value >> 24); }
        internal static void Write64(Span<byte> data, int offset, ulong value) { Write32(data, offset, (uint)value); Write32(data, offset + 4, (uint)(value >> 32)); }
        private static ulong Round(ulong value, ulong input) { value += input * Prime2; value = Rotate(value, 31); return value * Prime1; }
        private static ulong Merge(ulong hash, ulong value) { hash ^= Round(0, value); return hash * Prime1 + Prime4; }
        private static ulong Rotate(ulong value, int count) => value << count | value >> (64 - count);
        private static uint[] CreateCrcTable()
        {
            var table = new uint[256];
            for (uint i = 0; i < table.Length; i++) { var value = i; for (var bit = 0; bit < 8; bit++) value = (value & 1) != 0 ? 0xedb88320U ^ value >> 1 : value >> 1; table[i] = value; }
            return table;
        }
    }
}
