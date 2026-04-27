using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.RecordLinkage
{
    public class QGramGenerator : IDisposable
    {
        private readonly int _q;
        private readonly ArrayPool<char> _charPool;
        private readonly ArrayPool<uint> _hashPool;
        private bool _disposed;

        public QGramGenerator(int q = 3)
        {
            if (q < 1) throw new ArgumentOutOfRangeException(nameof(q));
            _q = q;
            _charPool = ArrayPool<char>.Shared;
            _hashPool = ArrayPool<uint>.Shared;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void GenerateHashes(ReadOnlySpan<char> input, uint[] hashBuffer, out int hashCount)
        {
            hashCount = 0;

            if (input.Length == 0)
            {
                hashBuffer[hashCount++] = HashQGram(ReadOnlySpan<char>.Empty);
                return;
            }

            if (input.Length < _q)
            {
                hashBuffer[hashCount++] = HashQGram(input);
                return;
            }

            for (int i = 0; i <= input.Length - _q; i++)
            {
                hashBuffer[hashCount++] = HashQGram(input.Slice(i, _q));
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static uint HashQGram(ReadOnlySpan<char> qgram)
        {
            uint hash = 0;
            foreach (var c in qgram)
            {
                hash = hash * 31 + c;
            }
            return hash;
        }

        public uint[] RentHashBuffer(int capacity) => _hashPool.Rent(capacity);
        public void ReturnHashBuffer(uint[] buffer) => _hashPool.Return(buffer);

        public void Dispose()
        {
            if (!_disposed)
            {
                _disposed = true;
            }
        }
    }
}
