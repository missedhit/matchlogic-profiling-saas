using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Common;
public class ConcurrentHashSet<T> : IDisposable
{
    private readonly ConcurrentDictionary<T, byte> _dictionary;

    public ConcurrentHashSet()
    {
        _dictionary = new ConcurrentDictionary<T, byte>();
    }

    public bool Add(T item) => _dictionary.TryAdd(item, 0);

    public bool Contains(T item) => _dictionary.ContainsKey(item);

    public bool Remove(T item) => _dictionary.TryRemove(item, out _);

    public void Clear() => _dictionary.Clear();

    public int Count => _dictionary.Count;

    public IEnumerable<T> Items => _dictionary.Keys;

    public void Dispose()
    {
        _dictionary.Clear();
    }
}
