using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.RecordLinkage;

public class InMemoryRecordStore : IRecordStore
{
    private readonly List<IDictionary<string, object>> _records;
    private readonly object _lock = new();
    private bool _isReadOnly;
    private bool _disposed;

    public InMemoryRecordStore(int initialCapacity = 100000)
    {
        _records = new List<IDictionary<string, object>>(initialCapacity);
    }

    public Task AddRecordAsync(IDictionary<string, object> record)
    {
        if (_isReadOnly)
            throw new InvalidOperationException("Store is in read-only mode");

        lock (_lock)
        {
            _records.Add(new Dictionary<string, object>(record)); // Defensive copy
        }

        return Task.CompletedTask;
    }

    public Task<IDictionary<string, object>> GetRecordAsync(int rowNumber)
    {
        lock (_lock)
        {
            if (rowNumber >= 0 && rowNumber < _records.Count)
                return Task.FromResult(_records[rowNumber]);
            return Task.FromResult<IDictionary<string, object>>(null);
        }
    }

    public Task<IList<IDictionary<string, object>>> GetRecordsAsync(IEnumerable<int> rowNumbers)
    {
        var results = new List<IDictionary<string, object>>();

        lock (_lock)
        {
            foreach (var rowNumber in rowNumbers)
            {
                if (rowNumber >= 0 && rowNumber < _records.Count)
                    results.Add(_records[rowNumber]);
            }
        }

        return Task.FromResult<IList<IDictionary<string, object>>>(results);
    }

    public Task SwitchToReadOnlyModeAsync()
    {
        lock (_lock)
        {
            _isReadOnly = true;
            _records.TrimExcess(); // Reduce memory overhead
        }
        return Task.CompletedTask;
    }

    public StorageStatistics GetStatistics()
    {
        lock (_lock)
        {
            var avgSize = _records.Count > 0
                ? _records.Take(Math.Min(100, _records.Count))
                         .Average(r => EstimateRecordSize(r))
                : 0;

            return new StorageStatistics
            {
                RecordCount = _records.Count,
                TotalSizeBytes = (long)(_records.Count * avgSize),
                IsReadOnly = _isReadOnly,
                StorageType = "InMemory"
            };
        }
    }

    private static double EstimateRecordSize(IDictionary<string, object> record)
    {
        return record.Sum(kvp =>
            (kvp.Key?.Length ?? 0) * 2 + // String key (UTF-16)
            EstimateValueSize(kvp.Value) +
            32); // Object overhead
    }

    private static double EstimateValueSize(object value) => value switch
    {
        string s => s.Length * 2,
        int => 4,
        long => 8,
        double => 8,
        bool => 1,
        DateTime => 8,
        Guid => 16,
        null => 0,
        _ => 32 // Default object overhead
    };

    public void Dispose()
    {
        if (!_disposed)
        {
            lock (_lock)
            {
                _records.Clear();
            }
            _disposed = true;
        }
    }
}
