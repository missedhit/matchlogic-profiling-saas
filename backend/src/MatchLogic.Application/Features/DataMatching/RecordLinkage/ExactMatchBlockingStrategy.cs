using MatchLogic.Application.Interfaces.DataMatching;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.RecordLinkage;
public class ExactMatchBlockingStrategy : IBlockingStrategy
{
    private readonly ILogger<ExactMatchBlockingStrategy> _logger;
    private bool _disposed;
    private const string MetadataField = "_metadata";
    private const string BlockingKeyField = "BlockingKey";

    public ExactMatchBlockingStrategy(ILogger<ExactMatchBlockingStrategy> logger)
    {
        _logger = logger;
    }

    public async Task<IAsyncEnumerable<IGrouping<string, IDictionary<string, object>>>> BlockRecordsAsync(
        IAsyncEnumerable<IDictionary<string, object>> records,
        IEnumerable<string> blockingFields,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var blockingGroups = new ConcurrentDictionary<string, List<IDictionary<string, object>>>();

        await foreach (var record in records.WithCancellation(cancellationToken))
        {
            var blockingKey = GenerateBlockingKey(record, blockingFields);
            if (record.ContainsKey(MetadataField) && record[MetadataField] != null && (record[MetadataField] is Dictionary<string, object>))
            {
                (record[MetadataField] as Dictionary<string, object>)[BlockingKeyField] = blockingKey;
            }
            blockingGroups.AddOrUpdate(
                blockingKey,
                _ => new List<IDictionary<string, object>> { record },
                (_, list) =>
                {
                    lock (list)
                    {
                        list.Add(record);
                        return list;
                    }
                });
        }

        return blockingGroups
            .ToAsyncEnumerable()
            .Select(group => new BlockingGroup(group.Key, group.Value) as IGrouping<string, IDictionary<string, object>>);
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;
        await Task.CompletedTask;
    }

    private string GenerateBlockingKey(IDictionary<string, object> record, IEnumerable<string> blockingFields)
    {
        return string.Join("|", blockingFields.Select(field =>
            record.TryGetValue(field, out var value) ? value?.ToString() ?? "" : ""));
    }

    private class BlockingGroup : IGrouping<string, IDictionary<string, object>>
    {
        public string Key { get; }
        private readonly IEnumerable<IDictionary<string, object>> _records;

        public BlockingGroup(string key, IEnumerable<IDictionary<string, object>> records)
        {
            Key = key;
            _records = records;
        }

        public IEnumerator<IDictionary<string, object>> GetEnumerator() => _records.GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
