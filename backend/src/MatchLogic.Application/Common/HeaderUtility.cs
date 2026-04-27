using MatchLogic.Application.Extensions;
using MatchLogic.Application.Interfaces.Common;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Common;

public class HeaderUtility : IHeaderUtility
{
    private readonly IDataStore _dataStore;

    public HeaderUtility(IDataStore dataStore)
    {
        _dataStore = dataStore;
    }

    public async Task<List<string>> GetHeadersAsync(DataSource dataSource, bool fetchCleanse)
    {
        // If cleanse not required → go directly to import
        var importCollectionName = DatasetNames.SnapshotRows(dataSource.ActiveSnapshotId.GetValueOrDefault());//$"{StepType.Import.ToString().ToLower()}_{GuidCollectionNameConverter.ToValidCollectionName(id)}";
        var cleanseCollectionName = $"{StepType.Cleanse.ToString().ToLower()}_{GuidCollectionNameConverter.ToValidCollectionName(dataSource.Id)}";
        if (!fetchCleanse)
            return await GetHeadersFromCollectionAsync(importCollectionName);

        // Try cleanse first
        var cleanseHeaders = await GetHeadersFromCollectionAsync(cleanseCollectionName);
        if (cleanseHeaders.Count > 0)
            return cleanseHeaders;

        // Fallback → import
        return await GetHeadersFromCollectionAsync(importCollectionName);
    }

    public async Task<List<string>> GetHeadersFromCollectionAsync(string collectionName)
    {
        var (data, _) = await _dataStore.GetPagedDataAsync(collectionName, 1, 1);
        var firstRow = data.FirstOrDefault();

        if (firstRow == null)
            return new List<string>();

        return firstRow.Keys
            .Where(k => k is not "_id" and not "_metadata")
            .ToList();
    }
}
