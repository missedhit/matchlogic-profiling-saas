using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Application.Interfaces.DataMatching;
using MatchLogic.Domain.Entities.Common;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Concurrent;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching;
public class MatchGroupingServiceWithSimilarRecordsInGroups : IMatchGroupingService
{
    private readonly int _maxDegreeOfParallelism;
    private int _nextGroupId;

    public MatchGroupingServiceWithSimilarRecordsInGroups(IOptions<RecordLinkageOptions> options)
    {
        _maxDegreeOfParallelism = options.Value.MaxDegreeOfParallelism;
    }

    /*public async IAsyncEnumerable<MatchGroup> CreateMatchGroupsAsync(
        IAsyncEnumerable<IDictionary<string, object>> matchResults,
        bool mergeOverlappingGroups = false,
        bool similarRecordsInGroup = false,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var recordToGroupMap = new ConcurrentDictionary<string, int>();
        var activeGroups = new ConcurrentDictionary<int, MatchGroup>();
        var matchMap = new ConcurrentDictionary<string, HashSet<string>>();

        await foreach (var result in matchResults.WithCancellation(cancellationToken))
        {
            var record1Hash = GetRecordHash(result[RecordComparisonService.Record1Field] as IDictionary<string, object>);
            var record2Hash = GetRecordHash(result[RecordComparisonService.Record2Field] as IDictionary<string, object>);

            var group1Id = recordToGroupMap.GetValueOrDefault(record1Hash);
            var group2Id = recordToGroupMap.GetValueOrDefault(record2Hash);

            UpdateMatchMap(matchMap, record1Hash, record2Hash);

            if (group1Id == 0 && group2Id == 0)
            {
                yield return CreateNewGroup(result, record1Hash, record2Hash, activeGroups, recordToGroupMap, similarRecordsInGroup);
                continue;
            }

            if (group1Id > 0 && group2Id > 0 && group1Id != group2Id && mergeOverlappingGroups)
            {
                if (activeGroups.TryGetValue(group1Id, out var group1) &&
                    activeGroups.TryGetValue(group2Id, out var group2))
                {
                    bool shouldMerge = !similarRecordsInGroup || VerifyFullMatch(group1.Records, group2.Records, matchMap);
                    if (shouldMerge)
                    {
                        yield return MergeGroups(group1Id, group2Id, group1, group2, activeGroups, recordToGroupMap);
                        continue;
                    }
                }
            }

            if (!AddToExistingGroup(result, group1Id, group2Id, record1Hash, record2Hash, activeGroups, recordToGroupMap, matchMap, similarRecordsInGroup))
            {
                yield return CreateNewGroup(result, record1Hash, record2Hash, activeGroups, recordToGroupMap, similarRecordsInGroup);
            }
        }
    }*/

    public async IAsyncEnumerable<MatchGroup> CreateMatchGroupsAsync(
        IAsyncEnumerable<IDictionary<string, object>> matchResults,
        bool mergeOverlappingGroups = false,
        bool similarRecordsInGroup = false,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var recordToGroupMap = new ConcurrentDictionary<string, int>();
        var activeGroups = new ConcurrentDictionary<int, MatchGroup>();
        var matchMap = new ConcurrentDictionary<string, HashSet<string>>();
        var tempResults = new List<IDictionary<string, object>>();

        // First, collect all matches
        await foreach (var result in matchResults.WithCancellation(cancellationToken))
        {
            var record1Hash = GetRecordHash(result[RecordComparisonService.Record1Field] as IDictionary<string, object>);
            var record2Hash = GetRecordHash(result[RecordComparisonService.Record2Field] as IDictionary<string, object>);
            UpdateMatchMap(matchMap, record1Hash, record2Hash);
            tempResults.Add(result);
        }

        // Now process all results with complete match information
        foreach (var result in tempResults)
        {
            var record1Hash = GetRecordHash(result[RecordComparisonService.Record1Field] as IDictionary<string, object>);
            var record2Hash = GetRecordHash(result[RecordComparisonService.Record2Field] as IDictionary<string, object>);

            var group1Id = recordToGroupMap.GetValueOrDefault(record1Hash);
            var group2Id = recordToGroupMap.GetValueOrDefault(record2Hash);

            if (group1Id == 0 && group2Id == 0)
            {
                CreateNewGroup(result, record1Hash, record2Hash, activeGroups, recordToGroupMap);
                continue;
            }

            if (group1Id > 0 && group2Id > 0 && mergeOverlappingGroups)
            {
                if (group1Id == group2Id)
                    continue;
                if (activeGroups.TryGetValue(group1Id, out var group1) &&
                    activeGroups.TryGetValue(group2Id, out var group2))
                {
                    bool shouldMerge = !similarRecordsInGroup || VerifyFullMatch(group1.Records, group2.Records, matchMap);
                    if (shouldMerge)
                    {
                        MergeGroups(group1Id, group2Id, group1, group2, activeGroups, recordToGroupMap);
                        continue;
                    }
                }
            }

            if (!AddToExistingGroup(result, group1Id, group2Id, record1Hash, record2Hash, activeGroups, recordToGroupMap, matchMap, similarRecordsInGroup))
            {
                CreateNewGroup(result, record1Hash, record2Hash, activeGroups, recordToGroupMap);
            }
        }

        // Finally, yield the fully processed groups
        foreach (var group in activeGroups.Values)
        {
            yield return group;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void UpdateMatchMap(
        ConcurrentDictionary<string, HashSet<string>> matchMap,
        string record1Hash,
        string record2Hash)
    {
        matchMap.AddOrUpdate(record1Hash,
            _ => new HashSet<string> { record2Hash },
            (_, set) => { set.Add(record2Hash); return set; });

        matchMap.AddOrUpdate(record2Hash,
            _ => new HashSet<string> { record1Hash },
            (_, set) => { set.Add(record1Hash); return set; });
    }

    private bool VerifyFullMatch(
        List<IDictionary<string, object>> records1,
        List<IDictionary<string, object>> records2,
        ConcurrentDictionary<string, HashSet<string>> matchMap)
    {
        foreach (var record1 in records1)
        {
            var record1Hash = GetRecordHash(record1);
            if (!matchMap.TryGetValue(record1Hash, out var matches))
                return false;

            foreach (var record2 in records2)
            {
                if (!matches.Contains(GetRecordHash(record2)))
                    return false;
            }
        }

        foreach (var record2 in records2)
        {
            var record2Hash = GetRecordHash(record2);
            if (!matchMap.TryGetValue(record2Hash, out var matches))
                return false;

            foreach (var record1 in records1)
            {
                if (!matches.Contains(GetRecordHash(record1)))
                    return false;
            }
        }

        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private MatchGroup CreateNewGroup(
        IDictionary<string, object> result,
        string record1Hash,
        string record2Hash,
        ConcurrentDictionary<int, MatchGroup> activeGroups,
        ConcurrentDictionary<string, int> recordToGroupMap,
        bool similarRecordsInGroup = false)
    {
        var groupId = Interlocked.Increment(ref _nextGroupId);
        var records = new List<IDictionary<string, object>>(2)
        {
            EnrichRecord(result[RecordComparisonService.Record1Field] as IDictionary<string, object>, result),
            EnrichRecord(result[RecordComparisonService.Record2Field] as IDictionary<string, object>, result)
        };

        var group = new MatchGroup { GroupId = groupId, Records = records };
        activeGroups.TryAdd(groupId, group);
        recordToGroupMap.TryAdd(record1Hash, groupId);
        recordToGroupMap.TryAdd(record2Hash, groupId);
        return group;
    }

    private MatchGroup MergeGroups(
        int group1Id,
        int group2Id,
        MatchGroup group1,
        MatchGroup group2,
        ConcurrentDictionary<int, MatchGroup> activeGroups,
        ConcurrentDictionary<string, int> recordToGroupMap)
    {
        var mergedGroupId = Interlocked.Increment(ref _nextGroupId);
        var mergedRecords = new List<IDictionary<string, object>>(group1.Records.Count + group2.Records.Count);

        lock (group1.Records)
            lock (group2.Records)
            {
                mergedRecords.AddRange(group1.Records);
                var existingHashes = new HashSet<string>(group1.Records.Select(GetRecordHash));

                foreach (var record in group2.Records)
                {
                    var hash = GetRecordHash(record);
                    if (!existingHashes.Contains(hash))
                    {
                        mergedRecords.Add(record);
                        recordToGroupMap[hash] = mergedGroupId;
                    }
                }
            }

        var mergedGroup = new MatchGroup { GroupId = mergedGroupId, Records = mergedRecords };
        activeGroups.TryAdd(mergedGroupId, mergedGroup);
        activeGroups.TryRemove(group1Id, out _);
        activeGroups.TryRemove(group2Id, out _);

        return mergedGroup;
    }

    private bool AddToExistingGroup(
        IDictionary<string, object> result,
        int group1Id,
        int group2Id,
        string record1Hash,
        string record2Hash,
        ConcurrentDictionary<int, MatchGroup> activeGroups,
        ConcurrentDictionary<string, int> recordToGroupMap,
        ConcurrentDictionary<string, HashSet<string>> matchMap,
        bool similarRecordsInGroup)
    {
        var existingGroupId = group1Id > 0 ? group1Id : group2Id;
        if (!activeGroups.TryGetValue(existingGroupId, out var group)) return false;

        var newRecordHash = group1Id > 0 ? record2Hash : record1Hash;

        bool canAddToGroup = !similarRecordsInGroup ||
           group.Records.All(r => matchMap.GetValueOrDefault(GetRecordHash(r))?.Contains(newRecordHash) == true);
        
        if(!canAddToGroup)
            return false;

        var newRecord = (group1Id > 0
            ? result[RecordComparisonService.Record2Field]
            : result[RecordComparisonService.Record1Field]) as IDictionary<string, object>;

        lock (group.Records)
        {
            if (group.Records.Any(r => GetRecordHash(r) == newRecordHash)) return true;
            group.Records.Add(EnrichRecord(newRecord, result));
            recordToGroupMap.TryAdd(newRecordHash, existingGroupId);
        }
        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private string GetRecordHash(IDictionary<string, object> record) =>
        ((record["_metadata"] as Dictionary<string, object>)?["RowNumber"]?.ToString()) ??
        throw new InvalidOperationException("Record hash not found in metadata");

    private IDictionary<string, object> EnrichRecord(
        IDictionary<string, object> record,
        IDictionary<string, object> matchResult)
    {
        var enrichedRecord = new Dictionary<string, object>(record);
        foreach (var key in matchResult.Keys.Where(k =>
            k.EndsWith("_Score") ||
            k.EndsWith("_Weight") ||
            k == RecordComparisonService.FinalScoreField ||
            k == RecordComparisonService.WeightedScoreField))
        {
            enrichedRecord[key] = matchResult[key];
        }
        return enrichedRecord;
    }
}
