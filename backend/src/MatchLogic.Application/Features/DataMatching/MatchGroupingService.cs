using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Application.Interfaces.DataMatching;
using MatchLogic.Domain.Entities.Common;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Channels;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching;
public class MatchGroupingService : IMatchGroupingService
{
    private readonly int _maxDegreeOfParallelism;
    private int _nextGroupId;
    private readonly Channel<MatchGroup> _groupChannel;

    public MatchGroupingService(IOptions<RecordLinkageOptions> options)
    {
        _maxDegreeOfParallelism = options.Value.MaxDegreeOfParallelism;
        _groupChannel = Channel.CreateBounded<MatchGroup>(new BoundedChannelOptions(_maxDegreeOfParallelism * 2)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false
        });
    }

    public async IAsyncEnumerable<MatchGroup> CreateMatchGroupsAsync(
        IAsyncEnumerable<IDictionary<string, object>> matchResults,
        bool mergeOverlappingGroups = false,
        bool similarRecordsInGroups = false,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var recordToGroupMap = new ConcurrentDictionary<string, int>();
        var activeGroups = new ConcurrentDictionary<int, MatchGroup>();

        await foreach (var result in matchResults.WithCancellation(cancellationToken))
        {
            var record1Hash = GetRecordHash(result[RecordComparisonService.Record1Field] as IDictionary<string, object>);
            var record2Hash = GetRecordHash(result[RecordComparisonService.Record2Field] as IDictionary<string, object>);

            await Console.Out.WriteLineAsync($"{record1Hash} | {record2Hash}");

            var group1Id = recordToGroupMap.GetValueOrDefault(record1Hash);
            var group2Id = recordToGroupMap.GetValueOrDefault(record2Hash);

            if (group1Id == 0 && group2Id == 0)
            {
                var groupId = Interlocked.Increment(ref _nextGroupId);
                var group = new MatchGroup
                {
                    GroupId = groupId,
                    Records = new List<IDictionary<string, object>>
                    {
                        EnrichRecord(result[RecordComparisonService.Record1Field] as IDictionary<string, object>, result),
                        EnrichRecord(result[RecordComparisonService.Record2Field] as IDictionary<string, object>, result)
                    }
                };

                activeGroups.TryAdd(groupId, group);
                recordToGroupMap.TryAdd(record1Hash, groupId);
                if(mergeOverlappingGroups)
                    recordToGroupMap.TryAdd(record2Hash, groupId);
                yield return group;
            }
            else if (group1Id > 0 && group2Id > 0 && group1Id != group2Id && mergeOverlappingGroups)
            {
                if (activeGroups.TryGetValue(group1Id, out var group1) &&
                    activeGroups.TryGetValue(group2Id, out var group2))
                {
                    var mergedGroupId = Interlocked.Increment(ref _nextGroupId);
                    var mergedRecords = new List<IDictionary<string, object>>();

                    lock (group1.Records)
                        lock (group2.Records)
                        {
                            mergedRecords.AddRange(group1.Records);
                            var distinctRecords = group2.Records.Where(r2 =>
                                                           !group1.Records.Any(r1 => GetRecordHash(r1) == GetRecordHash(r2)));
                            mergedRecords.AddRange(distinctRecords);
                        }

                    var mergedGroup = new MatchGroup { GroupId = mergedGroupId, Records = mergedRecords };
                    activeGroups.TryAdd(mergedGroupId, mergedGroup);

                    foreach (var record in mergedRecords)
                    {
                        var hash = GetRecordHash(record);
                        recordToGroupMap.TryUpdate(hash, mergedGroupId, group1Id);
                        recordToGroupMap.TryUpdate(hash, mergedGroupId, group2Id);
                    }

                    activeGroups.TryRemove(group1Id, out _);
                    activeGroups.TryRemove(group2Id, out _);
                    yield return mergedGroup;
                }
            }
            else
            {
                var existingGroupId = group1Id > 0 ? group1Id : group2Id;
                var newRecordHash = group1Id > 0 ? record2Hash : record1Hash;

                if (activeGroups.TryGetValue(existingGroupId, out var group))
                {
                    var newRecord = (group1Id > 0
                        ? result[RecordComparisonService.Record2Field]
                        : result[RecordComparisonService.Record1Field]) as IDictionary<string, object>;

                    lock (group.Records)
                    {
                        var recordAlreadyExists = group.Records.Any(r => GetRecordHash(r) == newRecordHash);
                        if(!recordAlreadyExists)
                            group.Records.Add(EnrichRecord(newRecord, result));
                    }
                    if(mergeOverlappingGroups)
                        recordToGroupMap.TryAdd(newRecordHash, existingGroupId);
                }
            }
        }
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
