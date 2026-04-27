using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Application.Features.DataMatching;
using MatchLogic.Domain.Entities.Common;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.UnitTests;
public class MatchGroupingServiceTests
{
    private readonly MatchGroupingService _service;
    private readonly MatchGroupingServiceWithSimilarRecordsInGroups _serviceWithSimilarRecordsInGroups;
    private readonly RecordLinkageOptions _options;

    public MatchGroupingServiceTests()
    {
        _options = new RecordLinkageOptions { MaxDegreeOfParallelism = 4 };
        _service = new MatchGroupingService(Options.Create(_options));
        _serviceWithSimilarRecordsInGroups = new MatchGroupingServiceWithSimilarRecordsInGroups(Options.Create(_options));
    }

    private static IDictionary<string, object> CreateRecord(string id, string hash)
    {
        return new Dictionary<string, object>
        {
            ["Id"] = id,
            ["_metadata"] = new Dictionary<string, object> { ["RowNumber"] = id, ["Hash"] = hash }
        };
    }

    private static IDictionary<string, object> CreateMatchResult(
        string id1,
        string hash1,
        string id2,
        string hash2,
        double score = 0.9)
    {
        return new Dictionary<string, object>
        {
            [RecordComparisonService.Record1Field] = CreateRecord(id1, hash1),
            [RecordComparisonService.Record2Field] = CreateRecord(id2, hash2),
            [RecordComparisonService.FinalScoreField] = score,
            ["field1_Score"] = 0.8,
            ["field1_Weight"] = 0.5
        };
    }

    [Fact]
    public async Task CreateMatchGroups_SinglePair_CreatesOneGroup()
    {
        // Arrange
        var matchResults = new[]
        {
            CreateMatchResult("1", "hash1", "2", "hash2")
        }.ToAsyncEnumerable();

        // Act
        var groups = await _service.CreateMatchGroupsAsync(matchResults).ToListAsync();

        // Assert
        Assert.Single(groups);
        var group = groups[0];
        Assert.Equal(2, group.Records.Count);
        Assert.Contains(group.Records, r => r["Id"].ToString() == "1");
        Assert.Contains(group.Records, r => r["Id"].ToString() == "2");
    }

    [Fact]
    public async Task CreateMatchGroups_MultiplePairsNoOverlap_CreatesMultipleGroups()
    {
        // Arrange
        var matchResults = new[]
        {
            CreateMatchResult("1", "hash1", "2", "hash2"),
            CreateMatchResult("3", "hash3", "4", "hash4")
        }.ToAsyncEnumerable();

        // Act
        var groups = await _service.CreateMatchGroupsAsync(matchResults).ToListAsync();

        // Assert
        Assert.Equal(2, groups.Count);
        Assert.All(groups, g => Assert.Equal(2, g.Records.Count));
    }

    [Fact]
    public async Task CreateMatchGroups_OverlappingPairs_MergesGroups()
    {
        // Arrange
        var matchResults = new[]
        {
            CreateMatchResult("1", "hash1", "2", "hash2"),
            CreateMatchResult("2", "hash2", "3", "hash3")
        }.ToAsyncEnumerable();

        // Act
        var groups = await _service.CreateMatchGroupsAsync(matchResults, mergeOverlappingGroups: true).ToListAsync();

        // Assert
        var finalGroup = Assert.Single(groups);
        Assert.Equal(3, finalGroup.Records.Count);
        Assert.All(new[] { "1", "2", "3" }, id =>
            Assert.Contains(finalGroup.Records, r => r["Id"].ToString() == id));
    }

    [Fact]
    public async Task CreateMatchGroups_OverlappingDisabled_KeepsSeparateGroups()
    {
        // Arrange
        var matchResults = new[]
        {
            CreateMatchResult("1", "hash1", "2", "hash2"),
            CreateMatchResult("2", "hash2", "3", "hash3")
        }.ToAsyncEnumerable();

        // Act
        var groups = await _service.CreateMatchGroupsAsync(matchResults, mergeOverlappingGroups: false).ToListAsync();

        // Assert
        Assert.Equal(2, groups.Count);
        Assert.All(groups, g => Assert.Equal(2, g.Records.Count));
    }

    [Fact]
    public async Task CreateMatchGroups_PreservesScores()
    {
        // Arrange
        var matchResults = new[]
        {
            CreateMatchResult("1", "hash1", "2", "hash2", 0.95)
        }.ToAsyncEnumerable();

        // Act
        var groups = await _service.CreateMatchGroupsAsync(matchResults).ToListAsync();

        // Assert
        var group = Assert.Single(groups);
        Assert.All(group.Records, record =>
        {
            Assert.Equal(0.8, record["field1_Score"]);
            Assert.Equal(0.5, record["field1_Weight"]);
            Assert.Equal(0.95, record[RecordComparisonService.FinalScoreField]);
        });
    }

    [Fact]
    public async Task CreateMatchGroups_ConcurrentProcessing_HandlesRaceConditions()
    {
        // Arrange
        var matchResults = Enumerable.Range(1, 100).Select(i =>
            CreateMatchResult(
                i.ToString(),
                $"hash{i}",
                (i + 1).ToString(),
                $"hash{i + 1}")
        ).ToAsyncEnumerable();

        // Act
        var groups = await _service.CreateMatchGroupsAsync(matchResults, mergeOverlappingGroups: true).ToListAsync();

        // Assert
        Assert.NotEmpty(groups);
        Assert.All(groups, g => Assert.True(g.Records.Count >= 2));
        var totalRecords = groups.Sum(g => g.Records.Count);
        Assert.True(totalRecords >= 100);
    }

    [Fact]
    public async Task CreateMatchGroups_LargeDataset_HandlesMemoryEfficiently()
    {
        // Arrange
        var matchResults = Enumerable.Range(1, 10000).Select(i =>
            CreateMatchResult(
                i.ToString(),
                $"hash{i}",
                (i + 1).ToString(),
                $"hash{i + 1}")
        ).ToAsyncEnumerable();

        // Act
        var groups = new List<MatchGroup>();
        await foreach (var group in _service.CreateMatchGroupsAsync(matchResults))
        {
            groups.Add(group);
        }

        // Assert
        Assert.NotEmpty(groups);
        Assert.True(groups.Count <= 10000);
        var totalRecords = groups.Sum(g => g.Records.Count);
        Assert.True(totalRecords >= 10000);
    }

    [Fact]
    public async Task CreateMatchGroups_FullGroupMatch_MergesOnlyWhenAllRecordsMatch()
    {
        // Arrange
        var matchResults = new[]
        {
            CreateMatchResult("1", "h1", "2", "h2"),
            CreateMatchResult("2", "h2", "3", "h3"),
            CreateMatchResult("1", "h1", "3", "h3"),
            CreateMatchResult("4", "h4", "5", "h5")
        }.ToAsyncEnumerable();

        // Act
        var groups = await _serviceWithSimilarRecordsInGroups.CreateMatchGroupsAsync(
            matchResults,
            mergeOverlappingGroups: true,
            similarRecordsInGroup: true
        ).ToListAsync();

        // Assert
        Assert.Equal(2, groups.Count);
        var largeGroup = groups.First(g => g.Records.Count == 3);
        Assert.Contains(largeGroup.Records, r => r["Id"].ToString() == "1");
        Assert.Contains(largeGroup.Records, r => r["Id"].ToString() == "2");
        Assert.Contains(largeGroup.Records, r => r["Id"].ToString() == "3");
    }

    [Fact]
    public async Task CreateMatchGroups_FullGroupMatch_PreventsMergeWhenNotAllMatch()
    {
        // Arrange
        var matchResults = new[]
        {
        CreateMatchResult("1", "h1", "2", "h2"),
        CreateMatchResult("2", "h2", "3", "h3"),
        // Note: No match between 1 and 3
        CreateMatchResult("4", "h4", "5", "h5")
    }.ToAsyncEnumerable();

        // Act
        var groups = await _serviceWithSimilarRecordsInGroups.CreateMatchGroupsAsync(
            matchResults,
            mergeOverlappingGroups: true,
            similarRecordsInGroup: true
        ).ToListAsync();

        // Assert
        Assert.True(groups.Count > 2);
        Assert.All(groups, g => Assert.True(g.Records.Count <= 2));
    }

    [Fact]
    public async Task CreateMatchGroups_FullGroupMatch_HandlesComplexGrouping()
    {
        // Arrange
        var matchResults = new[]
        {
        CreateMatchResult("1", "h1", "2", "h2"),
        CreateMatchResult("2", "h2", "3", "h3"),
        CreateMatchResult("3", "h3", "4", "h4"),
        CreateMatchResult("1", "h1", "3", "h3"),
        CreateMatchResult("2", "h2", "4", "h4"),
        CreateMatchResult("1", "h1", "4", "h4")
    }.ToAsyncEnumerable();

        // Act
        var groups = await _serviceWithSimilarRecordsInGroups.CreateMatchGroupsAsync(
            matchResults,
            mergeOverlappingGroups: true,
            similarRecordsInGroup: true
        ).ToListAsync();

        // Assert
        var finalGroup = Assert.Single(groups);
        Assert.Equal(4, finalGroup.Records.Count);
    }

    [Fact]
    public async Task CreateMatchGroups_FullGroupMatch_DisabledWhenOverlappingEnabled()
    {
        // Arrange
        var matchResults = new[]
        {
        CreateMatchResult("1", "h1", "2", "h2"),
        CreateMatchResult("2", "h2", "3", "h3"),
        CreateMatchResult("1", "h1", "4", "h4")
    }.ToAsyncEnumerable();

        // Act
        var groups = await _serviceWithSimilarRecordsInGroups.CreateMatchGroupsAsync(
            matchResults,
            mergeOverlappingGroups: true,
            similarRecordsInGroup: false
        ).ToListAsync();

        // Assert
        Assert.Equal(1, groups.Count);
        Assert.All(groups, g => Assert.Equal(4, g.Records.Count));
    }

    [Fact]
    public async Task CreateMatchGroups_FullGroupMatch_EnabledWhenOverlappingEnabled()
    {
        // Arrange
        var matchResults = new[]
        {
        CreateMatchResult("1", "h1", "2", "h2"),
        CreateMatchResult("2", "h2", "3", "h3"),
        CreateMatchResult("1", "h1", "3", "h3"),
        CreateMatchResult("1", "h1", "4", "h4")
    }.ToAsyncEnumerable();

        // Act
        var groups = await _serviceWithSimilarRecordsInGroups.CreateMatchGroupsAsync(
            matchResults,
            mergeOverlappingGroups: true,
            similarRecordsInGroup: true
        ).ToListAsync();

        // Assert
        Assert.Equal(2, groups.Count);
        //Assert.All(groups, g => Assert.Equal(4, g.Records.Count));
    }

}
