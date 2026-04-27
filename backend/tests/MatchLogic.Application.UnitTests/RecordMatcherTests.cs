using MatchLogic.Application.Features.DataMatching;
using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Application.Interfaces.Core;
using MatchLogic.Application.Interfaces.DataMatching;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Domain.Entities;
using MatchLogic.Infrastructure.Core.Telemetry;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestPlatform.ObjectModel.Client;
using Moq;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.UnitTests;
public class RecordMatcherTests
{
    private readonly IBlockingStrategy _blockingStrategy;
    private readonly IQGramIndexer _qGramIndexer;
    private readonly Mock<ILogger<ExactMatchBlockingStrategy>> _logger;
    private readonly IRecordMatcher _matcher;
    private readonly Mock<IStepProgressTracker> _mockProgressTracker;
    private readonly Mock<IStepProgressTracker> _mockProgressTracker1;

    public RecordMatcherTests()
    {
        _logger = new Mock<ILogger<ExactMatchBlockingStrategy>>();
        _mockProgressTracker = new Mock<IStepProgressTracker>();
        _mockProgressTracker1 = new Mock<IStepProgressTracker>();
        _blockingStrategy = new ExactMatchBlockingStrategy(_logger.Object);
        _qGramIndexer = new QGramIndexer(q: 2, _logger.Object);
        _matcher = new RecordMatcher(_blockingStrategy, _qGramIndexer, _logger.Object);
    }

    [Fact]
    public async Task FindMatchesAsync_WithMixedCriteria_FindsAllMatches()
    {
        // Arrange
        var records = new[]
        {
            new Dictionary<string, object>
            {
                { "id", 1 },
                { "name", "John Smith" },
                { "address", "123 Main St" },
                { "phone", "555-0123" }
            },
            new Dictionary<string, object>
            {
                { "id", 2 },
                { "name", "John Smith" },
                { "address", "123 Main Street" },
                { "phone", "555-0123" }
            },
            new Dictionary<string, object>
            {
                { "id", 3 },
                { "name", "Jon Smithh" },
                { "address", "124 Main St" },
                { "phone", "555-0124" }
            }
        }.ToAsyncEnumerable();

        var criteria = new[]
        {
            new MatchCriteria
            {
                FieldName = "name",
                MatchingType = MatchingType.Exact,
                DataType = CriteriaDataType.Text
            },
            new MatchCriteria
            {
                FieldName = "address",
                MatchingType = MatchingType.Fuzzy,
                DataType = CriteriaDataType.Text,
                Arguments = new Dictionary<ArgsValue, string>
                {
                    { ArgsValue.FastLevel, "0.7" }
                }
            }
        };

        // Act
        var result = await _matcher.FindMatchesAsync(records, criteria, _mockProgressTracker.Object, _mockProgressTracker1.Object);
        var matches = await result.ToListAsync();

        // Assert
        Assert.Single(matches);
        var (record1, record2) = matches[0];
        Assert.Equal("John Smith", record1["name"]);
        Assert.Equal("John Smith", record2["name"]);
        Assert.Equal("123 Main St", record1["address"]);
        Assert.Equal("123 Main Street", record2["address"]);
    }

    [Fact]
    public async Task FindMatchesAsync_WithOnlyFuzzyCriteria_FindsSimilarMatches()
    {
        // Arrange
        var records = new[]
        {
            new Dictionary<string, object>
            {
                { "name", "Michael Johnson" },
                { "address", "456 Oak Avenue" }
            },
            new Dictionary<string, object>
            {
                { "name", "Michael Johnsen" },
                { "address", "456 Oak Ave." }
            },
            new Dictionary<string, object>
            {
                { "name", "Michelle Johnson" },
                { "address", "789 Pine Road" }
            }
        }.ToAsyncEnumerable();

        var criteria = new[]
        {
            new MatchCriteria
            {
                FieldName = "name",
                MatchingType = MatchingType.Fuzzy,
                DataType = CriteriaDataType.Text,
                Arguments = new Dictionary<ArgsValue, string>
                {
                    { ArgsValue.FastLevel, "0.7" }
                }
            },
            new MatchCriteria
            {
                FieldName = "address",
                MatchingType = MatchingType.Fuzzy,
                DataType = CriteriaDataType.Text,
                Arguments = new Dictionary<ArgsValue, string>
                {
                    { ArgsValue.FastLevel, "0.7" }
                }
            }
        };

        // Act
        var result = await _matcher.FindMatchesAsync(records, criteria, _mockProgressTracker.Object, _mockProgressTracker1.Object);
        var matches = await result.ToListAsync();

        // Assert
        Assert.Single(matches);
        var (record1, record2) = matches[0];
        Assert.Equal("Michael Johnson", record1["name"]);
        Assert.Equal("Michael Johnsen", record2["name"]);
    }

    [Fact]
    public async Task FindMatchesAsync_WithMultipleExactMatches_FindsAllMatches()
    {
        // Arrange
        var records = new[]
        {
            new Dictionary<string, object>
            {
                { "name", "Robert Brown" },
                { "city", "New York" },
                { "state", "NY" }
            },
            new Dictionary<string, object>
            {
                { "name", "Robert Brown" },
                { "city", "New York" },
                { "state", "NY" }
            },
            new Dictionary<string, object>
            {
                { "name", "Robert Brown" },
                { "city", "Boston" },
                { "state", "MA" }
            }
        }.ToAsyncEnumerable();

        var criteria = new[]
        {
            new MatchCriteria
            {
                FieldName = "name",
                MatchingType = MatchingType.Exact,
                DataType = CriteriaDataType.Text
            },
            new MatchCriteria
            {
                FieldName = "city",
                MatchingType = MatchingType.Fuzzy,
                DataType = CriteriaDataType.Text,
                Arguments = new Dictionary<ArgsValue, string>
                {
                    { ArgsValue.FastLevel, "0.95" }
                }
            }
        };

        // Act
        var result = await _matcher.FindMatchesAsync(records, criteria, _mockProgressTracker.Object, _mockProgressTracker1.Object);
        var matches = await result.ToListAsync();

        // Assert
        Assert.Single(matches);
        var (record1, record2) = matches[0];
        Assert.Equal("Robert Brown", record1["name"]);
        Assert.Equal("Robert Brown", record2["name"]);
        Assert.Equal("New York", record1["city"]);
        Assert.Equal("New York", record2["city"]);
    }

    [Fact]
    public async Task FindMatchesAsync_WithHighSimilarityThreshold_FindsOnlyCloseMatches()
    {
        // Arrange
        var records = new[]
        {
            new Dictionary<string, object> { { "name", "Christopher Anderson" } },
            new Dictionary<string, object> { { "name", "Chris Anderson" } },
            new Dictionary<string, object> { { "name", "Christopher Andersen" } }
        }.ToAsyncEnumerable();

        var criteria = new[]
        {
            new MatchCriteria
            {
                FieldName = "name",
                MatchingType = MatchingType.Fuzzy,
                DataType = CriteriaDataType.Text,
                Arguments = new Dictionary<ArgsValue, string>
                {
                    { ArgsValue.FastLevel, "0.7" }
                }
            }
        };

        // Act
        var result = await _matcher.FindMatchesAsync(records, criteria, _mockProgressTracker.Object, _mockProgressTracker1.Object);
        var matches = await result.ToListAsync();

        // Assert
        Assert.Single(matches);
        var (record1, record2) = matches[0];
        Assert.Equal("Christopher Anderson", record1["name"]);
        Assert.Equal("Christopher Andersen", record2["name"]);
    }

    [Fact]
    public async Task FindMatchesAsync_WithMultipleFieldsAndMixedCriteria_FindsCorrectMatches()
    {
        // Arrange
        var records = new[]
        {
            new Dictionary<string, object>
            {
                { "firstName", "Thomas" },
                { "lastName", "Anderson" },
                { "address", "123 Matrix St" }
            },
            new Dictionary<string, object>
            {
                { "firstName", "Thomas" },
                { "lastName", "Anderson" },
                { "address", "123 Matrix Street" }
            },
            new Dictionary<string, object>
            {
                { "firstName", "Tom" },
                { "lastName", "Anderson" },
                { "address", "456 Reality Ave" }
            }
        }.ToAsyncEnumerable();

        var criteria = new[]
        {
            new MatchCriteria
            {
                FieldName = "firstName",
                MatchingType = MatchingType.Exact,
                DataType = CriteriaDataType.Text
            },
            new MatchCriteria
            {
                FieldName = "lastName",
                MatchingType = MatchingType.Exact,
                DataType = CriteriaDataType.Text
            },
            new MatchCriteria
            {
                FieldName = "address",
                MatchingType = MatchingType.Fuzzy,
                DataType = CriteriaDataType.Text,
                Arguments = new Dictionary<ArgsValue, string>
                {
                    { ArgsValue.FastLevel, "0.8" }
                }
            }
        };

        // Act
        var result = await _matcher.FindMatchesAsync(records, criteria, _mockProgressTracker.Object, _mockProgressTracker1.Object);
        var matches = await result.ToListAsync();

        // Assert
        Assert.Single(matches);
        var (record1, record2) = matches[0];
        Assert.Equal("Thomas", record1["firstName"]);
        Assert.Equal("Thomas", record2["firstName"]);
        Assert.Equal("Anderson", record1["lastName"]);
        Assert.Equal("Anderson", record2["lastName"]);
        Assert.Equal("123 Matrix St", record1["address"]);
        Assert.Equal("123 Matrix Street", record2["address"]);
    }

    // Empty/null tests remain the same as they verify empty results
    [Fact]
    public async Task FindMatchesAsync_WithNoMatches_ReturnsEmptyResult()
    {
        // Arrange
        var records = new[]
        {
            new Dictionary<string, object>
            {
                { "name", "Alice Wilson" },
                { "city", "Chicago" }
            },
            new Dictionary<string, object>
            {
                { "name", "Bob Wilson" },
                { "city", "Detroit" }
            },
            new Dictionary<string, object>
            {
                { "name", "Carol Wilson" },
                { "city", "Miami" }
            }
        }.ToAsyncEnumerable();

        var criteria = new[]
        {
            new MatchCriteria
            {
                FieldName = "name",
                MatchingType = MatchingType.Exact,
                DataType = CriteriaDataType.Text
            },
            new MatchCriteria
            {
                FieldName = "city",
                MatchingType = MatchingType.Exact,
                DataType = CriteriaDataType.Text
            }
        };

        // Act
        var result = await _matcher.FindMatchesAsync(records, criteria, _mockProgressTracker.Object, _mockProgressTracker1.Object);
        var matches = await result.ToListAsync();

        // Assert
        Assert.Empty(matches);
    }

    [Fact]
    public async Task FindMatchesAsync_WithEmptyRecords_ReturnsEmptyResult()
    {
        // Arrange
        var records = AsyncEnumerable.Empty<IDictionary<string, object>>();
        var criteria = new[]
        {
            new MatchCriteria
            {
                FieldName = "name",
                MatchingType = MatchingType.Exact,
                DataType = CriteriaDataType.Text
            }
        };

        // Act
        var result = await _matcher.FindMatchesAsync(records, criteria, _mockProgressTracker.Object, _mockProgressTracker1.Object);
        var matches = await result.ToListAsync();

        // Assert
        Assert.Empty(matches);
    }

    [Fact]
    public async Task FindMatchesAsync_WithNoCriteria_ReturnsEmptyResult()
    {
        // Arrange
        var records = new[]
        {
            new Dictionary<string, object> { { "name", "Test Name" } },
            new Dictionary<string, object> { { "name", "Test Name" } }
        }.ToAsyncEnumerable();

        var criteria = Array.Empty<MatchCriteria>();

        // Act
        var result = await _matcher.FindMatchesAsync(records, criteria, _mockProgressTracker.Object, _mockProgressTracker1.Object);
        var matches = await result.ToListAsync();

        // Assert
        Assert.Empty(matches);
    }
}
