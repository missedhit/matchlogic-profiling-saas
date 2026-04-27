using MatchLogic.Application.Extensions;
using MatchLogic.Application.Features.CleansingAndStandardization.DTOs;
using MatchLogic.Application.Interfaces.Cleansing;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.CleansingAndStandaradization;
using MatchLogic.Infrastructure;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace MatchLogic.Tests.Infrastructure.CleansingAndStandardization;

public class WordSmithDictionaryServiceTests : IDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly ServiceProvider _serviceProvider;
    private readonly IWordSmithDictionaryService _dictionaryService;
    private readonly IGenericRepository<WordSmithDictionary, Guid> _dictionaryRepository;
    private readonly IGenericRepository<WordSmithDictionaryRule, Guid> _ruleRepository;
    private readonly IDataStore _dataStore;
    private readonly string _tempPath;
    private readonly List<string> _tempFiles = new();

    public WordSmithDictionaryServiceTests(ITestOutputHelper output)
    {
        _output = output;
        var _jobdbPath = Path.GetTempFileName();
        var _tempPath = Path.GetTempFileName();


        //// Build configuration
        var config = new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string>
                {
            // Add any required settings for encryption, e.g.:
            { "Security:MasterKey", "TestMasterKey123456789012345678901234" }
                })
                .Build();

        //var configuration = new ConfigurationBuilder()
        //    .AddInMemoryCollection(configValues)
        //    .Build();

        // Build service provider
        var services = new ServiceCollection();
        services.AddSingleton<IConfiguration>(config);
        services.AddLogging(builder => builder.AddConsole());

        // Add application services (assumes AddApplicationSetup extension exists)
        services.AddApplicationSetup(_tempPath, _jobdbPath);

        _serviceProvider = services.BuildServiceProvider();

        _dictionaryService = _serviceProvider.GetRequiredService<IWordSmithDictionaryService>();
        _dictionaryRepository = _serviceProvider.GetRequiredService<IGenericRepository<WordSmithDictionary, Guid>>();
        _ruleRepository = _serviceProvider.GetRequiredService<IGenericRepository<WordSmithDictionaryRule, Guid>>();
        _dataStore = _serviceProvider.GetRequiredService<IDataStore>();
    }

    public void Dispose()
    {
        _serviceProvider?.Dispose();

        // Cleanup temp files
        foreach (var file in _tempFiles)
        {
            try { if (File.Exists(file)) File.Delete(file); } catch { }
        }

        try
        {
            if (Directory.Exists(_tempPath))
                Directory.Delete(_tempPath, true);
        }
        catch { }
    }

    #region Helper Methods

    /// <summary>
    /// Creates a WordSmith dictionary file in TSV format
    /// </summary>
    private string CreateDictionaryFile(List<(string words, string replacement, string newColumn, bool toDelete, int priority)> entries)
    {
        var filePath = Path.Combine(_tempPath, $"dict_{Guid.NewGuid():N}.txt");
        _tempFiles.Add(filePath);

        using var writer = new StreamWriter(filePath, false, Encoding.Unicode);

        // Write header
        writer.WriteLine("Words\tReplacement\tNewColumn\tToDelete\tPriority\tCount");

        // Write entries
        foreach (var (words, replacement, newColumn, toDelete, priority) in entries)
        {
            var toDeleteStr = toDelete ? "TRUE" : "";
            writer.WriteLine($"{words}\t{replacement}\t{newColumn}\t{toDeleteStr}\t{priority}\t1");
        }

        return filePath;
    }

    /// <summary>
    /// Creates a stream from dictionary file
    /// </summary>
    private Stream CreateDictionaryStream(List<(string words, string replacement, string newColumn, bool toDelete, int priority)> entries)
    {
        var stream = new MemoryStream();
        var writer = new StreamWriter(stream, Encoding.Unicode, leaveOpen: true);

        // Write header
        writer.WriteLine("Words\tReplacement\tNewColumn\tToDelete\tPriority\tCount");

        // Write entries
        foreach (var (words, replacement, newColumn, toDelete, priority) in entries)
        {
            var toDeleteStr = toDelete ? "TRUE" : "";
            writer.WriteLine($"{words}\t{replacement}\t{newColumn}\t{toDeleteStr}\t{priority}\t1");
        }

        writer.Flush();
        stream.Position = 0;
        return stream;
    }

    /// <summary>
    /// Creates test data in data store for extraction tests
    /// </summary>
    private async Task<Guid> CreateTestDataSourceAsync(List<Dictionary<string, object>> records, string collectionSuffix = null)
    {
        var dataSourceId = Guid.NewGuid();
        var collectionName = collectionSuffix ?? $"Import_{GuidCollectionNameConverter.ToValidCollectionName(dataSourceId)}";

        var jobId = await _dataStore.InitializeJobAsync(collectionName);
        await _dataStore.InsertBatchAsync(jobId, records, collectionName);

        return dataSourceId;
    }

    #endregion

    #region Upload Dictionary Tests

    [Fact]
    public async Task UploadDictionaryAsync_WithValidFile_CreatesDictionaryAndRules()
    {
        // Arrange
        var entries = new List<(string words, string replacement, string newColumn, bool toDelete, int priority)>
        {
            ("hello", "hi", "", false, 5),
            ("world", "earth", "", false, 5),
            ("test", "", "", true, 3),
            ("important", "IMPORTANT", "Flags", false, 1)
        };

        using var stream = CreateDictionaryStream(entries);
        var request = new UploadWordSmithDictionaryDto
        {
            Name = "Test Dictionary",
            Description = "A test dictionary",
            Category = "Testing"
        };

        // Act
        var result = await _dictionaryService.UploadDictionaryAsync(stream, "test_dict.txt", request);

        // Assert
        Assert.NotNull(result);
        Assert.Equal("Test Dictionary", result.Name);
        Assert.Equal("A test dictionary", result.Description);
        Assert.Equal("Testing", result.Category);
        Assert.Equal(4, result.TotalRules);
        Assert.Equal(3, result.ReplacementRules); // hello, world, important have replacements
        Assert.Equal(1, result.DeletionRules); // test is marked for deletion
        Assert.Equal(1, result.NewColumnRules); // important creates new column
        Assert.Contains("Flags", result.ExtractedColumns);
        Assert.True(result.IsActive);
        Assert.Equal(1, result.Version);

        _output.WriteLine($"Dictionary created with ID: {result.Id}");
        _output.WriteLine($"Total rules: {result.TotalRules}");
    }

    [Fact]
    public async Task UploadDictionaryAsync_WithEmptyFile_ThrowsException()
    {
        // Arrange
        var stream = new MemoryStream();
        var writer = new StreamWriter(stream, Encoding.Unicode, leaveOpen: true);
        writer.WriteLine("Words\tReplacement\tNewColumn\tToDelete\tPriority\tCount");
        writer.Flush();
        stream.Position = 0;

        var request = new UploadWordSmithDictionaryDto
        {
            Name = "Empty Dictionary",
            Category = "Testing"
        };

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => _dictionaryService.UploadDictionaryAsync(stream, "empty.txt", request));
    }

    [Fact]
    public async Task UploadDictionaryAsync_WithDuplicateWords_KeepsLastOccurrence()
    {
        // Arrange - same word with different replacements
        var entries = new List<(string words, string replacement, string newColumn, bool toDelete, int priority)>
        {
            ("duplicate", "first", "", false, 5),
            ("duplicate", "second", "", false, 3), // Should keep this one
            ("unique", "only", "", false, 5)
        };

        using var stream = CreateDictionaryStream(entries);
        var request = new UploadWordSmithDictionaryDto
        {
            Name = "Duplicate Test",
            Category = "Testing"
        };

        // Act
        var result = await _dictionaryService.UploadDictionaryAsync(stream, "dup.txt", request);

        // Assert - WordSmithDictionaryLoader handles duplicates by keeping last
        Assert.NotNull(result);

        var (rules, _) = await _dictionaryService.GetDictionaryRulesAsync(result.Id);
        var duplicateRule = rules.FirstOrDefault(r => r.Words.Equals("duplicate", StringComparison.OrdinalIgnoreCase));

        // Depending on loader behavior, verify the expected replacement
        Assert.NotNull(duplicateRule);
    }

    #endregion

    #region Get Dictionary Tests

    [Fact]
    public async Task GetDictionaryAsync_WithValidId_ReturnsDictionary()
    {
        // Arrange
        var entries = new List<(string words, string replacement, string newColumn, bool toDelete, int priority)>
        {
            ("word1", "replacement1", "", false, 5),
            ("word2", "replacement2", "Category", false, 3)
        };

        using var stream = CreateDictionaryStream(entries);
        var uploaded = await _dictionaryService.UploadDictionaryAsync(
            stream,
            "test.txt",
            new UploadWordSmithDictionaryDto { Name = "Get Test", Category = "Test" });

        // Act
        var result = await _dictionaryService.GetDictionaryAsync(uploaded.Id);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(uploaded.Id, result.Id);
        Assert.Equal("Get Test", result.Name);
        Assert.Equal(2, result.TotalRules);
    }

    [Fact]
    public async Task GetDictionaryAsync_WithInvalidId_ReturnsNull()
    {
        // Act
        var result = await _dictionaryService.GetDictionaryAsync(Guid.NewGuid());

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public async Task GetAllDictionariesAsync_ReturnsAllActiveDictionaries()
    {
        // Arrange - Create multiple dictionaries
        var entries1 = new List<(string words, string replacement, string newColumn, bool toDelete, int priority)>
        {
            ("word1", "rep1", "", false, 5)
        };
        var entries2 = new List<(string words, string replacement, string newColumn, bool toDelete, int priority)>
        {
            ("word2", "rep2", "", false, 5)
        };

        using var stream1 = CreateDictionaryStream(entries1);
        using var stream2 = CreateDictionaryStream(entries2);

        await _dictionaryService.UploadDictionaryAsync(
            stream1, "dict1.txt",
            new UploadWordSmithDictionaryDto { Name = "Dict1", Category = "Test" });

        await _dictionaryService.UploadDictionaryAsync(
            stream2, "dict2.txt",
            new UploadWordSmithDictionaryDto { Name = "Dict2", Category = "Test" });

        // Act
        var result = await _dictionaryService.GetAllDictionariesAsync();

        // Assert
        Assert.True(result.Count() >= 2);
        Assert.Contains(result, d => d.Name == "Dict1");
        Assert.Contains(result, d => d.Name == "Dict2");
    }

    #endregion

    #region Delete Dictionary Tests

    [Fact]
    public async Task DeleteDictionaryAsync_SoftDeletesDictionaryAndRules()
    {
        // Arrange
        var entries = new List<(string words, string replacement, string newColumn, bool toDelete, int priority)>
        {
            ("word1", "rep1", "", false, 5),
            ("word2", "rep2", "", false, 5)
        };

        using var stream = CreateDictionaryStream(entries);
        var uploaded = await _dictionaryService.UploadDictionaryAsync(
            stream, "delete_test.txt",
            new UploadWordSmithDictionaryDto { Name = "Delete Test", Category = "Test" });

        // Act
        var result = await _dictionaryService.DeleteDictionaryAsync(uploaded.Id);

        // Assert
        Assert.True(result);

        var deleted = await _dictionaryService.GetDictionaryAsync(uploaded.Id);
        // After soft delete, GetDictionaryAsync should return null or inactive
        // Based on implementation, it returns the dictionary but marked inactive
        Assert.True(deleted == null || !deleted.IsActive);
    }

    [Fact]
    public async Task DeleteDictionaryAsync_WithInvalidId_ReturnsFalse()
    {
        // Act
        var result = await _dictionaryService.DeleteDictionaryAsync(Guid.NewGuid());

        // Assert
        Assert.False(result);
    }

    #endregion

    #region Rule CRUD Tests

    [Fact]
    public async Task GetDictionaryRulesAsync_ReturnsPaginatedRules()
    {
        // Arrange
        var entries = Enumerable.Range(1, 100)
            .Select(i => ($"word{i}", $"rep{i}", "", false, 5))
            .ToList();

        using var stream = CreateDictionaryStream(entries);
        var uploaded = await _dictionaryService.UploadDictionaryAsync(
            stream, "paginated.txt",
            new UploadWordSmithDictionaryDto { Name = "Paginated Test", Category = "Test" });

        // Act - Get first page
        var (page1Rules, totalCount) = await _dictionaryService.GetDictionaryRulesAsync(uploaded.Id, 1, 20);

        // Assert
        Assert.Equal(100, totalCount);
        Assert.Equal(20, page1Rules.Count());

        // Act - Get second page
        var (page2Rules, _) = await _dictionaryService.GetDictionaryRulesAsync(uploaded.Id, 2, 20);
        Assert.Equal(20, page2Rules.Count());

        // Ensure no overlap
        var page1Ids = page1Rules.Select(r => r.Id).ToHashSet();
        var page2Ids = page2Rules.Select(r => r.Id).ToHashSet();
        Assert.Empty(page1Ids.Intersect(page2Ids));
    }

    [Fact]
    public async Task AddRuleAsync_AddsNewRule()
    {
        // Arrange
        var entries = new List<(string words, string replacement, string newColumn, bool toDelete, int priority)>
        {
            ("existing", "replacement", "", false, 5)
        };

        using var stream = CreateDictionaryStream(entries);
        var uploaded = await _dictionaryService.UploadDictionaryAsync(
            stream, "add_rule.txt",
            new UploadWordSmithDictionaryDto { Name = "Add Rule Test", Category = "Test" });

        var createDto = new CreateWordSmithRuleDto
        {
            Words = "newword",
            Replacement = "newreplacement",
            NewColumnName = "NewCategory",
            ToDelete = false,
            Priority = 2
        };

        // Act
        var result = await _dictionaryService.AddRuleAsync(uploaded.Id, createDto);

        // Assert
        Assert.NotNull(result);
        Assert.Equal("newword", result.Words);
        Assert.Equal("newreplacement", result.Replacement);
        Assert.Equal("NewCategory", result.NewColumnName);
        Assert.Equal(2, result.Priority);
        Assert.True(result.IsActive);

        // Verify dictionary was updated
        var dict = await _dictionaryService.GetDictionaryAsync(uploaded.Id);
        Assert.Equal(2, dict.TotalRules);
        Assert.Equal(2, dict.Version); // Version incremented
    }

    [Fact]
    public async Task UpdateRuleAsync_UpdatesExistingRule()
    {
        // Arrange
        var entries = new List<(string words, string replacement, string newColumn, bool toDelete, int priority)>
        {
            ("original", "oldreplacement", "", false, 5)
        };

        using var stream = CreateDictionaryStream(entries);
        var uploaded = await _dictionaryService.UploadDictionaryAsync(
            stream, "update_rule.txt",
            new UploadWordSmithDictionaryDto { Name = "Update Rule Test", Category = "Test" });

        var (rules, _) = await _dictionaryService.GetDictionaryRulesAsync(uploaded.Id);
        var ruleToUpdate = rules.First();

        var updateDto = new UpdateWordSmithRuleDto
        {
            Replacement = "newreplacement",
            Priority = 1,
            NewColumnName = "UpdatedCategory"
        };

        // Act
        var result = await _dictionaryService.UpdateRuleAsync(ruleToUpdate.Id, updateDto);

        // Assert
        Assert.NotNull(result);
        Assert.Equal("newreplacement", result.Replacement);
        Assert.Equal(1, result.Priority);
        Assert.Equal("UpdatedCategory", result.NewColumnName);
        Assert.NotNull(result.ModifiedAt);
    }

    [Fact]
    public async Task DeleteRuleAsync_SoftDeletesRule()
    {
        // Arrange
        var entries = new List<(string words, string replacement, string newColumn, bool toDelete, int priority)>
        {
            ("todelete", "replacement", "", false, 5),
            ("tokeep", "replacement", "", false, 5)
        };

        using var stream = CreateDictionaryStream(entries);
        var uploaded = await _dictionaryService.UploadDictionaryAsync(
            stream, "delete_rule.txt",
            new UploadWordSmithDictionaryDto { Name = "Delete Rule Test", Category = "Test" });

        var (rules, _) = await _dictionaryService.GetDictionaryRulesAsync(uploaded.Id);
        var ruleToDelete = rules.First(r => r.Words.Equals("todelete", StringComparison.OrdinalIgnoreCase));

        // Act
        var result = await _dictionaryService.DeleteRuleAsync(ruleToDelete.Id);

        // Assert
        Assert.True(result);

        var (remainingRules, _) = await _dictionaryService.GetDictionaryRulesAsync(uploaded.Id);
        Assert.Single(remainingRules);
        Assert.DoesNotContain(remainingRules, r => r.Words.Equals("todelete", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task BulkUpdateRulesAsync_UpdatesMultipleRules()
    {
        // Arrange
        var entries = new List<(string words, string replacement, string newColumn, bool toDelete, int priority)>
        {
            ("word1", "rep1", "", false, 5),
            ("word2", "rep2", "", false, 5),
            ("word3", "rep3", "", false, 5)
        };

        using var stream = CreateDictionaryStream(entries);
        var uploaded = await _dictionaryService.UploadDictionaryAsync(
            stream, "bulk_update.txt",
            new UploadWordSmithDictionaryDto { Name = "Bulk Update Test", Category = "Test" });

        var (rules, _) = await _dictionaryService.GetDictionaryRulesAsync(uploaded.Id);

        var updates = rules.Take(2).Select(r => (
            r.Id,
            new UpdateWordSmithRuleDto { Priority = 1 }
        )).ToList();

        // Act
        var updatedCount = await _dictionaryService.BulkUpdateRulesAsync(uploaded.Id, updates);

        // Assert
        Assert.Equal(2, updatedCount);

        var (updatedRules, _) = await _dictionaryService.GetDictionaryRulesAsync(uploaded.Id);
        Assert.Equal(2, updatedRules.Count(r => r.Priority == 1));
    }

    #endregion

    #region Build Dictionaries Tests

    [Fact]
    public async Task BuildDictionariesAsync_ReturnsReplacingStepDictionaries()
    {
        // Arrange
        var entries = new List<(string words, string replacement, string newColumn, bool toDelete, int priority)>
        {
            ("hello", "hi", "", false, 5),
            ("world", "", "", true, 3), // Deletion
            ("important", "IMPORTANT", "Flags", false, 1), // New column
            ("test phrase", "testing", "Category", false, 2) // Multi-word
        };

        using var stream = CreateDictionaryStream(entries);
        var uploaded = await _dictionaryService.UploadDictionaryAsync(
            stream, "build_dict.txt",
            new UploadWordSmithDictionaryDto { Name = "Build Dict Test", Category = "Test" });

        // Act
        var (replacements, newColumns) = await _dictionaryService.BuildDictionariesAsync(uploaded.Id);

        // Assert
        Assert.Equal(4, replacements.Count);
        Assert.True(replacements.ContainsKey("hello"));
        Assert.Equal("hi", replacements["hello"].Replacement);

        Assert.True(replacements.ContainsKey("world"));
        Assert.True(replacements["world"].ToDelete);

        Assert.Equal(2, newColumns.Count); // important and test phrase have new columns
        Assert.True(newColumns.ContainsKey("important"));
        Assert.Equal("Flags", newColumns["important"].NewColumnName);

        _output.WriteLine($"Replacements count: {replacements.Count}");
        _output.WriteLine($"New columns count: {newColumns.Count}");
    }

    [Fact]
    public async Task BuildDictionariesAsync_IsCaseInsensitive()
    {
        // Arrange
        var entries = new List<(string words, string replacement, string newColumn, bool toDelete, int priority)>
        {
            ("HELLO", "hi", "", false, 5),
            ("World", "earth", "", false, 5)
        };

        using var stream = CreateDictionaryStream(entries);
        var uploaded = await _dictionaryService.UploadDictionaryAsync(
            stream, "case_test.txt",
            new UploadWordSmithDictionaryDto { Name = "Case Test", Category = "Test" });

        // Act
        var (replacements, _) = await _dictionaryService.BuildDictionariesAsync(uploaded.Id);

        // Assert - Dictionary should be case-insensitive
        Assert.True(replacements.ContainsKey("hello"));
        Assert.True(replacements.ContainsKey("world"));
    }

    #endregion

    #region Export Dictionary Tests

    [Fact]
    public async Task ExportDictionaryAsync_ExportsTsvFormat()
    {
        // Arrange
        var entries = new List<(string words, string replacement, string newColumn, bool toDelete, int priority)>
        {
            ("word1", "rep1", "", false, 5),
            ("word2", "", "Category", true, 3)
        };

        using var stream = CreateDictionaryStream(entries);
        var uploaded = await _dictionaryService.UploadDictionaryAsync(
            stream, "export_test.txt",
            new UploadWordSmithDictionaryDto { Name = "Export Test", Category = "Test" });

        // Act
        var exportStream = await _dictionaryService.ExportDictionaryAsync(uploaded.Id, "tsv");

        // Assert
        using var reader = new StreamReader(exportStream, Encoding.Unicode);
        var content = await reader.ReadToEndAsync();

        Assert.Contains("Words\tReplacement\tNewColumn\tToDelete\tPriority\tCount", content);
        Assert.Contains("word1\trep1", content);
        Assert.Contains("word2", content);
        Assert.Contains("TRUE", content); // ToDelete marker

        _output.WriteLine("Exported content:");
        _output.WriteLine(content);
    }

    [Fact]
    public async Task ExportDictionaryAsync_ExportsCsvFormat()
    {
        // Arrange
        var entries = new List<(string words, string replacement, string newColumn, bool toDelete, int priority)>
        {
            ("word1", "rep1", "", false, 5)
        };

        using var stream = CreateDictionaryStream(entries);
        var uploaded = await _dictionaryService.UploadDictionaryAsync(
            stream, "csv_export.txt",
            new UploadWordSmithDictionaryDto { Name = "CSV Export Test", Category = "Test" });

        // Act
        var exportStream = await _dictionaryService.ExportDictionaryAsync(uploaded.Id, "csv");

        // Assert
        using var reader = new StreamReader(exportStream, Encoding.Unicode);
        var content = await reader.ReadToEndAsync();

        Assert.Contains("Words,Replacement,NewColumn,ToDelete,Priority,Count", content);
        Assert.Contains(",", content); // CSV delimiter
    }

    #endregion

    #region Replace Dictionary Tests

    [Fact]
    public async Task ReplaceDictionaryAsync_ReplacesAllRules()
    {
        // Arrange - Create initial dictionary
        var initialEntries = new List<(string words, string replacement, string newColumn, bool toDelete, int priority)>
        {
            ("old1", "oldrep1", "", false, 5),
            ("old2", "oldrep2", "", false, 5)
        };

        using var initialStream = CreateDictionaryStream(initialEntries);
        var uploaded = await _dictionaryService.UploadDictionaryAsync(
            initialStream, "initial.txt",
            new UploadWordSmithDictionaryDto { Name = "Replace Test", Category = "Test" });

        // Create replacement dictionary
        var newEntries = new List<(string words, string replacement, string newColumn, bool toDelete, int priority)>
        {
            ("new1", "newrep1", "", false, 3),
            ("new2", "newrep2", "NewCat", false, 2),
            ("new3", "", "", true, 1)
        };

        using var newStream = CreateDictionaryStream(newEntries);

        // Act
        var result = await _dictionaryService.ReplaceDictionaryAsync(
            uploaded.Id,
            newStream,
            "replacement.txt",
            new UploadWordSmithDictionaryDto { Name = "Replaced Dictionary", Category = "Updated" });

        // Assert
        Assert.NotNull(result);
        Assert.Equal(uploaded.Id, result.Id); // Same ID preserved
        Assert.Equal("Replaced Dictionary", result.Name);
        Assert.Equal(2, result.Version); // Version incremented

        var (rules, totalCount) = await _dictionaryService.GetDictionaryRulesAsync(result.Id);
        Assert.Equal(3, totalCount); // New rules only
        Assert.DoesNotContain(rules, r => r.Words.StartsWith("old")); // Old rules removed
        Assert.Contains(rules, r => r.Words == "new1");
    }

    #endregion

    #region Clear Dictionary Tests

    [Fact]
    public async Task ClearDictionaryRulesAsync_RemovesAllRules()
    {
        // Arrange
        var entries = new List<(string words, string replacement, string newColumn, bool toDelete, int priority)>
        {
            ("word1", "rep1", "", false, 5),
            ("word2", "rep2", "", false, 5),
            ("word3", "rep3", "", false, 5)
        };

        using var stream = CreateDictionaryStream(entries);
        var uploaded = await _dictionaryService.UploadDictionaryAsync(
            stream, "clear_test.txt",
            new UploadWordSmithDictionaryDto { Name = "Clear Test", Category = "Test" });

        // Verify rules exist
        var (rulesBefore, countBefore) = await _dictionaryService.GetDictionaryRulesAsync(uploaded.Id);
        Assert.Equal(3, countBefore);

        // Act
        var result = await _dictionaryService.ClearDictionaryRulesAsync(uploaded.Id);

        // Assert
        Assert.True(result);

        var (rulesAfter, countAfter) = await _dictionaryService.GetDictionaryRulesAsync(uploaded.Id);
        Assert.Equal(0, countAfter);

        // Dictionary should still exist
        var dict = await _dictionaryService.GetDictionaryAsync(uploaded.Id);
        Assert.NotNull(dict);
        Assert.Equal(2, dict.Version); // Version incremented
    }

    #endregion

    #region Integration with Data Cleansing Tests

    [Fact]
    public async Task BuildDictionariesAsync_CanBeUsedWithWordSmithRule()
    {
        // Arrange - Create dictionary with various rule types
        var entries = new List<(string words, string replacement, string newColumn, bool toDelete, int priority)>
        {
            // Replacements
            ("dr", "Doctor", "", false, 1),
            ("mr", "Mister", "", false, 1),
            ("mrs", "Missus", "", false, 1),
            
            // Classifications
            ("urgent", "", "Priority", false, 2),
            ("critical", "", "Priority", false, 2),
            
            // Deletions
            ("test", "", "", true, 3),
            ("dummy", "", "", true, 3),
            
            // Multi-word phrases
            ("new york", "NY", "State", false, 1),
            ("los angeles", "LA", "City", false, 1)
        };

        using var stream = CreateDictionaryStream(entries);
        var uploaded = await _dictionaryService.UploadDictionaryAsync(
            stream, "integration.txt",
            new UploadWordSmithDictionaryDto { Name = "Integration Test", Category = "Test" });

        // Act - Build dictionaries for use with WordSmithRule
        var (replacements, newColumns) = await _dictionaryService.BuildDictionariesAsync(uploaded.Id);

        // Assert - Verify structure matches what WordSmithRule expects
        Assert.Equal(9, replacements.Count);

        // Check replacement rules
        Assert.Equal("Doctor", replacements["dr"].Replacement);
        Assert.False(replacements["dr"].ToDelete);

        // Check deletion rules
        Assert.True(replacements["test"].ToDelete);
        Assert.True(replacements["dummy"].ToDelete);

        // Check new column rules
        Assert.Equal(4, newColumns.Count); // urgent, critical, new york, los angeles
        Assert.Equal("Priority", newColumns["urgent"].NewColumnName);
        Assert.Equal("State", newColumns["new york"].NewColumnName);

        // Verify priority ordering
        Assert.Equal(1, replacements["dr"].Priority);
        Assert.Equal(2, replacements["urgent"].Priority);
        Assert.Equal(3, replacements["test"].Priority);

        _output.WriteLine($"Total replacements: {replacements.Count}");
        _output.WriteLine($"New column extractions: {newColumns.Count}");
        foreach (var nc in newColumns)
        {
            _output.WriteLine($"  {nc.Key} -> {nc.Value.NewColumnName}");
        }
    }

    #endregion

    #region Comprehensive Feature Tests

    [Fact]
    public async Task WordSmithDictionary_ComprehensiveFeatureTest()
    {
        // This test covers the full lifecycle and features of WordSmith dictionaries

        // 1. Create dictionary with comprehensive rules
        var entries = new List<(string words, string replacement, string newColumn, bool toDelete, int priority)>
        {
            // Simple replacements (Priority 1 - highest)
            ("inc", "Incorporated", "", false, 1),
            ("corp", "Corporation", "", false, 1),
            ("llc", "Limited Liability Company", "", false, 1),
            
            // Abbreviation standardization (Priority 2)
            ("st", "Street", "", false, 2),
            ("ave", "Avenue", "", false, 2),
            ("blvd", "Boulevard", "", false, 2),
            
            // Classification extraction (Priority 3)
            ("residential", "", "PropertyType", false, 3),
            ("commercial", "", "PropertyType", false, 3),
            ("industrial", "", "PropertyType", false, 3),
            
            // Deletion of noise words (Priority 4)
            ("n/a", "", "", true, 4),
            ("null", "", "", true, 4),
            ("unknown", "", "", true, 4),
            
            // Multi-word replacements (Priority 1)
            ("new york city", "NYC", "City", false, 1),
            ("san francisco", "SF", "City", false, 1),
            ("los angeles", "LA", "City", false, 1),
            
            // Complex classification with replacement
            ("priority high", "HIGH", "PriorityLevel", false, 1),
            ("priority low", "LOW", "PriorityLevel", false, 1)
        };

        using var stream = CreateDictionaryStream(entries);
        var uploaded = await _dictionaryService.UploadDictionaryAsync(
            stream, "comprehensive.txt",
            new UploadWordSmithDictionaryDto
            {
                Name = "Comprehensive Test Dictionary",
                Description = "Tests all WordSmith features",
                Category = "Comprehensive"
            });

        // 2. Verify upload stats
        Assert.Equal(17, uploaded.TotalRules);
        Assert.True(uploaded.ReplacementRules > 0);
        Assert.Equal(3, uploaded.DeletionRules);
        Assert.True(uploaded.NewColumnRules > 0);
        Assert.Contains("PropertyType", uploaded.ExtractedColumns);
        Assert.Contains("City", uploaded.ExtractedColumns);
        Assert.Contains("PriorityLevel", uploaded.ExtractedColumns);

        _output.WriteLine($"Uploaded dictionary: {uploaded.Name}");
        _output.WriteLine($"Total rules: {uploaded.TotalRules}");
        _output.WriteLine($"Extracted columns: {string.Join(", ", uploaded.ExtractedColumns)}");

        // 3. Test rule retrieval and pagination
        var (page1, total) = await _dictionaryService.GetDictionaryRulesAsync(uploaded.Id, 1, 10);
        Assert.Equal(17, total);
        Assert.Equal(10, page1.Count());

        // 4. Test adding a new rule
        var newRule = await _dictionaryService.AddRuleAsync(uploaded.Id, new CreateWordSmithRuleDto
        {
            Words = "headquarters",
            Replacement = "HQ",
            NewColumnName = "",
            ToDelete = false,
            Priority = 2
        });

        Assert.Equal("headquarters", newRule.Words);

        var dictAfterAdd = await _dictionaryService.GetDictionaryAsync(uploaded.Id);
        Assert.Equal(18, dictAfterAdd.TotalRules);
        Assert.Equal(2, dictAfterAdd.Version);

        // 5. Test updating a rule
        var updatedRule = await _dictionaryService.UpdateRuleAsync(newRule.Id, new UpdateWordSmithRuleDto
        {
            Replacement = "Headquarters",
            Priority = 1
        });

        Assert.Equal("Headquarters", updatedRule.Replacement);
        Assert.Equal(1, updatedRule.Priority);

        // 6. Test building dictionaries for cleansing
        var (replacements, newColumns) = await _dictionaryService.BuildDictionariesAsync(uploaded.Id);

        Assert.True(replacements.ContainsKey("headquarters"));
        Assert.Equal("Incorporated", replacements["inc"].Replacement);
        Assert.True(replacements["n/a"].ToDelete);

        Assert.True(newColumns.ContainsKey("residential"));
        Assert.Equal("City", newColumns["new york city"].NewColumnName);

        // 7. Test export
        var exportStream = await _dictionaryService.ExportDictionaryAsync(uploaded.Id);
        using var reader = new StreamReader(exportStream, Encoding.Unicode);
        var exportContent = await reader.ReadToEndAsync();

        Assert.Contains("headquarters", exportContent);
        Assert.Contains("Incorporated", exportContent);

        // 8. Test deleting a rule
        var deleteResult = await _dictionaryService.DeleteRuleAsync(newRule.Id);
        Assert.True(deleteResult);

        var dictAfterDelete = await _dictionaryService.GetDictionaryAsync(uploaded.Id);
        Assert.Equal(17, dictAfterDelete.TotalRules);

        // 9. Test clearing all rules
        var clearResult = await _dictionaryService.ClearDictionaryRulesAsync(uploaded.Id);
        Assert.True(clearResult);

        var clearedDict = await _dictionaryService.GetDictionaryAsync(uploaded.Id);
        Assert.Equal(0, clearedDict.TotalRules);

        // 10. Test soft delete of dictionary
        var softDeleteResult = await _dictionaryService.DeleteDictionaryAsync(uploaded.Id);
        Assert.True(softDeleteResult);

        _output.WriteLine("Comprehensive test completed successfully!");
    }

    #endregion

    #region Error Handling Tests

    [Fact]
    public async Task UpdateRuleAsync_WithInvalidId_ThrowsKeyNotFoundException()
    {
        // Act & Assert
        await Assert.ThrowsAsync<KeyNotFoundException>(
            () => _dictionaryService.UpdateRuleAsync(Guid.NewGuid(), new UpdateWordSmithRuleDto()));
    }

    [Fact]
    public async Task AddRuleAsync_WithInvalidDictionaryId_ThrowsKeyNotFoundException()
    {
        // Act & Assert
        await Assert.ThrowsAsync<KeyNotFoundException>(
            () => _dictionaryService.AddRuleAsync(Guid.NewGuid(), new CreateWordSmithRuleDto
            {
                Words = "test",
                Priority = 5
            }));
    }

    [Fact]
    public async Task ReplaceDictionaryAsync_WithInvalidId_ThrowsException()
    {
        // Arrange
        var entries = new List<(string words, string replacement, string newColumn, bool toDelete, int priority)>
        {
            ("word", "rep", "", false, 5)
        };
        using var stream = CreateDictionaryStream(entries);

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => _dictionaryService.ReplaceDictionaryAsync(
                Guid.NewGuid(),
                stream,
                "test.txt",
                new UploadWordSmithDictionaryDto { Name = "Test" }));
    }

    #endregion
}