using MatchLogic.Infrastructure.CleansingAndStandardization.CleansingRuleHandlers.WordSmith;
using MatchLogic.Application.Common;
using MatchLogic.Application.Extensions;
using MatchLogic.Application.Features.CleansingAndStandardization.DTOs;
using MatchLogic.Application.Interfaces.Cleansing;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.CleansingAndStandaradization;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using MatchLogic.Infrastructure.CleansingAndStandardization.CleansingRuleHandlers.WordSmith;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using NPOI.POIFS.NIO;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.CleansingAndStandardization;
public class WordSmithDictionaryService : IWordSmithDictionaryService
{
    private readonly IGenericRepository<WordSmithDictionary, Guid> _dictionaryRepository;
    private readonly IGenericRepository<WordSmithDictionaryRule, Guid> _ruleRepository;
    private readonly WordSmithDictionaryLoader _loader;
    private readonly ILogger<WordSmithDictionaryService> _logger;
    private readonly string _dictionaryStoragePath;
    private readonly IDataStore _dataStore;

    public WordSmithDictionaryService(
        IGenericRepository<WordSmithDictionary, Guid> dictionaryRepository,
        IGenericRepository<WordSmithDictionaryRule, Guid> ruleRepository,
        WordSmithDictionaryLoader loader,
        IConfiguration configuration,
        ILogger<WordSmithDictionaryService> logger,
        IDataStore dataStore)
    {
        _dictionaryRepository = dictionaryRepository;
        _ruleRepository = ruleRepository;
        _loader = loader;
        _logger = logger;
        _dictionaryStoragePath = configuration["Storage:DictionaryPath"] ??
           Path.Combine(MatchLogic.Application.Common.StoragePaths.DefaultUploadPath, "WordSmithDictionary");

        Directory.CreateDirectory(_dictionaryStoragePath);
        _dataStore = dataStore;
    }

    public async Task<WordSmithDictionaryDto> UploadDictionaryAsync(
        Stream fileStream,
        string fileName,
        UploadWordSmithDictionaryDto request)
    {
        try
        {
            var fileId = Guid.NewGuid();
            var extension = Path.GetExtension(fileName);
            var storedFileName = $"{fileId}{extension}";
            var filePath = Path.Combine(_dictionaryStoragePath, storedFileName);

            // Save file to disk as backup
            using (var fileStreamDisk = File.Create(filePath))
            {
                await fileStream.CopyToAsync(fileStreamDisk);
            }

            // Load and validate dictionary
            var encoding = ResolveEncoding(request.Encoding, filePath);
            var loadResult = _loader.LoadDictionary(filePath, encoding, true);

            if (!loadResult.Success)
            {
                File.Delete(filePath);
                throw new InvalidOperationException(
                    $"Failed to load dictionary: {string.Join(", ", loadResult.ErrorMessages.Take(5))}");
            }

            // Create dictionary entity
            var dictionary = new WordSmithDictionary
            {
                Id = fileId,
                Name = request.Name,
                Description = request.Description,
                Category = request.Category,
                OriginalFilePath = filePath,
                OriginalFileName = fileName,
                Version = 1,
                IsActive = true
            };

            // Save dictionary
            await _dictionaryRepository.InsertAsync(dictionary, Constants.Collections.WordSmithDictionary);

            // Save individual rules to database
            var rules = new List<WordSmithDictionaryRule>();
            foreach (var kvp in loadResult.ReplacementsDictionary)
            {
                var rule = new WordSmithDictionaryRule
                {
                    Id = Guid.NewGuid(),
                    DictionaryId = dictionary.Id,
                    Words = kvp.Value.Words,
                    Replacement = kvp.Value.Replacement ?? string.Empty,
                    NewColumnName = kvp.Value.NewColumnName ?? string.Empty,
                    ToDelete = kvp.Value.ToDelete,
                    Priority = kvp.Value.Priority,
                    IsActive = true,
                    CreatedAt = DateTime.UtcNow
                };
                rules.Add(rule);
            }

            // Bulk insert rules
            //foreach (var rule in rules)
            //{
            await _ruleRepository.BulkInsertAsync(rules, Constants.Collections.WordSmithDictionaryRules);
            //}

            _logger.LogInformation(
                "Dictionary {Name} uploaded with {RuleCount} rules",
                request.Name, rules.Count);

            var extractedColumns = rules
            .Where(r => !string.IsNullOrEmpty(r.NewColumnName))
            .Select(r => r.NewColumnName)
            .Distinct()
            .ToList();

            return new WordSmithDictionaryDto
            {
                Id = dictionary.Id,
                Name = dictionary.Name,
                Description = dictionary.Description,
                Category = dictionary.Category,
                OriginalFileName = dictionary.OriginalFileName,
                Version = dictionary.Version,
                TotalRules = rules.Count,
                ReplacementRules = rules.Count(r => !string.IsNullOrEmpty(r.Replacement) && !r.ToDelete),
                DeletionRules = rules.Count(r => r.ToDelete),
                NewColumnRules = rules.Count(r => !string.IsNullOrEmpty(r.NewColumnName)),
                ExtractedColumns = extractedColumns,
                IsActive = dictionary.IsActive,
                CreatedAt = dictionary.CreatedAt,
                ModifiedAt = dictionary.ModifiedAt
            }; ;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error uploading dictionary");
            throw;
        }
    }
    public async Task<WordSmithDictionaryDto> ReplaceDictionaryAsync(Guid id, Stream file, string fileName, UploadWordSmithDictionaryDto meta)
    {
        // 1) Fetch dictionary (single read)
        var dictionary = await _dictionaryRepository.GetByIdAsync(id, Constants.Collections.WordSmithDictionary);
        if (dictionary == null || !dictionary.IsActive)
            throw new InvalidOperationException($"Dictionary '{id}' not found or inactive.");

        // 2) Save uploaded file to storage path (same as in UploadDictionaryAsync)
        var fileId = Guid.NewGuid();
        var extension = Path.GetExtension(fileName);
        var storedFileName = $"{fileId}{extension}";
        var filePath = Path.Combine(_dictionaryStoragePath, storedFileName);

        using (var fs = File.Create(filePath))
            await file.CopyToAsync(fs);

        // 3) Load/parse file to in-memory rules (same loader/encoding you already use)
        var encoding = ResolveEncoding(meta?.Encoding, filePath);
        var loadResult = _loader.LoadDictionary(filePath, encoding, true);
        if (!loadResult.Success)
        {
            File.Delete(filePath);
            throw new InvalidOperationException(
                $"Failed to load dictionary: {string.Join(", ", loadResult.ErrorMessages.Take(5))}");
        }

        // 4) Purge old rules for this dictionary (single call if your repo supports it)

        await _ruleRepository.DeleteAllAsync(r => r.DictionaryId == id, Constants.Collections.WordSmithDictionaryRules);


        // 5) Insert new rules (bulk insert preferred → 1 call)
        var now = DateTime.UtcNow;
        var toInsert = new List<WordSmithDictionaryRule>();
        foreach (var kv in loadResult.ReplacementsDictionary)
        {
            var step = kv.Value;
            toInsert.Add(new WordSmithDictionaryRule
            {
                Id = Guid.NewGuid(),
                DictionaryId = id,                     // << keep SAME Id
                Words = step.Words,
                Replacement = step.Replacement,
                NewColumnName = step.NewColumnName,
                ToDelete = step.ToDelete,
                Priority = step.Priority,
                IsActive = true,
                CreatedAt = now,
                ModifiedAt = now
            });
        }

        await _ruleRepository.BulkInsertAsync(toInsert, Constants.Collections.WordSmithDictionaryRules);


        // 6) Update dictionary metadata (single update)
        dictionary.Name = string.IsNullOrWhiteSpace(meta?.Name) ? dictionary.Name : meta.Name;
        dictionary.Description = meta?.Description ?? dictionary.Description;
        dictionary.Category = string.IsNullOrWhiteSpace(meta?.Category) ? dictionary.Category : meta.Category;
        dictionary.OriginalFileName = fileName;
        dictionary.OriginalFilePath = filePath;
        dictionary.Version++;

        await _dictionaryRepository.UpdateAsync(dictionary, Constants.Collections.WordSmithDictionary);

        // 7) Build DTO
        return new WordSmithDictionaryDto
        {
            Id = dictionary.Id,                       // same Id preserved
            Name = dictionary.Name,
            Description = dictionary.Description,
            Category = dictionary.Category,
            OriginalFileName = dictionary.OriginalFileName,
            Version = dictionary.Version,
            IsActive = dictionary.IsActive,
            CreatedAt = dictionary.CreatedAt,
            ModifiedAt = dictionary.ModifiedAt
        };
    }

    public async Task<WordSmithDictionaryDto> GetDictionaryAsync(Guid id)
    {
        var dictionary = await _dictionaryRepository.GetByIdAsync(id, Constants.Collections.WordSmithDictionary);
        if (dictionary == null) return null;

        // Get rule statistics
        var rules = await _ruleRepository.GetAllAsync(Constants.Collections.WordSmithDictionaryRules);
        var dictionaryRules = rules.Where(r => r.DictionaryId == id && r.IsActive).ToList();

        var extractedColumns = dictionaryRules
            .Where(r => !string.IsNullOrEmpty(r.NewColumnName))
            .Select(r => r.NewColumnName)
            .Distinct()
            .ToList();

        return new WordSmithDictionaryDto
        {
            Id = dictionary.Id,
            Name = dictionary.Name,
            Description = dictionary.Description,
            Category = dictionary.Category,
            OriginalFileName = dictionary.OriginalFileName,
            Version = dictionary.Version,
            TotalRules = dictionaryRules.Count,
            ReplacementRules = dictionaryRules.Count(r => !string.IsNullOrEmpty(r.Replacement) && !r.ToDelete),
            DeletionRules = dictionaryRules.Count(r => r.ToDelete),
            NewColumnRules = dictionaryRules.Count(r => !string.IsNullOrEmpty(r.NewColumnName)),
            ExtractedColumns = extractedColumns,
            IsActive = dictionary.IsActive,
            CreatedAt = dictionary.CreatedAt,
            ModifiedAt = dictionary.ModifiedAt
        };
    }

    public async Task<List<WordSmithDictionaryDto>> GetAllDictionariesAsync()
    {
        var dictionaries = await _dictionaryRepository.GetAllAsync(Constants.Collections.WordSmithDictionary);
        var allRules = await _ruleRepository.GetAllAsync(Constants.Collections.WordSmithDictionaryRules);

        var result = new List<WordSmithDictionaryDto>();

        foreach (var dict in dictionaries.Where(d => d.IsActive))
        {
            var dictRules = allRules.Where(r => r.DictionaryId == dict.Id && r.IsActive).ToList();

            result.Add(new WordSmithDictionaryDto
            {
                Id = dict.Id,
                Name = dict.Name,
                Description = dict.Description,
                Category = dict.Category,
                OriginalFileName = dict.OriginalFileName,
                Version = dict.Version,
                TotalRules = dictRules.Count,
                ReplacementRules = dictRules.Count(r => !string.IsNullOrEmpty(r.Replacement) && !r.ToDelete),
                DeletionRules = dictRules.Count(r => r.ToDelete),
                NewColumnRules = dictRules.Count(r => !string.IsNullOrEmpty(r.NewColumnName)),
                ExtractedColumns = dictRules
                    .Where(r => !string.IsNullOrEmpty(r.NewColumnName))
                    .Select(r => r.NewColumnName)
                    .Distinct()
                    .ToList(),
                IsActive = dict.IsActive,
                CreatedAt = dict.CreatedAt,
                ModifiedAt = dict.ModifiedAt
            });
        }

        return result;
    }

    public async Task<bool> DeleteDictionaryAsync(Guid id)
    {
        var dictionary = await _dictionaryRepository.GetByIdAsync(id, Constants.Collections.WordSmithDictionary);
        if (dictionary == null) return false;

        // Soft delete dictionary
        dictionary.IsActive = false;
        await _dictionaryRepository.UpdateAsync(dictionary, Constants.Collections.WordSmithDictionary);

        // Soft delete all rules
        var rules = await _ruleRepository.GetAllAsync(Constants.Collections.WordSmithDictionaryRules);
        var dictionaryRules = rules.Where(r => r.DictionaryId == id).ToList();

        foreach (var rule in dictionaryRules)
        {
            rule.IsActive = false;
            rule.ModifiedAt = DateTime.UtcNow;
            await _ruleRepository.UpdateAsync(rule, Constants.Collections.WordSmithDictionaryRules);
        }

        _logger.LogInformation("Dictionary {Name} and {RuleCount} rules soft deleted",
            dictionary.Name, dictionaryRules.Count);

        return true;
    }

    public async Task<(List<WordSmithRuleDto> rules, int totalCount)> GetDictionaryRulesAsync(
        Guid dictionaryId, int page = 1, int pageSize = 50)
    {
        var allRules = await _ruleRepository.GetAllAsync(Constants.Collections.WordSmithDictionaryRules);
        var dictionaryRules = allRules
            .Where(r => r.DictionaryId == dictionaryId && r.IsActive)
            .OrderBy(r => r.Priority)
            .ThenBy(r => r.Words)
            .ToList();

        var totalCount = dictionaryRules.Count;

        var pagedRules = dictionaryRules
            .Skip((page - 1) * pageSize)
            .Take(pageSize)
            .Select(r => new WordSmithRuleDto
            {
                Id = r.Id,
                Words = r.Words,
                Replacement = r.Replacement,
                NewColumnName = r.NewColumnName,
                ToDelete = r.ToDelete,
                Priority = r.Priority,
                IsActive = r.IsActive,
                ModifiedAt = r.ModifiedAt
            })
            .ToList();

        return (pagedRules, totalCount);
    }

    public async Task<WordSmithRuleDto> GetRuleAsync(Guid ruleId)
    {
        var rule = await _ruleRepository.GetByIdAsync(ruleId, Constants.Collections.WordSmithDictionaryRules);
        if (rule == null) return null;

        return new WordSmithRuleDto
        {
            Id = rule.Id,
            Words = rule.Words,
            Replacement = rule.Replacement,
            NewColumnName = rule.NewColumnName,
            ToDelete = rule.ToDelete,
            Priority = rule.Priority,
            IsActive = rule.IsActive,
            ModifiedAt = rule.ModifiedAt
        };
    }

    public async Task<WordSmithRuleDto> UpdateRuleAsync(
        Guid ruleId,
        UpdateWordSmithRuleDto updateDto,
        string userId = null)
    {
        var rule = await _ruleRepository.GetByIdAsync(ruleId, Constants.Collections.WordSmithDictionaryRules);
        if (rule == null)
            throw new KeyNotFoundException($"Rule {ruleId} not found");

        bool hasChanges = false;

        if (updateDto.Replacement != null && rule.Replacement != updateDto.Replacement)
        {
            rule.Replacement = updateDto.Replacement;
            hasChanges = true;
        }

        if (updateDto.NewColumnName != null && rule.NewColumnName != updateDto.NewColumnName)
        {
            rule.NewColumnName = updateDto.NewColumnName;
            hasChanges = true;
        }

        if (updateDto.ToDelete.HasValue && rule.ToDelete != updateDto.ToDelete.Value)
        {
            rule.ToDelete = updateDto.ToDelete.Value;
            hasChanges = true;
        }

        if (updateDto.Priority.HasValue && rule.Priority != updateDto.Priority.Value)
        {
            rule.Priority = updateDto.Priority.Value;
            hasChanges = true;
        }

        if (updateDto.IsActive.HasValue && rule.IsActive != updateDto.IsActive.Value)
        {
            rule.IsActive = updateDto.IsActive.Value;
            hasChanges = true;
        }

        if (hasChanges)
        {
            rule.ModifiedAt = DateTime.UtcNow;
            rule.ModifiedBy = userId ?? "system";
            await _ruleRepository.UpdateAsync(rule, Constants.Collections.WordSmithDictionaryRules);

            // Update dictionary version
            await IncrementDictionaryVersionAsync(rule.DictionaryId);
        }

        return new WordSmithRuleDto
        {
            Id = rule.Id,
            Words = rule.Words,
            Replacement = rule.Replacement,
            NewColumnName = rule.NewColumnName,
            ToDelete = rule.ToDelete,
            Priority = rule.Priority,
            IsActive = rule.IsActive,
            ModifiedAt = rule.ModifiedAt
        };
    }

    public async Task<WordSmithRuleDto> AddRuleAsync(
        Guid dictionaryId,
        CreateWordSmithRuleDto createDto,
        string userId = null)
    {
        var dictionary = await _dictionaryRepository.GetByIdAsync(dictionaryId, Constants.Collections.WordSmithDictionary);
        if (dictionary == null)
            throw new KeyNotFoundException($"Dictionary {dictionaryId} not found");

        var rule = new WordSmithDictionaryRule
        {
            Id = Guid.NewGuid(),
            DictionaryId = dictionaryId,
            Words = createDto.Words,
            Replacement = createDto.Replacement ?? string.Empty,
            NewColumnName = createDto.NewColumnName ?? string.Empty,
            ToDelete = createDto.ToDelete,
            Priority = createDto.Priority,
            IsActive = true,
            CreatedAt = DateTime.UtcNow,
            ModifiedBy = userId ?? "system"
        };

        await _ruleRepository.InsertAsync(rule, Constants.Collections.WordSmithDictionaryRules);
        await IncrementDictionaryVersionAsync(dictionaryId);

        return new WordSmithRuleDto
        {
            Id = rule.Id,
            Words = rule.Words,
            Replacement = rule.Replacement,
            NewColumnName = rule.NewColumnName,
            ToDelete = rule.ToDelete,
            Priority = rule.Priority,
            IsActive = rule.IsActive,
            ModifiedAt = rule.ModifiedAt
        };
    }

    public async Task<bool> DeleteRuleAsync(Guid ruleId)
    {
        var rule = await _ruleRepository.GetByIdAsync(ruleId, Constants.Collections.WordSmithDictionaryRules);
        if (rule == null) return false;

        rule.IsActive = false;
        rule.ModifiedAt = DateTime.UtcNow;
        await _ruleRepository.UpdateAsync(rule, Constants.Collections.WordSmithDictionaryRules);
        await IncrementDictionaryVersionAsync(rule.DictionaryId);

        return true;
    }

    public async Task<int> BulkUpdateRulesAsync(
        Guid dictionaryId,
        List<(Guid ruleId, UpdateWordSmithRuleDto update)> updates,
        string userId = null)
    {
        int updatedCount = 0;

        foreach (var (ruleId, update) in updates)
        {
            try
            {
                await UpdateRuleAsync(ruleId, update, userId);
                updatedCount++;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to update rule {RuleId}", ruleId);
            }
        }

        if (updatedCount > 0)
        {
            await IncrementDictionaryVersionAsync(dictionaryId);
        }

        return updatedCount;
    }

    public async Task<(Dictionary<string, ReplacingStep> replacements, Dictionary<string, ReplacingStep> newColumns)>
        BuildDictionariesAsync(Guid dictionaryId)
    {
        var allRules = await _ruleRepository.GetAllAsync(Constants.Collections.WordSmithDictionaryRules);
        var activeRules = allRules
            .Where(r => r.DictionaryId == dictionaryId && r.IsActive)
            .ToList();

        var replacementDict = new Dictionary<string, ReplacingStep>(StringComparer.OrdinalIgnoreCase);
        var newColumnDict = new Dictionary<string, ReplacingStep>(StringComparer.OrdinalIgnoreCase);

        foreach (var rule in activeRules)
        {
            var replacingStep = new ReplacingStep
            {
                Words = rule.Words,
                Replacement = rule.Replacement ?? string.Empty,
                NewColumnName = rule.NewColumnName,
                ToDelete = rule.ToDelete,
                Priority = rule.Priority
            };

            // Add to replacement dictionary (contains ALL rules)
            replacementDict[rule.Words.ToLower()] = replacingStep;

            // Add to new column dictionary if it creates a new column
            if (!string.IsNullOrEmpty(rule.NewColumnName))
            {
                newColumnDict[rule.Words.ToLower()] = replacingStep;
            }
        }

        _logger.LogInformation(
            "Built dictionaries for {DictionaryId}: {ReplacementCount} replacements, {NewColumnCount} new columns",
            dictionaryId, replacementDict.Count, newColumnDict.Count);

        return (replacementDict, newColumnDict);
    }

    public async Task<Stream> ExportDictionaryAsync(Guid dictionaryId, string format = "tsv")
    {
        var allRules = await _ruleRepository.GetAllAsync(Constants.Collections.WordSmithDictionaryRules);
        var rules = allRules
            .Where(r => r.DictionaryId == dictionaryId && r.IsActive)
            .OrderBy(r => r.Priority)
            .ThenBy(r => r.Words)
            .ToList();

        var stream = new MemoryStream();
        var writer = new StreamWriter(stream, Encoding.Unicode);

        // Write header
        var separator = format.ToLower() == "csv" ? "," : "\t";
        await writer.WriteLineAsync($"Words{separator}Replacement{separator}NewColumn{separator}ToDelete{separator}Priority{separator}Count");

        // Write rules
        foreach (var rule in rules)
        {
            var toDeleteValue = rule.ToDelete ? "TRUE" : "";
            await writer.WriteLineAsync(
                $"{rule.Words}{separator}" +
                $"{rule.Replacement ?? ""}{separator}" +
                $"{rule.NewColumnName ?? ""}{separator}" +
                $"{toDeleteValue}{separator}" +
                $"{rule.Priority}{separator}" +
                $"1");
        }

        await writer.FlushAsync();
        stream.Position = 0;
        return stream;
    }

    private async Task IncrementDictionaryVersionAsync(Guid dictionaryId)
    {
        var dictionary = await _dictionaryRepository.GetByIdAsync(
            dictionaryId,
            Constants.Collections.WordSmithDictionary);

        if (dictionary != null)
        {
            dictionary.Version++;
            await _dictionaryRepository.UpdateAsync(
                dictionary,
                Constants.Collections.WordSmithDictionary);
        }
    }

    /// <summary>
    /// Refresh dictionary from data source - core implementation
    /// </summary>
    public async Task<WordSmithDictionaryDto> RefreshDictionaryFromDataAsync(
        Guid dictionaryId,
        RefreshDictionaryRequest request,
        CancellationToken cancellationToken = default)
    {
        try
        {
            _logger.LogInformation(
                "Refreshing dictionary {DictionaryId} from column {Column}",
                dictionaryId, request.ColumnName);

            // 1. Verify dictionary exists
            var dictionary = await _dictionaryRepository.GetByIdAsync(
                dictionaryId,
                Constants.Collections.WordSmithDictionary);

            if (dictionary == null || !dictionary.IsActive)
            {
                throw new InvalidOperationException($"Dictionary {dictionaryId} not found or inactive");
            }

            // 2. Get existing rules with modifications (non-empty rows)
            var existingModifiedRules = await GetNonEmptyRulesAsync(dictionaryId);

            _logger.LogInformation(
                "Found {Count} existing rules with user modifications",
                existingModifiedRules.Count);

            // 3. Clear all current rules
            await _ruleRepository.DeleteAllAsync(
                r => r.DictionaryId == dictionaryId,
                Constants.Collections.WordSmithDictionaryRules);
            var dataSource = await _dataStore.GetByIdAsync<Domain.Project.DataSource, Guid>(request.DataSourceId, Constants.Collections.DataSources);
            // 4. Extract words from data
            var wordFrequencies = await ExtractWordsFromDataAsync(
                dataSource.ActiveSnapshotId.GetValueOrDefault(),
                request.ColumnName,
                request.Separators,
                request.MaxWordCount,
                request.IncludeFullText,
                request.IgnoreCase,
                cancellationToken);

            _logger.LogInformation(
                "Extracted {Count} unique words from data",
                wordFrequencies.Count);

            // 5. Build new rules, preserving modifications
            var newRules = BuildRulesWithPreservedModifications(
                dictionaryId,
                wordFrequencies,
                existingModifiedRules);

            // 6. Insert new rules
            if (newRules.Any())
            {
                await _ruleRepository.BulkInsertAsync(
                    newRules,
                    Constants.Collections.WordSmithDictionaryRules);
            }

            // 7. Update dictionary metadata
            dictionary.Version++;

            await _dictionaryRepository.UpdateAsync(
                dictionary,
                Constants.Collections.WordSmithDictionary);

            _logger.LogInformation(
                "Dictionary refresh complete: {Total} rules ({New} new, {Preserved} preserved)",
                newRules.Count,
                newRules.Count - existingModifiedRules.Count,
                existingModifiedRules.Count);

            return await GetDictionaryAsync(dictionaryId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error refreshing dictionary {DictionaryId}", dictionaryId);
            throw;
        }
    }

    /// <summary>
    /// Extract words from data source with streaming
    /// </summary>
    private async Task<Dictionary<string, int>> ExtractWordsFromDataAsync(
        Guid dataSourceActiveSnapShotId,
        string columnName,
        string separators,
        int maxWordCount,
        bool includeFullText,
        bool ignoreCase,
        CancellationToken cancellationToken)
    {
        var wordFrequencies = new Dictionary<string, int>(
            ignoreCase ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal);

        var collectionName = DatasetNames.SnapshotRows(dataSourceActiveSnapShotId);
        var separatorChars = separators.ToCharArray();
        var recordCount = 0;
        var progressInterval = 10000;

        try
        {
            await foreach (var row in _dataStore.StreamDataAsync(collectionName, cancellationToken))
            {
                cancellationToken.ThrowIfCancellationRequested();

                recordCount++;

                if (recordCount % progressInterval == 0)
                {
                    _logger.LogDebug(
                        "Processed {Count} records, {Words} unique words",
                        recordCount, wordFrequencies.Count);
                }

                if (!row.TryGetValue(columnName, out var value) || value == null)
                {
                    continue;
                }

                var textValue = value.ToString();

                if (string.IsNullOrWhiteSpace(textValue))
                {
                    continue;
                }

                if (ignoreCase)
                {
                    textValue = textValue.ToLower();
                }

                var words = GetCombinedWords(
                    textValue,
                    maxWordCount,
                    ref separatorChars,
                    includeFullText);

                foreach (var word in words)
                {
                    if (string.IsNullOrWhiteSpace(word))
                    {
                        continue;
                    }

                    var key = ignoreCase ? word.ToLower() : word;
                    wordFrequencies[key] = wordFrequencies.GetValueOrDefault(key, 0) + 1;
                }
            }

            _logger.LogInformation(
                "Extraction complete: {Records} records, {Words} unique words",
                recordCount, wordFrequencies.Count);
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("Word extraction cancelled after {Count} records", recordCount);
            throw;
        }

        return wordFrequencies;
    }

    /// <summary>
    /// Get combined words (matches legacy StringHelper.GetCombinedWords)
    /// </summary>
    private string[] GetCombinedWords(
        string text,
        int maxWordCount,
        ref char[] separators,
        bool includeFullText)
    {
        if (separators == null || separators.Length == 0)
        {
            return new string[] { text };
        }

        string[] words = text.Split(separators, StringSplitOptions.RemoveEmptyEntries);

        if (words.Length <= 1)
        {
            return words;
        }

        int finalLength = words.Length;

        for (int collocationLength = 2; collocationLength <= maxWordCount; ++collocationLength)
        {
            if (collocationLength <= words.Length)
            {
                finalLength += words.Length - collocationLength + 1;
            }
        }

        if (includeFullText && words.Length > maxWordCount)
        {
            ++finalLength;
        }

        string[] combinedWords = new string[finalLength];

        if (includeFullText)
        {
            combinedWords[finalLength - 1] = text;
        }

        Array.Copy(words, combinedWords, words.Length);
        int emptyIndex = words.Length;

        for (int collocationLength = 2; collocationLength <= maxWordCount; ++collocationLength)
        {
            for (int firstWord = 0; firstWord <= words.Length - collocationLength; ++firstWord)
            {
                var collocation = new StringBuilder();

                for (int wordInCollocation = 0; wordInCollocation < collocationLength; ++wordInCollocation)
                {
                    collocation.Append(words[firstWord + wordInCollocation]);

                    if (wordInCollocation < collocationLength - 1)
                    {
                        collocation.Append(" ");
                    }
                }

                combinedWords[emptyIndex] = collocation.ToString();
                ++emptyIndex;
            }
        }

        return combinedWords;
    }

    /// <summary>
    /// Get rules with user modifications (non-empty rows)
    /// Matches legacy WordSmithVisualizator.GetNonEmptyRows()
    /// </summary>
    private async Task<Dictionary<string, WordSmithDictionaryRule>> GetNonEmptyRulesAsync(
        Guid dictionaryId)
    {
        var nonEmptyRules = new Dictionary<string, WordSmithDictionaryRule>(
            StringComparer.OrdinalIgnoreCase);

        var allRules = await _ruleRepository.GetAllAsync(
            Constants.Collections.WordSmithDictionaryRules);

        var dictionaryRules = allRules
            .Where(r => r.DictionaryId == dictionaryId && r.IsActive)
            .ToList();

        foreach (var rule in dictionaryRules)
        {
            bool hasModification =
                !string.IsNullOrEmpty(rule.Replacement) ||
                rule.ToDelete ||
                !string.IsNullOrEmpty(rule.NewColumnName);

            if (hasModification)
            {
                var key = rule.Words.ToLower();
                nonEmptyRules[key] = rule;
            }
        }

        return nonEmptyRules;
    }

    /// <summary>
    /// Build rules preserving user modifications
    /// Matches legacy WordSmithVisualizator.Initialize()
    /// </summary>
    private List<WordSmithDictionaryRule> BuildRulesWithPreservedModifications(
        Guid dictionaryId,
        Dictionary<string, int> wordFrequencies,
        Dictionary<string, WordSmithDictionaryRule> existingModifications)
    {
        var newRules = new List<WordSmithDictionaryRule>();
        var now = DateTime.UtcNow;

        var sortedWords = wordFrequencies
            .OrderByDescending(kvp => kvp.Value)
            .ThenBy(kvp => kvp.Key);

        foreach (var kvp in sortedWords)
        {
            var word = kvp.Key;
            var count = kvp.Value;
            var key = word.ToLower();

            if (existingModifications.TryGetValue(key, out var existingRule))
            {
                // Preserve existing modifications, update count
                newRules.Add(new WordSmithDictionaryRule
                {
                    Id = Guid.NewGuid(),
                    DictionaryId = dictionaryId,
                    Words = word,
                    Replacement = existingRule.Replacement,
                    NewColumnName = existingRule.NewColumnName,
                    ToDelete = existingRule.ToDelete,
                    Priority = existingRule.Priority,
                    Count = count,
                    IsActive = true,
                    CreatedAt = now,
                    ModifiedAt = now
                });
            }
            else
            {
                // New rule with defaults
                newRules.Add(new WordSmithDictionaryRule
                {
                    Id = Guid.NewGuid(),
                    DictionaryId = dictionaryId,
                    Words = word,
                    Replacement = string.Empty,
                    NewColumnName = string.Empty,
                    ToDelete = false,
                    Priority = 5,
                    Count = count,
                    IsActive = true,
                    CreatedAt = now,
                    ModifiedAt = now
                });
            }
        }

        return newRules;
    }

    /// <summary>
    /// Clear all rules from dictionary
    /// Matches legacy WordSmithControl.Clear()
    /// </summary>
    public async Task<bool> ClearDictionaryRulesAsync(Guid dictionaryId)
    {
        try
        {
            _logger.LogInformation("Clearing dictionary {DictionaryId}", dictionaryId);

            var dictionary = await _dictionaryRepository.GetByIdAsync(
                dictionaryId,
                Constants.Collections.WordSmithDictionary);

            if (dictionary == null || !dictionary.IsActive)
            {
                throw new InvalidOperationException($"Dictionary {dictionaryId} not found");
            }

            await _ruleRepository.DeleteAllAsync(
                r => r.DictionaryId == dictionaryId,
                Constants.Collections.WordSmithDictionaryRules);

            dictionary.Version++;

            await _dictionaryRepository.UpdateAsync(
                dictionary,
                Constants.Collections.WordSmithDictionary);

            _logger.LogInformation("Dictionary {DictionaryId} cleared", dictionaryId);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error clearing dictionary {DictionaryId}", dictionaryId);
            return false;
        }
    }

    /// <summary>
    /// Preview word extraction (limited records)
    /// </summary>
    public async Task<WordExtractionPreviewDto> PreviewWordExtractionAsync(
        PreviewExtractionRequest request,
        int maxRecords = 100)
    {
        try
        {
            var wordFrequencies = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            var sampleValues = new List<string>();
            var dataSource = await _dataStore.GetByIdAsync<Domain.Project.DataSource, Guid>(request.DataSourceId, Constants.Collections.DataSources);
            var collectionName = DatasetNames.SnapshotRows(dataSource.ActiveSnapshotId.GetValueOrDefault());
            var separatorChars = request.Separators.ToCharArray();
            var recordCount = 0;

            await foreach (var row in _dataStore.StreamDataAsync(collectionName))
            {
                if (recordCount >= request.MaxRecords)
                {
                    break;
                }

                recordCount++;

                if (!row.TryGetValue(request.ColumnName, out var value) || value == null)
                {
                    continue;
                }

                var textValue = value.ToString();

                if (string.IsNullOrWhiteSpace(textValue))
                {
                    continue;
                }

                if (sampleValues.Count < 10)
                {
                    sampleValues.Add(textValue);
                }

                if (request.IgnoreCase)
                {
                    textValue = textValue.ToLower();
                }

                var words = GetCombinedWords(
                    textValue,
                    request.MaxWordCount,
                    ref separatorChars,
                    request.IncludeFullText);

                foreach (var word in words)
                {
                    if (!string.IsNullOrWhiteSpace(word))
                    {
                        var key = request.IgnoreCase ? word.ToLower() : word;
                        wordFrequencies[key] = wordFrequencies.GetValueOrDefault(key, 0) + 1;
                    }
                }
            }

            var topWords = wordFrequencies
                .OrderByDescending(kvp => kvp.Value)
                .Take(50)
                .Select(kvp => new WordFrequencyDto
                {
                    Word = kvp.Key,
                    Count = kvp.Value,
                    CharacterLength = kvp.Key.Length,
                    IsExistingRule = false
                })
                .ToList();

            return new WordExtractionPreviewDto
            {
                TotalUniqueWords = wordFrequencies.Count,
                RecordsScanned = recordCount,
                TopWords = topWords,
                SampleValues = sampleValues,
                EstimatedTotalWords = (int)(wordFrequencies.Count * 1.5) // Rough estimate
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error previewing extraction");
            throw;
        }
    }

    public async Task<WordSmithDictionaryDto> CreateDictionaryFromColumnAsync(
    CreateDictionaryFromColumnRequest request,
    CancellationToken cancellationToken = default)
    {
        var dictionary = new WordSmithDictionary
        {
            Id = Guid.NewGuid(),
            Name = request.DictionaryName,
            Description = request.Description ?? $"Extracted from {request.ColumnName}",
            Category = request.Category,
            OriginalFileName = $"{request.ColumnName}_{DateTime.UtcNow:yyyyMMdd}.txt",
            Version = 1,
            IsActive = true
        };

        await _dictionaryRepository.InsertAsync(dictionary, Constants.Collections.WordSmithDictionary);
        var dataSource = await _dataStore.GetByIdAsync<Domain.Project.DataSource,Guid>(request.DataSourceId, Constants.Collections.DataSources);
        var wordFrequencies = await ExtractWordsFromDataAsync(
            dataSource.ActiveSnapshotId.GetValueOrDefault(),
            request.ColumnName,
            request.Separators,
            request.MaxWordCount,
            request.IncludeFullText,
            request.IgnoreCase,
            cancellationToken);

        var rules = BuildRulesWithPreservedModifications(
            dictionary.Id,
            wordFrequencies,
            new Dictionary<string, WordSmithDictionaryRule>(StringComparer.OrdinalIgnoreCase));

        if (rules.Any())
        {
            await _ruleRepository.BulkInsertAsync(rules, Constants.Collections.WordSmithDictionaryRules);
        }

        return await GetDictionaryAsync(dictionary.Id);
    }
    private Encoding ResolveEncoding(string requested, string filePath)
    {
        if (!string.IsNullOrWhiteSpace(requested))
            return Encoding.GetEncoding(requested);
        return EncodingDetector.Detect(filePath);
    }
}
