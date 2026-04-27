using MatchLogic.Application.Features.CleansingAndStandardization.DTOs;
using MatchLogic.Domain.CleansingAndStandaradization;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.Cleansing;
public interface IWordSmithDictionaryService
{
    // Dictionary management
    Task<WordSmithDictionaryDto> UploadDictionaryAsync(Stream fileStream, string fileName, UploadWordSmithDictionaryDto request);
    Task<WordSmithDictionaryDto> ReplaceDictionaryAsync(Guid id, Stream file, string fileName, UploadWordSmithDictionaryDto meta);
    Task<WordSmithDictionaryDto> GetDictionaryAsync(Guid id);
    Task<List<WordSmithDictionaryDto>> GetAllDictionariesAsync();
    Task<bool> DeleteDictionaryAsync(Guid id);

    // Rule management
    Task<(List<WordSmithRuleDto> rules, int totalCount)> GetDictionaryRulesAsync(Guid dictionaryId, int page = 1, int pageSize = 50);
    Task<WordSmithRuleDto> GetRuleAsync(Guid ruleId);
    Task<WordSmithRuleDto> UpdateRuleAsync(Guid ruleId, UpdateWordSmithRuleDto updateDto, string userId = null);
    Task<WordSmithRuleDto> AddRuleAsync(Guid dictionaryId, CreateWordSmithRuleDto createDto, string userId = null);
    Task<bool> DeleteRuleAsync(Guid ruleId);
    Task<int> BulkUpdateRulesAsync(Guid dictionaryId, List<(Guid ruleId, UpdateWordSmithRuleDto update)> updates, string userId = null);

    // Dictionary building for processing
    Task<(Dictionary<string, ReplacingStep> replacements, Dictionary<string, ReplacingStep> newColumns)> BuildDictionariesAsync(Guid dictionaryId);

    // Export
    Task<Stream> ExportDictionaryAsync(Guid dictionaryId, string format = "tsv");

    /// <summary>
    /// Extracts words from data source column and creates/updates dictionary
    /// Preserves existing user modifications (replacements, new columns, deletions)
    /// </summary>
    Task<WordSmithDictionaryDto> RefreshDictionaryFromDataAsync(
    Guid dictionaryId,
    RefreshDictionaryRequest request,
    CancellationToken cancellationToken = default);

    Task<WordSmithDictionaryDto> CreateDictionaryFromColumnAsync(
    CreateDictionaryFromColumnRequest request,
    CancellationToken cancellationToken = default);

    Task<bool> ClearDictionaryRulesAsync(Guid dictionaryId);

    Task<WordExtractionPreviewDto> PreviewWordExtractionAsync(
        PreviewExtractionRequest request,
        int maxRecords = 100);
}
