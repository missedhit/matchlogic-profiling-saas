using MatchLogic.Application.Features.CleansingAndStandardization.DTOs;
using MatchLogic.Application.Interfaces.Cleansing;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;
using System.Threading;
using System;

namespace MatchLogic.Api.Handlers.Cleansing.WordSmith.Refresh;

public class RefreshDictionaryHandler : IRequestHandler<RefreshDictionaryCommand, Result<WordSmithDictionaryDto>>
{
    private readonly IWordSmithDictionaryService _dictionaryService;
    private readonly ILogger<RefreshDictionaryHandler> _logger;

    public RefreshDictionaryHandler(
        IWordSmithDictionaryService dictionaryService,
        ILogger<RefreshDictionaryHandler> logger)
    {
        _dictionaryService = dictionaryService;
        _logger = logger;
    }

    public async Task<Result<WordSmithDictionaryDto>> Handle(
        RefreshDictionaryCommand request,
        CancellationToken cancellationToken)
    {
        try
        {
            _logger.LogInformation(
                "Starting dictionary refresh for {DictionaryId} from column {Column}",
                request.DictionaryId, request.ColumnName);

            var refreshRequest = new RefreshDictionaryRequest
            {
                ProjectId = request.ProjectId,
                DataSourceId = request.DataSourceId,
                ColumnName = request.ColumnName,
                Separators = request.Separators,
                MaxWordCount = request.MaxWordCount,
                IncludeFullText = request.IncludeFullText,
                IgnoreCase = request.IgnoreCase
            };

            var dictionary = await _dictionaryService.RefreshDictionaryFromDataAsync(
                request.DictionaryId.GetValueOrDefault(),
                refreshRequest,
                cancellationToken);

            _logger.LogInformation(
                "Dictionary {DictionaryId} refreshed successfully with {Count} rules",
                request.DictionaryId, dictionary.TotalRules);

            return Result<WordSmithDictionaryDto>.Success(dictionary);
        }
        catch (InvalidOperationException ex)
        {
            _logger.LogWarning(ex, "Invalid operation during dictionary refresh");
            return Result<WordSmithDictionaryDto>.Error(ex.Message);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error refreshing dictionary {DictionaryId}", request.DictionaryId);
            return Result<WordSmithDictionaryDto>.Error($"Failed to refresh dictionary: {ex.Message}");
        }
    }
}
