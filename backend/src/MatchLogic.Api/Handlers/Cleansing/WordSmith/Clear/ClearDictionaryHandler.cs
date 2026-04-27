using MatchLogic.Application.Interfaces.Cleansing;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;
using System.Threading;
using System;

namespace MatchLogic.Api.Handlers.Cleansing.WordSmith.Clear;

public class ClearDictionaryHandler : IRequestHandler<ClearDictionaryCommand, Result<bool>>
{
    private readonly IWordSmithDictionaryService _dictionaryService;
    private readonly ILogger<ClearDictionaryHandler> _logger;

    public ClearDictionaryHandler(
        IWordSmithDictionaryService dictionaryService,
        ILogger<ClearDictionaryHandler> logger)
    {
        _dictionaryService = dictionaryService;
        _logger = logger;
    }

    public async Task<Result<bool>> Handle(
        ClearDictionaryCommand request,
        CancellationToken cancellationToken)
    {
        try
        {
            _logger.LogInformation("Clearing dictionary {DictionaryId}", request.DictionaryId);

            var success = await _dictionaryService.ClearDictionaryRulesAsync(request.DictionaryId);

            if (success)
            {
                _logger.LogInformation("Dictionary {DictionaryId} cleared successfully", request.DictionaryId);
                return Result<bool>.Success(true);
            }
            else
            {
                return Result<bool>.Error("Failed to clear dictionary rules");
            }
        }
        catch (InvalidOperationException ex)
        {
            _logger.LogWarning(ex, "Invalid operation during dictionary clear");
            return Result<bool>.Error(ex.Message);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error clearing dictionary {DictionaryId}", request.DictionaryId);
            return Result<bool>.Error($"Failed to clear dictionary: {ex.Message}");
        }
    }
}
