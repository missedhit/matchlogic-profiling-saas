using MatchLogic.Application.Features.CleansingAndStandardization.DTOs;
using MatchLogic.Application.Interfaces.Cleansing;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;
using System.Threading;
using System;

namespace MatchLogic.Api.Handlers.Cleansing.WordSmith.CreateFromColumn;

public class CreateDictionaryFromColumnHandler
    : IRequestHandler<CreateDictionaryFromColumnCommand, Result<WordSmithDictionaryDto>>
{
    private readonly IWordSmithDictionaryService _dictionaryService;
    private readonly ILogger<CreateDictionaryFromColumnHandler> _logger;

    public CreateDictionaryFromColumnHandler(
        IWordSmithDictionaryService dictionaryService,
        ILogger<CreateDictionaryFromColumnHandler> logger)
    {
        _dictionaryService = dictionaryService;
        _logger = logger;
    }

    public async Task<Result<WordSmithDictionaryDto>> Handle(
        CreateDictionaryFromColumnCommand request,
        CancellationToken cancellationToken)
    {
        try
        {
            var createRequest = new CreateDictionaryFromColumnRequest
            {
                DictionaryName = request.DictionaryName,
                Description = request.Description,
                Category = request.Category,
                ProjectId = request.ProjectId,
                DataSourceId = request.DataSourceId,
                ColumnName = request.ColumnName,
                Separators = request.Separators,
                MaxWordCount = request.MaxWordCount,
                IncludeFullText = request.IncludeFullText,
                IgnoreCase = request.IgnoreCase
            };

            var dictionary = await _dictionaryService.CreateDictionaryFromColumnAsync(
                createRequest, cancellationToken);

            return Result<WordSmithDictionaryDto>.Success(dictionary);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error creating dictionary from column");
            return Result<WordSmithDictionaryDto>.Error($"Failed: {ex.Message}");
        }
    }
}
