using MatchLogic.Application.Interfaces.Cleansing;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.CleansingAndStandaradization;
using System.Threading.Tasks;
using System.Threading;
using System;
using MatchLogic.Application.Common;

namespace MatchLogic.Api.Handlers.Cleansing.WordSmith.Export;

public class ExportWordSmithDictionaryQueryHandler : IRequestHandler<ExportWordSmithDictionaryQuery, Result<ExportResult>>
{
    private readonly IWordSmithDictionaryService _service;
    private readonly IGenericRepository<WordSmithDictionary, Guid> _repository;

    public ExportWordSmithDictionaryQueryHandler(
        IWordSmithDictionaryService service,
        IGenericRepository<WordSmithDictionary, Guid> repository)
    {
        _service = service;
        _repository = repository;
    }

    public async Task<Result<ExportResult>> Handle(ExportWordSmithDictionaryQuery request, CancellationToken cancellationToken)
    {
        try
        {
            var dictionary = await _repository.GetByIdAsync(request.Id, Constants.Collections.WordSmithDictionary);
            if (dictionary == null)
                return Result<ExportResult>.NotFound("Dictionary not found");

            var stream = await _service.ExportDictionaryAsync(request.Id, request.Format);

            var result = new ExportResult
            {
                FileStream = stream,
                FileName = $"{dictionary.Name}_{DateTime.UtcNow:yyyyMMdd}.{request.Format}",
                ContentType = request.Format.ToLower() == "csv"
                    ? "text/csv"
                    : "text/tab-separated-values"
            };

            return Result<ExportResult>.Success(result);
        }
        catch (Exception ex)
        {
            return Result<ExportResult>.Error($"Failed to export dictionary: {ex.Message}");
        }
    }
}
