using MatchLogic.Application.Features.CleansingAndStandardization.DTOs;
using MatchLogic.Application.Interfaces.Cleansing;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading;

namespace MatchLogic.Api.Handlers.Cleansing.WordSmith.Get;

public class ListWordSmithDictionariesQueryHandler : IRequestHandler<ListWordSmithDictionariesQuery, Result<List<WordSmithDictionaryDto>>>
{
    private readonly IWordSmithDictionaryService _service;

    public ListWordSmithDictionariesQueryHandler(IWordSmithDictionaryService service)
    {
        _service = service;
    }

    public async Task<Result<List<WordSmithDictionaryDto>>> Handle(ListWordSmithDictionariesQuery request, CancellationToken cancellationToken)
    {
        var dictionaries = await _service.GetAllDictionariesAsync();
        return Result<List<WordSmithDictionaryDto>>.Success(dictionaries);
    }
}