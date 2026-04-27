using MatchLogic.Application.Features.CleansingAndStandardization.DTOs;
using MatchLogic.Application.Interfaces.Cleansing;
using System.Threading.Tasks;
using System.Threading;
using MatchLogic.Application.Interfaces.Persistence;
using System.Collections.Generic;

namespace MatchLogic.Api.Handlers.Cleansing.WordSmith.Get;

public class GetDictionaryRulesQueryHandler : IRequestHandler<GetDictionaryRulesQuery, Result<WordSmithRulesResponse>>
{
    private readonly IWordSmithDictionaryService _service;
    private readonly IDataStore _store;

    public GetDictionaryRulesQueryHandler(IWordSmithDictionaryService service, IDataStore dataStore)
    {
        _service = service;
        
    }

    public async Task<Result<WordSmithRulesResponse>> Handle(GetDictionaryRulesQuery request, CancellationToken cancellationToken)
    {
        var (rules, totalCount) = await _service.GetDictionaryRulesAsync(
            request.DictionaryId,
            request.Page,
            request.PageSize);

        

        return Result<WordSmithRulesResponse>.Success(new WordSmithRulesResponse(rules, totalCount));
    }
}
