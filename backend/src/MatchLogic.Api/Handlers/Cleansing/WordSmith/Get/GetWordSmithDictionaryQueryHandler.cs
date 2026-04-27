using MatchLogic.Application.Features.CleansingAndStandardization.DTOs;
using MatchLogic.Application.Interfaces.Cleansing;
using System.Threading.Tasks;
using System.Threading;

namespace MatchLogic.Api.Handlers.Cleansing.WordSmith.Get;

public class GetWordSmithDictionaryQueryHandler : IRequestHandler<GetWordSmithDictionaryQuery, Result<WordSmithDictionaryDto>>
{
    private readonly IWordSmithDictionaryService _service;

    public GetWordSmithDictionaryQueryHandler(IWordSmithDictionaryService service)
    {
        _service = service;
    }

    public async Task<Result<WordSmithDictionaryDto>> Handle(GetWordSmithDictionaryQuery request, CancellationToken cancellationToken)
    {
        var dictionary = await _service.GetDictionaryAsync(request.Id);
        if (dictionary == null)
            return Result<WordSmithDictionaryDto>.NotFound("Dictionary not found");

        return Result<WordSmithDictionaryDto>.Success(dictionary);
    }
}