using MatchLogic.Application.Interfaces.Cleansing;
using System.Threading.Tasks;
using System.Threading;

namespace MatchLogic.Api.Handlers.Cleansing.WordSmith.Delete;

public class DeleteWordSmithDictionaryCommandHandler : IRequestHandler<DeleteWordSmithDictionaryCommand, Result<bool>>
{
    private readonly IWordSmithDictionaryService _service;

    public DeleteWordSmithDictionaryCommandHandler(IWordSmithDictionaryService service)
    {
        _service = service;
    }

    public async Task<Result<bool>> Handle(DeleteWordSmithDictionaryCommand request, CancellationToken cancellationToken)
    {
        var result = await _service.DeleteDictionaryAsync(request.Id);
        return result
            ? Result<bool>.Success(true)
            : Result<bool>.NotFound("Dictionary not found");
    }
}
