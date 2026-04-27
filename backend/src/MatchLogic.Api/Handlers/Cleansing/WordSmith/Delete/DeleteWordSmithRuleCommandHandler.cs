using MatchLogic.Application.Interfaces.Cleansing;
using System.Threading.Tasks;
using System.Threading;

namespace MatchLogic.Api.Handlers.Cleansing.WordSmith.Delete;

public class DeleteWordSmithRuleCommandHandler : IRequestHandler<DeleteWordSmithRuleCommand, Result<bool>>
{
    private readonly IWordSmithDictionaryService _service;

    public DeleteWordSmithRuleCommandHandler(IWordSmithDictionaryService service)
    {
        _service = service;
    }

    public async Task<Result<bool>> Handle(DeleteWordSmithRuleCommand request, CancellationToken cancellationToken)
    {
        var result = await _service.DeleteRuleAsync(request.RuleId);
        return result
            ? Result<bool>.Success(true)
            : Result<bool>.NotFound("Rule not found");
    }
}
