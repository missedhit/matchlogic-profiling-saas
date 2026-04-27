using MatchLogic.Application.Interfaces.Regex;
using System.Threading.Tasks;
using System.Threading;

namespace MatchLogic.Api.Handlers.RegexInfo.Create;

public class CreateRegexInfoHandler(IRegexInfoService _regexInfoService) : IRequestHandler<CreateRegexInfoRequest, Result<CreateRegexInfoResponse>>
{
    public async Task<Result<CreateRegexInfoResponse>> Handle(CreateRegexInfoRequest request, CancellationToken cancellationToken)
    {
        var createdPattern = await _regexInfoService.CreateRegexInfo(request.Name, request.Description, request.RegexExpression,request.IsDefault);
        return Result<CreateRegexInfoResponse>.Success(new CreateRegexInfoResponse()
        {
            Id = createdPattern.Id,
            Name = createdPattern.Name,
            Description = createdPattern.Description,
            RegexExpression = createdPattern.RegexExpression,
            IsDefault = createdPattern.IsDefault,
            IsSystem = createdPattern.IsSystem,
            IsSystemDefault = createdPattern.IsSystemDefault,
            Version = createdPattern.Version                
        });
    }
}