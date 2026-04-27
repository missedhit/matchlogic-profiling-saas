using MatchLogic.Application.Interfaces.Regex;
using Microsoft.Extensions.Logging;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.RegexInfo.ResetSystemDefaults;

public class ResetRegexHandler(IRegexInfoService regexInfoService, ILogger<ResetRegexHandler> logger) : IRequestHandler<ResetRegexRequest, Result<ResetRegexResponse>>
{
    public async Task<Result<ResetRegexResponse>> Handle(ResetRegexRequest request, CancellationToken cancellationToken)
    {
        logger.LogInformation("Resetting system defaults for Regex Info");
        await regexInfoService.ResetSystemDefaults();
        return Result<ResetRegexResponse>.Success(new ResetRegexResponse(true, "Successfully reset Regex Info to system defaults"));
    }
}