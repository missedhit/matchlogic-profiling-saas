using MatchLogic.Application.Interfaces.Regex;
using Mapster;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;

namespace MatchLogic.Api.Handlers.RegexInfo.List;
public class ListRegexInfoHandler(IRegexInfoService regexInfoService,ILogger<ListRegexInfoHandler> logger) : IRequestHandler<ListRegexInfoRequest, Result<List<RegexInfoDTO>>>
{
    public async Task<Result<List<RegexInfoDTO>>> Handle(ListRegexInfoRequest request, CancellationToken cancellationToken)
    {
        var regexPatternsAll = await regexInfoService.GetAllRegexInfo();
        var regexPatterns = regexPatternsAll.Where(x => !x.IsDeleted).ToList();
        if (regexPatterns == null || regexPatterns.Count == 0)
        {
            logger.LogInformation("No regex patterns found");
            return Result<List<RegexInfoDTO>>.NotFound("No regex patterns found.");
        }
        // Return success with the list of regex patterns
        var response = regexPatterns.Adapt<List<RegexInfoDTO>>();
        return Result<List<RegexInfoDTO>>.Success(response);
    }
}
