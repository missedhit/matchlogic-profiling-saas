using MatchLogic.Application.Interfaces.Regex;
using Microsoft.Extensions.Logging;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.RegexInfo.Delete;

public class DeleteRegexInfoHandler(IRegexInfoService regexInfoService,ILogger<DeleteRegexInfoHandler> logger) : IRequestHandler<DeleteRegexInfoRequest, Result<DeleteRegexInfoResponse>>
{
    public async Task<Result<DeleteRegexInfoResponse>> Handle(DeleteRegexInfoRequest request, CancellationToken cancellationToken)
    {
        // Perform the delete operation
        await regexInfoService.DeleteRegexInfo(request.Id);
        logger.LogInformation("Regex pattern with ID {Id} deleted successfully", request.Id);
        return Result<DeleteRegexInfoResponse>.Success(new DeleteRegexInfoResponse(true, "Regex pattern deleted successfully."));
    }
}
