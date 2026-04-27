using MatchLogic.Application.Interfaces.MatchConfiguration;
using Microsoft.Extensions.Logging;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.MatchConfiguration.Get;

public class GetMatchConfigurationHandler(IMatchConfigurationService matchConfigurationService,ILogger<GetMatchConfigurationHandler> logger) : IRequestHandler<GetMatchConfigurationRequest, Result<BaseMatchConfigurationResponse>>
{
    public async Task<Result<BaseMatchConfigurationResponse>> Handle(GetMatchConfigurationRequest request, CancellationToken cancellationToken)
    {
        logger.LogInformation("Handling GetMatchConfigurationRequest for ProjectId: {ProjectId}", request.ProjectId);
        var result = await matchConfigurationService.GetDataSourcePairsByProjectIdAsync(request.ProjectId);
        return Result<BaseMatchConfigurationResponse>.Success(new BaseMatchConfigurationResponse(result));
    }
}