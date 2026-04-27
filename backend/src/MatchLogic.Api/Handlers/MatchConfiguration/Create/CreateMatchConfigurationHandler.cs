using MatchLogic.Application.Interfaces.MatchConfiguration;
using MatchLogic.Domain.MatchConfiguration;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading;
using Mapster;

namespace MatchLogic.Api.Handlers.MatchConfiguration.Create;
public class CreateMatchConfigurationHandler(IMatchConfigurationService matchConfigurationService, ILogger<CreateMatchConfigurationHandler> logger)
    : IRequestHandler<CreateMatchConfigurationRequest, Result<BaseMatchConfigurationResponse>>
{
    public async Task<Result<BaseMatchConfigurationResponse>> Handle(CreateMatchConfigurationRequest request, CancellationToken cancellationToken)
    {
        logger.LogInformation("Create Match Configuration for ProjectId: {ProjectId}", request.ProjectId);
        // Map the request to MatchingDataSourcePair
        var pairs = request.Pairs.Adapt<List<MatchingDataSourcePair>>();
        // Create Match Configuration
        var dataSourcePairs = new MatchingDataSourcePairs(pairs)
        {
            ProjectId = request.ProjectId // Set the ProjectId for the MatchingDataSourcePairs
        };
        var result = await matchConfigurationService.UpdateDataSourcePairsAsync(request.ProjectId, dataSourcePairs);
        return Result<BaseMatchConfigurationResponse>.Success(new BaseMatchConfigurationResponse(result));
    }
}

