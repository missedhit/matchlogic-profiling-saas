using MatchLogic.Api.Common;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.DataProfiling;
using MatchLogic.Domain.Import;
using Microsoft.Extensions.Logging;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.DataProfile.StatisticAnalysis;

public class StatisticAnalysisHandler(IGenericRepository<ProfileResult, Guid> profileResultRepository,ILogger<StatisticAnalysisHandler> logger) : IRequestHandler<StatisticAnalysisRequest, Result<StatisticAnalysisResponse>>
{
    public async Task<Result<StatisticAnalysisResponse>> Handle(StatisticAnalysisRequest request, CancellationToken cancellationToken)
    {
        var dataSourceId = request.DataSourceId;
        var profileCollectionName = StepType.Profile.ToCollectionName(dataSourceId);
        // Query the profile result using the data source ID
        var profileList = await profileResultRepository.QueryAsync(x => x.DataSourceId == dataSourceId, profileCollectionName);
        if (profileList == null || profileList.Count == 0)
        {
            logger.LogError("No profile data found for DataSourceId: {DataSourceId}", dataSourceId);
            //return Result<StatisticAnalysisResponse>.Invalid(new ValidationError("DataSourceId", "No profile data found for the given Data Source ID."));
            return Result<StatisticAnalysisResponse>.NotFound($"No profile data found for DataSourceId: {dataSourceId}");
        }
        var profileResult = profileList.First();

        return Result<StatisticAnalysisResponse>.Success(new StatisticAnalysisResponse(profileResult));
    }
}
