using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.DataProfiling;
using MatchLogic.Domain.Import;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;
using System.Threading;
using System;
using MatchLogic.Api.Common;
using System.Linq;

namespace MatchLogic.Api.Handlers.DataProfile.AdvanceStatisticAnalysis;
public class AdvanceStatisticAnalysisHandler(IGenericRepository<AdvancedProfileResult, Guid> profileResultRepository,ILogger<AdvanceStatisticAnalysisHandler> logger) : IRequestHandler<AdvanceStatisticAnalysisRequest, Result<AdvanceStatisticAnalysisResponse>>
{
    public async Task<Result<AdvanceStatisticAnalysisResponse>> Handle(AdvanceStatisticAnalysisRequest request, CancellationToken cancellationToken)
    {
        var dataSourceId = request.DataSourceId;
        var profileCollectionName = StepType.AdvanceProfile.ToCollectionName(dataSourceId);

        var profileList = await profileResultRepository.QueryAsync(x => x.DataSourceId == dataSourceId, profileCollectionName);
        if (profileList == null || profileList.Count == 0)
        {
            logger.LogError("No profile data found for DataSourceId: {DataSourceId}", dataSourceId);
            return Result<AdvanceStatisticAnalysisResponse>.NotFound($"No profile data found for DataSourceId: {dataSourceId}");
        }
        var profileResult = profileList.First();
        profileResult.RowReferenceDocumentIds = []; // We dont need this we can empty it
        return Result<AdvanceStatisticAnalysisResponse>.Success(new AdvanceStatisticAnalysisResponse(profileResult));
    }
}
