using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.CleansingAndStandaradization;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.Cleansing.GetRules;

public class GetCleansingRulesHandler(IGenericRepository<EnhancedCleaningRules,Guid> genericRepository) : IRequestHandler<GetCleansingRulesRequest, Result<GetCleansingRulesResponse>>
{
    public async Task<Result<GetCleansingRulesResponse>> Handle(GetCleansingRulesRequest request, CancellationToken cancellationToken)
    {
        var resp = await genericRepository.QueryAsync(rule => rule.ProjectId == request.ProjectId && rule.DataSourceId == request.DataSourceId,
            Constants.Collections.CleaningRules);
        if (resp == null || resp.Count == 0)
        {
            return Result<GetCleansingRulesResponse>.NotFound($"No cleansing rules found for DataSourceId: {request.DataSourceId}");
        }
        return Result<GetCleansingRulesResponse>.Success(new GetCleansingRulesResponse(resp));

    }
}
