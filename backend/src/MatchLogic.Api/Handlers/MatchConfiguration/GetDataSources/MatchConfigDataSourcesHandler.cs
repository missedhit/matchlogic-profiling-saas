using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Persistence;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.MatchConfiguration.GetDataSources;

public class MatchConfigDataSourcesHandler(IGenericRepository<Domain.Project.DataSource, Guid> _dataSourceRepository,ILogger<MatchConfigDataSourcesHandler> logger) : IRequestHandler<MatchConfigDataSourcesRequest, Result<List<MatchConfigDataSourcesResponse>>>
{
    public async Task<Result<List<MatchConfigDataSourcesResponse>>> Handle(MatchConfigDataSourcesRequest request, CancellationToken cancellationToken)
    {
        var dataSources = await _dataSourceRepository.QueryAsync(x => x.ProjectId == request.ProjectId, Constants.Collections.DataSources).ConfigureAwait(false);

        if (dataSources == null || dataSources.Count == 0)
        {
            logger.LogError("No data sources found for project ID: {ProjectId}", request.ProjectId);
            return Result<List<MatchConfigDataSourcesResponse>>.NotFound($"No data sources found for project ID: {request.ProjectId}.");
        }

        var responseList = new List<MatchConfigDataSourcesResponse>(dataSources.Count);
        foreach (var item in dataSources)
        {
            responseList.Add(new MatchConfigDataSourcesResponse(item.Id, item.Name));
        }

        return Result<List<MatchConfigDataSourcesResponse>>.Success(responseList);
    }
}
