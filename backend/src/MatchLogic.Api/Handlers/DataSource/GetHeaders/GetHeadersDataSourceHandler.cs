using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Common;
using MatchLogic.Application.Interfaces.Persistence;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.DataSource.GetHeaders;

public class GetHeadersDataSourceHandler(IHeaderUtility headerUtility, IGenericRepository<Domain.Project.DataSource, Guid> genericRepository)
    : IRequestHandler<GetHeadersDataSourceRequest, Result<List<string>>>
{
    public async Task<Result<List<string>>> Handle(GetHeadersDataSourceRequest request, CancellationToken cancellationToken)
    {
        var dataSource = await genericRepository.GetByIdAsync(request.Id, Constants.Collections.DataSources);
        var headers = await headerUtility.GetHeadersAsync(dataSource, request.fetchCleanse);
        return Result<List<string>>.Success(headers);
    }
}
