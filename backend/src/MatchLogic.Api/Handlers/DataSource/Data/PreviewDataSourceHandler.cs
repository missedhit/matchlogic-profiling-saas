using MatchLogic.Api.Common;
using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Import;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.DataSource.Data;
public class PreviewDataSourceHandler(IDataStore dataStore) : IRequestHandler<PreviewDataSourceRequest, Result<PreviewDataSourceResponse>>
{
    public async Task<Result<PreviewDataSourceResponse>> Handle(PreviewDataSourceRequest request, CancellationToken cancellationToken)
    {
        var dataSource = await dataStore.GetByIdAsync<Domain.Project.DataSource, Guid>(request.Id, Constants.Collections.DataSources);
        var collectionName = DatasetNames.SnapshotRows(dataSource.ActiveSnapshotId.Value);

        var data = await dataStore.GetPagedJobWithSortingAndFilteringDataAsync(
            collectionName: collectionName,
            pageNumber: request.PageNumber,
            pageSize :request.PageSize,
            filterText: request.FilterText,
            sortColumn: request.SortColumn,
            ascending: request.Ascending
        );
        return Result<PreviewDataSourceResponse>.Success(new PreviewDataSourceResponse(data.Data, data.TotalCount));
    }
}
