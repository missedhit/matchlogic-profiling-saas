using MatchLogic.Api.Common;
using MatchLogic.Api.Handlers.DataProfile.AdvanceStatisticAnalysis;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Import;
using Microsoft.Extensions.Logging;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.Cleansing.DataPreview;

public class DataPreviewCleansingHandler(IDataStore dataStore, ILogger<AdvanceStatisticAnalysisHandler> logger) : IRequestHandler<DataPreviewCleansingRequest, Result<DataPreviewCleansingResponse>>
{
    public async Task<Result<DataPreviewCleansingResponse>> Handle(DataPreviewCleansingRequest request, CancellationToken cancellationToken)
    {
        var dataSourceId = request.DataSourceId;
        var profileCollectionName = request.IsPreview.GetValueOrDefault(false) ? StepType.Cleanse.ToCollectionName(dataSourceId) + "_preview" : StepType.Cleanse.ToCollectionName(dataSourceId);
        var (Data, TotalCount) = await dataStore.GetPagedJobWithSortingAndFilteringDataAsync(
            collectionName: profileCollectionName,
            pageNumber: request.PageNumber,
            pageSize: request.PageSize,
            filterText: request.FilterText,
            sortColumn: request.SortColumn,
            ascending: request.Ascending
        );

        if (Data == null || !Data.Any())
        {
            logger.LogError("No cleansing data found for DataSourceId: {DataSourceId}", dataSourceId);
            return Result<DataPreviewCleansingResponse>.NotFound($"No cleansing data found for DataSourceId: {dataSourceId}");
        }
        return Result<DataPreviewCleansingResponse>.Success(new DataPreviewCleansingResponse(Data, TotalCount));
    }
}
