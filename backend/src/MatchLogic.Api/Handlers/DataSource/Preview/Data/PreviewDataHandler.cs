using System.Threading;
using System.Threading.Tasks;
using MatchLogic.Application.Features.Project;
using MatchLogic.Domain.Project;
using MatchLogic.Api.Handlers.DataSource.Base;
using Microsoft.Extensions.Logging;

namespace MatchLogic.Api.Handlers.DataSource.Preview.Data;
public class PreviewDataHandler(IDataSourceService dataSourceService,ILogger<PreviewDataHandler> logger) : BaseConnectionInfoHandler, IRequestHandler<PreviewDataRequest, Result<PreviewDataResponse>>
{
    public async Task<Result<PreviewDataResponse>> Handle(PreviewDataRequest request, CancellationToken cancellationToken)
    {
        logger.LogInformation("PreviewDataHandler: Configuring ConnectionInfo with : {Request}", request);

        var ds = new Domain.Project.DataSource
        {
            Type = request.Connection.Type,
            ConnectionDetails = ConfigureConnectionInfo(request.Connection),
            Configuration = new DataSourceConfiguration
            {
                TableOrSheet = request?.TableName,
                Query = request?.Query,
                ColumnMappings = request?.ColumnMappings,
            }
        };
        var metaData = await dataSourceService.PreviewDataSourceAsync(ds, cancellationToken);
        
        return Result<PreviewDataResponse>.Success(new PreviewDataResponse(
            metaData.Data,
            metaData.RowCount,
            metaData.DuplicateHeaderCount,
            metaData.ErrorMessages != null ? [.. metaData.ErrorMessages] : []
            ));
        
    }
}
