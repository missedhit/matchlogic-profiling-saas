using System.Threading;
using System.Threading.Tasks;
using MatchLogic.Api.Handlers.DataSource.Base;
using MatchLogic.Application.Features.Project;
using MatchLogic.Application.Interfaces.Storage;
using MatchLogic.Domain.Project;
using Microsoft.Extensions.Logging;

namespace MatchLogic.Api.Handlers.DataSource.Preview.Data;
public class PreviewDataHandler(
    IDataSourceService dataSourceService,
    IFileSourceResolver fileSourceResolver,
    ILogger<PreviewDataHandler> logger)
    : IRequestHandler<PreviewDataRequest, Result<PreviewDataResponse>>
{
    public async Task<Result<PreviewDataResponse>> Handle(PreviewDataRequest request, CancellationToken cancellationToken)
    {
        logger.LogInformation("PreviewDataHandler: Configuring ConnectionInfo with : {Request}", request);

        var (connection, lease) = await ConnectionInfoConfigurator.ConfigureAsync(
            request.Connection, fileSourceResolver, cancellationToken);
        await using (lease)
        {
            var ds = new Domain.Project.DataSource
            {
                Type = request.Connection.Type,
                ConnectionDetails = connection,
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
}
