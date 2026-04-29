using MatchLogic.Api.Handlers.DataSource.Base;
using MatchLogic.Application.Features.Project;
using MatchLogic.Application.Interfaces.Storage;
using MatchLogic.Domain.Project;
using Microsoft.Extensions.Logging;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.DataSource.Preview.Columns;

public class PreviewColumnsHandler(
    IDataSourceService dataSourceService,
    IFileSourceResolver fileSourceResolver,
    ILogger<PreviewColumnsHandler> logger)
    : IRequestHandler<PreviewColumnsRequest, Result<PreviewColumnsResponse>>
{
    public async Task<Result<PreviewColumnsResponse>> Handle(PreviewColumnsRequest request, CancellationToken cancellationToken)
    {
        logger.LogInformation("PreviewColumnsHandler: Configuring ConnectionInfo with : {Request}", request);

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
                    //TableOrSheet = request.TableName ?? string.Empty,
                }
            };

            var metaData = await dataSourceService.GetMetadataAsync(ds, cancellationToken);
            return Result<PreviewColumnsResponse>.Success(new PreviewColumnsResponse(metaData));
        }
    }
}
