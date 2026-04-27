using MatchLogic.Api.Handlers.DataSource.Base;
using MatchLogic.Application.Features.Project;
using MatchLogic.Domain.Project;
using Microsoft.Extensions.Logging;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.DataSource.Preview.Columns;

public class PreviewColumnsHandler(IDataSourceService dataSourceService,ILogger<PreviewColumnsHandler> logger)
    : BaseConnectionInfoHandler, IRequestHandler<PreviewColumnsRequest, Result<PreviewColumnsResponse>>
{
    public async Task<Result<PreviewColumnsResponse>> Handle(PreviewColumnsRequest request, CancellationToken cancellationToken)
    {
        logger.LogInformation("PreviewColumnsHandler: Configuring ConnectionInfo with : {Request}", request);
        var ds = new Domain.Project.DataSource
        {
            Type = request.Connection.Type,
            ConnectionDetails = ConfigureConnectionInfo(request.Connection),
            Configuration = new DataSourceConfiguration {
                //TableOrSheet = request.TableName ?? string.Empty,
            }
        };

        var metaData = await dataSourceService.GetMetadataAsync(ds, cancellationToken);
        return Result<PreviewColumnsResponse>.Success(new PreviewColumnsResponse(metaData));
    }
}