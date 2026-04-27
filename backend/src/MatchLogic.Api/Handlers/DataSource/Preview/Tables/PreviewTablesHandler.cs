using MatchLogic.Api.Handlers.DataSource.Base;
using Microsoft.Extensions.Logging;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.DataSource.Preview.Tables;

public class PreviewTablesHandler(Application.Features.Import.IConnectionBuilder connectionBuilder, ILogger<PreviewTablesHandler> logger) : BaseConnectionInfoHandler, IRequestHandler<PreviewTablesRequest, Result<PreviewTablesResponse>>
{
    public async Task<Result<PreviewTablesResponse>> Handle(PreviewTablesRequest request, CancellationToken cancellationToken)
    {
        logger.LogInformation("PreviewTablesHandler: Configuring ConnectionInfo with : {Request}", request);
        var conn = connectionBuilder
            .WithArgs(request.Connection.Type, ConfigureConnectionInfo(request.Connection).Parameters)
            .Build();
        return Result<PreviewTablesResponse>.Success(new PreviewTablesResponse(await conn.GetAvailableTables()));
    }
}
