using MatchLogic.Api.Handlers.DataSource.Base;
using MatchLogic.Application.Interfaces.Storage;
using Microsoft.Extensions.Logging;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.DataSource.Preview.Tables;

public class PreviewTablesHandler(
    Application.Features.Import.IConnectionBuilder connectionBuilder,
    IFileSourceResolver fileSourceResolver,
    ILogger<PreviewTablesHandler> logger)
    : IRequestHandler<PreviewTablesRequest, Result<PreviewTablesResponse>>
{
    public async Task<Result<PreviewTablesResponse>> Handle(PreviewTablesRequest request, CancellationToken cancellationToken)
    {
        logger.LogInformation("PreviewTablesHandler: Configuring ConnectionInfo with : {Request}", request);

        var (connection, lease) = await ConnectionInfoConfigurator.ConfigureAsync(
            request.Connection, fileSourceResolver, cancellationToken);
        await using (lease)
        {
            var conn = connectionBuilder
                .WithArgs(connection.Type, connection.Parameters)
                .Build();
            return Result<PreviewTablesResponse>.Success(new PreviewTablesResponse(await conn.GetAvailableTables()));
        }
    }
}
