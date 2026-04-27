using MatchLogic.Application.Features.Import;
using MatchLogic.Application.Interfaces.Import;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.DataSource.AvailableDatabases;

public class AvailableDatabasesHandler(IConnectionBuilder connectionBuilder) : IRequestHandler<AvailableDatabasesRequest, Result<List<string>>>
{
    public async Task<Result<List<string>>> Handle(AvailableDatabasesRequest request, CancellationToken cancellationToken)
    {
        var connectionReader = connectionBuilder
            .WithArgs(request.Connection.Type,request.Connection.Parameters)
            .Build();

        if(connectionReader is not IDataBaseConnectionReaderStrategy dataBaseConnectionReaderStrategy)
        {
            return Result<List<string>>.Error($"The connection type '{request.Connection.Type}' does not support fetching available databases.");
        }
        var result = await dataBaseConnectionReaderStrategy.GetAvailableDatabasesAsync(cancellationToken);
        return Result<List<string>>.Success(result);
    }
}
