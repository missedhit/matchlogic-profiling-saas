using MatchLogic.Application.Features.Project;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.DataSource.TestConnection;

public class TestConnectionHandler(IDataSourceService dataSourceService) : IRequestHandler<TestConnectionRequest, Result<TestConnectionResponse>>
{
    public async Task<Result<TestConnectionResponse>> Handle(TestConnectionRequest request, CancellationToken cancellationToken)
    {
        var result = await dataSourceService.TestConnectionAsync(new Domain.Project.DataSource() { ConnectionDetails = request.Connection,Type =  request.Connection.Type}, cancellationToken);
        return result.Success ?
            Result<TestConnectionResponse>.Success(new TestConnectionResponse(result.Success,result.Message)) :
            Result<TestConnectionResponse>.Error(result.Message);
    }
}
