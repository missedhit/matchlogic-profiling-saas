using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;

namespace MatchLogic.Api.Handlers.DataSource.TestConnection;
public record TestConnectionRequest(BaseConnectionInfo Connection) : IRequest<Result<TestConnectionResponse>>;