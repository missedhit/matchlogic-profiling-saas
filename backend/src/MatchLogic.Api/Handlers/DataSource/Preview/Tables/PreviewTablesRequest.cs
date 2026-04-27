using MatchLogic.Domain.Project;

namespace MatchLogic.Api.Handlers.DataSource.Preview.Tables;
public record PreviewTablesRequest(BaseConnectionInfo Connection) : IRequest<Result<PreviewTablesResponse>>;
