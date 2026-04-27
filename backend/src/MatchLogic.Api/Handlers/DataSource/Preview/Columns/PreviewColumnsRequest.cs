using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
namespace MatchLogic.Api.Handlers.DataSource.Preview.Columns;
public record PreviewColumnsRequest(BaseConnectionInfo Connection) : IRequest<Result<PreviewColumnsResponse>>;
