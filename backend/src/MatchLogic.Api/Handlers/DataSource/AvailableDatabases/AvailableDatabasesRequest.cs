using MatchLogic.Domain.Project;
using System.Collections.Generic;

namespace MatchLogic.Api.Handlers.DataSource.AvailableDatabases;

public record AvailableDatabasesRequest(BaseConnectionInfo Connection) : IRequest<Result<List<string>>>;
