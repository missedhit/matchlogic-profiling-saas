using System;
using System.Collections.Generic;

namespace MatchLogic.Api.Handlers.DataSource.List;

public record ListDataSourceRequest(Guid ProjectId) : IRequest<Result<List<ListDataSourceResponse>>>;

