using MatchLogic.Domain.Project;
using System;
using System.Collections.Generic;
namespace MatchLogic.Api.Handlers.DataSource.Refresh;
public record RefreshDataSourceRequest(
     Guid ProjectId,
    Guid DataSourceId,
    Guid FileImportId
    ) : IRequest<Result<RefreshDataSourceResponse>>;
