using MatchLogic.Domain.Project;
using System.Collections.Generic;

namespace MatchLogic.Api.Handlers.DataSource.Preview.Tables;

public record PreviewTablesResponse(List<TableInfo> Tables);
