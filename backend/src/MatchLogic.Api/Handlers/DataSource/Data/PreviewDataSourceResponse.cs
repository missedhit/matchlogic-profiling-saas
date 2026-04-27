using System.Collections.Generic;

namespace MatchLogic.Api.Handlers.DataSource.Data;

public record PreviewDataSourceResponse(IEnumerable<IDictionary<string, object>> Data, int TotalCount);
