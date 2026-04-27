using System.Collections.Generic;

namespace MatchLogic.Api.Handlers.DataSource.Preview.Data;

public record PreviewDataResponse(List<IDictionary<string, object>> Data,long TotalRecords, long SameNameColumnsCount, string[] ErrorMessages);
