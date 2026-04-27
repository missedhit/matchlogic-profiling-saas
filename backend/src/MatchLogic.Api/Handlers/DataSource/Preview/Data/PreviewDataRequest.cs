using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using System.Collections.Generic;
namespace MatchLogic.Api.Handlers.DataSource.Preview.Data;
public record PreviewDataRequest(
    string? TableName,   
    BaseConnectionInfo Connection,
    Dictionary<string, ColumnMapping> ColumnMappings,
    string? Query = null
) : IRequest<Result<PreviewDataResponse>>;

