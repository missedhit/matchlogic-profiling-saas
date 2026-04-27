using MatchLogic.Domain.Project;
using System;
using System.Collections.Generic;
namespace MatchLogic.Api.Handlers.DataSource.Create;
public record CreateDataSourceRequest(
    Guid ProjectId,
    BaseConnectionInfo Connection,
    List<DataSourceRequest> DataSources
    ) : IRequest<Result<CreateDataSourceResponse>>;

public record DataSourceRequest(
    string? Name,   
    string TableName,    
    Dictionary<string, ColumnMapping> ColumnMappings,
    string? Query = null);
