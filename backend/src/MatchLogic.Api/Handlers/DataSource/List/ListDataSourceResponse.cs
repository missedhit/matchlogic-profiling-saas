using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using System;
namespace MatchLogic.Api.Handlers.DataSource.List;
public record ListDataSourceResponse(Guid Id, string Name, string SourceType, long Size, long RecordCount, long ColumnsCount, long ValidCount, long InvalidCount, string[] ErrorMessages, DateTime CreatedAt, DateTime? ModifiedAt);
