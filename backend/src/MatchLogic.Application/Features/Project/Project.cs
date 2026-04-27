using MatchLogic.Domain.Project;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.Project;

public record ColumnMappingRequest(
  string SourceColumn,
  string TargetColumn,
  bool Include = true,
  string DataType = null,
  string Format = null,
  string DefaultValue = null);

public record DataSourcePreviewResult(
    List<IDictionary<string, object>> Data,
    long RowCount,
    long DuplicateHeaderCount,
    List<string>? ErrorMessages);

public record DataSourceMetadata(
   List<TableInfo> Tables,  
   Dictionary<string, ColumnMapping> ColumnMappings);


