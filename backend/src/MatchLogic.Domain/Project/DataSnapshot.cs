using MatchLogic.Domain.Entities.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Domain.Project;

public class DataSnapshot : IEntity
{
    public Guid ProjectId { get; set; }
    public Guid DataSourceId { get; set; }

    public DateTime CreatedDate { get; set; }
    public string? Watermark { get; set; }          // JSON (file/db)
    public string SchemaSignature { get; set; } = string.Empty;

    public long RecordCount { get; set; }
    public long ColumnsCount { get; set; }

    // For file refresh traceability
    public Guid? FileImportId { get; set; }

    // Storage pointer (today LiteDB collections, tomorrow parquet/duckdb etc.)
    public string StoragePrefix { get; set; } = string.Empty;
}
