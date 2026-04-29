using MatchLogic.Domain.Entities.Common;
using System;

namespace MatchLogic.Domain.Import;
public class FileImport : IEntity
{
    public Guid ProjectId { get; set; }
    public DataSourceType DataSourceType { get; set; }
    public string FileName { get; set; }
    public string OriginalName { get; set; }
    public string FilePath { get; set; }
    public long FileSize { get; set; }
    public string FileExtension { get; set; }
    public DateTime CreatedDate { get; set; }

    // M2: S3 object key (e.g. "uploads/<fileId>.csv"). Populated for files uploaded
    // via the presigned-PUT flow; empty for legacy local-disk uploads. Source of
    // truth for where to read the file when running import / preview / profile.
    public string S3Key { get; set; } = string.Empty;
}
