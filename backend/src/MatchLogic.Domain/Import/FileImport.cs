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
}
