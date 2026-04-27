using MatchLogic.Domain.Entities.Common;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Domain.DataProfiling;

public class ProfileResult : IEntity
{    
    public string DataSourceName { get; set; }
    public Guid DataSourceId { get; set; }
    public DateTime ProfiledAt { get; set; }
    public long TotalRecords { get; set; }
    public virtual ConcurrentDictionary<string, ColumnProfile> ColumnProfiles { get; set; } = new();
    public TimeSpan ProfilingDuration { get; set; }

    // Store references to row data document IDs for cleanup
    public List<Guid> RowReferenceDocumentIds { get; set; } = new();

}
