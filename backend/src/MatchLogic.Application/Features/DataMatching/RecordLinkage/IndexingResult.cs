using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.RecordLinkage
{
    public class IndexingResult
    {
        public Guid DataSourceId { get; init; }
        public string DataSourceName { get; init; }
        public int ProcessedRecords { get; set; }
        public List<string> IndexedFields { get; init; } = new();
        public TimeSpan IndexingDuration { get; set; }
        public long StorageSizeBytes { get; set; }
        public bool UsedDiskStorage { get; set; }
        public DateTime CompletedAt { get; init; } = DateTime.UtcNow;
    }
}
