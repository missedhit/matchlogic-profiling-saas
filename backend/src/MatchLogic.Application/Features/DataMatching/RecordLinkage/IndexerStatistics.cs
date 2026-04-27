using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.RecordLinkage
{
    public class IndexerStatistics
    {
        public int TotalDataSources { get; set; }
        public int TotalRecords { get; set; }
        public int TotalIndexedFields { get; set; }
        public long IndexSizeBytes { get; set; }
        public long TotalStorageSizeBytes { get; set; }
        public List<DataSourceStats> DataSources { get; set; } = new();
        public DateTime GeneratedAt { get; set; } = DateTime.UtcNow;

        public string IndexSizeMB => $"{IndexSizeBytes / (1024.0 * 1024.0):F1} MB";
        public string TotalStorageSizeMB => $"{TotalStorageSizeBytes / (1024.0 * 1024.0):F1} MB";
    }
}
