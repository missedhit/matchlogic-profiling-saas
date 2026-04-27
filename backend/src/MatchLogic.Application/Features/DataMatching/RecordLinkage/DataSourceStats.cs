using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.RecordLinkage
{
    public class DataSourceStats
    {
        public Guid DataSourceId { get; set; }
        public int RecordCount { get; set; }
        public long StorageSizeBytes { get; set; }
        public string StorageType { get; set; }
        public bool IsReadOnly { get; set; }

        public string StorageSizeMB => $"{StorageSizeBytes / (1024.0 * 1024.0):F1} MB";
    }
}
