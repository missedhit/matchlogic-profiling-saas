using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.RecordLinkage
{
    public class StorageStatistics
    {
        public int RecordCount { get; init; }
        public long TotalSizeBytes { get; init; }
        public bool IsReadOnly { get; init; }
        public string StorageType { get; init; }
        public TimeSpan AverageReadTime { get; set; }
        public long TotalReads { get; set; }
    }
}
