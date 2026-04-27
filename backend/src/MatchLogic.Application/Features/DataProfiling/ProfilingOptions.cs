using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataProfiling
{
    public class ProfilingOptions
    {
        public int BatchSize { get; set; } = 1000;
        public int MaxDegreeOfParallelism { get; set; } = Environment.ProcessorCount;
        public int BufferSize { get; set; } = 10000;
        public int SampleSize { get; set; } = 100; // Number of sample values to store
        public int MaxRowsPerCategory { get; set; } = 50; // Max rows to store per category
        public int MaxDistinctValuesToTrack { get; set; } = 100; // Max distinct values to track rows for
        public bool StoreCompleteRows { get; set; } = true; // Whether to store complete row data
    }
}
