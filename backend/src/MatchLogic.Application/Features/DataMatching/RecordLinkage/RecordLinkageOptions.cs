using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.RecordLinkage;
public class RecordLinkageOptions
{
    public int MaxDegreeOfParallelism { get; set; } = Environment.ProcessorCount;

    public int BlockSize { get; set; } = 10000;

    public int BatchSize { get; set; } = 5000;

    public int BufferSize { get; set; } = 100000;

    public int MaxMemoryMB { get; set; } = 1024;

    public int MaxRecordsPerBatch { get; set; } = 100000;
    
    public int MaxPairsPerRecord { get; set; } = 500;
    public double? MinimumMatchScore { get; set; } = 0.0;
}
