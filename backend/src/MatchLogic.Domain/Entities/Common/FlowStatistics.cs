using System;
using System.Collections.Generic;

namespace MatchLogic.Domain.Entities.Common;

public class FlowStatistics
{
    public int RecordsProcessed { get; set; }
    public int ErrorRecords { get; set; }
    public int BatchesProcessed { get; set; }
    public int TransformationsApplied { get; set; }
    public DateTime StartTime { get; set; }
    public DateTime? EndTime { get; private set; }
    public TimeSpan Duration => (EndTime ?? DateTime.UtcNow) - StartTime;
    public Dictionary<string, int> ErrorCategories { get; set; } = new();
    public string OperationType { get; set; }

    public double RecordsPerSecond => Duration.TotalSeconds > 0
        ? RecordsProcessed / Duration.TotalSeconds
        : 0;

    public void RecordError(string category = "General")
    {
        ErrorRecords++;
        if (!ErrorCategories.ContainsKey(category))
        {
            ErrorCategories[category] = 0;
        }
        ErrorCategories[category]++;
    }

    public void MarkComplete()
    {
        EndTime = DateTime.UtcNow;
    }
}
