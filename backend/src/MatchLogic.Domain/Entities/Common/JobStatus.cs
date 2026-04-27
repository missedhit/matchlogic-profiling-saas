using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Domain.Entities.Common;
public class JobStatus : AuditableEntity
{
    public Guid JobId { get; set; }
    public int ProcessedRecords { get; set; }
    public int TotalRecords { get; set; }
    public string Status { get; set; }  // "Processing", "Completed", "Failed"
    public string Error { get; set; }
    public DateTime StartTime { get; set; }
    public DateTime? EndTime { get; set; }      
    public Dictionary<string, JobStepInfo> Steps { get; set; } = new();
    public Dictionary<string, object> Metadata { get; set; } = new();    

    public string DataSourceName { get; set; }
    // Add FlowStatistics
    public FlowStatistics Statistics { get; set; } = new FlowStatistics();
}

public class JobStepInfo
{
    public string StepKey { get; set; }
    public string StepName { get; set; }
    public int StepNumber { get; set; }
    public int TotalSteps { get; set; }
    public int ProcessedItems { get; set; }
    public string Message { get; set; }
    public int TotalItems { get; set; }
    public string Status { get; set; }
    public string Error { get; set; }
    public DateTime StartTime { get; set; }
    public DateTime? EndTime { get; set; }

}