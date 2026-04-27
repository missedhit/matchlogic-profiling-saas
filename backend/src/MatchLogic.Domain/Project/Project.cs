using MatchLogic.Domain.Entities.Common;
using MatchLogic.Domain.Import;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Domain.Project;
public class Project : AuditableEntity
{
    public string Name { get; set; }
    public string Description { get; set; }
    public DateTime? LastRunDate { get; set; }
    public int DataSourceCount { get; set; } = 0;
    public List<StepType> CompletedSteps { get; set; } = new();
    public StepType LastRunStep { get; set; }
    public int RetentionRuns { get; set; } = 2;    
}

public class ProjectRun : AuditableEntity
{    
    public Guid PreviousRunId { get; set; }
    public Guid ProjectId { get; set; }
    public DateTime StartTime { get; set; }
    public DateTime? EndTime { get; set; }
    public RunStatus Status { get; set; }
    public int RunNumber { get; set; }
    public Dictionary<Guid, string> DataImportResult { get; set; }

    /// <summary>
    /// Link to scheduled task execution (if triggered by scheduler)
    /// Enables callback to update scheduler statistics
    /// </summary>
    public Guid? ScheduledTaskExecutionId { get; set; }
}

public class StepJob : AuditableEntity
{    
    public Guid RunId { get; set; }
    public StepType Type { get; set; }
    public DateTime StartTime { get; set; }
    public DateTime? EndTime { get; set; }
    public RunStatus Status { get; set; }
    public Guid? DataSourceId { get; set; }
    
    public Dictionary<string, object> Configuration { get; set; } = new();
    public List<StepData> StepData { get; set; } = new();
}

public class StepData : IEntity
{    
    public Guid StepJobId { get; set; }
    public Guid DataSourceId { get; set; }
    public string CollectionName { get; set; }
    public string DataFormat { get; set; }

    public Dictionary<string, object> Metadata { get; set; }
}

