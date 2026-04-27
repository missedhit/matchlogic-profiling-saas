using MatchLogic.Domain.CleansingAndStandaradization;
using MatchLogic.Domain.Entities.Common;
using MatchLogic.Domain.Import;
using MediatR;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.Events;
public abstract class BaseEvent : INotification
{
    public string Id { get; } = Guid.NewGuid().ToString();
    public DateTime Timestamp { get; } = DateTime.UtcNow;
}

public class JobEvent : BaseEvent
{
    public Guid JobId { get; set; }
    public string Status { get; set; }
    public string Message { get; set; }
    public int? ProcessedRecords { get; set; }      
    public int? TotalRecords { get; set; }
    public string Error { get; set; }
    public string StepKey { get; set; }
    public JobStepInfo CurrentStep { get; set; }
    public DateTime EventTime { get; set; } = DateTime.UtcNow;
    public Dictionary<string, object> Metadata { get; set; } = new();
    public Dictionary<string, JobStepInfo> Steps { get; set; } = new();

    public string DataSourceName { get; set; }
    public FlowStatistics Statistics { get; set; }
}

/// <summary>
/// Event raised when a ProjectRun completes (success or failure)
/// Uses MediatR notification pattern
/// </summary>
public class ProjectRunCompletedEvent : BaseEvent
{
    public Guid RunId { get; set; }
    public Guid ProjectId { get; set; }
    public RunStatus Status { get; set; }
    public DateTime CompletedAt { get; set; }

    /// <summary>
    /// Link to scheduled task execution (if this was a scheduled run)
    /// </summary>
    public Guid? ScheduledTaskExecutionId { get; set; }

    /// <summary>
    /// Error message if failed
    /// </summary>
    public string ErrorMessage { get; set; }

    /// <summary>
    /// IDs of failed steps
    /// </summary>
    public List<Guid> FailedStepIds { get; set; } = new();

    /// <summary>
    /// Total number of steps
    /// </summary>
    public int TotalSteps { get; set; }

    /// <summary>
    /// Number of completed steps
    /// </summary>
    public int CompletedSteps { get; set; }
}

#region Options
public class JobProgressOptions
{
    public int DefaultReportInterval { get; set; } = 100;
    public int DefaultBatchSize { get; set; } = 1000;
    public double EstimatedPairMultiplier { get; set; } = 0.1;
}
#endregion