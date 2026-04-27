using MatchLogic.Domain.Entities.Common;
using MatchLogic.Domain.Import;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Domain.Scheduling;

/// <summary>
/// Execution history record for a scheduled task
/// Tracks each execution attempt with full details
/// Stored in MongoDB "ScheduledTaskExecutions" collection
/// </summary>
public class ScheduledTaskExecution : AuditableEntity
{
    #region References

    /// <summary>
    /// Foreign key to ScheduledTask
    /// </summary>
    public Guid ScheduledTaskId { get; set; }

    /// <summary>
    /// Foreign key to ProjectRun (if execution was successful)
    /// Links scheduled trigger to actual data processing run
    /// </summary>
    public Guid? ProjectRunId { get; set; }

    /// <summary>
    /// Hangfire job ID for this execution
    /// </summary>
    public string HangfireJobId { get; set; }

    #endregion

    #region Timing

    /// <summary>
    /// When this execution was scheduled to start
    /// </summary>
    public DateTime ScheduledTime { get; set; }

    /// <summary>
    /// Actual start time (may differ from scheduled due to system load)
    /// </summary>
    public DateTime? ActualStartTime { get; set; }

    /// <summary>
    /// Execution end time
    /// </summary>
    public DateTime? EndTime { get; set; }

    /// <summary>
    /// Execution duration in seconds
    /// </summary>
    public double? DurationSeconds =>
        EndTime.HasValue && ActualStartTime.HasValue
            ? (EndTime.Value - ActualStartTime.Value).TotalSeconds
            : null;

    #endregion

    #region Status & Results

    /// <summary>
    /// Execution status (reuse existing RunStatus)
    /// </summary>
    public RunStatus Status { get; set; }

    /// <summary>
    /// Trigger type for this execution
    /// </summary>
    public TriggerType TriggerType { get; set; }

    /// <summary>
    /// User who triggered manual execution (if TriggerType = Manual)
    /// </summary>
    public string TriggeredBy { get; set; }

    /// <summary>
    /// Detailed execution report (logs, errors, warnings)
    /// </summary>
    public string ExecutionReport { get; set; }

    /// <summary>
    /// Error message if failed
    /// </summary>
    public string ErrorMessage { get; set; }

    /// <summary>
    /// Stack trace if exception occurred
    /// </summary>
    public string StackTrace { get; set; }

    #endregion

    #region Statistics

    /// <summary>
    /// Aggregated execution statistics from all steps
    /// Contains: TotalRecordsProcessed, TotalErrors, TotalWarnings, StepBreakdowns, etc.
    /// Populated by SchedulerService.UpdateExecutionStatisticsAsync
    /// </summary>
    public Dictionary<string, object> Statistics { get; set; } = new();

    /// <summary>
    /// Number of records processed
    /// </summary>
    public int? RecordsProcessed { get; set; }

    /// <summary>
    /// Number of errors encountered
    /// </summary>
    public int? ErrorCount { get; set; }

    /// <summary>
    /// Number of warnings
    /// </summary>
    public int? WarningCount { get; set; }

    #endregion

    #region Retry Information

    /// <summary>
    /// Is this a retry attempt?
    /// </summary>
    public bool IsRetry { get; set; }

    /// <summary>
    /// Retry attempt number (0 = first attempt)
    /// </summary>
    public int RetryCount { get; set; }

    /// <summary>
    /// Reference to original execution (if this is a retry)
    /// </summary>
    public Guid? OriginalExecutionId { get; set; }

    #endregion

    #region Server Information

    /// <summary>
    /// Server/instance that executed this task
    /// </summary>
    public string ExecutedByServer { get; set; }

    /// <summary>
    /// Application version at execution time
    /// </summary>
    public string ApplicationVersion { get; set; }

    #endregion
}

// ============================================================================
// Enums
// ============================================================================

/// <summary>
/// How the execution was triggered
/// </summary>
public enum TriggerType
{
    /// <summary>
    /// Triggered by schedule (cron or simple interval)
    /// </summary>
    Scheduled = 0,

    /// <summary>
    /// Triggered manually by user
    /// </summary>
    Manual = 1,

    /// <summary>
    /// Triggered by API call (webhook)
    /// </summary>
    Api = 2,

    /// <summary>
    /// Triggered as a retry after failure
    /// </summary>
    Retry = 3
}
