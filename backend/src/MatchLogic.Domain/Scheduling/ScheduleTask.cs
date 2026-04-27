using MatchLogic.Domain.Entities.Common;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using System;
using System.Collections.Generic;

namespace MatchLogic.Domain.Scheduling;

/// <summary>
/// Scheduled task definition - defines WHEN and WHAT to execute
/// Stored in MongoDB "ScheduledTasks" collection
/// </summary>
public class ScheduledTask : AuditableEntity
{
    #region Basic Information

    /// <summary>
    /// Foreign key to Project
    /// </summary>
    public Guid ProjectId { get; set; }

    /// <summary>
    /// User-friendly name (e.g., "Daily Customer Import")
    /// </summary>
    public string Name { get; set; }

    /// <summary>
    /// Optional description
    /// </summary>
    public string Description { get; set; }

    #endregion

    #region Schedule Configuration

    /// <summary>
    /// Type of schedule: Simple (interval), Cron (expression), or OnDemand (manual)
    /// </summary>
    public ScheduleType ScheduleType { get; set; }

    /// <summary>
    /// For ScheduleType.Cron: Cron expression (e.g., "0 9 * * MON-FRI")
    /// </summary>
    public string CronExpression { get; set; }

    /// <summary>
    /// For ScheduleType.Simple: Recurrence interval (e.g., TimeSpan.FromHours(2))
    /// </summary>
    public TimeSpan? RecurrenceInterval { get; set; }

    /// <summary>
    /// For ScheduleType.Simple: First execution time
    /// </summary>
    public DateTime? StartTime { get; set; }

    /// <summary>
    /// Timezone for cron execution (default: UTC)
    /// </summary>
    public string TimeZone { get; set; } = "UTC";

    #endregion

    #region Hangfire Integration

    /// <summary>
    /// Hangfire recurring job ID
    /// Format: "schedule_{ScheduleId}"
    /// </summary>
    public string HangfireJobId { get; set; }

    #endregion

    #region Execution Configuration

    /// <summary>
    /// Steps to execute when triggered (Import, Match, Export, etc.)
    /// </summary>
    public List<StepType> StepsToExecute { get; set; } = new();

    /// <summary>
    /// Configuration for each step type
    /// Key: StepType as string, Value: Configuration dictionary
    /// </summary>
    public Dictionary<string, object> StepConfigurations { get; set; } = new();
    /// <summary>
    /// Scheduler task export destination information
    /// </summary>
    public BaseConnectionInfo ConnectionInfo { get; set; }
    #endregion

    #region Export Options (Legacy Compatibility)

    /// <summary>
    /// Export cleaned/transformed data
    /// </summary>
    public bool ExportCleanedData { get; set; }

    /// <summary>
    /// Export match results (pairs and groups)
    /// </summary>
    public bool ExportMatchedData { get; set; }

    /// <summary>
    /// Export final formatted data (merged records)
    /// </summary>
    public bool ExportFormattedData { get; set; }

    /// <summary>
    /// Export data profiling statistics
    /// </summary>
    public bool ExportDataProfile { get; set; }

    /// <summary>
    /// Export summary report (PDF)
    /// </summary>
    public bool ExportSummaryReport { get; set; }

    /// <summary>
    /// Use bulk copy for database exports
    /// Legacy: BulkCopy
    /// </summary>
    public bool BulkCopy { get; set; } = true;

    /// <summary>
    /// Preserve input data types when exporting
    /// Legacy: PreserveInputTypes
    /// </summary>
    public bool PreserveInputTypes { get; set; }
    #endregion

    #region Status Tracking

    /// <summary>
    /// Enable/disable this schedule
    /// </summary>
    public bool IsEnabled { get; set; } = true;

    /// <summary>
    /// Current schedule status
    /// </summary>
    public ScheduleStatus Status { get; set; } = ScheduleStatus.Active;

    /// <summary>
    /// Last successful execution time
    /// CRITICAL: Prevents duplicate executions
    /// </summary>
    public DateTime? LastRun { get; set; }

    /// <summary>
    /// Calculated next execution time (indexed for query performance)
    /// </summary>
    public DateTime? NextRun { get; set; }

    /// <summary>
    /// Total number of times this task has been executed
    /// </summary>
    public int ExecutionCount { get; set; }

    /// <summary>
    /// Number of consecutive failures (reset on success)
    /// </summary>
    public int ConsecutiveFailures { get; set; }

    #endregion

    #region Statistics

    /// <summary>
    /// Total number of times this task has been executed
    /// Legacy: Not tracked (added for analytics)
    /// </summary>
    public int TotalExecutions { get; set; }

    /// <summary>
    /// Number of successful executions
    /// </summary>
    public int SuccessfulExecutions { get; set; }

    /// <summary>
    /// Number of failed executions
    /// </summary>
    public int FailedExecutions { get; set; }

    #endregion

    #region Notification Configuration

    /// <summary>
    /// Send notification on successful execution
    /// </summary>
    public bool NotifyOnSuccess { get; set; }

    /// <summary>
    /// Send notification on failure
    /// </summary>
    public bool NotifyOnFailure { get; set; } = true;

    /// <summary>
    /// Email addresses for notifications (comma-separated)
    /// </summary>
    public string NotificationEmails { get; set; }

    #endregion

    #region Retry Policy

    /// <summary>
    /// Enable automatic retry on failure (handled by Hangfire)
    /// </summary>
    public bool EnableRetry { get; set; } = true;

    /// <summary>
    /// Maximum retry attempts (default: 3)
    /// </summary>
    public int MaxRetryAttempts { get; set; } = 3;

    #endregion

    #region Metadata

    /// <summary>
    /// Tags for categorization/filtering
    /// </summary>
    public List<string> Tags { get; set; } = new();

    #endregion
}

// ============================================================================
// Enums
// ============================================================================

/// <summary>
/// Type of schedule
/// </summary>
public enum ScheduleType
{
    /// <summary>
    /// Simple interval-based schedule (e.g., every 2 hours)
    /// Maps to legacy RecurrencePeriod
    /// </summary>
    Simple = 0,

    /// <summary>
    /// Cron expression schedule (e.g., "0 9 * * MON")
    /// Maps to legacy XtraScheduler appointments
    /// </summary>
    Cron = 1,

    /// <summary>
    /// On-demand execution only (manual trigger)
    /// Maps to legacy StartWhenProjectFilesDetected + manual execution
    /// </summary>
    OnDemand = 2
}

/// <summary>
/// Schedule status
/// </summary>
public enum ScheduleStatus
{
    /// <summary>
    /// Schedule is active and will be processed
    /// </summary>
    Active = 0,

    /// <summary>
    /// Schedule is paused (temporarily disabled)
    /// </summary>
    Paused = 1,

    /// <summary>
    /// Schedule has been archived (soft delete)
    /// </summary>
    Archived = 2,

    /// <summary>
    /// Schedule has exceeded max failures and is suspended
    /// </summary>
    Suspended = 3,

    /// <summary>
    /// Schedule execution was cancelled by user
    /// </summary>
    Cancelled = 4
}
