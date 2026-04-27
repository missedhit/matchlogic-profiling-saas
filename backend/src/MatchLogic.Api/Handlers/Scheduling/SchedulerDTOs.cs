// ============================================================================
// Location: MatchLogic.Application/Features/Scheduling/DTOs/SchedulerDtos.cs
// ============================================================================

using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using MatchLogic.Domain.Scheduling;
using System;
using System.Collections.Generic;

namespace MatchLogic.Api.Handlers.Scheduling.DTOs;

// ============================================================================
// Request DTOs
// ============================================================================

/// <summary>
/// DTO for creating a new schedule
/// </summary>
public class CreateScheduleDto
{
    public Guid ProjectId { get; set; }
    public string Name { get; set; }
    public string Description { get; set; }

    // Schedule Configuration
    public ScheduleType ScheduleType { get; set; }
    public string CronExpression { get; set; }
    public TimeSpan? RecurrenceInterval { get; set; }
    public DateTime? StartTime { get; set; }
    public string TimeZone { get; set; } = "UTC";

    // Execution Configuration
    public List<StepType> StepsToExecute { get; set; } = new();
    public Dictionary<string, object> StepConfigurations { get; set; } = new();

    public BaseConnectionInfo ConnectionInfo { get; set; }
    // Export Options
    public bool ExportCleanedData { get; set; }
    public bool ExportMatchedData { get; set; }
    public bool ExportFormattedData { get; set; }
    public bool ExportDataProfile { get; set; }
    public bool ExportSummaryReport { get; set; }

    // Notification Settings
    public bool NotifyOnSuccess { get; set; }
    public bool NotifyOnFailure { get; set; } = true;
    public string NotificationEmails { get; set; }

    // Retry Policy
    public bool EnableRetry { get; set; } = true;
    public int MaxRetryAttempts { get; set; } = 3;

    // Metadata
    public string CreatedBy { get; set; }
    public List<string> Tags { get; set; } = new();
}

/// <summary>
/// DTO for updating an existing schedule
/// </summary>
public class UpdateScheduleDto
{
    public string Name { get; set; }
    public string Description { get; set; }

    // Schedule Configuration
    public ScheduleType? ScheduleType { get; set; }
    public string CronExpression { get; set; }
    public TimeSpan? RecurrenceInterval { get; set; }
    public DateTime? StartTime { get; set; }
    public string TimeZone { get; set; }

    // Execution Configuration
    public List<StepType> StepsToExecute { get; set; }
    public Dictionary<string, object> StepConfigurations { get; set; }

    public BaseConnectionInfo ConnectionInfo { get; set; }

    // Export Options
    public bool? ExportCleanedData { get; set; }
    public bool? ExportMatchedData { get; set; }
    public bool? ExportFormattedData { get; set; }
    public bool? ExportDataProfile { get; set; }
    public bool? ExportSummaryReport { get; set; }

    // Notification Settings
    public bool? NotifyOnSuccess { get; set; }
    public bool? NotifyOnFailure { get; set; }
    public string NotificationEmails { get; set; }

    // Retry Policy
    public bool? EnableRetry { get; set; }
    public int? MaxRetryAttempts { get; set; }

    // Status
    public bool? IsEnabled { get; set; }

    // Metadata
    public string LastModifiedBy { get; set; }
    public List<string> Tags { get; set; }
}

/// <summary>
/// DTO for triggering manual execution
/// </summary>
public class TriggerScheduleDto
{
    public string TriggeredBy { get; set; }
}

/// <summary>
/// DTO for validating cron expression
/// </summary>
public class ValidateCronDto
{
    public string CronExpression { get; set; }
}

// ============================================================================
// Response DTOs
// ============================================================================

/// <summary>
/// DTO for schedule information
/// </summary>
public class ScheduleDto
{
    public Guid Id { get; set; }
    public Guid ProjectId { get; set; }
    public string Name { get; set; }
    public string Description { get; set; }

    // Schedule Configuration
    public ScheduleType ScheduleType { get; set; }
    public string CronExpression { get; set; }
    public TimeSpan? RecurrenceInterval { get; set; }
    public DateTime? StartTime { get; set; }
    public string TimeZone { get; set; }

    // Status
    public bool IsEnabled { get; set; }
    public ScheduleStatus Status { get; set; }
    public DateTime? LastRun { get; set; }
    public DateTime? NextRun { get; set; }
    public int ExecutionCount { get; set; }
    public int ConsecutiveFailures { get; set; }

    // Execution Configuration
    public List<StepType> StepsToExecute { get; set; }

    public BaseConnectionInfo ConnectionInfo { get; set; }
    // Export Options
    public bool ExportCleanedData { get; set; }
    public bool ExportMatchedData { get; set; }
    public bool ExportFormattedData { get; set; }
    public bool ExportDataProfile { get; set; }
    public bool ExportSummaryReport { get; set; }

    // Metadata
    public string CreatedBy { get; set; }
    public DateTime CreatedAt { get; set; }
    public string LastModifiedBy { get; set; }
    public DateTime? UpdatedAt { get; set; }
    public List<string> Tags { get; set; }
}

/// <summary>
/// DTO for execution history
/// </summary>
public class ExecutionDto
{
    public Guid Id { get; set; }
    public Guid ScheduledTaskId { get; set; }
    public Guid? ProjectRunId { get; set; }
    public string HangfireJobId { get; set; }

    public DateTime ScheduledTime { get; set; }
    public DateTime? ActualStartTime { get; set; }
    public DateTime? EndTime { get; set; }
    public double? DurationSeconds { get; set; }

    public RunStatus Status { get; set; }
    public TriggerType TriggerType { get; set; }
    public string TriggeredBy { get; set; }

    public string ErrorMessage { get; set; }
    public Dictionary<string, object> Statistics { get; set; }

    public int? RecordsProcessed { get; set; }
    public int? ErrorCount { get; set; }
    public int? WarningCount { get; set; }

    public string ExecutedByServer { get; set; }
}

/// <summary>
/// Response for create schedule operation
/// </summary>
public record CreateScheduleResponse(Guid ScheduleId, DateTime? NextRun);

/// <summary>
/// Response for update schedule operation
/// </summary>
public record UpdateScheduleResponse(Guid ScheduleId, DateTime? NextRun);

/// <summary>
/// Response for trigger schedule operation
/// </summary>
public record TriggerScheduleResponse(Guid ExecutionId, string HangfireJobId);

/// <summary>
/// Response for get schedules operation
/// </summary>
public record GetSchedulesResponse(List<ScheduleDto> Schedules);

/// <summary>
/// Response for get schedule by ID operation
/// </summary>
public record GetScheduleResponse(ScheduleDto Schedule);

/// <summary>
/// Response for get execution history operation
/// </summary>
public record GetExecutionHistoryResponse(List<ExecutionDto> Executions, int TotalCount);

/// <summary>
/// Response for validate cron operation
/// </summary>
public record ValidateCronResponse(bool IsValid, string Message, List<DateTime> NextOccurrences);

/// <summary>
/// Response for get next occurrences operation
/// </summary>
public record GetNextOccurrencesResponse(Guid ScheduleId, string ScheduleName, List<DateTime> NextOccurrences);