using MatchLogic.Api.Handlers.Scheduling.DTOs;
using MatchLogic.Domain.Scheduling;
using System.Collections.Generic;
using System;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;

namespace MatchLogic.Api.Handlers.Scheduling.Update;

public class UpdateScheduleCommand : IRequest<Result<UpdateScheduleResponse>>
{
    public Guid ScheduleId { get; set; }
    public string Name { get; set; }
    public string Description { get; set; }
    public ScheduleType? ScheduleType { get; set; }
    public string CronExpression { get; set; }
    public TimeSpan? RecurrenceInterval { get; set; }
    public DateTime? StartTime { get; set; }
    public string TimeZone { get; set; }
    public List<StepType> StepsToExecute { get; set; }
    public Dictionary<string, object> StepConfigurations { get; set; }
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

    public BaseConnectionInfo? ConnectionInfo { get; set; }
    // Status
    public bool? IsEnabled { get; set; }

    // Metadata
    public string LastModifiedBy { get; set; }
    public List<string> Tags { get; set; }
}

