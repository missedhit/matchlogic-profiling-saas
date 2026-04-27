using MatchLogic.Api.Handlers.Scheduling.DTOs;
using MatchLogic.Domain.Scheduling;
using System.Collections.Generic;
using System;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;

namespace MatchLogic.Api.Handlers.Scheduling.Create;

public class CreateScheduleCommand : IRequest<Result<CreateScheduleResponse>>
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
