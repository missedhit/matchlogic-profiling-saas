using MatchLogic.Domain.Import;
using MatchLogic.Domain.Scheduling;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.Scheduling;

/// <summary>
/// Core scheduler service - handles schedule management and execution
/// </summary>
public interface ISchedulerService
{
    Task<ScheduledTask> CreateScheduleAsync(ScheduledTask schedule);
    Task<ScheduledTask> UpdateScheduleAsync(ScheduledTask schedule);
    Task DeleteScheduleAsync(Guid scheduleId);
    Task DeleteSchedulesByProjectAsync(Guid projectId);
    Task<ScheduledTask> GetScheduleByIdAsync(Guid scheduleId);
    Task<List<ScheduledTask>> GetSchedulesByProjectAsync(Guid projectId, bool includeDisabled = false);
    Task<List<ScheduledTask>> GetAllSchedulesAsync(bool includeDisabled = false);

    Task<ScheduledTaskExecution> TriggerManualExecutionAsync(Guid scheduleId, string triggeredBy);
    /// <summary>
    /// Execute scheduled task (called by Hangfire)
    /// </summary>
    /// <param name="scheduleId">Schedule to execute</param>
    /// <param name="existingExecutionId">Optional execution ID (for manual triggers)</param>
    Task ExecuteScheduledTaskAsync(Guid scheduleId, Guid? existingExecutionId = null);

    /// <summary>
    /// Update execution statistics after run completes
    /// Called by ProjectService.NotifySchedulerOfCompletionAsync
    /// </summary>
    Task UpdateExecutionStatisticsAsync(Guid executionId, RunStatus finalStatus, string errorMessage = null);

    Task<DateTime?> CalculateNextRunAsync(ScheduledTask schedule);
    Task<List<DateTime>> GetNextOccurrencesAsync(ScheduledTask schedule, int count = 5);
    Task<(bool IsValid, string Message, List<DateTime> NextOccurrences)> ValidateCronExpressionAsync(string cronExpression);

    Task EnableScheduleAsync(Guid scheduleId);
    Task DisableScheduleAsync(Guid scheduleId);

    /// <summary>
    /// Re-register an active schedule with the scheduler engine after app restart.
    /// Unlike EnableScheduleAsync, does NOT check IsEnabled guard — assumes
    /// schedule is enabled in DB but missing from in-memory scheduler.
    /// </summary>
    /// <param name="skipSimpleInterval">
    /// If true, skip Simple interval schedules (Server mode — Hangfire recovers them,
    /// rehydration would create duplicates).
    /// </param>
    Task RehydrateScheduleAsync(Guid scheduleId, bool skipSimpleInterval = false);

    Task<(List<ScheduledTaskExecution> Items, int TotalCount)> GetExecutionHistoryAsync(Guid scheduleId, int pageNumber = 1, int pageSize = 50);
}
