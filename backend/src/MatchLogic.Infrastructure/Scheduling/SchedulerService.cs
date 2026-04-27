using Google.Apis.Util.Store;
using MatchLogic.Application.Common;
using MatchLogic.Application.Features.Project;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Application.Interfaces.Scheduling;
using MatchLogic.Domain.CleansingAndStandaradization;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using MatchLogic.Domain.Scheduling;
using MatchLogic.Infrastructure.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NCrontab;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using IScheduler = MatchLogic.Application.Interfaces.Scheduling.IScheduler;
using IDataStore = MatchLogic.Application.Interfaces.Persistence.IDataStore;

namespace MatchLogic.Infrastructure.Scheduling;

/// <summary>
/// Service for managing scheduled tasks
/// Provides CRUD operations and execution management
/// </summary>
public class SchedulerService : ISchedulerService
{
    private readonly IScheduler _scheduler;
    private readonly IProjectService _projectService;
    private readonly IGenericRepository<ScheduledTask, Guid> _scheduleRepository;
    private readonly IGenericRepository<ScheduledTaskExecution, Guid> _executionRepository;
    private readonly IGenericRepository<DataSource, Guid> _dataSourceRepository;
    private readonly IGenericRepository<ProjectRun, Guid> _projectRunRepository;
    private readonly IGenericRepository<StepJob, Guid> _stepJobRepository;
    private readonly SchedulerSettings _settings;
    private readonly IDataStore _dataStore;
    private readonly ILogger<SchedulerService> _logger;

    public SchedulerService(
        IScheduler scheduler,
        IProjectService projectService,
        IGenericRepository<ScheduledTask, Guid> scheduleRepository,
        IGenericRepository<ScheduledTaskExecution, Guid> executionRepository,
        IGenericRepository<DataSource, Guid> dataSourceRepository,
        IGenericRepository<ProjectRun, Guid> projectRunRepository,
        IGenericRepository<StepJob, Guid> stepJobRepository,
        IOptions<SchedulerSettings> settings,
        IDataStore dataStore,
        ILogger<SchedulerService> logger)
    {
        _scheduler = scheduler;
        _projectService = projectService;
        _scheduleRepository = scheduleRepository;
        _executionRepository = executionRepository;
        _dataSourceRepository = dataSourceRepository;
        _projectRunRepository = projectRunRepository;
        _stepJobRepository = stepJobRepository;
        _settings = settings.Value;
        _dataStore = dataStore;
        _logger = logger;
    }

    #region CRUD Operations

    public async Task<ScheduledTask> CreateScheduleAsync(ScheduledTask schedule)
    {
        _logger.LogInformation("Creating schedule: {Name} for project {ProjectId}",
            schedule.Name, schedule.ProjectId);

        schedule.Id = Guid.NewGuid();

        // Use settings for defaults if not specified
        if (string.IsNullOrEmpty(schedule.TimeZone))
        {
            schedule.TimeZone = _settings.DefaultTimeZone;
        }

        if (schedule.MaxRetryAttempts == 0)
        {
            schedule.MaxRetryAttempts = _settings.DefaultRetryAttempts;
        }

        // Register with Hangfire if not OnDemand
        if (schedule.ScheduleType != ScheduleType.OnDemand && schedule.IsEnabled)
        {
            await RegisterWithHangfireAsync(schedule);
        }

        // Calculate next run
        schedule.NextRun = await CalculateNextRunAsync(schedule);

        await _scheduleRepository.InsertAsync(schedule, Constants.Collections.ScheduledTasks);

        _logger.LogInformation("Schedule created: {ScheduleId}, NextRun: {NextRun}",
            schedule.Id, schedule.NextRun);

        return schedule;
    }

    public async Task<ScheduledTask> UpdateScheduleAsync(ScheduledTask schedule)
    {
        _logger.LogInformation("Updating schedule: {ScheduleId}", schedule.Id);

        var existing = await _scheduleRepository.GetByIdAsync(schedule.Id, Constants.Collections.ScheduledTasks);

        if (existing == null)
        {
            throw new InvalidOperationException($"Schedule {schedule.Id} not found");
        }

        // Check if schedule configuration changed
        bool scheduleChanged =
            existing.ScheduleType != schedule.ScheduleType ||
            existing.CronExpression != schedule.CronExpression ||
            existing.RecurrenceInterval != schedule.RecurrenceInterval ||
            existing.StartTime != schedule.StartTime ||
            existing.TimeZone != schedule.TimeZone;

        bool enabledChanged = existing.IsEnabled != schedule.IsEnabled;

        // Update Hangfire registration if needed
        if (schedule.ScheduleType != ScheduleType.OnDemand)
        {
            if (enabledChanged && !schedule.IsEnabled)
            {
                // Disable schedule
                await _scheduler.RemoveRecurringJobAsync(schedule.HangfireJobId);
                schedule.Status = ScheduleStatus.Paused;
            }
            else if ((scheduleChanged || enabledChanged) && schedule.IsEnabled)
            {
                // Re-register with new configuration
                await RegisterWithHangfireAsync(schedule);
                schedule.Status = ScheduleStatus.Active;
            }
        }

        // Recalculate next run if schedule changed
        if (scheduleChanged)
        {
            schedule.NextRun = await CalculateNextRunAsync(schedule);
        }

        await _scheduleRepository.UpdateAsync(schedule, Constants.Collections.ScheduledTasks);

        _logger.LogInformation("Schedule updated: {ScheduleId}, NextRun: {NextRun}",
            schedule.Id, schedule.NextRun);

        return schedule;
    }

    public async Task DeleteScheduleAsync(Guid scheduleId)
    {
        _logger.LogInformation("Deleting schedule: {ScheduleId}", scheduleId);

        var schedule = await _scheduleRepository.GetByIdAsync(scheduleId, Constants.Collections.ScheduledTasks);

        if (schedule != null)
        {
            // Remove from Hangfire
            if (!string.IsNullOrEmpty(schedule.HangfireJobId))
            {
                await _scheduler.RemoveRecurringJobAsync(schedule.HangfireJobId);
            }

            // Soft delete (archive)
            schedule.Status = ScheduleStatus.Archived;
            schedule.IsEnabled = false;

            await _scheduleRepository.UpdateAsync(schedule, Constants.Collections.ScheduledTasks);
        }
    }

    public async Task DeleteSchedulesByProjectAsync(Guid projectId)
    {
        _logger.LogInformation("Deleting all schedules for project: {ProjectId}", projectId);

        var schedules = await _scheduleRepository.QueryAsync(
            s => s.ProjectId == projectId,
            Constants.Collections.ScheduledTasks);

        foreach (var schedule in schedules)
        {
            // Remove Hangfire job
            if (!string.IsNullOrEmpty(schedule.HangfireJobId))
            {
                if (schedule.ScheduleType == ScheduleType.Cron)
                    await _scheduler.RemoveRecurringJobAsync(schedule.HangfireJobId);
                else
                    await _scheduler.DeleteJobAsync(schedule.HangfireJobId);
            }

            // Hard-delete execution history for this schedule
            await _executionRepository.DeleteAllAsync(
                e => e.ScheduledTaskId == schedule.Id,
                Constants.Collections.ScheduledTaskExecutions);
        }

        // Hard-delete all schedules for the project
        await _scheduleRepository.DeleteAllAsync(
            s => s.ProjectId == projectId,
            Constants.Collections.ScheduledTasks);

        _logger.LogInformation("Deleted {Count} schedules for project: {ProjectId}",
            schedules.Count, projectId);
    }

    public async Task<ScheduledTask> GetScheduleByIdAsync(Guid scheduleId)
    {
        return await _scheduleRepository.GetByIdAsync(scheduleId, Constants.Collections.ScheduledTasks);
    }

    public async Task<List<ScheduledTask>> GetSchedulesByProjectAsync(Guid projectId, bool includeDisabled = false)
    {
        var schedules = await _scheduleRepository.QueryAsync(
            s => s.ProjectId == projectId &&
                 s.Status != ScheduleStatus.Archived &&
                 (includeDisabled || s.IsEnabled),
            Constants.Collections.ScheduledTasks);

        return schedules.OrderBy(s => s.Name).ToList();
    }

    public async Task<List<ScheduledTask>> GetAllSchedulesAsync(bool includeDisabled = false)
    {
        var schedules = await _scheduleRepository.QueryAsync(
            s => s.Status != ScheduleStatus.Archived &&
                 (includeDisabled || s.IsEnabled),
            Constants.Collections.ScheduledTasks);

        return schedules.OrderBy(s => s.Name).ToList();
    }

    #endregion

    #region Execution

    public async Task<ScheduledTaskExecution> TriggerManualExecutionAsync(Guid scheduleId, string triggeredBy)
    {
        _logger.LogInformation("Manual trigger requested for schedule {ScheduleId} by {User}",
            scheduleId, triggeredBy);

        var schedule = await _scheduleRepository.GetByIdAsync(scheduleId, Constants.Collections.ScheduledTasks);

        if (schedule == null)
        {
            throw new InvalidOperationException($"Schedule {scheduleId} not found");
        }

        // Create execution record
        var execution = new ScheduledTaskExecution
        {
            Id = Guid.NewGuid(),
            ScheduledTaskId = scheduleId,
            ScheduledTime = DateTime.UtcNow,
            Status = RunStatus.NotStarted,
            TriggerType = TriggerType.Manual,
            TriggeredBy = triggeredBy,
            CreatedAt = DateTime.UtcNow
        };

        await _executionRepository.InsertAsync(execution, Constants.Collections.ScheduledTaskExecutions);

        // For manual trigger, create a one-time job and trigger immediately
        var manualJobId = $"manual_{execution.Id}";

        await _scheduler.AddOrUpdateRecurringJobAsync<ISchedulerService>(
            manualJobId,
            "0 0 1 1 *", // Never runs (Jan 1st only) - we'll trigger manually
            service => service.ExecuteScheduledTaskAsync(scheduleId, execution.Id),
            schedule.TimeZone ?? _settings.DefaultTimeZone);

        // Trigger immediately
        await _scheduler.TriggerRecurringJobAsync(manualJobId);

        // Clean up the recurring job after trigger
        _ = Task.Run(async () =>
        {
            await Task.Delay(TimeSpan.FromMinutes(1));
            await _scheduler.RemoveRecurringJobAsync(manualJobId);
        });

        execution.HangfireJobId = manualJobId;
        await _executionRepository.UpdateAsync(execution, Constants.Collections.ScheduledTaskExecutions);

        return execution;
    }

    /// <summary>
    /// Main execution method - Called by Hangfire
    /// Legacy equivalent: ProjectProcessor.AutoProcessProject()
    /// </summary>
    /// <param name="scheduleId">Schedule to execute</param>
    /// <param name="existingExecutionId">Optional execution ID (used for manual triggers)</param>
    public async Task ExecuteScheduledTaskAsync(Guid scheduleId, Guid? existingExecutionId = null)
    {
        _logger.LogInformation(
            "⏰ Executing scheduled task: {ScheduleId}, ExistingExecution: {ExecutionId}",
            scheduleId, existingExecutionId);

        var schedule = await _scheduleRepository.GetByIdAsync(
            scheduleId, Constants.Collections.ScheduledTasks);

        if (schedule == null || !schedule.IsEnabled)
        {
            _logger.LogWarning(
                "Schedule {ScheduleId} not found or disabled, skipping execution",
                scheduleId);
            return;
        }

        ScheduledTaskExecution execution;

        // Use existing execution record if provided (manual trigger)
        if (existingExecutionId.HasValue)
        {
            execution = await _executionRepository.GetByIdAsync(
                existingExecutionId.Value,
                Constants.Collections.ScheduledTaskExecutions);

            if (execution == null)
            {
                _logger.LogError(
                    "Execution {ExecutionId} not found, cannot proceed",
                    existingExecutionId.Value);
                return;
            }

            _logger.LogInformation(
                "Using existing execution record {ExecutionId} (manual trigger)",
                execution.Id);
        }
        else
        {
            // Create new execution record for scheduled triggers
            execution = new ScheduledTaskExecution
            {
                Id = Guid.NewGuid(),
                ScheduledTaskId = scheduleId,
                ScheduledTime = DateTime.UtcNow,
                ActualStartTime = DateTime.UtcNow,
                Status = RunStatus.NotStarted,
                TriggerType = TriggerType.Scheduled,
                ExecutedByServer = Environment.MachineName,
                CreatedAt = DateTime.UtcNow
            };

            await _executionRepository.InsertAsync(
                execution, Constants.Collections.ScheduledTaskExecutions);

            _logger.LogInformation(
                "Created new execution record {ExecutionId} (scheduled trigger)",
                execution.Id);
        }

        execution.ActualStartTime = DateTime.UtcNow;
        execution.Status = RunStatus.NotStarted;
        execution.ExecutedByServer = Environment.MachineName;

        await _executionRepository.UpdateAsync(
            execution, Constants.Collections.ScheduledTaskExecutions);

        try
        {
            var steps = await BuildStepConfigurationsAsync(schedule);

            _logger.LogInformation(
                "Starting project run for schedule {ScheduleId} with {StepCount} steps",
                scheduleId, steps.Count);

            var run = await _projectService.StartNewRun(
                schedule.ProjectId,
                steps,
                execution.Id);

            execution.ProjectRunId = run.Id;
            execution.Status = RunStatus.InProgress;

            await _executionRepository.UpdateAsync(
                execution, Constants.Collections.ScheduledTaskExecutions);

            // ★ Option A: For Simple interval, schedule the next occurrence NOW
            // We do this immediately after run starts — not after it completes —
            // so the next run is always queued regardless of success or failure.
            // Manual triggers (existingExecutionId.HasValue) never self-schedule.
            if (schedule.ScheduleType == ScheduleType.Simple
                && schedule.RecurrenceInterval.HasValue
                && schedule.IsEnabled
                && !existingExecutionId.HasValue)
            {
                var nextRunTime = DateTime.UtcNow.Add(schedule.RecurrenceInterval.Value);

                var nextHangfireJobId = await _scheduler.ScheduleServiceJobAsync<ISchedulerService>(
                    service => service.ExecuteScheduledTaskAsync(scheduleId, null),
                    schedule.RecurrenceInterval.Value);

                // Store the next job ID so DisableScheduleAsync can cancel it
                schedule.HangfireJobId = nextHangfireJobId;
                schedule.NextRun = nextRunTime;

                await _scheduleRepository.UpdateAsync(
                    schedule, Constants.Collections.ScheduledTasks);

                _logger.LogInformation(
                    "Next run for schedule {ScheduleId} scheduled at {NextRun}, " +
                    "HangfireJobId: {JobId}",
                    schedule.Id, nextRunTime, nextHangfireJobId);
            }

            _logger.LogInformation(
                "✅ Started scheduled execution {ExecutionId}, ProjectRun: {RunId}, " +
                "TriggerType: {TriggerType}",
                execution.Id, run.Id, execution.TriggerType);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "❌ Scheduled task execution failed: {ScheduleId}", scheduleId);

            execution.Status = RunStatus.Failed;
            execution.ErrorMessage = ex.Message;
            execution.StackTrace = ex.StackTrace;
            execution.EndTime = DateTime.UtcNow;

            await _executionRepository.UpdateAsync(
                execution, Constants.Collections.ScheduledTaskExecutions);

            await UpdateExecutionStatisticsAsync(
                execution.Id, RunStatus.Failed, ex.Message);

            throw;
        }
    }

    /// <summary>
    /// Build step configurations from schedule settings
    /// Legacy equivalent: Building steps from ProjectTask.Jobs configuration
    /// </summary>
    private async Task<List<StepConfiguration>> BuildStepConfigurationsAsync(ScheduledTask schedule)
    {
        var dataSources = await _dataSourceRepository.QueryAsync(
            ds => ds.ProjectId == schedule.ProjectId,
            Constants.Collections.DataSources);

        var dataSourceIds = dataSources.Select(ds => ds.Id).ToList();

        // Fetch all cleanse rules for this project's datasources up front
        var cleanseRules = await _dataStore.QueryAsync<EnhancedCleaningRules>(
            x => dataSourceIds.Contains(x.DataSourceId),
            Constants.Collections.CleaningRules);

        // Pre-compute the set of datasource IDs that have at least one rule
        var dataSourceIdsWithCleanseRules = cleanseRules
            .Select(r => r.DataSourceId)
            .Distinct()
            .ToList();

        var configurations = new List<StepConfiguration>();

        foreach (var stepType in schedule.StepsToExecute)
        {
            var config = new Dictionary<string, object>();

            var stepConfigKey = stepType.ToString();
            if (schedule.StepConfigurations?.ContainsKey(stepConfigKey) == true)
            {
                config = new Dictionary<string, object>(
                    (Dictionary<string, object>)schedule.StepConfigurations[stepConfigKey]);
            }

            config["ExportCleanedData"] = schedule.ExportCleanedData;
            config["ExportMatchedData"] = schedule.ExportMatchedData;
            config["ExportFormattedData"] = schedule.ExportFormattedData;
            config["ExportDataProfile"] = schedule.ExportDataProfile;
            config["ExportSummaryReport"] = schedule.ExportSummaryReport;
            config["BulkCopy"] = schedule.BulkCopy;
            config["PreserveInputTypes"] = schedule.PreserveInputTypes;

            if (!IsDataSourceSpecific(stepType))
            {
                config["ProjectId"] = schedule.ProjectId;
            }

            var stepConfig = new StepConfiguration(stepType, config);

            if (IsDataSourceSpecific(stepType))
            {
                if (stepType == StepType.Cleanse)
                {
                    if (!dataSourceIdsWithCleanseRules.Any())
                    {
                        _logger.LogInformation(
                            "Skipping Cleanse step for schedule {ScheduleId} — " +
                            "no datasources have cleanse rules configured",
                            schedule.Id);
                        continue; // skip — don't add to configurations at all
                    }

                    stepConfig.DataSourceIds = dataSourceIdsWithCleanseRules;

                    _logger.LogInformation(
                        "Cleanse step will run for {Count}/{Total} datasources in schedule {ScheduleId}",
                        dataSourceIdsWithCleanseRules.Count, dataSourceIds.Count, schedule.Id);
                }
                else
                {
                    stepConfig.DataSourceIds = dataSourceIds;
                }
            }
            else
            {
                stepConfig.DataSourceIds = new List<Guid> { schedule.ProjectId };
            }

            configurations.Add(stepConfig);
        }

        return configurations;
    }

    /// <summary>
    /// Determine if step type is DataSource-specific (runs per DataSource)
    /// </summary>
    private bool IsDataSourceSpecific(StepType stepType)
    {
        return stepType switch
        {
            StepType.Import or StepType.Profile or StepType.Cleanse => true,
            _ => false
        };
    }

    /// <summary>
    /// Update execution statistics after run completes
    /// Called by ProjectService.NotifySchedulerOfCompletionAsync
    /// Legacy equivalent: Updates to ScheduledTask after ProjectProcessor completes
    /// </summary>
    public async Task UpdateExecutionStatisticsAsync(
        Guid executionId,
        RunStatus finalStatus,
        string errorMessage = null)
    {
        try
        {
            var execution = await _executionRepository.GetByIdAsync(
                executionId,
                Constants.Collections.ScheduledTaskExecutions);

            if (execution == null)
            {
                _logger.LogWarning("Execution {ExecutionId} not found", executionId);
                return;
            }

            // Update execution record
            execution.Status = finalStatus;
            execution.EndTime = DateTime.UtcNow;
            execution.ErrorMessage = errorMessage;

            // ✅ Populate aggregated statistics from ProjectRun
            if (execution.ProjectRunId.HasValue)
            {
                var run = await _projectRunRepository.GetByIdAsync(
                    execution.ProjectRunId.Value,
                    Constants.Collections.ProjectRuns);

                if (run != null)
                {
                    execution.Statistics = await BuildExecutionStatisticsAsync(run);

                    // Populate dedicated fields from statistics
                    if (execution.Statistics.TryGetValue("TotalRecordsProcessed", out var totalRecords))
                    {
                        execution.RecordsProcessed = Convert.ToInt32(totalRecords);
                    }
                    if (execution.Statistics.TryGetValue("TotalErrors", out var totalErrors))
                    {
                        execution.ErrorCount = Convert.ToInt32(totalErrors);
                    }
                    if (execution.Statistics.TryGetValue("TotalWarnings", out var totalWarnings))
                    {
                        execution.WarningCount = Convert.ToInt32(totalWarnings);
                    }
                }
            }

            // ✅ Execution metadata
            execution.ExecutedByServer = Environment.MachineName;
            execution.ApplicationVersion = GetApplicationVersion();

            await _executionRepository.UpdateAsync(
                execution,
                Constants.Collections.ScheduledTaskExecutions);

            // Get schedule and update its statistics
            var schedule = await _scheduleRepository.GetByIdAsync(
                execution.ScheduledTaskId,
                Constants.Collections.ScheduledTasks);

            if (schedule == null)
            {
                _logger.LogWarning("Schedule {ScheduleId} not found", execution.ScheduledTaskId);
                return;
            }

            // ✅ Update schedule statistics
            schedule.TotalExecutions++;
            schedule.LastRun = DateTime.UtcNow;

            if (finalStatus == RunStatus.Completed)
            {
                schedule.SuccessfulExecutions++;
                schedule.ConsecutiveFailures = 0;

                _logger.LogInformation(
                    "Schedule {ScheduleId} succeeded. Stats: {Success}/{Total}",
                    schedule.Id,
                    schedule.SuccessfulExecutions,
                    schedule.TotalExecutions);
            }
            else if (finalStatus == RunStatus.Failed)
            {
                schedule.FailedExecutions++;
                schedule.ConsecutiveFailures++;

                _logger.LogWarning(
                    "Schedule {ScheduleId} failed. Consecutive: {Failures}",
                    schedule.Id,
                    schedule.ConsecutiveFailures);

                // Auto-suspend after max consecutive failures
                if (schedule.ConsecutiveFailures >= _settings.MaxConsecutiveFailuresBeforeSuspend)
                {
                    schedule.Status = ScheduleStatus.Suspended;
                    schedule.IsEnabled = false;

                    if (!string.IsNullOrEmpty(schedule.HangfireJobId))
                    {
                        await _scheduler.RemoveRecurringJobAsync(schedule.HangfireJobId);
                    }

                    _logger.LogError(
                        "Schedule {ScheduleId} auto-suspended after {Failures} failures",
                        schedule.Id,
                        schedule.ConsecutiveFailures);
                }
            }

            // Calculate next run
            schedule.NextRun = await CalculateNextRunAsync(schedule);

            await _scheduleRepository.UpdateAsync(schedule, Constants.Collections.ScheduledTasks);

            _logger.LogInformation(
                "✅ Updated execution {ExecutionId}: Status={Status}, Records={Records}, Duration={Duration}s",
                executionId,
                finalStatus,
                execution.RecordsProcessed,
                execution.DurationSeconds);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error updating execution statistics for {ExecutionId}", executionId);
        }
    }

    /// <summary>
    /// Build aggregated statistics from ProjectRun and all its StepJobs
    /// </summary>
    private async Task<Dictionary<string, object>> BuildExecutionStatisticsAsync(ProjectRun run)
    {
        var statistics = new Dictionary<string, object>();

        // Get all steps for this run
        var steps = await _stepJobRepository.QueryAsync(
            s => s.RunId == run.Id,
            Constants.Collections.StepJobs);

        var stepsList = steps.ToList();

        // Aggregate metrics across all steps
        long totalRecords = 0;
        int totalErrors = 0;
        int totalWarnings = 0;
        double totalDurationSeconds = 0;

        // Per-step breakdowns
        var stepBreakdowns = new List<Dictionary<string, object>>();

        foreach (var step in stepsList)
        {
            var stepStats = new Dictionary<string, object>
            {
                ["StepType"] = step.Type.ToString(),
                ["Status"] = step.Status.ToString(),
                ["DataSourceId"] = step.DataSourceId
            };

            // Calculate duration
            if (step.StartTime != null && step.EndTime.HasValue)
            {
                var duration = (step.EndTime.Value - step.StartTime).TotalSeconds;
                stepStats["DurationSeconds"] = duration;
                totalDurationSeconds += duration;
            }

            // Extract step-level statistics from Configuration
            if (step.Configuration != null)
            {
                if (step.Configuration.TryGetValue("RecordsProcessed", out var records))
                {
                    var recordCount = Convert.ToInt64(records);
                    stepStats["RecordsProcessed"] = recordCount;
                    totalRecords += recordCount;
                }
                if (step.Configuration.TryGetValue("ErrorCount", out var errors))
                {
                    var errorCount = Convert.ToInt32(errors);
                    stepStats["ErrorCount"] = errorCount;
                    totalErrors += errorCount;
                }
                if (step.Configuration.TryGetValue("WarningCount", out var warnings))
                {
                    var warningCount = Convert.ToInt32(warnings);
                    stepStats["WarningCount"] = warningCount;
                    totalWarnings += warningCount;
                }
            }

            stepBreakdowns.Add(stepStats);
        }

        // Populate aggregated statistics
        statistics["TotalRecordsProcessed"] = totalRecords;
        statistics["TotalErrors"] = totalErrors;
        statistics["TotalWarnings"] = totalWarnings;
        statistics["TotalDurationSeconds"] = totalDurationSeconds;
        statistics["TotalSteps"] = stepsList.Count;
        statistics["CompletedSteps"] = stepsList.Count(s => s.Status == RunStatus.Completed);
        statistics["FailedSteps"] = stepsList.Count(s => s.Status == RunStatus.Failed);
        statistics["StepBreakdowns"] = stepBreakdowns;

        // Performance metrics
        if (totalDurationSeconds > 0)
        {
            statistics["RecordsPerSecond"] = totalRecords / totalDurationSeconds;
        }

        // Execution metadata
        statistics["ProjectId"] = run.ProjectId;
        statistics["RunId"] = run.Id;
        statistics["RunNumber"] = run.RunNumber;

        return statistics;
    }

    /// <summary>
    /// Get application version
    /// </summary>
    private string GetApplicationVersion()
    {
        try
        {
            var assembly = System.Reflection.Assembly.GetExecutingAssembly();
            var version = assembly.GetName().Version;
            return version?.ToString() ?? "Unknown";
        }
        catch
        {
            return "Unknown";
        }
    }

    #endregion

    #region Schedule Calculations

    public async Task<DateTime?> CalculateNextRunAsync(ScheduledTask schedule)
    {
        if (!schedule.IsEnabled || schedule.Status != ScheduleStatus.Active)
            return null;

        try
        {
            if (schedule.ScheduleType == ScheduleType.Cron)
            {
                // Cron: always calculate from expression — Hangfire owns the schedule
                var cronSchedule = CrontabSchedule.Parse(schedule.CronExpression);
                return cronSchedule.GetNextOccurrence(DateTime.UtcNow);
            }
            else if (schedule.ScheduleType == ScheduleType.Simple)
            {
                if (!schedule.RecurrenceInterval.HasValue)
                    return null;

                // ★ Option A: NextRun is set directly in two places:
                //   1. RegisterWithHangfireAsync  → first run = StartTime (or now if past)
                //   2. ExecuteScheduledTaskAsync  → subsequent runs = now + interval
                // So we just return what is already stored.
                // Only fallback to StartTime calculation on very first creation
                // before ExecuteScheduledTaskAsync has ever run.
                if (schedule.NextRun.HasValue && schedule.NextRun.Value.ToUniversalTime() > DateTime.UtcNow)
                {
                    return schedule.NextRun;
                }

                // Fallback: first creation, NextRun not yet set by Hangfire
                if (!schedule.StartTime.HasValue)
                    return null;

                var now = DateTime.UtcNow;
                var startUtc = schedule.StartTime.Value.ToUniversalTime();
                return startUtc > now
                    ? schedule.StartTime.Value
                    : now.Add(schedule.RecurrenceInterval.Value);
            }

            return null; // OnDemand has no next run
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Error calculating next run for schedule {ScheduleId}", schedule.Id);
            return null;
        }
    }

    public async Task<List<DateTime>> GetNextOccurrencesAsync(ScheduledTask schedule, int count = 5)
    {
        var occurrences = new List<DateTime>();

        try
        {
            if (schedule.ScheduleType == ScheduleType.Cron)
            {
                var cronSchedule = CrontabSchedule.Parse(schedule.CronExpression);
                var current = DateTime.UtcNow;

                for (int i = 0; i < count; i++)
                {
                    current = cronSchedule.GetNextOccurrence(current);
                    occurrences.Add(current);
                }
            }
            else if (schedule.ScheduleType == ScheduleType.Simple &&
                     schedule.StartTime.HasValue &&
                     schedule.RecurrenceInterval.HasValue)
            {
                var current = await CalculateNextRunAsync(schedule);

                if (current.HasValue)
                {
                    occurrences.Add(current.Value);

                    for (int i = 1; i < count; i++)
                    {
                        current = current.Value.Add(schedule.RecurrenceInterval.Value);
                        occurrences.Add(current.Value);
                    }
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting next occurrences for schedule {ScheduleId}", schedule.Id);
        }

        return occurrences;
    }

    public async Task<(bool IsValid, string Message, List<DateTime> NextOccurrences)> ValidateCronExpressionAsync(string cronExpression)
    {
        try
        {
            var cronSchedule = CrontabSchedule.Parse(cronExpression);

            // Get next 5 occurrences
            var occurrences = new List<DateTime>();
            var current = DateTime.UtcNow;

            for (int i = 0; i < 5; i++)
            {
                current = cronSchedule.GetNextOccurrence(current);
                occurrences.Add(current);
            }

            return (true, "Valid cron expression", occurrences);
        }
        catch (Exception ex)
        {
            return (false, ex.Message, null);
        }
    }

    #endregion

    #region Enable/Disable

    public async Task EnableScheduleAsync(Guid scheduleId)
    {
        var schedule = await _scheduleRepository.GetByIdAsync(scheduleId, Constants.Collections.ScheduledTasks);

        if (schedule != null && !schedule.IsEnabled)
        {
            schedule.IsEnabled = true;
            schedule.Status = ScheduleStatus.Active;

            if (schedule.ScheduleType != ScheduleType.OnDemand)
            {
                await RegisterWithHangfireAsync(schedule);
            }

            schedule.NextRun = await CalculateNextRunAsync(schedule);

            await _scheduleRepository.UpdateAsync(schedule, Constants.Collections.ScheduledTasks);

            _logger.LogInformation("Schedule enabled: {ScheduleId}", scheduleId);
        }
    }

    public async Task DisableScheduleAsync(Guid scheduleId)
    {
        var schedule = await _scheduleRepository.GetByIdAsync(
            scheduleId, Constants.Collections.ScheduledTasks);

        if (schedule != null && schedule.IsEnabled)
        {
            schedule.IsEnabled = false;
            schedule.Status = ScheduleStatus.Paused;

            if (!string.IsNullOrEmpty(schedule.HangfireJobId))
            {
                if (schedule.ScheduleType == ScheduleType.Cron)
                {
                    // Recurring job
                    await _scheduler.RemoveRecurringJobAsync(schedule.HangfireJobId);
                }
                else
                {
                    // One-time scheduled job — delete it so it won't fire
                    await _scheduler.DeleteJobAsync(schedule.HangfireJobId);
                }
            }

            schedule.NextRun = null;
            await _scheduleRepository.UpdateAsync(
                schedule, Constants.Collections.ScheduledTasks);
        }
    }

    public async Task RehydrateScheduleAsync(Guid scheduleId, bool skipSimpleInterval = false)
    {
        var schedule = await _scheduleRepository.GetByIdAsync(
            scheduleId, Constants.Collections.ScheduledTasks);

        if (schedule == null)
        {
            _logger.LogWarning("RehydrateScheduleAsync: schedule {Id} not found", scheduleId);
            return;
        }

        if (!schedule.IsEnabled || schedule.Status != ScheduleStatus.Active)
        {
            _logger.LogDebug("RehydrateScheduleAsync: schedule {Id} not active, skipping", scheduleId);
            return;
        }

        if (schedule.ScheduleType == ScheduleType.OnDemand)
            return;

        // Server mode: skip Simple interval — Hangfire recovers them from MongoDB.
        // Rehydrating would create duplicate delayed jobs.
        if (skipSimpleInterval && schedule.ScheduleType == ScheduleType.Simple)
        {
            _logger.LogDebug(
                "RehydrateScheduleAsync: skipping Simple interval schedule {Id} (Hangfire handles recovery)",
                scheduleId);
            return;
        }

        _logger.LogInformation("Rehydrating schedule {Id} ({Name})", schedule.Id, schedule.Name);

        await RegisterWithHangfireAsync(schedule);
        schedule.NextRun = await CalculateNextRunAsync(schedule);
        await _scheduleRepository.UpdateAsync(schedule, Constants.Collections.ScheduledTasks);
    }

    #endregion

    #region Execution History

    public async Task<(List<ScheduledTaskExecution> Items, int TotalCount)> GetExecutionHistoryAsync(Guid scheduleId, int pageNumber = 1, int pageSize = 50)
    {
        var executions = await _executionRepository.QueryAsync(
            e => e.ScheduledTaskId == scheduleId,
            Constants.Collections.ScheduledTaskExecutions);

        
        var totalCount = executions.Count;

        var items = executions
            .OrderByDescending(e => e.ScheduledTime)
            .Skip((pageNumber - 1) * pageSize)
            .Take(pageSize)
            .ToList();

        return (items, totalCount);
    }

    #endregion

    #region Private Helpers

    /// <summary>
    /// Register schedule with Hangfire
    /// </summary>
    private async Task RegisterWithHangfireAsync(ScheduledTask schedule)
    {
        schedule.HangfireJobId = $"schedule_{schedule.Id}";

        if (schedule.ScheduleType == ScheduleType.Cron)
        {
            // Cron stays exactly as before
            await _scheduler.AddOrUpdateRecurringJobAsync<ISchedulerService>(
                schedule.HangfireJobId,
                schedule.CronExpression,
                service => service.ExecuteScheduledTaskAsync(schedule.Id, null),
                schedule.TimeZone ?? _settings.DefaultTimeZone);
        }
        else // Simple interval — Option A
        {
            // Remove any existing recurring job if switching from Cron
            await _scheduler.RemoveRecurringJobAsync(schedule.HangfireJobId);

            if (!schedule.StartTime.HasValue || !schedule.RecurrenceInterval.HasValue)
                throw new InvalidOperationException(
                    "StartTime and RecurrenceInterval are required for Simple interval schedule.");

            var now = DateTime.UtcNow;
            var startTimeUtc = schedule.StartTime.Value.ToUniversalTime();
            TimeSpan delay;
            if (startTimeUtc > now)
            {
                // First creation or future StartTime: honor it
                delay = startTimeUtc - now;
            }
            else
            {
                // Re-enable, rehydrate, or past StartTime: wait one full interval
                delay = schedule.RecurrenceInterval.Value;
            }

            var hangfireJobId = await _scheduler.ScheduleServiceJobAsync<ISchedulerService>(
                service => service.ExecuteScheduledTaskAsync(schedule.Id, null),
                delay);

            // Store the one-time job ID so we can track/cancel it
            schedule.HangfireJobId = hangfireJobId;

            _logger.LogInformation(
                "Simple interval schedule {ScheduleId} first run in {Delay}, " +
                "then every {Interval}",
                schedule.Id, delay, schedule.RecurrenceInterval.Value);
        }
    }

    /// <summary>
    /// Convert TimeSpan interval to cron expression
    /// </summary>
    private string ConvertIntervalToCron(TimeSpan interval)
    {
        if (interval.TotalMinutes < 60)
        {
            // Every X minutes
            return $"*/{(int)interval.TotalMinutes} * * * *";
        }
        else if (interval.TotalHours < 24)
        {
            // Every X hours
            return $"0 */{(int)interval.TotalHours} * * *";
        }
        else
        {
            // Every X days
            return $"0 0 */{(int)interval.TotalDays} * *";
        }
    }

    #endregion
}