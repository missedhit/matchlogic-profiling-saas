using MatchLogic.Application.Interfaces.Common;
using MatchLogic.Application.Interfaces.Scheduling;
using MatchLogic.Domain.Entities.Common;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NCrontab;
using System;
using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Scheduling;

/// <summary>
/// Legacy scheduler implementation using BackgroundService + Semaphore
/// Used when UseHangfire = false in configuration
/// Maintains compatibility with ProjectBackgroundService
/// </summary>
public class BackgroundServiceScheduler : IScheduler
{
    private readonly IBackgroundJobQueue<ProjectJobInfo> _jobQueue;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<BackgroundServiceScheduler> _logger;

    // Stores recurring job definitions (Cron + manual trigger pattern)
    private readonly ConcurrentDictionary<string, RecurringJobEntry> _recurringJobs = new();

    // Stores pending delayed jobs (Simple interval Option A)
    private readonly ConcurrentDictionary<string, CancellationTokenSource> _scheduledJobs = new();

    private record RecurringJobEntry(
     Func<IServiceScope, Task> Invoker,
     string CronExpression,
     CancellationTokenSource Cts);
    public BackgroundServiceScheduler(
        IBackgroundJobQueue<ProjectJobInfo> jobQueue,
         IServiceScopeFactory scopeFactory,
        ILogger<BackgroundServiceScheduler> logger)
    {
        _jobQueue = jobQueue;
        _scopeFactory = scopeFactory;
        _logger = logger;
    }

    // ─── Pipeline step jobs (used by ProjectService.QueueStepJob) ────────────

    public async Task<string> EnqueueJobAsync(ProjectJobInfo job)
    {
        _logger.LogInformation(
            "Enqueueing job {JobId} for project {ProjectId} - Step: {StepType}",
            job.JobId, job.ProjectId, job.CurrentStep.Type);

        await _jobQueue.QueueJobAsync(job);
        return job.JobId.ToString();
    }

    public Task<string> ScheduleJobAsync(ProjectJobInfo job, TimeSpan delay)
    {
        // BackgroundService doesn't support delayed execution
        // Degrade to immediate — acceptable for non-scheduler use
        _logger.LogWarning(
            "ScheduleJobAsync not supported in BackgroundService mode. " +
            "Job {JobId} will be enqueued immediately.",
            job.JobId);

        return EnqueueJobAsync(job);
    }

    // ─── Recurring jobs (Cron schedules + manual trigger pattern) ────────────

    public Task AddOrUpdateRecurringJobAsync<T>(
        string jobId,
        string cronExpression,
        Expression<Func<T, Task>> methodCall,
        string timeZone = "UTC")
    {
        _logger.LogInformation(
            "Registering recurring job {JobId} with cron: {Cron} (BackgroundService mode)",
            jobId, cronExpression);

        // Cancel existing job if updating
        if (_recurringJobs.TryRemove(jobId, out var existing))
        {
            existing.Cts.Cancel();
            _logger.LogInformation(
                "Cancelled existing recurring job {JobId} before update", jobId);
        }

        // Build invoker — compiles expression once, reuses across executions
        var invoker = BuildInvoker(methodCall);
        var cts = new CancellationTokenSource();

        var entry = new RecurringJobEntry(invoker, cronExpression, cts);
        _recurringJobs[jobId] = entry;

        // Manual trigger pattern: "0 0 1 1 *" — Jan 1st only
        // Just store the entry, TriggerRecurringJobAsync will invoke it
        bool isManualTriggerPattern = cronExpression == "0 0 1 1 *";

        if (!isManualTriggerPattern)
        {
            // Real Cron schedule — start background loop
            _ = RunCronLoopAsync(jobId, entry, cts.Token);
        }

        return Task.CompletedTask;
    }


    public Task TriggerRecurringJobAsync(string jobId)
    {
        if (!_recurringJobs.TryGetValue(jobId, out var entry))
        {
            _logger.LogWarning(
                "TriggerRecurringJobAsync: job {JobId} not found", jobId);
            return Task.CompletedTask;
        }

        _logger.LogInformation(
            "Triggering recurring job {JobId} immediately", jobId);

        // Fire and forget — invoke in background
        _ = Task.Run(async () =>
        {
            try
            {
                using var scope = _scopeFactory.CreateScope();
                await entry.Invoker(scope);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    "Error executing triggered job {JobId}", jobId);
            }
        });

        return Task.CompletedTask;
    }

    public Task RemoveRecurringJobAsync(string jobId)
    {
        if (_recurringJobs.TryRemove(jobId, out var entry))
        {
            entry.Cts.Cancel();
            _logger.LogInformation(
                "Removed recurring job {JobId}", jobId);
        }

        return Task.CompletedTask;
    }


    public Task<bool> DeleteJobAsync(string jobId)
    {
        if (_scheduledJobs.TryRemove(jobId, out var cts))
        {
            cts.Cancel();
            _logger.LogInformation(
                "Cancelled scheduled job {JobId}", jobId);
            return Task.FromResult(true);
        }

        _logger.LogWarning(
            "DeleteJobAsync: job {JobId} not found in scheduled jobs", jobId);
        return Task.FromResult(false);
    }

    // ─── Not meaningfully supported ──────────────────────────────────────────

    public Task<bool> RequeueJobAsync(string jobId)
    {
        _logger.LogWarning(
            "RequeueJobAsync not supported in BackgroundService mode. JobId: {JobId}",
            jobId);
        return Task.FromResult(false);
    }

    public Task<JobStateInfo> GetJobStateAsync(string jobId)
    {
        _logger.LogWarning(
            "GetJobStateAsync not supported in BackgroundService mode. JobId: {JobId}",
            jobId);
        return Task.FromResult<JobStateInfo>(null);
    }

    // ─── Scheduled one-time jobs (Simple interval Option A) ──────────────────

    public Task<string> ScheduleServiceJobAsync<T>(
        Expression<Func<T, Task>> methodCall,
        TimeSpan delay)
    {
        var jobId = Guid.NewGuid().ToString();
        var cts = new CancellationTokenSource();
        _scheduledJobs[jobId] = cts;

        var invoker = BuildInvoker(methodCall);

        _logger.LogInformation(
            "Scheduling service job {JobId} with delay {Delay} (BackgroundService mode)",
            jobId, delay);

        _ = Task.Run(async () =>
        {
            try
            {
                await Task.Delay(delay, cts.Token);

                if (cts.Token.IsCancellationRequested)
                {
                    _logger.LogInformation(
                        "Scheduled job {JobId} was cancelled before execution", jobId);
                    return;
                }

                _logger.LogInformation(
                    "Executing scheduled job {JobId}", jobId);

                using var scope = _scopeFactory.CreateScope();
                await invoker(scope);
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation(
                    "Scheduled job {JobId} cancelled", jobId);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    "Error executing scheduled job {JobId}", jobId);
            }
            finally
            {
                _scheduledJobs.TryRemove(jobId, out _);
            }
        });

        return Task.FromResult(jobId);
    }

    // ─── Private helpers ─────────────────────────────────────────────────────

    /// <summary>
    /// Compile Expression<Func<T, Task>> into a reusable Func<IServiceScope, Task>
    /// that resolves T from DI and invokes the method — same as Hangfire does internally
    /// </summary>
    private static Func<IServiceScope, Task> BuildInvoker<T>(
        Expression<Func<T, Task>> methodCall)
    {
        var compiled = methodCall.Compile();

        return async scope =>
        {
            var service = scope.ServiceProvider.GetRequiredService<T>();
            await compiled(service);
        };
    }

    /// <summary>
    /// Background loop for Cron-based recurring jobs
    /// Uses NCrontab (already a dependency) to calculate next occurrence
    /// </summary>
    private async Task RunCronLoopAsync(
        string jobId,
        RecurringJobEntry entry,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation(
            "Starting cron loop for job {JobId} with cron: {Cron}",
            jobId, entry.CronExpression);

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var cronSchedule = CrontabSchedule.Parse(entry.CronExpression);
                var now = DateTime.UtcNow;
                var next = cronSchedule.GetNextOccurrence(now);
                var delay = next - now;

                if (delay > TimeSpan.Zero)
                {
                    _logger.LogInformation(
                        "Cron job {JobId} next run at {Next} (in {Delay})",
                        jobId, next, delay);

                    await Task.Delay(delay, cancellationToken);
                }

                if (cancellationToken.IsCancellationRequested)
                    break;

                _logger.LogInformation(
                    "Executing cron job {JobId}", jobId);

                using var scope = _scopeFactory.CreateScope();
                await entry.Invoker(scope);
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation(
                    "Cron loop for job {JobId} cancelled", jobId);
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    "Error in cron loop for job {JobId}. Will retry at next occurrence.",
                    jobId);

                // Wait a bit before retrying to avoid tight loop on persistent errors
                await Task.Delay(TimeSpan.FromSeconds(30), cancellationToken);
            }
        }

        _logger.LogInformation(
            "Cron loop for job {JobId} stopped", jobId);
    }
}