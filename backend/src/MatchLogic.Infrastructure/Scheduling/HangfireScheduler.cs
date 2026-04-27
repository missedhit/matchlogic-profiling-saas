using MatchLogic.Application.Interfaces.Scheduling;
using MatchLogic.Domain.Entities.Common;
using Hangfire;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Scheduling;

/// <summary>
/// Hangfire implementation of IScheduler
/// Wraps Hangfire APIs to provide abstraction layer
/// </summary>
public class HangfireScheduler : IScheduler
{
    private readonly IBackgroundJobClient _backgroundJobClient;
    private readonly IRecurringJobManager _recurringJobManager;
    private readonly IJobExecutor _jobExecutor;
    private readonly ILogger<HangfireScheduler> _logger;

    public HangfireScheduler(
        IBackgroundJobClient backgroundJobClient,
        IRecurringJobManager recurringJobManager,
        IJobExecutor jobExecutor,
        ILogger<HangfireScheduler> logger)
    {
        _backgroundJobClient = backgroundJobClient;
        _recurringJobManager = recurringJobManager;
        _jobExecutor = jobExecutor;
        _logger = logger;
    }

    public Task<string> EnqueueJobAsync(ProjectJobInfo job)
    {
        _logger.LogInformation(
            "Enqueueing job {JobId} for project {ProjectId} - Step: {StepType}",
            job.JobId, job.ProjectId, job.CurrentStep.Type);

        // Enqueue for immediate execution
        var hangfireJobId = _backgroundJobClient.Enqueue(() =>
            _jobExecutor.ExecuteAsync(job));

        _logger.LogDebug("Job enqueued with Hangfire ID: {HangfireJobId}", hangfireJobId);

        return Task.FromResult(hangfireJobId);
    }

    public Task<string> ScheduleJobAsync(ProjectJobInfo job, TimeSpan delay)
    {
        _logger.LogInformation(
            "Scheduling job {JobId} with delay: {Delay}",
            job.JobId, delay);

        var hangfireJobId = _backgroundJobClient.Schedule(() =>
            _jobExecutor.ExecuteAsync(job), delay);

        return Task.FromResult(hangfireJobId);
    }

    public Task AddOrUpdateRecurringJobAsync<T>(
        string jobId,
        string cronExpression,
        Expression<Func<T, Task>> methodCall,
        string timeZone = "UTC")
    {
        _logger.LogInformation(
            "Creating/updating recurring job {JobId} with cron: {Cron}, timezone: {TimeZone}",
            jobId, cronExpression, timeZone);

        TimeZoneInfo tz;
        try
        {
            tz = TimeZoneInfo.FindSystemTimeZoneById(timeZone);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex,
                "Invalid timezone {TimeZone}, falling back to UTC", timeZone);
            tz = TimeZoneInfo.Utc;
        }

        _recurringJobManager.AddOrUpdate(
            jobId,
            methodCall,
            cronExpression,
            new RecurringJobOptions
            {
                TimeZone = tz,
                MisfireHandling = MisfireHandlingMode.Ignorable
            });

        return Task.CompletedTask;
    }

    public Task TriggerRecurringJobAsync(string jobId)
    {
        _logger.LogInformation("Triggering recurring job {JobId} immediately", jobId);

        _recurringJobManager.Trigger(jobId);

        return Task.CompletedTask;
    }

    public Task RemoveRecurringJobAsync(string jobId)
    {
        _logger.LogInformation("Removing recurring job {JobId}", jobId);

        _recurringJobManager.RemoveIfExists(jobId);

        return Task.CompletedTask;
    }

    public Task<bool> DeleteJobAsync(string jobId)
    {
        _logger.LogInformation("Deleting job {JobId}", jobId);

        var result = _backgroundJobClient.Delete(jobId);

        return Task.FromResult(result);
    }

    public Task<bool> RequeueJobAsync(string jobId)
    {
        _logger.LogInformation("Requeuing failed job {JobId}", jobId);

        var result = _backgroundJobClient.Requeue(jobId);

        return Task.FromResult(result);
    }

    public Task<JobStateInfo> GetJobStateAsync(string jobId)
    {
        try
        {
            var jobData = JobStorage.Current.GetConnection().GetJobData(jobId);

            if (jobData == null)
            {
                return Task.FromResult<JobStateInfo>(null);
            }

            var stateData = JobStorage.Current.GetMonitoringApi().JobDetails(jobId);

            var info = new JobStateInfo
            {
                JobId = jobId,
                State = jobData.State,
                CreatedAt = jobData.CreatedAt,
                ErrorMessage = stateData?.History?.FirstOrDefault(h => h.StateName == "Failed")?.Reason
            };

            return Task.FromResult(info);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting job state for {JobId}", jobId);
            return Task.FromResult<JobStateInfo>(null);
        }
    }

    public Task<string> ScheduleServiceJobAsync<T>(
    Expression<Func<T, Task>> methodCall,
    TimeSpan delay)
    {
        _logger.LogInformation(
            "Scheduling service job with delay: {Delay}", delay);

        var hangfireJobId = _backgroundJobClient.Schedule(methodCall, delay);

        return Task.FromResult(hangfireJobId);
    }
}
