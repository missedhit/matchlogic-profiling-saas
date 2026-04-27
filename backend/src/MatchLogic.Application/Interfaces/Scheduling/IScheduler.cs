using MatchLogic.Domain.Entities.Common;
using System;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.Scheduling;

/// <summary>
/// Abstraction over job scheduling engine (Hangfire)
/// NEVER call Hangfire directly - always use this interface
/// This allows swapping Hangfire with another scheduler if needed
/// </summary>
public interface IScheduler
{
    /// <summary>
    /// Enqueue a job for immediate background execution
    /// Replaces: IBackgroundJobQueue.QueueJobAsync()
    /// </summary>
    /// <param name="job">Job information with steps to execute</param>
    /// <returns>Hangfire job ID for tracking</returns>
    Task<string> EnqueueJobAsync(ProjectJobInfo job);

    /// <summary>
    /// Schedule a job to run after a specified delay
    /// </summary>
    /// <param name="job">Job information</param>
    /// <param name="delay">Delay before execution</param>
    /// <returns>Hangfire job ID</returns>
    Task<string> ScheduleJobAsync(ProjectJobInfo job, TimeSpan delay);

    /// <summary>
    /// Add or update recurring job with strongly-typed method call
    /// </summary>
    /// <typeparam name="T">Service type containing the method</typeparam>
    /// <param name="jobId">Unique job identifier</param>
    /// <param name="cronExpression">Cron expression</param>
    /// <param name="methodCall">Expression representing method to call</param>
    /// <param name="timeZone">Timezone for schedule</param>
    Task AddOrUpdateRecurringJobAsync<T>(
        string jobId,
        string cronExpression,
        Expression<Func<T, Task>> methodCall,
        string timeZone = "UTC");

    /// <summary>
    /// Trigger a recurring job immediately (doesn't affect schedule)
    /// </summary>
    /// <param name="jobId">Recurring job ID</param>
    Task TriggerRecurringJobAsync(string jobId);

    /// <summary>
    /// Remove a recurring job completely
    /// </summary>
    /// <param name="jobId">Recurring job ID</param>
    Task RemoveRecurringJobAsync(string jobId);

    /// <summary>
    /// Delete a queued or scheduled job
    /// </summary>
    /// <param name="jobId">Job ID to delete</param>
    /// <returns>True if deleted successfully</returns>
    Task<bool> DeleteJobAsync(string jobId);

    /// <summary>
    /// Requeue a failed job for retry
    /// </summary>
    /// <param name="jobId">Failed job ID</param>
    /// <returns>True if requeued successfully</returns>
    Task<bool> RequeueJobAsync(string jobId);

    /// <summary>
    /// Get job state information
    /// </summary>
    /// <param name="jobId">Job ID</param>
    /// <returns>Job state details (queued, processing, succeeded, failed)</returns>
    Task<JobStateInfo> GetJobStateAsync(string jobId);

    // Add this — schedules a service method call with a delay
    // Different from ScheduleJobAsync which only accepts ProjectJobInfo
    Task<string> ScheduleServiceJobAsync<T>(
        Expression<Func<T, Task>> methodCall,
        TimeSpan delay);
}

/// <summary>
/// Job state information returned by scheduler
/// </summary>
public class JobStateInfo
{
    public string JobId { get; set; }
    public string State { get; set; } // Enqueued, Processing, Succeeded, Failed, Scheduled, Deleted
    public DateTime? CreatedAt { get; set; }
    public DateTime? StartedAt { get; set; }
    public DateTime? FinishedAt { get; set; }
    public string ErrorMessage { get; set; }
}
