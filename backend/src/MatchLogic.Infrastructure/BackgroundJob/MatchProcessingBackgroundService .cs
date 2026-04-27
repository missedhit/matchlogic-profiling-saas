using MatchLogic.Application.Features.DataMatching;
using MatchLogic.Application.Interfaces.Common;
using MatchLogic.Application.Interfaces.DataMatching;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.Entities.Common;
using MatchLogic.Domain.Project;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.BackgroundJob;

#region MatchProcessingBackgroundService
public class MatchProcessingBackgroundService : BackgroundService
{
    private readonly ILogger<MatchProcessingBackgroundService> _logger;
    private readonly IBackgroundJobQueue<MatchJobInfo> _jobQueue;
    private readonly IServiceScopeFactory _serviceProvider;
    private readonly int _maxConcurrentJobs;
    private readonly SemaphoreSlim _semaphore;
    public MatchProcessingBackgroundService(
        ILogger<MatchProcessingBackgroundService> logger,
        IServiceScopeFactory serviceProvider,
        IBackgroundJobQueue<MatchJobInfo> jobQueue)
    {
        _logger = logger;
        _serviceProvider = serviceProvider;
        _jobQueue = jobQueue;
        _maxConcurrentJobs = 2;
        _semaphore = new SemaphoreSlim(_maxConcurrentJobs, _maxConcurrentJobs);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var jobInfo = await _jobQueue.DequeueAsync(stoppingToken);
                if (jobInfo != null)
                {
                    // Wait for a slot to be available
                    await _semaphore.WaitAsync(stoppingToken);

                    // Start processing in the background and don't await it
                    _ = ProcessJobWithSemaphoreReleaseAsync(jobInfo, stoppingToken)
                        .ContinueWith(
                            task =>
                            {
                                if (task.IsFaulted)
                                {
                                    _logger.LogError(task.Exception,
                                        "Unhandled error in job processing");
                                }
                            },
                            stoppingToken);
                }
                else
                {
                    // If no job is available, wait a bit before checking again
                    await Task.Delay(1000, stoppingToken);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in background job loop");
                await Task.Delay(1000, stoppingToken); // Wait before retrying
            }
        }
    }

    private async Task ProcessJobWithSemaphoreReleaseAsync(MatchJobInfo jobInfo,
           CancellationToken stoppingToken)
    {
        try
        {
            await ProcessJobAsync(jobInfo, stoppingToken);
        }
        finally
        {
            _semaphore.Release();
        }
    }
    private async Task ProcessJobAsync(MatchJobInfo jobInfo, CancellationToken stoppingToken)
    {
        try
        {
            await using AsyncServiceScope scope = _serviceProvider.CreateAsyncScope();

            IJobEventPublisher _jobEventPublisher =
                scope.ServiceProvider.GetRequiredService<IJobEventPublisher>();

            IRecordMatchingFacade _recordMatchingFacade =
                scope.ServiceProvider.GetRequiredService<IRecordMatchingFacade>();

            try
            {

                await _recordMatchingFacade.ProcessMatchingJobAsync(
                    jobInfo.JobId,
                    jobInfo.Criteria,
                    jobInfo.MergeOverlappingGroups,
                    jobInfo.IsProbabilistic,
                    stoppingToken
                );

            }
            catch (Exception ex)
            {
                await _jobEventPublisher.PublishJobFailedAsync(jobInfo.JobId, ex.Message);
                throw;
            }

        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing job {JobId}", jobInfo.JobId);
        }
    }
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Stopping background processing service");
        await base.StopAsync(cancellationToken);
    }

    public override void Dispose()
    {
        _semaphore.Dispose();
        base.Dispose();
    }
}

#endregion

#region ProjectBackgroundService
public class ProjectBackgroundService : BackgroundService
{
    private readonly ILogger<ProjectBackgroundService> _logger;
    private readonly IBackgroundJobQueue<ProjectJobInfo> _jobQueue;
    private readonly IServiceScopeFactory _serviceProvider;
    private readonly IJobCancellationRegistry _cancellationRegistry;
    private readonly SemaphoreSlim _semaphore;
    private readonly int _maxConcurrentJobs = 1;

    public ProjectBackgroundService(
        ILogger<ProjectBackgroundService> logger,
        IServiceScopeFactory serviceProvider,
        IBackgroundJobQueue<ProjectJobInfo> jobQueue,
        IJobCancellationRegistry cancellationRegistry)
    {
        _logger = logger;
        _serviceProvider = serviceProvider;
        _jobQueue = jobQueue;
        _cancellationRegistry = cancellationRegistry;
        _semaphore = new SemaphoreSlim(_maxConcurrentJobs, _maxConcurrentJobs);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var jobInfo = await _jobQueue.DequeueAsync(stoppingToken);
                if (jobInfo != null)
                {
                    await _semaphore.WaitAsync(stoppingToken);

                    _ = ProcessJobAsync(jobInfo, stoppingToken)
                        .ContinueWith(
                            task =>
                            {
                                if (task.IsFaulted)
                                {
                                    _logger.LogError(task.Exception, "Error processing job {JobId}", jobInfo.JobId);
                                }
                                _semaphore.Release();
                            },
                            stoppingToken);
                }
                else
                {
                    await Task.Delay(1000, stoppingToken);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in background job loop");
                await Task.Delay(1000, stoppingToken);
            }
        }
    }

    private async Task ProcessJobAsync(ProjectJobInfo jobInfo, CancellationToken stoppingToken)
    {
        // Register for external cancellation, linked with host stoppingToken
        var jobToken = _cancellationRegistry.Register(jobInfo.RunId, stoppingToken);

        try
        {
            await using var scope = _serviceProvider.CreateAsyncScope();
            var commandFactory = scope.ServiceProvider.GetRequiredService<ICommandFactory>();
            var projectRunRepository = scope.ServiceProvider.GetRequiredService<IGenericRepository<ProjectRun, Guid>>();
            var stepJobRepository = scope.ServiceProvider.GetRequiredService<IGenericRepository<StepJob, Guid>>();
            var jobEventPublisher = scope.ServiceProvider.GetRequiredService<IJobEventPublisher>();

            var context = new CommandContext(
                jobInfo.RunId,
                jobInfo.ProjectId,
                jobInfo.CurrentStep.Id,
                projectRunRepository,
                stepJobRepository
            );

            var command = commandFactory.GetCommand(jobInfo.CurrentStep.Type);
            await command.ExecuteAsync(context, jobInfo.CurrentStep, jobToken);
        }
        catch (OperationCanceledException) when (jobToken.IsCancellationRequested)
        {
            _logger.LogInformation("Job run {RunId} was cancelled", jobInfo.RunId);
            // CancelJobRunHandler already marks the DB status — no extra work needed
        }
        catch (Exception ex)
        {
            await using var scope = _serviceProvider.CreateAsyncScope();
            var jobEventPublisher = scope.ServiceProvider.GetRequiredService<IJobEventPublisher>();
            // Use step.Id (not jobInfo.JobId) — JobStatus is keyed by step ID
            try { await jobEventPublisher.PublishJobFailedAsync(jobInfo.CurrentStep.Id, ex.Message); }
            catch (Exception pubEx) { _logger.LogCritical(pubEx, "Failed to publish failure event for step {StepId}", jobInfo.CurrentStep.Id); }
            _logger.LogError(ex,
                "Error processing step {StepType} for run {RunId}",
                jobInfo.CurrentStep.Type,
                jobInfo.RunId);
            throw;
        }
        finally
        {
            _cancellationRegistry.Remove(jobInfo.RunId);
        }
    }
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Stopping project processing service");
        await base.StopAsync(cancellationToken);
    }
    public override void Dispose()
    {
        _semaphore.Dispose();
        base.Dispose();
    }
}

#endregion
