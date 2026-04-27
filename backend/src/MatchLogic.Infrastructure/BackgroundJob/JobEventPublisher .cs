using MatchLogic.Application.Events.Builders;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Domain.CleansingAndStandaradization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.BackgroundJob;
public class JobEventPublisher : IJobEventPublisher
{
    private readonly IEventBus _eventBus;

    public JobEventPublisher(IEventBus eventBus)
    {
        _eventBus = eventBus;
    }

    public IJobEventBuilder CreateEvent(Guid jobId)
    {
        return new JobEventBuilder(jobId, _eventBus);
    }

    public Task PublishJobStartedAsync(Guid jobId, int totalSteps, string message = null, string dataSourceName = null, CancellationToken cancellationToken = default)
    {
        return CreateEvent(jobId)
            .WithStatus("Processing")
            .WithMessage(message ?? $"Job started with {totalSteps} steps")
            .WithProgress(0, totalSteps)
            .WithMetadata("totalSteps", totalSteps)
            .WithDataSourceName(dataSourceName)
            .PublishAsync(cancellationToken);
    }


    public Task PublishJobCompletedAsync(Guid jobId, string message = null, FlowStatistics statistics = null, CancellationToken cancellationToken = default)
    {
        return CreateEvent(jobId)
            .WithStatus("Completed")
            .WithMessage(message ?? "Job completed successfully")
            //.WithMetadata("endTime", DateTime.UtcNow)
            .WithStatistics(statistics ?? new FlowStatistics())            
            .PublishAsync(cancellationToken);
    }

    public Task PublishJobFailedAsync(Guid jobId, string error, FlowStatistics statistics = null, CancellationToken cancellationToken = default)
    {
        return CreateEvent(jobId)
            .WithStatus("Failed")
            .WithError(error)
            .WithMessage("Job failed")
            .WithStatistics(statistics ?? new FlowStatistics())            
            //.WithMetadata("endTime", DateTime.UtcNow)
            .PublishAsync(cancellationToken);
    }


    /// <summary>
    /// Publish ProjectRun completion event via MediatR
    /// This is handled by ProjectRunCompletedEventHandler which updates scheduler statistics
    /// </summary>
    public Task PublishRunCompletedAsync(ProjectRunCompletedEvent runEvent, CancellationToken cancellationToken = default)
    {
        return _eventBus.PublishAsync(runEvent, cancellationToken);
    }
    public IStepProgressTracker CreateStepTracker(Guid jobId, string stepName, int stepNumber, int totalSteps)
    {
        return new StepProgressTracker(jobId, stepName, stepNumber, totalSteps, _eventBus);
    }
}

