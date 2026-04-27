using MatchLogic.Domain.Entities.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.Events;
public interface IJobEventPublisher
{
    Task PublishJobStartedAsync(Guid jobId, int totalSteps, string message = null, string dataSoruceName=null, CancellationToken cancellationToken = default);    
    Task PublishJobCompletedAsync(Guid jobId, string message = null, FlowStatistics statistics = null, CancellationToken cancellationToken = default);
    Task PublishJobFailedAsync(Guid jobId, string error, FlowStatistics statistics = null, CancellationToken cancellationToken = default);

    // For custom statuses and messages
    IJobEventBuilder CreateEvent(Guid jobId);

    /// <summary>
    /// Publish ProjectRun completion event
    /// This is handled by MediatR handlers (e.g., updates scheduler statistics)
    /// </summary>
    Task PublishRunCompletedAsync(ProjectRunCompletedEvent runEvent, CancellationToken cancellationToken = default);
    IStepProgressTracker CreateStepTracker(Guid jobId, string stepName, int stepNumber, int totalSteps);
}

public interface IStepProgressTracker
{
    Task StartStepAsync(int totalItems, CancellationToken cancellationToken = default);
    Task UpdateProgressAsync(int currentItem, string message = null, CancellationToken cancellationToken = default);
    Task CompleteStepAsync(string message = null, CancellationToken cancellationToken = default);
    Task FailStepAsync(string error, CancellationToken cancellationToken = default);
}