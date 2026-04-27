using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Application.Interfaces.Scheduling;
using MediatR;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.EventHandlers;

/// <summary>
/// MediatR handler for ProjectRunCompletedEvent
/// Updates scheduler statistics when a run completes
/// Breaks the circular dependency between ProjectService and SchedulerService
/// </summary>
public class ProjectRunCompletedEventHandler : INotificationHandler<ProjectRunCompletedEvent>
{
    private readonly ISchedulerService _schedulerService;
    private readonly ILogger<ProjectRunCompletedEventHandler> _logger;

    public ProjectRunCompletedEventHandler(
        ISchedulerService schedulerService,
        ILogger<ProjectRunCompletedEventHandler> logger)
    {
        _schedulerService = schedulerService;
        _logger = logger;
    }

    public async Task Handle(ProjectRunCompletedEvent notification, CancellationToken cancellationToken)
    {
        // Only process if this was a scheduled run
        if (!notification.ScheduledTaskExecutionId.HasValue)
        {
            _logger.LogDebug(
                "ProjectRun {RunId} was not triggered by scheduler. Skipping statistics update.",
                notification.RunId);
            return;
        }

        try
        {           

            await _schedulerService.UpdateExecutionStatisticsAsync(
                notification.ScheduledTaskExecutionId.Value,
                notification.Status,
                notification.ErrorMessage);
           
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Failed to update scheduler statistics for execution {ExecutionId}",
                notification.ScheduledTaskExecutionId.Value);

            // Don't throw - statistics update failure shouldn't break the system
            // MediatR will continue processing other handlers
        }
    }
}