using MatchLogic.Application.Events.Builders;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Domain.Entities.Common;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.BackgroundJob;
public class StepProgressTracker : IStepProgressTracker
{
    private readonly Guid _jobId;
    private readonly string _stepName;
    private readonly int _stepNumber;
    private readonly int _totalSteps;
    private readonly IEventBus _eventBus;
    private string _stepKey;
    private int _totalItems;
    private int _processedItems;
    private DateTime _startTime;   
    private readonly JobStepInfo _stepInfo;
    private readonly SemaphoreSlim _updateLock = new(1, 1);

    public StepProgressTracker(
        Guid jobId,
        string stepName,
        int stepNumber,
        int totalSteps,
        IEventBus eventBus)
    {
        _jobId = jobId;
        _stepName = stepName;
        _stepNumber = stepNumber;
        _totalSteps = totalSteps;
        _eventBus = eventBus;
        _stepKey = $"step_{stepNumber}";
        _startTime = DateTime.UtcNow;        

        // Create single instance of StepInfo that we'll reuse
        _stepInfo = new JobStepInfo
        {
            StepKey = _stepKey,
            StepName = _stepName,
            StepNumber = _stepNumber,
            TotalSteps = _totalSteps,
            StartTime = _startTime
        };
    }

    public async Task StartStepAsync(int totalItems, CancellationToken cancellationToken = default)
    {
        _totalItems = totalItems;
        _processedItems = 0;
        _startTime = DateTime.UtcNow;        

        // Update reusable step info
        _stepInfo.TotalItems = _totalItems;
        _stepInfo.ProcessedItems = 0;
        _stepInfo.Status = "Processing";
        _stepInfo.StartTime = _startTime;
        _stepInfo.EndTime = null;
        _stepInfo.Error = null;

        await PublishProgressEvent(
            "Processing",
            $"Started step {_stepNumber}/{_totalSteps}: {_stepName}",
            cancellationToken);
    }

    public async Task UpdateProgressAsync(int currentItem, string message = null, CancellationToken cancellationToken = default)
    {
        var now = DateTime.UtcNow;
        _processedItems = currentItem;

        // Check if we should update based on time or progress threshold
        if (!ShouldPublishUpdate(now))
        {
            return;
        }
        
        try
        {
            // Double-check after acquiring lock
            if (!ShouldPublishUpdate(now))
            {
                return;
            }

            _stepInfo.ProcessedItems = currentItem;
            _stepInfo.Message = message ?? $"Processing {_stepName}: {currentItem}/{_totalItems}";            

            await PublishProgressEvent(
                "Processing",
                message ?? $"Processing {_stepName}: {currentItem}/{_totalItems}",
                cancellationToken);
        }
        finally
        {            
        }
    }

    public async Task CompleteStepAsync(string message = null, CancellationToken cancellationToken = default)
    {
        _stepInfo.ProcessedItems = _totalItems;
        _stepInfo.Status = "Completed";
        _stepInfo.EndTime = DateTime.UtcNow;

        await PublishProgressEvent(
            "StepCompleted",
            message ?? $"Completed step {_stepNumber}/{_totalSteps}: {_stepName}",
            cancellationToken);
    }

    public async Task FailStepAsync(string error, CancellationToken cancellationToken = default)
    {
        _stepInfo.Status = "Failed";
        _stepInfo.Error = error;
        _stepInfo.EndTime = DateTime.UtcNow;

        await PublishProgressEvent(
            "StepFailed",
            $"Failed step {_stepNumber}/{_totalSteps}: {_stepName}",
            cancellationToken,
            error);
    }
    private bool ShouldPublishUpdate(DateTime now)
    {
        // Update if enough time has passed or if it's the first/last item
        return (_processedItems == 1 || _processedItems == _totalItems) ||
               (_processedItems % 1000 == 0); // Adjust batch size as needed
    }

    private async Task PublishProgressEvent(
        string status,
        string message,
        CancellationToken cancellationToken,
        string error = null)
    {
        var builder = new JobEventBuilder(_jobId, _eventBus)
            .WithStatus(status)
            .WithProgress(_processedItems, _totalItems)
            .WithMessage(message)
            .WithStepInfo(_stepInfo)
            .WithMetadata("stepKey", _stepKey);

        if (error != null)
        {
            builder.WithError(error);
        }

        await builder.PublishAsync(cancellationToken);
    }
    public void Dispose()
    {
        
    }
}
