using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Entities.Common;
using MediatR;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.EventHandlers;
public class DatabaseUpdateEventHandler : INotificationHandler<JobEvent>
{
    private readonly ILogger<DatabaseUpdateEventHandler> _logger;
    private readonly IJobStatusRepository _genericRepository;
    // may be required to push FlowStatistics for project creating new genericRepository
    public DatabaseUpdateEventHandler(
        ILogger<DatabaseUpdateEventHandler> logger, IJobStatusRepository genericRepository)
    {
        _logger = logger;
        _genericRepository = genericRepository;
    }

    public async Task Handle(JobEvent notification, CancellationToken cancellationToken)
    {
        string jobStatusCollection = Constants.Collections.JobStatus;
        try
        {
            var jobCol = await _genericRepository.QueryAsync(x => x.JobId == notification.JobId, jobStatusCollection);
            var jobStatus = jobCol.FirstOrDefault()
                ?? new JobStatus { Id = Guid.Empty, JobId = notification.JobId, Steps = new Dictionary<string, JobStepInfo>() };

            // Update job status based on the event
            UpdateJobStatus(jobStatus, notification);

            // Save or update
            if (jobStatus.Id == Guid.Empty)
            {
                jobStatus.StartTime = notification.EventTime;
                jobStatus.Id = notification.JobId;
                await _genericRepository.InsertAsync(jobStatus, jobStatusCollection);
            }
            else
            {
                await _genericRepository.UpdateAsync(jobStatus, jobStatusCollection);
            }

            //_logger.LogInformation("Updated job status for job {JobId}: {Status}", notification.JobId, notification.Status);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error updating job status for job {JobId}", notification.JobId);
            throw;
        }
    }
    private void UpdateJobStatus(JobStatus jobStatus, JobEvent @event)
    {
        jobStatus.Status = @event.Status;

        if (@event.ProcessedRecords.HasValue)
        {
            jobStatus.ProcessedRecords = @event.ProcessedRecords.Value;
        }

        if (@event.Error != null)
        {
            jobStatus.Error = @event.Error;
        }
        else if (@event.Status is "Completed" or "Failed")
        {
            jobStatus.EndTime = @event.EventTime;
        }
        // Update step information
        if (@event.CurrentStep != null)
        {
            jobStatus.Steps[@event.CurrentStep.StepKey] = @event.CurrentStep;
        }

        // Merge metadata
        if (@event.Metadata != null)
        {
            foreach (var kvp in @event.Metadata)
            {
                jobStatus.Metadata[kvp.Key] = kvp.Value;
            }
        }
        if (@event.Statistics != null)
        {
            jobStatus.Statistics = @event.Statistics;
        }

        if (@event.DataSourceName != null)
        {
            jobStatus.DataSourceName = @event.DataSourceName;
        }
    }
}
