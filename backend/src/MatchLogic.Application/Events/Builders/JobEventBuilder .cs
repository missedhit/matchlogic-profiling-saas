using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Domain.Entities.Common;
using MatchLogic.Domain.Entities.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Events.Builders;
public class JobEventBuilder : IJobEventBuilder
{
    private readonly JobEvent _event;
    private readonly IEventBus _eventBus;

    public JobEventBuilder(Guid jobId, IEventBus eventBus)
    {
        _event = new JobEvent { JobId = jobId };
        _eventBus = eventBus;
    }

    public IJobEventBuilder WithStatus(string status)
    {
        _event.Status = status;
        return this;
    }

    public IJobEventBuilder WithMessage(string message)
    {
        _event.Message = message;
        return this;
    }

    public IJobEventBuilder WithProgress(int processed, int total)
    {
        _event.ProcessedRecords = processed;
        _event.TotalRecords = total;
        return this;
    }

    public IJobEventBuilder WithError(string error)
    {
        _event.Error = error;
        return this;
    }

    public IJobEventBuilder WithMetadata(string key, object value)
    {
        _event.Metadata[key] = value;
        return this;
    }
    public IJobEventBuilder WithStepInfo(JobStepInfo stepInfo)
    {
        _event.CurrentStep = stepInfo;
        _event.StepKey = stepInfo.StepKey;
        return this;
    }

    public IJobEventBuilder WithStatistics(FlowStatistics statistics)
    {
        _event.Statistics = statistics;
        return this;
    }
    public Task PublishAsync(CancellationToken cancellationToken = default)
    {
        return _eventBus.PublishAsync(_event, cancellationToken);
    }

    public IJobEventBuilder WithDataSourceName(string dataSourceName)
    {
        _event.DataSourceName = dataSourceName;
        return this;
    }
}

