using MatchLogic.Domain.Entities.Common;
using MatchLogic.Domain.Entities.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.Events;
public interface IJobEventBuilder
{
    IJobEventBuilder WithStatus(string status);
    IJobEventBuilder WithMessage(string message);
    IJobEventBuilder WithProgress(int processed, int total);
    IJobEventBuilder WithError(string error);
    IJobEventBuilder WithMetadata(string key, object value);
    IJobEventBuilder WithStepInfo(JobStepInfo stepInfo);
    IJobEventBuilder WithStatistics(FlowStatistics statistics);
    IJobEventBuilder WithDataSourceName(string dataSourceName);
    Task PublishAsync(CancellationToken cancellationToken = default);
}
