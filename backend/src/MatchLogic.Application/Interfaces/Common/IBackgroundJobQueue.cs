using MatchLogic.Domain.Entities.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.Common;
public interface IBackgroundJobQueue<T> where T : class
{
    ValueTask QueueJobAsync(T jobInfo);
    ValueTask<T> DequeueAsync(CancellationToken cancellationToken);
}
