using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Domain.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.DataMatching;
public interface IRecordMatcher : IAsyncDisposable
{
    Task<IAsyncEnumerable<(IDictionary<string, object>, IDictionary<string, object>)>> FindMatchesAsync(
        IAsyncEnumerable<IDictionary<string, object>> records,
        IEnumerable<MatchCriteria> criteria,
        IStepProgressTracker progressTracker,
        IStepProgressTracker stepProgressTracker1,
        CancellationToken cancellationToken = default);
}
