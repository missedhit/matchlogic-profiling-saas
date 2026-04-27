using MatchLogic.Domain.Entities.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.DataMatching;
public interface IMatchGroupingService
{
    IAsyncEnumerable<MatchGroup> CreateMatchGroupsAsync(
        IAsyncEnumerable<IDictionary<string, object>> matchResults,
        bool mergeOverlappingGroups = false,
        bool similarRecordsInGroups = false,
        CancellationToken cancellationToken = default);
}
