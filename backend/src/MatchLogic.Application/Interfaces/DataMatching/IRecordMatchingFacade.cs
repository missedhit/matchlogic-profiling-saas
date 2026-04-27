using MatchLogic.Domain.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.DataMatching;
public interface IRecordMatchingFacade
{
    Task ProcessMatchingJobAsync(Guid sourceJobId, IEnumerable<MatchCriteria> matchCriteria, bool mergeOverlappingGroups = false, bool useProbabilistic = false, CancellationToken cancellationToken = default);
}
