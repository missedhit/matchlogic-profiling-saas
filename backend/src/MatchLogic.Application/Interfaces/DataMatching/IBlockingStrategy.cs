using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.DataMatching;
public interface IBlockingStrategy : IAsyncDisposable
{
    Task<IAsyncEnumerable<IGrouping<string, IDictionary<string, object>>>> BlockRecordsAsync(
        IAsyncEnumerable<IDictionary<string, object>> records,
        IEnumerable<string> blockingFields,
        CancellationToken cancellationToken = default);
}
