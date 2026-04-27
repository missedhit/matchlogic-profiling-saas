using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.Project;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.DataProfiling
{
    public interface IDataProfiler
    {
        /// <summary>
        /// Profiles data from a stream
        /// </summary>
        Task<MatchLogic.Domain.DataProfiling.ProfileResult> ProfileDataAsync(
            IAsyncEnumerable<IDictionary<string, object>> dataStream,
            DataSource dataSource = null,
            IEnumerable<string> columnsToProfile = null,
            ICommandContext commandContext = null,
            string collectionName = null,
            CancellationToken cancellationToken = default);
    }
}
