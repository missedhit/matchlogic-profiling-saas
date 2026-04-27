using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.RecordLinkage
{
    public interface IRecordStore : IDisposable
    {
        Task AddRecordAsync(IDictionary<string, object> record);
        Task<IDictionary<string, object>> GetRecordAsync(int rowNumber);
        Task<IList<IDictionary<string, object>>> GetRecordsAsync(IEnumerable<int> rowNumbers);
        Task SwitchToReadOnlyModeAsync();
        StorageStatistics GetStatistics();
    }
}
