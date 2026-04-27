using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.RecordLinkage
{
    public class RowMetadata
    {
        public Guid DataSourceId { get; init; }
        public int RowNumber { get; init; }
        public Dictionary<string, HashSet<uint>> FieldHashes { get; init; } = new(StringComparer.OrdinalIgnoreCase);
        public DateTime IndexedAt { get; init; } = DateTime.UtcNow;
    }
}
