using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.RecordLinkage
{
    public class CandidateWithRecords
    {
        public IDictionary<string, object> Record1 { get; init; }
        public IDictionary<string, object> Record2 { get; init; }
        public Guid DataSource1Id { get; init; }
        public Guid DataSource2Id { get; init; }
        public int Row1Number { get; init; }
        public int Row2Number { get; init; }
        public HashSet<Guid> MatchDefinitionIds { get; init; } = new();
        public double EstimatedSimilarity { get; init; }
        public string MatchDefinitionIdsString => string.Join(",", MatchDefinitionIds.Select(id => id.ToString("N")));
    }
}
