using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.RecordLinkage
{
    public class DataSourceIndexingConfig
    {
        public Guid DataSourceId { get; init; }
        public string DataSourceName { get; init; }
        public List<string> FieldsToIndex { get; init; } = new();
        public bool UseInMemoryStore { get; set; } = true;
        public int InMemoryThreshold { get; set; } = 100000;
        public bool EnableWithinSourceMatching { get; set; } = false;
        public Dictionary<string, double> FieldWeights { get; init; } = new();
    }
}
