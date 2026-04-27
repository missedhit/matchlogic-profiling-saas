using MatchLogic.Application.Interfaces.LiveSearch;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.LiveSearch
{
    public class LiveSearchConfiguration
    {
        public bool IsIndexingNode { get; set; } = false;
        public Guid ProjectId { get; set; }
        public Guid DataSourceId { get; set; }
        public bool EnableCleansing { get; set; } = true;
        public int IndexPollIntervalSeconds { get; set; } = 5;
        public int IndexPollTimeoutMinutes { get; set; } = 5;
        public int QGramSize { get; set; } = 3;
        public double MinScoreThreshold { get; set; } = 0.7;
        public int MaxDegreeOfParallelism { get; set; } = 4;
        public bool UseMemoryMappedStores { get; set; } = true;
        public int InMemoryThreshold { get; set; } = 100000;
        public int MaxCandidates { get; set; } = 10000;
        public int MaxResults { get; set; } = 10;
        public SearchStrategy SearchStrategy { get; set; } = SearchStrategy.TopN;

        // Per-request CPU fan-out cap for the comparison phase. Lower values favour higher overall
        // throughput under concurrent load at the cost of per-request latency. Set to -1 to inherit
        // the shared RecordLinkageOptions.MaxDegreeOfParallelism (batch default).
        public int QueryMaxDegreeOfParallelism { get; set; } = 2;

        // Max concurrent search requests per query node. Excess requests queue up to
        // QueryRequestQueueLimit before returning 503. Set to -1 to skip the limiter.
        public int QueryMaxConcurrentRequests { get; set; } = -1;
        public int QueryRequestQueueLimit { get; set; } = 50;
    }

    
}
