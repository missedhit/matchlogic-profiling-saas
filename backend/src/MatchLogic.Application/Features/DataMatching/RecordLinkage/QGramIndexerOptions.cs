using MatchLogic.Application.Features.DataMatching.RecordLinkage.QGramIndexer;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.RecordLinkage
{
    public class QGramIndexerOptions
    {
        public int QGramSize { get; set; } = 3;
        public int DefaultInMemoryThreshold { get; set; } = 100000;
        public int DiskBufferSize { get; set; } = 256 * 1024; // 256KB
        public int IndexSaveFrequency { get; set; } = 10000;
        public int MaxRecordSize { get; set; } = 10 * 1024 * 1024; // 10MB
        public bool EnableCompression { get; set; } = true;
        public string TempDirectory { get; set; } = null; // null = system temp
        public int MaxParallelism { get; set; } = Environment.ProcessorCount;
        public int CandidateChannelCapacity { get; set; } = 10000;

        public QGramSimilarityAlgorithm SimilarityAlgorithm { get; set; } = QGramSimilarityAlgorithm.Jaccard;

        public int MaxCandidatesPerRecord { get; set; } = 500;
        public int MaxRecordsPerHashBucket { get; set; } = 50000;

        // Bucket optimization strategy
        public BucketOptimizationStrategy BucketStrategy { get; set; } = BucketOptimizationStrategy.Sample;
        public double BucketSamplingRate { get; set; } = 0.2; // Used only when strategy is Sample
    }

    public enum BucketOptimizationStrategy
    {
        None = 0,      // Process all buckets regardless of size
        Skip = 1,      // Skip oversized buckets entirely
        Sample = 2     // Sample oversized buckets        
    }
}
