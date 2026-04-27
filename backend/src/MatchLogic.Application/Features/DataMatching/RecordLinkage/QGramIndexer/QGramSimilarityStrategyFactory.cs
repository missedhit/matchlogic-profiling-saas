using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.RecordLinkage.QGramIndexer
{
    public enum QGramSimilarityAlgorithm
    {
        Jaccard,
        Dice,
        Overlap,
        Cosine,
        WeightedHybrid,
        LengthAdjusted
    }

    public static class QGramSimilarityStrategyFactory
    {
        public static IQGramSimilarityStrategy CreateStrategy(QGramSimilarityAlgorithm algorithm)
        {
            return algorithm switch
            {
                QGramSimilarityAlgorithm.Jaccard => new JaccardSimilarity(),
                QGramSimilarityAlgorithm.Dice => new DiceSimilarity(),
                QGramSimilarityAlgorithm.Overlap => new OverlapSimilarity(),
                QGramSimilarityAlgorithm.Cosine => new CosineSimilarity(),
                QGramSimilarityAlgorithm.WeightedHybrid => new WeightedHybridSimilarity(),
                QGramSimilarityAlgorithm.LengthAdjusted => new LengthAdjustedSimilarity(),
                _ => new JaccardSimilarity() // Default fallback
            };
        }

        public static IQGramSimilarityStrategy CreateStrategy(string algorithmName)
        {
            if (Enum.TryParse<QGramSimilarityAlgorithm>(algorithmName, true, out var algorithm))
            {
                return CreateStrategy(algorithm);
            }
            return new JaccardSimilarity(); // Default fallback
        }
    }
}
