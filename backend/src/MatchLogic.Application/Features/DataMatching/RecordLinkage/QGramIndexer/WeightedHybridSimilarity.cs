using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.RecordLinkage.QGramIndexer
{
    public class WeightedHybridSimilarity : IQGramSimilarityStrategy
    {
        public string Name => "WeightedHybrid";

        public double CalculateSimilarity(HashSet<uint> set1, HashSet<uint> set2)
        {
            if (set1.Count == 0 && set2.Count == 0) return 1.0;
            if (set1.Count == 0 || set2.Count == 0) return 0.0;

            var intersection = set1.Intersect(set2).Count();

            // Jaccard
            var union = set1.Union(set2).Count();
            var jaccard = union > 0 ? (double)intersection / union : 0.0;

            // Dice
            var dice = (2.0 * intersection) / (set1.Count + set2.Count);

            // Overlap
            var overlap = (double)intersection / Math.Min(set1.Count, set2.Count);

            // Weighted combination (adjust weights as needed)
            return (((0.3 * jaccard) + (0.4 * dice) + (0.3 * overlap)) + 0.2);
        }
    }
}
