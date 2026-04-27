using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.RecordLinkage.QGramIndexer
{
    public class LengthAdjustedSimilarity : IQGramSimilarityStrategy
    {
        public string Name => "LengthAdjusted";

        public double CalculateSimilarity(HashSet<uint> hashes1, HashSet<uint> hashes2)
        {
            var intersection = hashes1.Intersect(hashes2).Count();
            var union = hashes1.Union(hashes2).Count();
            var lengthRatio = Math.Min(hashes1.Count, hashes2.Count) / (double)Math.Max(hashes1.Count, hashes2.Count);
            var jaccard = union > 0 ? (double)intersection / union : 0.0;

            // Boost score based on length similarity
            return (jaccard + (0.2 * lengthRatio * (1 - jaccard)));
        }
    }
}
