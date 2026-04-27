using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.RecordLinkage.QGramIndexer
{
    public class OverlapSimilarity : IQGramSimilarityStrategy
    {
        public string Name => "Overlap";

        public double CalculateSimilarity(HashSet<uint> set1, HashSet<uint> set2)
        {
            if (set1.Count == 0 && set2.Count == 0) return 1.0;
            if (set1.Count == 0 || set2.Count == 0) return 0.0;

            var intersection = set1.Intersect(set2).Count();
            var minSize = Math.Min(set1.Count, set2.Count);
            return minSize > 0 ? (double)intersection / minSize : 0.0;
        }
    }
}
