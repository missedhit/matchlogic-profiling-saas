using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.RecordLinkage.QGramIndexer
{
    public class DiceSimilarity : IQGramSimilarityStrategy
    {
        public string Name => "Dice";

        public double CalculateSimilarity(HashSet<uint> set1, HashSet<uint> set2)
        {
            if (set1.Count == 0 && set2.Count == 0) return 1.0;
            if (set1.Count == 0 || set2.Count == 0) return 0.0;

            var intersection = set1.Intersect(set2).Count();
            var totalSize = set1.Count + set2.Count;
            return totalSize > 0 ? (2.0 * intersection) / totalSize : 0.0;
        }
    }
}
