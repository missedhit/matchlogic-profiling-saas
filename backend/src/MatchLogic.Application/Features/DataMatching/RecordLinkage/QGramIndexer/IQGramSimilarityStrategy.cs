using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.RecordLinkage.QGramIndexer
{
    public interface IQGramSimilarityStrategy
    {
        double CalculateSimilarity(HashSet<uint> set1, HashSet<uint> set2);
        string Name { get; }
    }
}
