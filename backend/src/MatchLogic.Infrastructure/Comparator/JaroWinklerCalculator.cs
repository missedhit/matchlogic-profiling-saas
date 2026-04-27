using MatchLogic.Application.Interfaces.Comparator;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SimMetrics.Net.Metric;


namespace MatchLogic.Infrastructure.Comparator;
public class JaroWinklerCalculator : IStringSimilarityCalculator
{
    private readonly JaroWinkler _jaroWinkler;

    public JaroWinklerCalculator()
    {
        _jaroWinkler = new JaroWinkler();
    }

    public double CalculateSimilarity(string str1, string str2)
    {
        if (string.IsNullOrEmpty(str1) || string.IsNullOrEmpty(str2))
            return 0;

        return _jaroWinkler.GetSimilarity(str1, str2);  // Already returns value between 0 and 1
    }
}
