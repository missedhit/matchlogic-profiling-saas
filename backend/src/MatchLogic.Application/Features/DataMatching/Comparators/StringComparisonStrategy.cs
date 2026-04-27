using MatchLogic.Application.Interfaces.Comparator;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.Comparators;

[HandlesConfig(typeof(StringComparatorConfig))]
public class StringComparisonStrategy : IComparisonStrategy
{
    private readonly IStringSimilarityCalculator _similarityCalculator;

    public StringComparisonStrategy(IStringSimilarityCalculator similarityCalculator)
    {
        _similarityCalculator = similarityCalculator ?? throw new ArgumentNullException(nameof(similarityCalculator));
    }

    public double Compare(string input1, string input2, ComparatorConfig config)
    {
        if (!(config is StringComparatorConfig stringConfig))
            throw new ArgumentException("Invalid configuration type");

        if (string.IsNullOrEmpty(input1) || string.IsNullOrEmpty(input2))
            return 0;

        input1 = input1.ToLower();
        input2 = input2.ToLower();

        var similarity = _similarityCalculator.CalculateSimilarity(input1, input2);

        return similarity >= stringConfig.Level ? similarity : 0;
    }
}
