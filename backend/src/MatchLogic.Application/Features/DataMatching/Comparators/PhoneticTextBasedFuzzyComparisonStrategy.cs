using MatchLogic.Application.Interfaces.Comparator;
using MatchLogic.Application.Interfaces.Phonetics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.Comparators;

[HandlesConfig(typeof(PhoneticFuzzyComparatorConfig))]
public class PhoneticTextBasedFuzzyComparisonStrategy : IComparisonStrategy
{
    private readonly IPhoneticConverter _phoneticConverter;
    private readonly IStringSimilarityCalculator _similarityCalculator;

    public PhoneticTextBasedFuzzyComparisonStrategy(IPhoneticConverter phoneticConverter
        , IStringSimilarityCalculator stringSimilarityCalculator)
    {
        _phoneticConverter = phoneticConverter ?? throw new ArgumentNullException(nameof(phoneticConverter));
        _similarityCalculator = stringSimilarityCalculator ?? throw new ArgumentNullException(nameof(stringSimilarityCalculator));
    }

    public double Compare(string input1, string input2, ComparatorConfig config)
    {
        if (!(config is PhoneticFuzzyComparatorConfig stringConfig))
            throw new ArgumentException("Invalid configuration type");

        if (string.IsNullOrEmpty(input1) || string.IsNullOrEmpty(input2))
            return 0;


        if(_phoneticConverter.IsSimilar(input1, input2))
        {
            double similarity = _similarityCalculator.CalculateSimilarity(input1, input2);
            return (0.7 + (0.3 * similarity));
        }

        return 0;

    }


}
