using MatchLogic.Application.Interfaces.Phonetics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.Comparators;


public class PhoneticFuzzyComparisonStrategy : IComparisonStrategy
{
    private readonly IPhoneticConverter _phoneticConverter;

    public PhoneticFuzzyComparisonStrategy(IPhoneticConverter phoneticConverter)
    {
        _phoneticConverter = phoneticConverter ?? throw new ArgumentNullException(nameof(phoneticConverter));
    }

    public double Compare(string input1, string input2, ComparatorConfig config)
    {
        if (!(config is PhoneticFuzzyComparatorConfig stringConfig))
            throw new ArgumentException("Invalid configuration type");

        if (string.IsNullOrEmpty(input1) || string.IsNullOrEmpty(input2))
            return 0;

        
        int rating = _phoneticConverter.MatchRating(input1, input2);
        int minRating = _phoneticConverter.MinimumRating(input1, input2);
        int maxRating = 5;

        decimal totalRange = minRating + maxRating;
        decimal distance = Math.Abs(maxRating - rating);

        var score = (double)(1 - (distance / totalRange));

        // Only return score if it meets or exceeds the required level
        return score >= stringConfig.PhoneticRating ? score : 0;
    }
}
