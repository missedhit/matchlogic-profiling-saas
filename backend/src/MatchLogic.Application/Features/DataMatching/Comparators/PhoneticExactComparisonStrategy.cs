using MatchLogic.Application.Interfaces.Phonetics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.Comparators;

[HandlesConfig(typeof(PhoneticExactComparatorConfig))]
public class PhoneticExactComparisonStrategy : IComparisonStrategy
{
    private readonly IPhoneticConverter _phoneticConverter;

    public PhoneticExactComparisonStrategy(IPhoneticConverter phoneticConverter)
    {
        _phoneticConverter = phoneticConverter ?? throw new ArgumentNullException(nameof(phoneticConverter));
    }

    public double Compare(string input1, string input2, ComparatorConfig config)
    {
        if (!(config is PhoneticExactComparatorConfig stringConfig))
            throw new ArgumentException("Invalid configuration type");

        if (string.IsNullOrEmpty(input1) || string.IsNullOrEmpty(input2))
            return 0;

        // Only return score if it meets or exceeds the required level
        return _phoneticConverter.IsSimilar(input1, input2) ? 1 : 0;
    }
}
