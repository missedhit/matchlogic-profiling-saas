using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.Comparators;

[HandlesConfig(typeof(NumericComparatorConfig))]
public class NumericComparisonStrategy : IComparisonStrategy
{
    public double Compare(string input1, string input2, ComparatorConfig config)
    {
        if (!(config is NumericComparatorConfig numericConfig))
            throw new ArgumentException("Invalid configuration type");

        if (!decimal.TryParse(input1, out decimal value1) ||
            !decimal.TryParse(input2, out decimal value2))
        {
            return 0;
        }

        if (numericConfig.UpperLimit == 0 && numericConfig.LowerLimit == 0)
        {
            return value1 == value2 ? 1 : 0;
        }

        decimal upperBound = value1 + numericConfig.UpperLimit;
        decimal lowerBound = value1 - numericConfig.LowerLimit;

        // Check if value2 is within bounds
        if (value2 < lowerBound || value2 > upperBound)
            return 0;

        // If within bounds, calculate the score
        if (value2 == value1)
            return 1;

        decimal totalRange = numericConfig.UpperLimit + numericConfig.LowerLimit;
        decimal distance = Math.Abs(value2 - value1);

        return (double)(1 - (distance / totalRange));
    }
}
