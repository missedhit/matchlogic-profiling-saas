using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.Comparators;

[HandlesConfig(typeof(PercentageNumericComparatorConfig))]
public class PercentageNumericComparisonStrategy : IComparisonStrategy
{
    public double Compare(string input1, string input2, ComparatorConfig config)
    {
        if (!(config is PercentageNumericComparatorConfig percentageConfig))
            throw new ArgumentException("Invalid configuration type");

        if (!decimal.TryParse(input1, out decimal value1) ||
            !decimal.TryParse(input2, out decimal value2))
        {
            return 0;
        }

        // If both percentages are 0, perform exact match
        if (percentageConfig.UpperPercentage == 0 && percentageConfig.LowerPercentage == 0)
        {
            return value1 == value2 ? 1 : 0;
        }

        // Calculate percentage-based bounds
        // For value1 = 100 and UpperPercentage = 10%, upper bound = 110
        // For value1 = 100 and LowerPercentage = 10%, lower bound = 90
        decimal upperBound, lowerBound;

        // Handle zero value case
        if (value1 == 0)
        {
            // For zero values, we can either:
            // Option 1: Use a small absolute tolerance
            // Option 2: Only match exact zeros
            // Here we'll use Option 2 for simplicity
            return value2 == 0 ? 1 : 0;
        }

        // Calculate bounds as percentages of value1
        decimal upperDelta = Math.Abs(value1) * (percentageConfig.UpperPercentage / 100m);
        decimal lowerDelta = Math.Abs(value1) * (percentageConfig.LowerPercentage / 100m);

        upperBound = value1 + upperDelta;
        lowerBound = value1 - lowerDelta;

        // Check if value2 is within bounds
        if (value2 < lowerBound || value2 > upperBound)
            return 0;

        // If within bounds, calculate the score
        if (value2 == value1)
            return 1;

        // Calculate similarity score based on how close value2 is to value1
        decimal totalRange = upperDelta + lowerDelta;

        // Avoid division by zero
        if (totalRange == 0)
            return value1 == value2 ? 1 : 0;

        decimal distance = Math.Abs(value2 - value1);

        return Math.Round((double)(1 - (distance / totalRange)), 2);
    }
}