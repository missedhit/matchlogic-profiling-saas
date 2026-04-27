using MatchLogic.Application.Interfaces.Comparator;
using MatchLogic.Domain.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.Comparators;
public class Comparator : IComparator
{
    private readonly IComparisonStrategy _strategy;
    private readonly ComparatorConfig _config;

    public Comparator(IComparisonStrategy strategy, ComparatorConfig config)
    {
        _strategy = strategy;
        _config = config;
    }

    public double Compare(string input1, string input2)
    {
        return _strategy.Compare(input1, input2, _config);
    }
}
