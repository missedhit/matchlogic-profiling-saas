using MatchLogic.Application.Interfaces.Comparator;
using MatchLogic.Domain.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.Comparators;
public class ComparatorBuilder : IComparatorBuilder
{
    private readonly ComparatorConfigFactory _configFactory;
    private readonly ComparatorStrategyFactory _strategyFactory;
    private ComparatorConfig? _config;
    private IComparisonStrategy? _strategy;

    public ComparatorBuilder(ComparatorConfigFactory configFactory, ComparatorStrategyFactory strategyFactory)
    {
        _configFactory = configFactory;
        _strategyFactory = strategyFactory;
    }

    public IComparatorBuilder WithArgs(Dictionary<ArgsValue, string> args)
    {
        _config = _configFactory.CreateFromArgs(args);
        _strategy = _strategyFactory.GetStrategy(_config);
        return this;
    }

    public IComparator Build()
    {
        if (_config == null || _strategy == null)
        {
            throw new InvalidOperationException("Configuration must be set before building the comparator");
        }

        return new Comparator(_strategy, _config);
    }
}
public interface IComparatorBuilder
{
    IComparatorBuilder WithArgs(Dictionary<ArgsValue, string> args);
    IComparator Build();
}