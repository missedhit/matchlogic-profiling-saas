using MatchLogic.Application.Interfaces.Comparator;
using MatchLogic.Application.Interfaces.Phonetics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.Comparators;
public class ComparatorStrategyFactory
{
    private readonly Dictionary<Type, (Type StrategyType, object[] Dependencies)> _strategyMap;

    public ComparatorStrategyFactory(IStringSimilarityCalculator similarityCalculator,
        IPhoneticConverter phoneticConverter)
    {
        var dependencies = new object[] { similarityCalculator, phoneticConverter };
        _strategyMap = DiscoverStrategyTypes(dependencies);
    }

    private static Dictionary<Type, (Type, object[])> DiscoverStrategyTypes(object[] availableDependencies)
    {
#pragma warning disable CS8619 // Nullability of reference types in value doesn't match target type.
        return Assembly.GetExecutingAssembly()
            .GetTypes()
            .Where(t => t.IsClass && !t.IsAbstract && typeof(IComparisonStrategy).IsAssignableFrom(t))
            .Select(t => new
            {
                StrategyType = t,
                Attribute = t.GetCustomAttribute<HandlesConfigAttribute>(),
                Dependencies = t.GetConstructors()
                    .First()
                    .GetParameters()
                    .Select(p => availableDependencies.FirstOrDefault(d => p.ParameterType.IsInstanceOfType(d)))
                    .ToArray()
            })
            .Where(x => x.Attribute != null)
            .ToDictionary(
                x => x.Attribute.ConfigType,
                x => (x.StrategyType, x.Dependencies)
            );
#pragma warning restore CS8619 // Nullability of reference types in value doesn't match target type.
    }

    public IComparisonStrategy GetStrategy(ComparatorConfig config)
    {
        var configType = config.GetType();

        if (!_strategyMap.TryGetValue(configType, out var strategyInfo))
        {
            throw new ArgumentException($"No strategy found for configuration type: {configType.Name}");
        }

        return (IComparisonStrategy)Activator.CreateInstance(strategyInfo.StrategyType, strategyInfo.Dependencies);
    }
}
