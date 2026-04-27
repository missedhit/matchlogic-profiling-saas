using MatchLogic.Domain.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.Comparators;
public class ComparatorConfigFactory
{
    private readonly IEnumerable<ComparatorConfig> _configPrototypes;

    public ComparatorConfigFactory()
    {
        _configPrototypes = DiscoverConfigurations();
    }

    private IEnumerable<ComparatorConfig> DiscoverConfigurations()
    {
        // Get all types that inherit from ComparatorConfig
        var configTypes = Assembly.GetExecutingAssembly()
            .GetTypes()
            .Where(t => t.IsClass
                && !t.IsAbstract
                && typeof(ComparatorConfig).IsAssignableFrom(t));

        // Create instances of each configuration type
        return configTypes.Select(type =>
            (ComparatorConfig)Activator.CreateInstance(type))
            .ToList();
    }

    public ComparatorConfig CreateFromArgs(Dictionary<ArgsValue, string> args)
    {
        var config = _configPrototypes
            .FirstOrDefault(c => c.CanCreateFromArgs(args));

        if (config == null)
        {
            throw new ArgumentException("No suitable configuration found for the provided arguments");
        }

        return config.CreateFromArgs(args);
    }
}
