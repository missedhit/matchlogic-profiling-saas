using MatchLogic.Application.Interfaces.Import;
using MatchLogic.Domain.Import;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace MatchLogic.Application.Features.Import;
public class ConnectionReaderStrategyFactory
{
    private readonly Dictionary<Type, Type> _strategyMap;

    public ConnectionReaderStrategyFactory()
    {
        _strategyMap = DiscoverStrategyTypes();
        
    }

    private static Dictionary<Type, Type> DiscoverStrategyTypes()
    {
#pragma warning disable CS8619 // Nullability of reference types in value doesn't match target type.
        return
            Assembly.Load("MatchLogic.Infrastructure")
            .GetTypes()
            .Where(t => t.IsClass && !t.IsAbstract && typeof(IConnectionReaderStrategy).IsAssignableFrom(t))
            .Select(t => new
            {
                StrategyType = t,
                Attribute = t.GetCustomAttribute<HandlesConnectionConfig>(),
            })
            .Where(x => x.Attribute != null)
            .ToDictionary(
                x => x.Attribute.ConfigType,
                x => x.StrategyType
            );
#pragma warning restore CS8619 // Nullability of reference types in value doesn't match target type.
    }

    
    public IConnectionReaderStrategy GetStrategy(ConnectionConfig config,ILogger logger)
    {
        var configType = config.GetType();
        if (!_strategyMap.TryGetValue(configType, out var strategyType))
        {
            throw new ArgumentException($"No strategy found for configuration type: {configType.Name}");
        }
        return (IConnectionReaderStrategy)Activator.CreateInstance(strategyType,config, logger);
    }
}