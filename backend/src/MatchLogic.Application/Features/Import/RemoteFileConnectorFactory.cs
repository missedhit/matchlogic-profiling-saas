using MatchLogic.Application.Interfaces.Import;
using MatchLogic.Domain.Import;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace MatchLogic.Application.Features.Import;

/// <summary>
/// Factory that creates IRemoteFileConnector instances based on DataSourceType.
/// Uses reflection to discover connector implementations decorated with [HandlesRemoteConnector].
/// </summary>
public class RemoteFileConnectorFactory
{
    private readonly Dictionary<DataSourceType, Type> _connectorMap;

    public RemoteFileConnectorFactory()
    {
        _connectorMap = DiscoverConnectorTypes();
    }

    private static Dictionary<DataSourceType, Type> DiscoverConnectorTypes()
    {
        return Assembly.Load("MatchLogic.Infrastructure")
            .GetTypes()
            .Where(t => t.IsClass && !t.IsAbstract && typeof(IRemoteFileConnector).IsAssignableFrom(t))
            .Select(t => new
            {
                ConnectorType = t,
                Attribute = t.GetCustomAttribute<HandlesRemoteConnector>(),
            })
            .Where(x => x.Attribute != null)
            .ToDictionary(
                x => x.Attribute!.DataSourceType,
                x => x.ConnectorType
            );
    }

    public IRemoteFileConnector Create(DataSourceType type, RemoteFileConnectionConfig config, ILogger logger)
    {
        if (!_connectorMap.TryGetValue(type, out var connectorType))
        {
            throw new ArgumentException($"No remote file connector found for DataSourceType: {type}");
        }
        return (IRemoteFileConnector)Activator.CreateInstance(connectorType, config, logger)!;
    }

    public bool IsSupported(DataSourceType type) => _connectorMap.ContainsKey(type);
}
