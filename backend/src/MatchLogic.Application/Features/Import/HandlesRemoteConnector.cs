using MatchLogic.Domain.Import;
using System;

namespace MatchLogic.Application.Features.Import;

/// <summary>
/// Marks a class as a remote file connector implementation for a specific DataSourceType.
/// Used by RemoteFileConnectorFactory for reflection-based discovery.
/// </summary>
[AttributeUsage(AttributeTargets.Class)]
public class HandlesRemoteConnector : Attribute
{
    public DataSourceType DataSourceType { get; }

    public HandlesRemoteConnector(DataSourceType dataSourceType)
    {
        DataSourceType = dataSourceType;
    }
}
