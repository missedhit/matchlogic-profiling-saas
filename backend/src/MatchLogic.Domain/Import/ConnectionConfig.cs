using MatchLogic.Domain.Project;
using System.Collections.Generic;

namespace MatchLogic.Domain.Import;
public abstract class ConnectionConfig : IConnectionConfig
{
    public Dictionary<string, string> Parameters { get; set; } = new();
    public DataSourceConfiguration? SourceConfig { get; set; } = null;

    public abstract bool CanCreateFromArgs(DataSourceType Type);
    public abstract ConnectionConfig CreateFromArgs(DataSourceType Type, Dictionary<string, string> args, DataSourceConfiguration? sourceConfiguration = null);

    public virtual bool ValidateConnection()
    {
        return Parameters != null && Parameters.Count != 0;
    }

    protected bool ValidateParameter(string key)
    {
        return Parameters.ContainsKey(key) && !string.IsNullOrEmpty(Parameters[key]);
    }
}



