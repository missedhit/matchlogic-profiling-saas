using MatchLogic.Domain.Project;
using System.Collections.Generic;

namespace MatchLogic.Domain.Import;

public interface IConnectionConfig
{
    ConnectionConfig CreateFromArgs(DataSourceType Type, Dictionary<string, string> args, DataSourceConfiguration? sourceConfiguration = null);
    public Dictionary<string, string> Parameters { get; set; }
    bool ValidateConnection();
}



