using MatchLogic.Domain.Import;
using System.Collections.Generic;
namespace MatchLogic.Domain.Project;
public class BaseConnectionInfo
{
    public DataSourceType Type { get; set; }
    public Dictionary<string, string> Parameters { get; set; } = new();

}
