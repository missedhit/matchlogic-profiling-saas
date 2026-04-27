using MatchLogic.Domain.Import;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.Project;
public class StepConfiguration
{
    public StepType Type { get; set; }
    public List<Guid> DataSourceIds { get; set; } = new();
    public Dictionary<string, object> Configuration { get; set; }

    public StepConfiguration(StepType type, params Guid[] dataSourceIds)
    {
        Type = type;
        DataSourceIds = dataSourceIds.ToList();
    }

    public StepConfiguration(StepType type, Dictionary<string, object> configuration, params Guid[] dataSourceIds)
    {
        Type = type;
        DataSourceIds = dataSourceIds.ToList();
        Configuration = configuration;
    }
}
