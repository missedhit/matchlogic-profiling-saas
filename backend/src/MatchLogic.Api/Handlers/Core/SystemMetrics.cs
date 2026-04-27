using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.Core;

public class SystemMetrics
{
    public long AvailableMemoryMB { get; set; }
    public double CpuUsagePercentage { get; set; }
    public long Gen0Collections { get; set; }
    public long Gen1Collections { get; set; }
    public long Gen2Collections { get; set; }
    public double Gen2CollectionRate { get; set; }
}
