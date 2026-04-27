using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Configuration;

public class SchedulerSettings
{
    public const string SectionName = "Scheduler";

    public bool Enabled { get; set; } = true;
    public int MaxConsecutiveFailuresBeforeSuspend { get; set; } = 5;
    public string DefaultTimeZone { get; set; } = "UTC";
    public int DefaultRetryAttempts { get; set; } = 3;
}
