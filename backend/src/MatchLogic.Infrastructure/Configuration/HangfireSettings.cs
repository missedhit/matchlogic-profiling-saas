using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Configuration;

public class HangfireSettings
{
    public const string SectionName = "Hangfire";

    public string ServerName { get; set; } = "MatchLogic";
    public int WorkerCount { get; set; } = 10;
    public int ServerTimeout { get; set; } = 300;
    public int SchedulePollingInterval { get; set; } = 15;
    public string[] Queues { get; set; } = new[] { "default", "critical", "low" };
    public bool DashboardEnabled { get; set; } = true;
    public string DashboardPath { get; set; } = "/hangfire";
    public int AutomaticRetryAttempts { get; set; } = 3;
    public StorageOptions StorageOptions { get; set; } = new();
}

public class StorageOptions
{
    public string Prefix { get; set; } = "hangfire";
    public bool CheckConnection { get; set; } = true;
    public TimeSpan InvisibilityTimeout { get; set; } = TimeSpan.FromMinutes(30);
}
