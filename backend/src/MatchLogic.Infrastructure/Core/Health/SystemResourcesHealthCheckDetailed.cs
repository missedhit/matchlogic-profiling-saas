using MatchLogic.Application.Features.Core;
using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Application.Interfaces.Core;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Core.Health;
public class SystemResourcesHealthCheckDetailed : IHealthCheck, ISystemResourcesHealthCheck
{
    private readonly RecordLinkageOptions _options;
    private readonly ILogger _logger;
    private readonly PerformanceMetricsProvider _metricsProvider;

    public SystemResourcesHealthCheckDetailed(
        IOptions<RecordLinkageOptions> options,
        ILogger logger)
    {
        _options = options.Value;
        _logger = logger;
        _metricsProvider = new PerformanceMetricsProvider(logger);
    }

    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var metrics = await _metricsProvider.GetMetricsAsync(cancellationToken);

            var data = new Dictionary<string, object>
            {
                { "AvailableMemoryMB", metrics.AvailableMemoryMB },
                { "CpuUsage", metrics.CpuUsagePercentage },
                { "MaxDegreeOfParallelism", _options.MaxDegreeOfParallelism },
                { "GCGen0Collections", metrics.Gen0Collections },
                { "GCGen1Collections", metrics.Gen1Collections },
                { "GCGen2Collections", metrics.Gen2Collections }
            };

            // Determine health status
            var status = CalculateHealthStatus(metrics);

            return status.healthStatus switch
            {
                HealthStatus.Healthy => HealthCheckResult.Healthy("System resources are healthy", data),
                HealthStatus.Degraded => HealthCheckResult.Degraded(status.message, data: data),
                _ => HealthCheckResult.Unhealthy(status.message, data: data)
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error checking system resources");
            return HealthCheckResult.Unhealthy("Error checking system resources", ex);
        }
    }

    private (HealthStatus healthStatus, string message) CalculateHealthStatus(SystemMetrics metrics)
    {
        var issues = new List<string>();

        // Memory checks
        if (metrics.AvailableMemoryMB < _options.MaxMemoryMB * 0.2)
        {
            issues.Add("Low memory availability");
        }

        // CPU checks
        if (metrics.CpuUsagePercentage > 85)
        {
            issues.Add("High CPU usage");
        }

        // GC checks
        if (metrics.Gen2Collections > 10 && metrics.Gen2CollectionRate > 2)
        {
            issues.Add("High Gen2 collection rate");
        }

        return issues.Count switch
        {
            0 => (HealthStatus.Healthy, "System resources are healthy"),
            1 => (HealthStatus.Degraded, issues[0]),
            _ => (HealthStatus.Unhealthy, string.Join(", ", issues))
        };
    }
}

public class PerformanceMetricsProvider
{
    private readonly ILogger _logger;
    private DateTime _lastCheck = DateTime.MinValue;
    private long _lastGen2Count = 0;

    public PerformanceMetricsProvider(ILogger logger)
    {
        _logger = logger;
    }

    public async Task<SystemMetrics> GetMetricsAsync(CancellationToken cancellationToken)
    {
        var metrics = new SystemMetrics
        {
            AvailableMemoryMB = GetAvailableMemoryMB(),
            CpuUsagePercentage = await GetCpuUsageAsync(cancellationToken),
            Gen0Collections = GC.CollectionCount(0),
            Gen1Collections = GC.CollectionCount(1),
            Gen2Collections = GC.CollectionCount(2)
        };

        // Calculate Gen2 collection rate (collections per minute)
        if (_lastCheck != DateTime.MinValue)
        {
            var timeDiff = DateTime.UtcNow - _lastCheck;
            var gen2Diff = metrics.Gen2Collections - _lastGen2Count;
            metrics.Gen2CollectionRate = gen2Diff / timeDiff.TotalMinutes;
        }

        _lastCheck = DateTime.UtcNow;
        _lastGen2Count = metrics.Gen2Collections;

        return metrics;
    }

    private long GetAvailableMemoryMB()
    {
        try
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                return GC.GetGCMemoryInfo().TotalAvailableMemoryBytes / 1024 / 1024;
            }

            // On Linux, read from /proc/meminfo
            var memInfo = File.ReadAllText("/proc/meminfo");
            var available = memInfo
                .Split('\n')
                .FirstOrDefault(l => l.StartsWith("MemAvailable:"))
                ?.Split(':')[1]
                .Trim()
                .Split(' ')[0];

            return available != null ? long.Parse(available) / 1024 : 0;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting available memory");
            return 0;
        }
    }

    private async Task<double> GetCpuUsageAsync(CancellationToken cancellationToken)
    {
        try
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                using var pc = new System.Diagnostics.PerformanceCounter(
                    "Processor", "% Processor Time", "_Total");
                pc.NextValue(); // First call will always return 0
                await Task.Delay(1000, cancellationToken); // Wait for a second
                return pc.NextValue();
            }

            // On Linux, read from /proc/stat
            static (long Idle, long Total) GetCpuTimes()
            {
                var statLine = File.ReadAllLines("/proc/stat")
                    .First(l => l.StartsWith("cpu "));
                var times = statLine.Split(' ', StringSplitOptions.RemoveEmptyEntries)
                    .Skip(1)
                    .Select(long.Parse)
                    .ToArray();

                var idle = times[3];
                var total = times.Sum();
                return (idle, total);
            }

            var (idleStart, totalStart) = GetCpuTimes();
            await Task.Delay(1000, cancellationToken);
            var (idleEnd, totalEnd) = GetCpuTimes();

            var idleDelta = idleEnd - idleStart;
            var totalDelta = totalEnd - totalStart;

            return 100.0 * (1.0 - (double)idleDelta / totalDelta);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting CPU usage");
            return 0;
        }
    }
}

