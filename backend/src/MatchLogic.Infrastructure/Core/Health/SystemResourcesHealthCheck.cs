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



public class SystemResourcesHealthCheck : IHealthCheck, ISystemResourcesHealthCheck
{
    private readonly RecordLinkageOptions _options;
    private readonly ILogger<SystemResourcesHealthCheck> _logger;

    public SystemResourcesHealthCheck(
        IOptions<RecordLinkageOptions> options,
        ILogger<SystemResourcesHealthCheck> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        try
        {

            cancellationToken.ThrowIfCancellationRequested();

            var availableMemoryMB = GetAvailableMemoryMB();
            var cpuUsage = await GetCpuUsageAsync();

            var data = new Dictionary<string, object>
            {
                { "AvailableMemoryMB", availableMemoryMB },
                { "CpuUsage", cpuUsage },
                { "MaxDegreeOfParallelism", _options.MaxDegreeOfParallelism }
            };

            if (availableMemoryMB < _options.MaxMemoryMB * 0.2) // Less than 20% of max memory available
            {
                return HealthCheckResult.Degraded(
                    "Low memory availability",
                    data: data);
            }

            if (cpuUsage > 85) // CPU usage above 85%
            {
                return HealthCheckResult.Degraded(
                    "High CPU usage",
                    data: data);
            }

            return HealthCheckResult.Healthy("System resources are healthy", data);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error checking system resources");
            return HealthCheckResult.Unhealthy("Error checking system resources", ex);
        }
    }

    private long GetAvailableMemoryMB()
    {
        if (IsWindowsPlatform())
        {
            return GC.GetGCMemoryInfo().TotalAvailableMemoryBytes / 1024 / 1024;
        }
        else
        {
            // On Linux, read from /proc/meminfo
            var memInfo = ReadAllText("/proc/meminfo");
            var available = memInfo
                .Split('\n')
                .FirstOrDefault(l => l.StartsWith("MemAvailable:"))
                ?.Split(':')[1]
                .Trim()
                .Split(' ')[0];

            return available != null ? long.Parse(available) / 1024 : 0;
        }
    }

    protected virtual async Task<double> GetCpuUsageAsync()
    {
        if (IsWindowsPlatform())
        {
            using var pc = new System.Diagnostics.PerformanceCounter(
                "Processor", "% Processor Time", "_Total");
            pc.NextValue(); // First call will always return 0
            await Task.Delay(1000); // Wait for a second
            return pc.NextValue();
        }
        else
        {
            // On Linux, read from /proc/stat
             (long Idle, long Total) GetCpuTimes()
            {
                var statLine = ReadAllLines("/proc/stat")
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
            await Task.Delay(1000);
            var (idleEnd, totalEnd) = GetCpuTimes();

            var idleDelta = idleEnd - idleStart;
            var totalDelta = totalEnd - totalStart;

            return 100.0 * (1.0 - (double)idleDelta / totalDelta);
        }
    }

     protected virtual bool IsWindowsPlatform()
    {
        return RuntimeInformation.IsOSPlatform(OSPlatform.Windows);
    }
    protected virtual string ReadAllText(string path) => File.ReadAllText(path);
    protected virtual string[] ReadAllLines(string path) => File.ReadAllLines(path);
}
