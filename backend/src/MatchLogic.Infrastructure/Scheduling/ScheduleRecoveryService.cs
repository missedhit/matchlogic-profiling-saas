using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Scheduling;
using MatchLogic.Domain.Scheduling;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Scheduling;

/// <summary>
/// On startup, re-registers all active schedules with the scheduler engine.
/// Essential for Desktop mode (BackgroundServiceScheduler loses state on restart).
/// Safety net for Server mode (Cron only — Simple interval is handled by Hangfire).
/// Must run AFTER StaleJobCleanupService (registration order controls execution order).
/// </summary>
public class ScheduleRecoveryService : IHostedService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly AppModeSettings _appMode;
    private readonly ILogger<ScheduleRecoveryService> _logger;

    public ScheduleRecoveryService(
        IServiceScopeFactory scopeFactory,
        IOptions<AppModeSettings> appMode,
        ILogger<ScheduleRecoveryService> logger)
    {
        _scopeFactory = scopeFactory;
        _appMode = appMode.Value;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation(
            "ScheduleRecoveryService: rehydrating active schedules (AppMode: {Mode})...",
            _appMode.Mode);

        try
        {
            await using var scope = _scopeFactory.CreateAsyncScope();
            var scheduleRepo = scope.ServiceProvider
                .GetRequiredService<IGenericRepository<ScheduledTask, Guid>>();
            var schedulerService = scope.ServiceProvider
                .GetRequiredService<ISchedulerService>();

            var activeSchedules = await scheduleRepo.QueryAsync(
                s => s.IsEnabled && s.Status == ScheduleStatus.Active,
                Constants.Collections.ScheduledTasks);

            var list = activeSchedules.ToList();

            if (list.Count == 0)
            {
                _logger.LogInformation("ScheduleRecoveryService: no active schedules to rehydrate");
                return;
            }

            // Server mode: skip Simple interval (Hangfire recovers them, rehydration would duplicate)
            bool skipSimple = _appMode.IsServer;

            int succeeded = 0, failed = 0, skipped = 0;

            foreach (var schedule in list)
            {
                try
                {
                    if (skipSimple && schedule.ScheduleType == ScheduleType.Simple)
                    {
                        skipped++;
                        continue;
                    }

                    await schedulerService.RehydrateScheduleAsync(schedule.Id, skipSimple);
                    succeeded++;
                }
                catch (Exception ex)
                {
                    failed++;
                    _logger.LogError(ex,
                        "ScheduleRecoveryService: failed to rehydrate schedule {Id} ({Name})",
                        schedule.Id, schedule.Name);
                }
            }

            _logger.LogInformation(
                "ScheduleRecoveryService: rehydrated {Succeeded}/{Total} schedules ({Failed} failed, {Skipped} skipped)",
                succeeded, list.Count, failed, skipped);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ScheduleRecoveryService: error during recovery (non-fatal)");
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
    