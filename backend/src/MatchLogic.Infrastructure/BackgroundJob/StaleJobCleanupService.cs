using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.BackgroundJob;

/// <summary>
/// On startup, marks any ProjectRuns and StepJobs stuck in InProgress as Failed.
/// This handles jobs that were interrupted by a server restart.
/// Registered BEFORE processing services so cleanup runs first.
/// </summary>
public class StaleJobCleanupService : IHostedService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<StaleJobCleanupService> _logger;

    public StaleJobCleanupService(
        IServiceScopeFactory scopeFactory,
        ILogger<StaleJobCleanupService> logger)
    {
        _scopeFactory = scopeFactory;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("StaleJobCleanupService: checking for stuck jobs...");

        try
        {
            await using var scope = _scopeFactory.CreateAsyncScope();
            var projectRunRepo = scope.ServiceProvider.GetRequiredService<IGenericRepository<ProjectRun, Guid>>();
            var stepJobRepo = scope.ServiceProvider.GetRequiredService<IGenericRepository<StepJob, Guid>>();

            // Mark stale StepJobs as Failed
            var staleSteps = await stepJobRepo.QueryAsync(
                s => s.Status == RunStatus.InProgress,
                Constants.Collections.StepJobs);

            var staleStepList = staleSteps.ToList();
            foreach (var step in staleStepList)
            {
                step.Status = RunStatus.Failed;
                step.EndTime = DateTime.UtcNow;
                await stepJobRepo.UpdateAsync(step, Constants.Collections.StepJobs);
            }

            if (staleStepList.Count > 0)
                _logger.LogWarning("StaleJobCleanupService: marked {Count} stuck StepJobs as Failed", staleStepList.Count);

            // Mark stale ProjectRuns as Failed
            var staleRuns = await projectRunRepo.QueryAsync(
                r => r.Status == RunStatus.InProgress,
                Constants.Collections.ProjectRuns);

            var staleRunList = staleRuns.ToList();
            foreach (var run in staleRunList)
            {
                run.Status = RunStatus.Failed;
                run.EndTime = DateTime.UtcNow;
                await projectRunRepo.UpdateAsync(run, Constants.Collections.ProjectRuns);
            }

            if (staleRunList.Count > 0)
                _logger.LogWarning("StaleJobCleanupService: marked {Count} stuck ProjectRuns as Failed", staleRunList.Count);

            if (staleStepList.Count == 0 && staleRunList.Count == 0)
                _logger.LogInformation("StaleJobCleanupService: no stuck jobs found");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "StaleJobCleanupService: error during cleanup (non-fatal)");
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
