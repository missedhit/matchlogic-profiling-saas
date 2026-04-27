using MatchLogic.Domain.Entities.Common;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.Scheduling;

/// <summary>
/// Executes a ProjectJobInfo - Called by Hangfire workers
/// Replaces: ProjectBackgroundService
/// Each job executes in its own Hangfire worker thread (parallel execution)
/// </summary>
public interface IJobExecutor
{
    Task ExecuteAsync(ProjectJobInfo job);
}
