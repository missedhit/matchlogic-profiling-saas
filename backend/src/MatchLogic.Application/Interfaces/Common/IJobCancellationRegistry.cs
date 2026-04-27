using System;
using System.Threading;

namespace MatchLogic.Application.Interfaces.Common;

/// <summary>
/// Registry for managing cancellation tokens of running jobs.
/// Enables external cancellation of long-running background jobs.
/// </summary>
public interface IJobCancellationRegistry
{
    /// <summary>
    /// Registers a run and returns a CancellationToken linked to the provided parent token.
    /// </summary>
    CancellationToken Register(Guid runId, CancellationToken parentToken = default);

    /// <summary>
    /// Attempts to cancel a registered run. Returns true if the run was found and cancelled.
    /// </summary>
    bool TryCancel(Guid runId);

    /// <summary>
    /// Removes a run registration (call in finally block after job completes).
    /// </summary>
    void Remove(Guid runId);

    /// <summary>
    /// Checks if a run is registered.
    /// </summary>
    bool IsRegistered(Guid runId);
}
