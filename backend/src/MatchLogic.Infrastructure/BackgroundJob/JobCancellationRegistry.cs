using MatchLogic.Application.Interfaces.Common;
using System;
using System.Collections.Concurrent;
using System.Threading;

namespace MatchLogic.Infrastructure.BackgroundJob;

/// <summary>
/// Thread-safe registry for managing cancellation tokens of running jobs.
/// Registered as a singleton so all background services share the same registry.
/// </summary>
public class JobCancellationRegistry : IJobCancellationRegistry
{
    private readonly ConcurrentDictionary<Guid, CancellationTokenSource> _tokens = new();

    public CancellationToken Register(Guid runId, CancellationToken parentToken = default)
    {
        var cts = CancellationTokenSource.CreateLinkedTokenSource(parentToken);
        _tokens[runId] = cts;
        return cts.Token;
    }

    public bool TryCancel(Guid runId)
    {
        if (_tokens.TryGetValue(runId, out var cts))
        {
            cts.Cancel();
            return true;
        }
        return false;
    }

    public void Remove(Guid runId)
    {
        if (_tokens.TryRemove(runId, out var cts))
        {
            cts.Dispose();
        }
    }

    public bool IsRegistered(Guid runId)
    {
        return _tokens.ContainsKey(runId);
    }
}
