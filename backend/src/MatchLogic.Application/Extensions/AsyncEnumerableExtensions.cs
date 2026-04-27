using MatchLogic.Application.Interfaces.Events;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Extensions;
public static class AsyncEnumerableExtensions
{
    public static async IAsyncEnumerable<T[]> ChunkAsync<T>(
        this IAsyncEnumerable<T> source,
        int size,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(size, 1);

        var chunk = new List<T>(size);
        await foreach (var item in source.WithCancellation(cancellationToken))
        {
            chunk.Add(item);
            if (chunk.Count == size)
            {
                yield return chunk.ToArray();
                chunk.Clear();
            }
        }

        if (chunk.Count > 0)
        {
            yield return chunk.ToArray();
        }
    }
    public static IAsyncEnumerable<T> TrackProgress<T>(
            this IAsyncEnumerable<T> source,
            IStepProgressTracker progressTracker, IOptions<JobProgressOptions> options = null)
    {
        var defaultOptions = options ?? Options.Create(new JobProgressOptions());
        var tracker = new StreamProgressTracker<T>(progressTracker, defaultOptions);
        return tracker.TrackProgress(source);
    }
}

#region Stream Extensions
public class StreamProgressTracker<T>
{
    private readonly IStepProgressTracker _progressTracker;
    private int _processedCount;
    private int _totalItemsCount;
    private readonly JobProgressOptions _options;    
    private readonly ILogger<StreamProgressTracker<T>> _logger;

    public StreamProgressTracker(
        IStepProgressTracker progressTracker,
        IOptions<JobProgressOptions> options,
        ILogger<StreamProgressTracker<T>> logger = null)
    {
        _progressTracker = progressTracker;
        _options = options.Value;
        _processedCount = 0;
        _logger = logger;
    }

    public async IAsyncEnumerable<T> TrackProgress(
        IAsyncEnumerable<T> source,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        _totalItemsCount = await source.CountAsync();
        await _progressTracker.StartStepAsync(_totalItemsCount, cancellationToken);
        await foreach (var item in source.WithCancellation(cancellationToken))
        {
            _processedCount++;
            var now = DateTime.UtcNow;

            if (ShouldReportProgress(now))
            {
                await UpdateProgressSafelyAsync(now, cancellationToken);
            }

            yield return item;
        }
    }

    private bool ShouldReportProgress(DateTime now)
    {
        return _processedCount % _options.DefaultBatchSize == 0 ||
               _processedCount == _totalItemsCount;
    }

    private async Task UpdateProgressSafelyAsync(DateTime now, CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested)
            return;

        try
        {            
            try
            {
                await _progressTracker.UpdateProgressAsync(
                    _processedCount,
                    null,
                    cancellationToken);                
            }
            finally
            {               
            }
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error updating progress at count: {Count}", _processedCount);
            await _progressTracker.FailStepAsync(ex.Message, cancellationToken);
            // Don't rethrow - we want to continue processing even if progress updates fail
        }
    }

    public int ProcessedCount => _processedCount;
}

public static class StreamProgressExtensions
{
    public static IAsyncEnumerable<T> TrackProgress<T>(
        this IAsyncEnumerable<T> source,
        IStepProgressTracker progressTracker,
        IOptions<JobProgressOptions> options,
        ILogger<StreamProgressTracker<T>> logger = null)
    {
        var tracker = new StreamProgressTracker<T>(progressTracker, options, logger);
        return tracker.TrackProgress(source);
    }
}
#endregion
