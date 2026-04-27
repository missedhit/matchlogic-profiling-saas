using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Channels;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Common;
public class BroadcastChannel<T>
{
    private readonly Channel<T> _source;
    private readonly List<Channel<T>> _targets;
    private readonly SemaphoreSlim _lock = new(1, 1);

    public BroadcastChannel(int capacity, int targetCount)
    {
        _source = Channel.CreateBounded<T>(new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = true
        });

        _targets = Enumerable.Range(0, targetCount)
            .Select(_ => Channel.CreateBounded<T>(capacity))
            .ToList();
    }

    public ChannelWriter<T> Writer => _source.Writer;
    public IReadOnlyList<ChannelReader<T>> Readers => _targets.Select(c => c.Reader).ToList();

    public async Task StartBroadcastAsync(CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var item in _source.Reader.ReadAllAsync(cancellationToken))
            {
                await _lock.WaitAsync(cancellationToken);
                try
                {
                    await Task.WhenAll(_targets.Select(target =>
                        target.Writer.WriteAsync(item, cancellationToken).AsTask()));
                }
                finally
                {
                    _lock.Release();
                }
            }
        }
        finally
        {
            foreach (var target in _targets)
            {
                target.Writer.Complete();
            }
        }
    }
}
