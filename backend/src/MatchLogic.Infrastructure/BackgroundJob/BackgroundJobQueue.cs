using MatchLogic.Application.Interfaces.Common;
using MatchLogic.Domain.Entities.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.BackgroundJob;
public class BackgroundJobQueue<T> : IBackgroundJobQueue<T> where T : class
{
    private readonly Channel<T> _queue;

    public BackgroundJobQueue(int capacity = 100)
    {
        var options = new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,
            SingleWriter = false,
        };
        _queue = Channel.CreateBounded<T>(options);
    }

    public async ValueTask QueueJobAsync(T jobInfo)
    {
        if (jobInfo == null) throw new ArgumentNullException(nameof(jobInfo));
        await _queue.Writer.WriteAsync(jobInfo);
    }

    public async ValueTask<T> DequeueAsync(CancellationToken cancellationToken)
    {
        try
        {
            return await _queue.Reader.ReadAsync(cancellationToken);
        }
        catch (ChannelClosedException)
        {
            return null;
        }
    }
}
