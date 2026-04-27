using MediatR;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MatchLogic.Application.Interfaces.Events;

namespace MatchLogic.Infrastructure.Events.Providers;
public class MediatREventBus : IEventBus
{
    private readonly IMediator _mediator;
    private readonly ILogger<MediatREventBus> _logger;

    public MediatREventBus(IMediator mediator, ILogger<MediatREventBus> logger)
    {
        _mediator = mediator;
        _logger = logger;
    }

    public async Task PublishAsync<TEvent>(TEvent @event, CancellationToken cancellationToken = default)
        where TEvent : BaseEvent
    {
        try
        {
            await _mediator.Publish(@event, cancellationToken);
            //_logger.LogInformation("Event {EventType} published successfully", typeof(TEvent).Name);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error publishing event {EventType}", typeof(TEvent).Name);
            throw;
        }
    }
}