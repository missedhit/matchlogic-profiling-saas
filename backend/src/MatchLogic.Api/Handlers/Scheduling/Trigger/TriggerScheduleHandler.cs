using MatchLogic.Api.Handlers.Scheduling.DTOs;
using MatchLogic.Application.Interfaces.Scheduling;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;
using System.Threading;

namespace MatchLogic.Api.Handlers.Scheduling.Trigger;

public class TriggerScheduleHandler : IRequestHandler<TriggerScheduleCommand, Result<TriggerScheduleResponse>>
{
    private readonly ISchedulerService _schedulerService;
    private readonly ILogger<TriggerScheduleHandler> _logger;

    public TriggerScheduleHandler(ISchedulerService schedulerService, ILogger<TriggerScheduleHandler> logger)
    {
        _schedulerService = schedulerService;
        _logger = logger;
    }

    public async Task<Result<TriggerScheduleResponse>> Handle(TriggerScheduleCommand request, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Manual trigger requested for schedule {ScheduleId}", request.ScheduleId);

        var execution = await _schedulerService.TriggerManualExecutionAsync(request.ScheduleId, request.TriggeredBy);

        return Result<TriggerScheduleResponse>.Success(
            new TriggerScheduleResponse(execution.Id, execution.HangfireJobId));
    }
}

