using MatchLogic.Application.Interfaces.Scheduling;
using System.Threading.Tasks;
using System.Threading;

namespace MatchLogic.Api.Handlers.Scheduling.Enable;

public class DisableScheduleHandler : IRequestHandler<DisableScheduleCommand, Result<bool>>
{
    private readonly ISchedulerService _schedulerService;

    public DisableScheduleHandler(ISchedulerService schedulerService)
    {
        _schedulerService = schedulerService;
    }

    public async Task<Result<bool>> Handle(DisableScheduleCommand request, CancellationToken cancellationToken)
    {
        await _schedulerService.DisableScheduleAsync(request.ScheduleId);
        return Result<bool>.Success(true);
    }
}