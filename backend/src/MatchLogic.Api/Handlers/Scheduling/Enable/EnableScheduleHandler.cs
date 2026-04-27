using MatchLogic.Application.Interfaces.Scheduling;
using System.Threading.Tasks;
using System.Threading;

namespace MatchLogic.Api.Handlers.Scheduling.Enable;

public class EnableScheduleHandler : IRequestHandler<EnableScheduleCommand, Result<bool>>
{
    private readonly ISchedulerService _schedulerService;

    public EnableScheduleHandler(ISchedulerService schedulerService)
    {
        _schedulerService = schedulerService;
    }

    public async Task<Result<bool>> Handle(EnableScheduleCommand request, CancellationToken cancellationToken)
    {
        await _schedulerService.EnableScheduleAsync(request.ScheduleId);
        return Result<bool>.Success(true);
    }
}
