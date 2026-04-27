using MatchLogic.Application.Interfaces.Scheduling;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;
using System.Threading;

namespace MatchLogic.Api.Handlers.Scheduling.Delete;

public class DeleteScheduleHandler : IRequestHandler<DeleteScheduleCommand, Result<bool>>
{
    private readonly ISchedulerService _schedulerService;
    private readonly ILogger<DeleteScheduleHandler> _logger;

    public DeleteScheduleHandler(ISchedulerService schedulerService, ILogger<DeleteScheduleHandler> logger)
    {
        _schedulerService = schedulerService;
        _logger = logger;
    }

    public async Task<Result<bool>> Handle(DeleteScheduleCommand request, CancellationToken cancellationToken)
    {
        await _schedulerService.DeleteScheduleAsync(request.ScheduleId);
        return Result<bool>.Success(true);
    }
}
