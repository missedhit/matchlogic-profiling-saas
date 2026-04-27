using MatchLogic.Api.Handlers.Scheduling.DTOs;
using MatchLogic.Application.Interfaces.Scheduling;
using System.Threading.Tasks;
using System.Threading;

namespace MatchLogic.Api.Handlers.Scheduling.ValidateCron;

public class ValidateCronHandler : IRequestHandler<ValidateCronQuery, Result<ValidateCronResponse>>
{
    private readonly ISchedulerService _schedulerService;

    public ValidateCronHandler(ISchedulerService schedulerService)
    {
        _schedulerService = schedulerService;
    }

    public async Task<Result<ValidateCronResponse>> Handle(ValidateCronQuery request, CancellationToken cancellationToken)
    {
        var (isValid, message, occurrences) = await _schedulerService.ValidateCronExpressionAsync(request.CronExpression);

        return Result<ValidateCronResponse>.Success(
            new ValidateCronResponse(isValid, message, occurrences));
    }
}
