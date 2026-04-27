using MatchLogic.Api.Handlers.Scheduling.DTOs;
using MatchLogic.Application.Interfaces.Scheduling;
using System.Threading.Tasks;
using System.Threading;

namespace MatchLogic.Api.Handlers.Scheduling.GetNextOccurrences;

public class GetNextOccurrencesHandler : IRequestHandler<GetNextOccurrencesQuery, Result<GetNextOccurrencesResponse>>
{
    private readonly ISchedulerService _schedulerService;

    public GetNextOccurrencesHandler(ISchedulerService schedulerService)
    {
        _schedulerService = schedulerService;
    }

    public async Task<Result<GetNextOccurrencesResponse>> Handle(GetNextOccurrencesQuery request, CancellationToken cancellationToken)
    {
        var schedule = await _schedulerService.GetScheduleByIdAsync(request.ScheduleId);

        if (schedule == null)
        {
            return Result<GetNextOccurrencesResponse>.Error("Schedule not found");
        }

        var occurrences = await _schedulerService.GetNextOccurrencesAsync(schedule, request.Count);

        return Result<GetNextOccurrencesResponse>.Success(
            new GetNextOccurrencesResponse(schedule.Id, schedule.Name, occurrences));
    }
}