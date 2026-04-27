using MatchLogic.Api.Handlers.Scheduling.DTOs;
using MatchLogic.Application.Interfaces.Scheduling;
using System.Threading.Tasks;
using System.Threading;

namespace MatchLogic.Api.Handlers.Scheduling.GetById;

public class GetScheduleByIdHandler : IRequestHandler<GetScheduleByIdQuery, Result<GetScheduleResponse>>
{
    private readonly ISchedulerService _schedulerService;

    public GetScheduleByIdHandler(ISchedulerService schedulerService)
    {
        _schedulerService = schedulerService;
    }

    public async Task<Result<GetScheduleResponse>> Handle(GetScheduleByIdQuery request, CancellationToken cancellationToken)
    {
        var schedule = await _schedulerService.GetScheduleByIdAsync(request.ScheduleId);

        if (schedule == null)
        {
            return Result<GetScheduleResponse>.Error("Schedule not found");
        }

        var dto = new ScheduleDto
        {
            Id = schedule.Id,
            ProjectId = schedule.ProjectId,
            Name = schedule.Name,
            Description = schedule.Description,
            ScheduleType = schedule.ScheduleType,
            CronExpression = schedule.CronExpression,
            RecurrenceInterval = schedule.RecurrenceInterval,
            StartTime = schedule.StartTime,
            TimeZone = schedule.TimeZone,
            IsEnabled = schedule.IsEnabled,
            Status = schedule.Status,
            LastRun = schedule.LastRun,
            NextRun = schedule.NextRun,
            ExecutionCount = schedule.ExecutionCount,
            ConsecutiveFailures = schedule.ConsecutiveFailures,
            StepsToExecute = schedule.StepsToExecute,
            ExportCleanedData = schedule.ExportCleanedData,
            ExportMatchedData = schedule.ExportMatchedData,
            ExportFormattedData = schedule.ExportFormattedData,
            ConnectionInfo = schedule.ConnectionInfo,
            CreatedBy = schedule.CreatedBy.ToString(),
            CreatedAt = schedule.CreatedAt,
            Tags = schedule.Tags
        };

        return Result<GetScheduleResponse>.Success(new GetScheduleResponse(dto));
    }
}

