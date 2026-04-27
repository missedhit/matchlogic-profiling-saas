using MatchLogic.Api.Handlers.Scheduling.DTOs;
using MatchLogic.Application.Interfaces.Scheduling;
using System.Threading.Tasks;
using System.Threading;
using System.Linq;

namespace MatchLogic.Api.Handlers.Scheduling.Get;

public class GetSchedulesHandler : IRequestHandler<GetSchedulesQuery, Result<GetSchedulesResponse>>
{
    private readonly ISchedulerService _schedulerService;

    public GetSchedulesHandler(ISchedulerService schedulerService)
    {
        _schedulerService = schedulerService;
    }

    public async Task<Result<GetSchedulesResponse>> Handle(GetSchedulesQuery request, CancellationToken cancellationToken)
    {
        var schedules = request.ProjectId.HasValue
            ? await _schedulerService.GetSchedulesByProjectAsync(request.ProjectId.Value, request.IncludeDisabled)
            : await _schedulerService.GetAllSchedulesAsync(request.IncludeDisabled);

        var dtos = schedules.Select(s => new ScheduleDto
        {
            Id = s.Id,
            ProjectId = s.ProjectId,
            Name = s.Name,
            Description = s.Description,
            ScheduleType = s.ScheduleType,
            CronExpression = s.CronExpression,
            RecurrenceInterval = s.RecurrenceInterval,
            StartTime = s.StartTime,
            TimeZone = s.TimeZone,
            IsEnabled = s.IsEnabled,
            Status = s.Status,
            LastRun = s.LastRun,
            NextRun = s.NextRun,
            ExecutionCount = s.ExecutionCount,
            ConsecutiveFailures = s.ConsecutiveFailures,
            StepsToExecute = s.StepsToExecute,
            ExportCleanedData = s.ExportCleanedData,
            ExportMatchedData = s.ExportMatchedData,
            ExportFormattedData = s.ExportFormattedData,
            ExportDataProfile = s.ExportDataProfile,
            ExportSummaryReport = s.ExportSummaryReport,
            CreatedBy = s.CreatedBy.ToString(),
            CreatedAt = s.CreatedAt,
            LastModifiedBy = s.ModifiedBy?.ToString(),
            UpdatedAt = s.ModifiedAt,
            ConnectionInfo = s.ConnectionInfo,
            Tags = s.Tags
        }).ToList();

        return Result<GetSchedulesResponse>.Success(new GetSchedulesResponse(dtos));
    }
}
