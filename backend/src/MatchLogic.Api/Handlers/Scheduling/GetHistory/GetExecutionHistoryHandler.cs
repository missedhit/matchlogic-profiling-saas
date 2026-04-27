using MatchLogic.Api.Handlers.Scheduling.DTOs;
using MatchLogic.Application.Interfaces.Scheduling;
using System.Threading.Tasks;
using System.Threading;
using System.Linq;

namespace MatchLogic.Api.Handlers.Scheduling.GetHistory;

public class GetExecutionHistoryHandler : IRequestHandler<GetExecutionHistoryQuery, Result<GetExecutionHistoryResponse>>
{
    private readonly ISchedulerService _schedulerService;

    public GetExecutionHistoryHandler(ISchedulerService schedulerService)
    {
        _schedulerService = schedulerService;
    }

    public async Task<Result<GetExecutionHistoryResponse>> Handle(GetExecutionHistoryQuery request, CancellationToken cancellationToken)
    {
        var executions = await _schedulerService.GetExecutionHistoryAsync(
            request.ScheduleId,
            request.PageNumber,
            request.PageSize);

        var dtos = executions.Items.Select(e => new ExecutionDto
        {
            Id = e.Id,
            ScheduledTaskId = e.ScheduledTaskId,
            ProjectRunId = e.ProjectRunId,
            HangfireJobId = e.HangfireJobId,
            ScheduledTime = e.ScheduledTime,
            ActualStartTime = e.ActualStartTime,
            EndTime = e.EndTime,
            DurationSeconds = e.DurationSeconds,
            Status = e.Status,
            TriggerType = e.TriggerType,
            TriggeredBy = e.TriggeredBy,
            ErrorMessage = e.ErrorMessage,
            Statistics = e.Statistics,
            RecordsProcessed = e.RecordsProcessed,
            ErrorCount = e.ErrorCount,
            WarningCount = e.WarningCount,
            ExecutedByServer = e.ExecutedByServer
        }).ToList();

        return Result<GetExecutionHistoryResponse>.Success(
            new GetExecutionHistoryResponse(dtos, executions.TotalCount));
    }
}