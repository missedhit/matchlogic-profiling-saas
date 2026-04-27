using MatchLogic.Api.Handlers.Scheduling.DTOs;
using System;

namespace MatchLogic.Api.Handlers.Scheduling.GetHistory;

public class GetExecutionHistoryQuery : IRequest<Result<GetExecutionHistoryResponse>>
{
    public Guid ScheduleId { get; set; }
    public int PageNumber { get; set; } = 1;
    public int PageSize { get; set; } = 50;
}
