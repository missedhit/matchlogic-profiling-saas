using MatchLogic.Api.Handlers.Scheduling.DTOs;
using System;

namespace MatchLogic.Api.Handlers.Scheduling.GetById;

public class GetScheduleByIdQuery : IRequest<Result<GetScheduleResponse>>
{
    public Guid ScheduleId { get; set; }
}
