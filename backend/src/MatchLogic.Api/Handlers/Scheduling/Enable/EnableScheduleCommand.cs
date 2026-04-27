using System;

namespace MatchLogic.Api.Handlers.Scheduling.Enable;

public class EnableScheduleCommand : IRequest<Result<bool>>
{
    public Guid ScheduleId { get; set; }
}

public class DisableScheduleCommand : IRequest<Result<bool>>
{
    public Guid ScheduleId { get; set; }
}