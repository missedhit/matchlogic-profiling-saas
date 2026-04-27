using System;

namespace MatchLogic.Api.Handlers.Scheduling.Delete;

public class DeleteScheduleCommand : IRequest<Result<bool>>
{
    public Guid ScheduleId { get; set; }
}
