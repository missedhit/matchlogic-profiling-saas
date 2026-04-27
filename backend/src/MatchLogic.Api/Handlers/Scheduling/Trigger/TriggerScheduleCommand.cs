using MatchLogic.Api.Handlers.Scheduling.DTOs;
using System;

namespace MatchLogic.Api.Handlers.Scheduling.Trigger;

public class TriggerScheduleCommand : IRequest<Result<TriggerScheduleResponse>>
{
    public Guid ScheduleId { get; set; }
    public string TriggeredBy { get; set; }
}
