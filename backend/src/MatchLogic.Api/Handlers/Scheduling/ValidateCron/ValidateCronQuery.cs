using MatchLogic.Api.Handlers.Scheduling.DTOs;

namespace MatchLogic.Api.Handlers.Scheduling.ValidateCron;

public class ValidateCronQuery : IRequest<Result<ValidateCronResponse>>
{
    public string CronExpression { get; set; }
}
