using MatchLogic.Api.Handlers.Scheduling.DTOs;
using System;

namespace MatchLogic.Api.Handlers.Scheduling.Get;

public class GetSchedulesQuery : IRequest<Result<GetSchedulesResponse>>
{
    public Guid? ProjectId { get; set; }
    public bool IncludeDisabled { get; set; }
}
