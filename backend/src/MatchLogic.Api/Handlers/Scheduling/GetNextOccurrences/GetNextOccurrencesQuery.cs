using MatchLogic.Api.Handlers.Scheduling.DTOs;
using System;

namespace MatchLogic.Api.Handlers.Scheduling.GetNextOccurrences;

public class GetNextOccurrencesQuery : IRequest<Result<GetNextOccurrencesResponse>>
{
    public Guid ScheduleId { get; set; }
    public int Count { get; set; } = 5;
}