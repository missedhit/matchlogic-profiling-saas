using System;

namespace MatchLogic.Api.Handlers.MatchResult.Create;
public class CreateMatchResultCommand : IRequest<Result<CreateMatchResultResponse>>
{
    public Guid ProjectId { get; set; }       
}
