using System;

namespace MatchLogic.Api.Handlers.Survivorship.DetermineMaster;
public class DetermineMasterCommand : IRequest<Result<DetermineMasterResponse>>
{
    public Guid ProjectId { get; set; }       
}
