using System;

namespace MatchLogic.Api.Handlers.Survivorship.DetermineOverwrite;
public class DetermineOverwriteCommand : IRequest<Result<DetermineOverwriteResponse>>
{
    public Guid ProjectId { get; set; }       
}
