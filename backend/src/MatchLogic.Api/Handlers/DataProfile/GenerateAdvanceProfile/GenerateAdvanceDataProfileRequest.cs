using System;
using System.Collections.Generic;

namespace MatchLogic.Api.Handlers.DataProfile.GenerateAdvanceProfile;
public record GenerateAdvanceDataProfileRequest : IRequest<Result<GenerateAdvanceDataProfileResponse>>
{
    public Guid ProjectId { get; set; }
    public List<Guid> DataSourceIds { get; set; }
}
