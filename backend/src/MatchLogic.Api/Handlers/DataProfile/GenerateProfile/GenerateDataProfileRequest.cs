using System;
using System.Collections.Generic;

namespace MatchLogic.Api.Handlers.DataProfile.GenerateProfile;

public record GenerateDataProfileRequest : IRequest<Result<GenerateDataProfileResponse>> 
{
    public Guid ProjectId { get; set; }
    public List<Guid> DataSourceIds { get; set; }
}