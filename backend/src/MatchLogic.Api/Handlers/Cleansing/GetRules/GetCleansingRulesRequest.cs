using System;
using System.Collections.Generic;

namespace MatchLogic.Api.Handlers.Cleansing.GetRules;

public record GetCleansingRulesRequest(Guid DataSourceId,Guid ProjectId) : IRequest<Result<GetCleansingRulesResponse>>;

