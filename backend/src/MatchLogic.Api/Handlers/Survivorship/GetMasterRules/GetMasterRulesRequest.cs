using System.Collections.Generic;
using System;

namespace MatchLogic.Api.Handlers.Survivorship.GetMasterRules;

public record GetMasterRulesRequest(Guid ProjectId) : IRequest<Result<GetMasterRulesResponse>>;

public class GetMasterRulesResponse
{
    public List<MasterRuleDto> Rules { get; set; } = new();
}