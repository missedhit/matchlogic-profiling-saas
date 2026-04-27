using System.Collections.Generic;
using System;

namespace MatchLogic.Api.Handlers.Survivorship.GetOverwriteRules;

public record GetOverwriteRulesRequest(Guid ProjectId) : IRequest<Result<GetOverwriteRulesResponse>>;

public class GetOverwriteRulesResponse
{
    public List<OverwriteRuleDto> Rules { get; set; } = new();
}