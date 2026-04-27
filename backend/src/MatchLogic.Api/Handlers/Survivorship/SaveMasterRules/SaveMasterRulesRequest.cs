using System.Collections.Generic;
using System;

namespace MatchLogic.Api.Handlers.Survivorship.SaveMasterRules;

public record SaveMasterRulesRequest(
    Guid ProjectId,
    List<MasterRuleDto> Rules
) : IRequest<Result<SaveMasterRulesResponse>>;

public class SaveMasterRulesResponse
{
    public bool Success { get; set; }
    public string Message { get; set; } = string.Empty;
    public int RulesSaved { get; set; }
}