using System.Collections.Generic;
using System;

namespace MatchLogic.Api.Handlers.Survivorship.SaveOverwriteRules;

public record SaveOverwriteRulesRequest(
    Guid ProjectId,
    List<OverwriteRuleDto> Rules
) : IRequest<Result<SaveOverwriteRulesResponse>>;

public class SaveOverwriteRulesResponse
{
    public bool Success { get; set; }
    public string Message { get; set; } = string.Empty;
    public int RulesSaved { get; set; }
}