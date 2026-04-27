using System.Collections.Generic;
using System;

namespace MatchLogic.Api.Handlers.Survivorship.PreviewOverwriteRules;

public record PreviewOverwriteRulesRequest(
    Guid ProjectId,
    List<OverwriteRuleDto> Rules
) : IRequest<Result<IEnumerable<IDictionary<string, object>>>>;