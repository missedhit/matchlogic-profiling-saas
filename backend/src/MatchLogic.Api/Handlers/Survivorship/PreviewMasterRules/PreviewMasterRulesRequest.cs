using System.Collections.Generic;
using System;

namespace MatchLogic.Api.Handlers.Survivorship.PreviewMasterRules;

public record PreviewMasterRulesRequest(
    Guid ProjectId,
    List<MasterRuleDto> Rules
) : IRequest<Result<IEnumerable<IDictionary<string, object>>>>;