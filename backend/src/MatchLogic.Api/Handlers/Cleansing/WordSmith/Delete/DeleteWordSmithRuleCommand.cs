using System;

namespace MatchLogic.Api.Handlers.Cleansing.WordSmith.Delete;

public class DeleteWordSmithRuleCommand : IRequest<Result<bool>>
{
    public Guid RuleId { get; set; }
}
