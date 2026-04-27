using MatchLogic.Application.Features.CleansingAndStandardization.DTOs;
using System;

namespace MatchLogic.Api.Handlers.Cleansing.WordSmith.Update;

public class UpdateWordSmithRuleCommand : IRequest<Result<WordSmithRuleDto>>
{
    public Guid RuleId { get; set; }
    public UpdateWordSmithRuleDto UpdateDto { get; set; }
}
