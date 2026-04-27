using MatchLogic.Application.Features.CleansingAndStandardization.DTOs;
using System;

namespace MatchLogic.Api.Handlers.Cleansing.WordSmith.Create;

public class AddWordSmithRuleCommand : IRequest<Result<WordSmithRuleDto>>
{
    public Guid DictionaryId { get; set; }
    public CreateWordSmithRuleDto CreateDto { get; set; }
}
