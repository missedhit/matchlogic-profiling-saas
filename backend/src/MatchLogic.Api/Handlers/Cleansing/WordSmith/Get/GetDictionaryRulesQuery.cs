using MatchLogic.Application.Features.CleansingAndStandardization.DTOs;
using System;
using System.Collections.Generic;

namespace MatchLogic.Api.Handlers.Cleansing.WordSmith.Get;

public class GetDictionaryRulesQuery : IRequest<Result<WordSmithRulesResponse>>
{
    public Guid DictionaryId { get; set; }
    public int Page { get; set; } = 1;
    public int PageSize { get; set; } = 50;
}

public record WordSmithRulesResponse(List<WordSmithRuleDto> rules, int totalCount);