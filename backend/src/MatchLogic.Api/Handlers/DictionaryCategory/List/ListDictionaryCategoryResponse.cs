using System;
using System.Collections.Generic;

namespace MatchLogic.Api.Handlers.DictionaryCategory.List;

public record ListDictionaryCategoryResponse
{
    public List<BaseDictionaryCategoryDTO> Categories { get; init; } = [];
}
