using System.Collections.Generic;

namespace MatchLogic.Api.Handlers.DictionaryCategory.Create;

public record CreateDictionaryCategoryRequest : IRequest<Result<CreateDictionaryCategoryResponse>>
{
    public string Name { get; init; }
    public string Description { get; init; }
    public List<string> Items { get; init; } = [];
}
