using System.Collections.Generic;

namespace MatchLogic.Api.Handlers.DictionaryCategory.List;

public record ListDictionaryCategoryRequest : IRequest<Result<List<BaseDictionaryCategoryDTO>>>;
