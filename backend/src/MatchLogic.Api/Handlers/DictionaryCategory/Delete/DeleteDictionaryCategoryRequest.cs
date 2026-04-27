using System;
namespace MatchLogic.Api.Handlers.DictionaryCategory.Delete;
public record DeleteDictionaryCategoryRequest(Guid Id) : IRequest<Result<DeleteDictionaryCategoryResponse>>;
