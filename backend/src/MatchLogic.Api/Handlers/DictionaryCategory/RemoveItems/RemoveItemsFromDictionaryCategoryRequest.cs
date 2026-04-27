using Ardalis.Result;
using MediatR;
using System;
using System.Collections.Generic;

namespace MatchLogic.Api.Handlers.DictionaryCategory.RemoveItems;

public record RemoveItemsFromDictionaryCategoryRequest : IRequest<Result<RemoveItemsFromDictionaryCategoryResponse>>
{
    public Guid Id { get; init; }
    public List<string> Items { get; init; } = new();
}
