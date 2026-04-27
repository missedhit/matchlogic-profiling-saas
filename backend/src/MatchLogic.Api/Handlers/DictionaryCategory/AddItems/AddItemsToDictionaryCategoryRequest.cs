using Ardalis.Result;
using MediatR;
using System;
using System.Collections.Generic;

namespace MatchLogic.Api.Handlers.DictionaryCategory.AddItems;

public record AddItemsToDictionaryCategoryRequest : IRequest<Result<AddItemsToDictionaryCategoryResponse>>
{
    public Guid Id { get; init; }
    public List<string> Items { get; init; } = new();
}
