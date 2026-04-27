using Ardalis.Result;
using MediatR;
using System;
using System.Collections.Generic;
namespace MatchLogic.Api.Handlers.DictionaryCategory.Update;

public record UpdateDictionaryCategoryRequest : IRequest<Result<UpdateDictionaryCategoryResponse>>
{
    public Guid Id { get; init; }
    public string Name { get; init; }
    public string Description { get; init; }
    public List<string> Items { get; init; } = [];
}
