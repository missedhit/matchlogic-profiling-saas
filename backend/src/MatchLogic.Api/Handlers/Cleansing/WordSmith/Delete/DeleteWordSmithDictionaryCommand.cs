using System;

namespace MatchLogic.Api.Handlers.Cleansing.WordSmith.Delete;

public class DeleteWordSmithDictionaryCommand : IRequest<Result<bool>>
{
    public Guid Id { get; set; }
}