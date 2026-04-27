using System;

namespace MatchLogic.Api.Handlers.Cleansing.WordSmith.Clear;

public class ClearDictionaryCommand : IRequest<Result<bool>>
{
    public Guid DictionaryId { get; set; }
}
