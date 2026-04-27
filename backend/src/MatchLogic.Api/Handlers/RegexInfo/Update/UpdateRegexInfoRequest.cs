using MatchLogic.Api.Handlers.RegexInfo.Create;
using System;
namespace MatchLogic.Api.Handlers.RegexInfo.Update;
public record UpdateRegexInfoRequest : IRequest<Result<UpdateRegexInfoResponse>>
{
    public Guid Id { get; init; }
    public string Name { get; init; }
    public string Description { get; init; }
    public string RegexExpression { get; init; }
    public bool IsDefault { get; init; }
}

