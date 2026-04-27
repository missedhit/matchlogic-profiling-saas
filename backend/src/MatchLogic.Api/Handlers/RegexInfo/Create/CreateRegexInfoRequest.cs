namespace MatchLogic.Api.Handlers.RegexInfo.Create;

public record CreateRegexInfoRequest : IRequest<Result<CreateRegexInfoResponse>>
{
    public string Name { get; init; }
    public string Description { get; init; }
    public string RegexExpression { get; init; }
    public bool IsDefault { get; init; }
}
