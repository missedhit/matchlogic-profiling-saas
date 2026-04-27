using System;

namespace MatchLogic.Api.Handlers.RegexInfo;

public record RegexInfoDTO
{
    public new Guid Id { get; set; }

    public string Name { get; set; }

    public string Description { get; set; }

    public string RegexExpression { get; set; }

    public bool IsDefault { get; set; }

    public bool IsSystem { get; set; }

    public bool IsSystemDefault { get; set; }

    public int Version { get; set; }
}
