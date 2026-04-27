using System;

namespace MatchLogic.Api.Handlers.MatchConfiguration.GetDataSources;

public record MatchConfigDataSourcesResponse
{
    public Guid Id { get; init; }
    public string Name { get; init; }
    public MatchConfigDataSourcesResponse(Guid id, string name)
    {
        Id = id;
        Name = name;
    }
}
