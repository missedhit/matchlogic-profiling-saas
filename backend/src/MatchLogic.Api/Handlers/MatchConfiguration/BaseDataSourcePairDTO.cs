using System;

namespace MatchLogic.Api.Handlers.MatchConfiguration;
public record BaseDataSourcePairDTO
{
    public Guid DataSourceA { get; set; }
    public Guid DataSourceB { get; set; }

    public BaseDataSourcePairDTO(Guid dataSourceA, Guid dataSourceB)
    {
        DataSourceA = dataSourceA;
        DataSourceB = dataSourceB;
    }
}
