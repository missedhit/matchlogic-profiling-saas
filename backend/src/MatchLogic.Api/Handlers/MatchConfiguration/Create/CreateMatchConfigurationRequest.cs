using System.Collections.Generic;
using System;

namespace MatchLogic.Api.Handlers.MatchConfiguration.Create;
public record CreateMatchConfigurationRequest : IRequest<Result<BaseMatchConfigurationResponse>>
{
    /// <summary>
    /// ID of the project containing the data source for Match Configuration
    /// </summary>
    public Guid ProjectId { get; set; }

    /// <summary>
    /// List of Data Source Pairs
    /// </summary>
    public List<BaseDataSourcePairDTO> Pairs { get; set; } = [];
}
