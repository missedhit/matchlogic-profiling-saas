using System;

namespace MatchLogic.Api.Handlers.Cleansing.TransformationGraph.Get;

public class GetTransformationGraphQuery : IRequest<Result<string?>>
{
    public Guid ProjectId { get; set; }
    public Guid DataSourceId { get; set; }
}
