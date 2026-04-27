using MatchLogic.Application.Interfaces.Persistence;
using System.Threading.Tasks;
using System.Threading;
using System;
using System.Linq;
using MatchLogic.Application.Common;

namespace MatchLogic.Api.Handlers.Cleansing.TransformationGraph.Get;

public class GetTransformationGraphHandler : IRequestHandler<GetTransformationGraphQuery,Result<string?>>
{
    private readonly IGenericRepository<Domain.CleansingAndStandaradization.TransformationGraph, Guid> _repository;
    private const string CollectionName = Constants.Collections.TransformationGraphs;

    public GetTransformationGraphHandler(IGenericRepository<Domain.CleansingAndStandaradization.TransformationGraph, Guid> repository)
    {
        _repository = repository;
    }

    public async Task<Result<string?>> Handle(GetTransformationGraphQuery request, CancellationToken cancellationToken)
    {
        var results = await _repository.QueryAsync(
            x => x.ProjectId == request.ProjectId && x.DataSourceId == request.DataSourceId,
            CollectionName);

        if (results == null)
            return Result.NotFound("Graph not found");

        return Result.Success(results.FirstOrDefault()?.GraphJson);
    }
}
