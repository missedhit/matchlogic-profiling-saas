using MatchLogic.Application.Interfaces.Persistence;
using System.Threading.Tasks;
using System.Threading;
using System;
using MatchLogic.Application.Common;
using System.Linq;

namespace MatchLogic.Api.Handlers.Cleansing.TransformationGraph.Create;

public class SaveTransformationGraphHandler : IRequestHandler<SaveTransformationGraphCommand, Result<Domain.CleansingAndStandaradization.TransformationGraph>>
{
    private readonly IGenericRepository<Domain.CleansingAndStandaradization.TransformationGraph, Guid> _repository;
    private const string CollectionName = Constants.Collections.TransformationGraphs;
    public SaveTransformationGraphHandler(IGenericRepository<Domain.CleansingAndStandaradization.TransformationGraph, Guid> repository)
    {
        _repository = repository;
    }

    public async Task<Result<Domain.CleansingAndStandaradization.TransformationGraph>> Handle(SaveTransformationGraphCommand request, CancellationToken cancellationToken)
    {
        //try
        //{
        //    var rule = await _service.AddRuleAsync(request.DictionaryId, request.CreateDto);
        //    return Result<WordSmithRuleDto>.Success(rule);
        //}
        //catch (KeyNotFoundException ex)
        //{
        //    return Result<TransformationGraph>.NotFound(ex.Message);
        //}
        //catch (Exception ex)
        //{
        //    return Result<TransformationGraph>.Error($"Failed to add rule: {ex.Message}");
        //}
        var existing = await _repository.QueryAsync(
                x => x.ProjectId == request.ProjectId && x.DataSourceId == request.DataSourceId,
                CollectionName);

        if (existing.Any())
        {
            // Update existing record
            var record = existing.First();
            record.GraphJson = request.GraphJson.RootElement.GetRawText();
            record.UpdatedAt = DateTime.UtcNow;
            await _repository.UpdateAsync(record, CollectionName);
        }
        else
        {
            // Insert new record
            var newRecord = new Domain.CleansingAndStandaradization.TransformationGraph
            {
                ProjectId = request.ProjectId,
                DataSourceId = request.DataSourceId,
                GraphJson = request.GraphJson.RootElement.GetRawText(),
                CreatedAt = DateTime.UtcNow
            };

            await _repository.InsertAsync(newRecord, CollectionName);
        }
        existing = await _repository.QueryAsync(x => x.DataSourceId == request.DataSourceId && x.ProjectId == request.ProjectId, CollectionName);
        return Result<Domain.CleansingAndStandaradization.TransformationGraph>.Success(existing?.FirstOrDefault());
    }
}

