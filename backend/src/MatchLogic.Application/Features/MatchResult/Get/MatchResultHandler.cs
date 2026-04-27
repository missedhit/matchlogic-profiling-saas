using Ardalis.Result;
using MediatR;
using System;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using MatchLogic.Application.Interfaces.Persistence;
using Microsoft.Extensions.Logging;
using MatchLogic.Application.Common;

namespace MatchLogic.Application.Features.MatchResult.Get;
public class MatchResultHandler : IRequestHandler<MatchResultRequest, Result<MatchResultResponse>>
{
    private readonly ILogger<MatchResultHandler> _logger;
    private readonly IGenericRepository<Domain.Entities.MatchDefinition, Guid> _mathcDefinitionRepository;

    public MatchResultHandler(IGenericRepository<Domain.Entities.MatchDefinition, Guid> mathcDefinitionRepository, ILogger<MatchResultHandler> logger)
    {
        _mathcDefinitionRepository = mathcDefinitionRepository;
        _logger = logger;
    }
    async Task<Result<MatchResultResponse>> IRequestHandler<MatchResultRequest, Result<MatchResultResponse>>.Handle(MatchResultRequest request, CancellationToken cancellationToken)
    {
        const string matchDefinition = Constants.Collections.MatchDefinition;
        var jobId = request.JobId;
        var matchDef = await _mathcDefinitionRepository.QueryAsync(x => x.JobId == jobId, matchDefinition);

        if (matchDef == null || matchDef?.Count == 0)
        {
            _logger.LogError("No matchdefinition foubd for  JobId: {JobId}", jobId);
            return Result.NotFound($"No data found for job ID: {jobId}");
        }

        var dto = matchDef?.FirstOrDefault();
        
        var response = new MatchResultResponse
        {
            Id = dto.Id,
            JobId = dto.JobId,
            Name = dto.Name,
            Criteria = dto.Criteria.Select(c => new MatchCriteriaResponse
            {                
                FieldName = c.FieldName,
                MatchingType = c.MatchingType,
                DataType = c.DataType,
                Arguments = c.Arguments
            }).ToList()
        };

        _logger.LogInformation("Match defintion found successfully. JobId: {JobId}", jobId);

        return new Result<MatchResultResponse>(response);
    }
}
