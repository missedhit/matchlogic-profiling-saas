using Ardalis.Result;
using MediatR;
using System;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using MatchLogic.Application.Interfaces.Persistence;
using Microsoft.Extensions.Logging;
using MatchLogic.Application.Common;
using MatchLogic.Domain.Entities.Common;

namespace MatchLogic.Application.Features.MatchResult.Get;
public class MatchResultJobHandler : IRequestHandler<MatchResultJobRequest, Result<MatchResultJobResponse>>
{
    private readonly ILogger<MatchResultHandler> _logger;
    private readonly IJobStatusRepository _jobStatusRepository;
    private readonly IGenericRepository<Domain.Entities.MatchDefinition, Guid> _matchDefinitionRepository;
    public MatchResultJobHandler(IGenericRepository<Domain.Entities.MatchDefinition, Guid> matchDefinitionRepository, IJobStatusRepository jobStatusRepository, ILogger<MatchResultHandler> logger)
    {
        _jobStatusRepository = jobStatusRepository;
        _matchDefinitionRepository = matchDefinitionRepository;
        _logger = logger;
    }
    async Task<Result<MatchResultJobResponse>> IRequestHandler<MatchResultJobRequest, Result<MatchResultJobResponse>>.Handle(MatchResultJobRequest request, CancellationToken cancellationToken)
    {
        const string matchDefinition = Constants.Collections.MatchDefinition;
        var jobId = request.JobId;
        //var matchDef = await _matchDefinitionRepository.QueryAsync(x => x.JobId == jobId, matchDefinition);

        //if (matchDef == null || matchDef?.Count == 0)
        //{
        //    _logger.LogError("No matchdefinition found for  JobId: {JobId}", jobId);
        //    return Result.NotFound($"No data found for job ID: {jobId}");
        //}

        const string jobStatusCollection = Constants.Collections.JobStatus;

        var jobStatuses = await _jobStatusRepository.QueryAsync(x => x.JobId == jobId, jobStatusCollection);

        if (jobStatuses == null || jobStatuses?.Count == 0)
        {
            _logger.LogError("No Job Status found for  JobId: {JobId}", jobId);
            return Result.NotFound($"No data found for job ID: {jobId}");
        }

        var dto = jobStatuses?.FirstOrDefault();
        
        var response = new MatchResultJobResponse
        {
            Id = dto.Id,
            JobId = dto.JobId,
            Status = dto.Status,  
            Metadata = dto.Metadata,
            EndTime = dto.EndTime,
            TotalRecords = dto.TotalRecords,
            Error = dto.Error,
            ProcessedRecords = dto.ProcessedRecords,
            StartTime = dto.StartTime,
            Steps = dto.Steps,
            //StatusUrl = Url.Action()            
        };

        _logger.LogInformation("Match defintion found successfully. JobId: {JobId}", jobId);

        return new Result<MatchResultJobResponse>(response);
    }
}
