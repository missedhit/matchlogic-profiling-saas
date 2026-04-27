//using Ardalis.Result;
//using MediatR;
//using System;
//using System.Linq;
//using System.Threading.Tasks;
//using System.Threading;
//using MatchLogic.Application.Interfaces.Persistence;
//using Microsoft.Extensions.Logging;
//using Mapster;
//using MatchLogic.Application.Common;
//using MatchLogic.Domain.Entities.Common;
//using System.Collections.Generic;
//using MatchLogic.Application.Interfaces.Events;
//using MatchLogic.Application.Interfaces.Common;

//namespace MatchLogic.Application.Features.MatchResult.Create;
//public class CreateMatchResultJobHandler : IRequestHandler<CreateMatchResultJobCommand, Result<CreateMatchResultJobResponse>>
//{
//    private readonly ILogger<CreateMatchResultJobHandler> _logger;
//    private readonly IJobStatusRepository _jobStatusRepository;
//    private readonly IGenericRepository<Domain.Entities.MatchDefinition, Guid> _mathcDefinitionRepository;
//    private readonly IJobEventPublisher _jobEventPublisher;
//    private readonly IBackgroundJobQueue<MatchJobInfo> _jobQueue;    

//    public CreateMatchResultJobHandler(IJobStatusRepository jobStatusRepositor
//        , IGenericRepository<Domain.Entities.MatchDefinition, Guid> mathcDefinitionRepository
//        , IBackgroundJobQueue<MatchJobInfo> jobQueue
//        , IJobEventPublisher jobEventPublisher
//        , ILogger<CreateMatchResultJobHandler> logger)
//    {
//        _jobStatusRepository = jobStatusRepositor;
//        _mathcDefinitionRepository = mathcDefinitionRepository;
//        _jobQueue = jobQueue;
//        _logger = logger;
//        _jobEventPublisher = jobEventPublisher;
//    }
//    async Task<Result<CreateMatchResultJobResponse>> IRequestHandler<CreateMatchResultJobCommand, Result<CreateMatchResultJobResponse>>.Handle(CreateMatchResultJobCommand request, CancellationToken cancellationToken)
//    {
//        const string matchDefinition = Constants.Collections.MatchDefinition;
//        const string jobStatus = Constants.Collections.JobStatus;
//        var jobId = request.JobId;
//        var matchDef = await _mathcDefinitionRepository.QueryAsync(x => x.JobId == jobId, matchDefinition);
        

//        List<JobStatus>? jobStat = default;
//        try
//        {
//            await _jobEventPublisher.CreateEvent(jobId)
//                .WithStatus("Queued")
//                .WithProgress(0, 100)
//                .PublishAsync(cancellationToken);
//            var mD = matchDef.First();
//            await _jobQueue.QueueJobAsync(new MatchJobInfo
//            {
//                JobId = jobId,
//                MergeOverlappingGroups = mD.MergeOverlappingGroups,
//                IsProbabilistic = mD.IsProbabilistic,
//                Criteria = mD.Criteria
//            });            

//        }
//        catch (Exception ex)
//        {
//            _logger.LogError("match definition not created");
//            throw;
//        }
//        jobStat = await _jobStatusRepository.QueryAsync(x => x.JobId == jobId, jobStatus);

//        if (jobStat == null || jobStat?.Count == 0)
//        {
//            _logger.LogError("No job status found for  JobId: {JobId}", jobId);
//            return Result.NotFound($"No data found for job ID: {jobId}");
//        }

//        var mat = jobStat?.FirstOrDefault();

//        var response = mat.Adapt<CreateMatchResultJobResponse>();

//        return new Result<CreateMatchResultJobResponse>(response);
//    }
//}
