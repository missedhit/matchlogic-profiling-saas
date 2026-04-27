using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.CleansingAndStandaradization;
using MatchLogic.Domain.Project;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.Cleansing.Create;

public class CreateCleansingRuleHandler :
        BaseCleansingRuleHandler<CreateCleansingRuleCommand, CreateCleansingRuleResponse>,
        IRequestHandler<CreateCleansingRuleCommand, Result<CreateCleansingRuleResponse>>
{
    private readonly IGenericRepository<EnhancedCleaningRules, Guid> _cleansingRuleRepository;
    public CreateCleansingRuleHandler(
        IProjectService projectService,
        IGenericRepository<EnhancedCleaningRules, Guid> cleansingRuleRepository,
        ILogger<CreateCleansingRuleHandler> logger)
        : base(projectService, logger)
    {
        _cleansingRuleRepository = cleansingRuleRepository;
    }

    protected override async Task BeforeRuleCreation(CreateCleansingRuleCommand request)
    {
        // Delete old rules before creating new ones against ProjectId and DataSourceId
        var exists = await _cleansingRuleRepository.QueryAsync(x =>
            x.ProjectId == request.ProjectId && x.DataSourceId == request.DataSourceId,
            Constants.Collections.CleaningRules);
        // Check if any existing rules are found
        if (exists.Count > 0)
        {
            // If rules exist, delete them to avoid duplicates
            await _cleansingRuleRepository.DeleteAllAsync(x =>
            x.ProjectId == request.ProjectId && x.DataSourceId == request.DataSourceId,
            Constants.Collections.CleaningRules);
        }

    }

    protected override Result<CreateCleansingRuleResponse> CreateSuccessResult(ProjectRun queuedRun)
    {
        return Result<CreateCleansingRuleResponse>.Success(new CreateCleansingRuleResponse(queuedRun));
    }
}
