using MatchLogic.Api.Handlers.DataSource.Create;
using MatchLogic.Application.Common;
using MatchLogic.Application.Features.Project;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.CleansingAndStandaradization;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.Cleansing.Update;

public class UpdateCleansingRuleHandler :
        BaseCleansingRuleHandler<UpdateCleansingRuleCommand, UpdateCleansingRuleResponse>,
        IRequestHandler<UpdateCleansingRuleCommand, Result<UpdateCleansingRuleResponse>>
{
    private readonly IGenericRepository<EnhancedCleaningRules, Guid> _cleansingRuleRepository;

    public UpdateCleansingRuleHandler(
        IProjectService projectService,
        IGenericRepository<EnhancedCleaningRules, Guid> cleansingRuleRepository,
        ILogger<UpdateCleansingRuleHandler> logger)
        : base(projectService, logger)
    {
        _cleansingRuleRepository = cleansingRuleRepository;
    }

    protected override async Task BeforeRuleCreation(UpdateCleansingRuleCommand request)
    {
        // Delete old rules before creating new ones
        await _cleansingRuleRepository.DeleteAsync(request.Id, Constants.Collections.CleaningRules);
    }

    protected override Result<UpdateCleansingRuleResponse> CreateSuccessResult(ProjectRun queuedRun)
    {
        return Result<UpdateCleansingRuleResponse>.Success(new UpdateCleansingRuleResponse(queuedRun));
    }
}
