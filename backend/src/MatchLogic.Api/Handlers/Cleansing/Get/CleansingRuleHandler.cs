using MatchLogic.Application.Interfaces.Project;
using Microsoft.Extensions.Logging;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.Cleansing.Get;

public class CleansingRuleHandler :        
        IRequestHandler<CleansingRuleRequest, Result<CleansingRuleResponse>>
{
    public CleansingRuleHandler(
        IProjectService projectService,
        ILogger<CleansingRuleHandler> logger)
       
    {
    }

    public Task<Result<CleansingRuleResponse>> Handle(CleansingRuleRequest request, CancellationToken cancellationToken)
    {        
        var lstOperationType = CleansingOperatorHelper.OperationTypeOverrides;
        var lstCleaningRuleType = CleansingOperatorHelper.CleaningRuleTypeOverrides;
        var lstMappingOperationType = CleansingOperatorHelper.MappingOperationTypeOverrides;

        var cleansingRuleParameters = CleansingOperatorHelper.GetCleaningRuleParameters();
        var mappingOperationParamters = CleansingOperatorHelper.GetMappingOperationParameters();
        var mappingRequirements = CleansingOperatorHelper.GetMappingRequirements();

        var response = new CleansingRuleResponse
        {
            OperationType = lstOperationType,
            CleansingType = lstCleaningRuleType,
            CleansingTypeParameters = cleansingRuleParameters,
            MappingOperationType = lstMappingOperationType,
            MappingTypeParameters = mappingOperationParamters,
            MappingTypeRequirements = mappingRequirements
        };

        return Task.FromResult(Result<CleansingRuleResponse>.Success(response));
    }
}
