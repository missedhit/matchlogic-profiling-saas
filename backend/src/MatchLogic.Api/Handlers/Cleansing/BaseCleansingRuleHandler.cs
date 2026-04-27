using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.CleansingAndStandaradization;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using System;
using Microsoft.Extensions.Logging;
using MatchLogic.Application.Features.Project;

namespace MatchLogic.Api.Handlers.Cleansing;

public abstract class BaseCleansingRuleHandler<TCommand, TResponse>
        where TCommand : BaseCleansingRuleCommand<TResponse>
        where TResponse : ICleansingRuleResponse
{
    protected readonly IProjectService ProjectService;
    
    protected readonly ILogger Logger;

    protected BaseCleansingRuleHandler(
        IProjectService projectService,       
        ILogger logger)
    {
        ProjectService = projectService;        
        Logger = logger;
    }

    // Template method defining the algorithm structure
    public async Task<Result<TResponse>> Handle(TCommand request, CancellationToken cancellationToken)
    {        
        // Perform any pre-processing
        await BeforeRuleCreation(request);

        // Create a new cleaning rules instance
        var cleaningRules = new EnhancedCleaningRules
        {
            Id = GetRulesId(request),
            ProjectId = request.ProjectId,
            DataSourceId = request.DataSourceId,
        };

        // Add rules
        AddRulesToInstance(cleaningRules, request);

        // Persist changes
        await PersistCleaningRules(request, cleaningRules);

        // Start a new run
        var queuedRun = await StartNewRun(request);

        // Create and return response
        return CreateSuccessResult(queuedRun);
    }


    protected virtual async Task BeforeRuleCreation(TCommand request) { }

    protected virtual Guid GetRulesId(TCommand request) => Guid.NewGuid();

    protected abstract Result<TResponse> CreateSuccessResult(ProjectRun queuedRun);

    // Common implementation
    protected void AddRulesToInstance(EnhancedCleaningRules cleaningRules, TCommand request)
    {
        // Add standard rules
        foreach (var ruleDto in request.StandardRules)
        {
            var rule = new CleaningRule(ruleDto.ColumnName, ruleDto.RuleType)
            {
                Id = ruleDto.Id ?? Guid.NewGuid(),
                Arguments = ruleDto.Arguments
            };
            cleaningRules.AddRule(rule);
        }

        // Add extended rules
        foreach (var ruleDto in request.ExtendedRules)
        {
            var rule = new ExtendedCleaningRule
            {
                Id = ruleDto.Id ?? Guid.NewGuid(),
                ColumnName = ruleDto.ColumnName,
                RuleType = ruleDto.RuleType,
                Arguments = ruleDto.Arguments,
                OperationType = ruleDto.OperationType,
                ExecutionOrder = ruleDto.ExecutionOrder,
                DependsOnRules = ruleDto.DependsOnRules,
                ColumnMappings = ruleDto.ColumnMappings.Select(m => new DataCleansingColumnMapping
                {
                    SourceColumn = m.SourceColumn,
                    TargetColumn = m.TargetColumn,
                    OutputColumns = m.OutputColumns
                }).ToList()
            };
            cleaningRules.AddExtendedRule(rule);
        }

        // Add mapping rules
        foreach (var ruleDto in request.MappingRules)
        {
            var rule = new MappingRule
            {
                Id = ruleDto.Id ?? Guid.NewGuid(),
                OperationType = ruleDto.OperationType,
                SourceColumn = ruleDto.SourceColumn,
                MappingConfig = ruleDto.MappingConfig,
                OutputColumns = ruleDto.OutputColumns
            };
            cleaningRules.AddMappingRule(rule);
        }
    }

    protected virtual async Task PersistCleaningRules(TCommand request, EnhancedCleaningRules cleaningRules)
    {
        await ProjectService.AddCleaningRules(request.ProjectId, request.DataSourceId, cleaningRules);
    }

    protected async Task<ProjectRun> StartNewRun(TCommand request)
    {
        var stepInformation = new List<StepConfiguration>();
        var dataSourceIds = new[] { request.DataSourceId };

        // Add cleanse step
        stepInformation.Add(new StepConfiguration(
            StepType.Cleanse,
            new Dictionary<string, object> { ["IsPreview"] = request.isPreview },
            dataSourceIds           
        ));

        var queuedRun = await ProjectService.StartNewRun(request.ProjectId, stepInformation);
        Logger.LogInformation("{HandlerName}: Started JobRun : {queuedRun}",
            GetType().Name, queuedRun);

        return queuedRun;
    }
}
