using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Cleansing;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.CleansingAndStandaradization;
using MatchLogic.Domain.Project;
using MatchLogic.Infrastructure.CleansingAndStandardization.Enhance;
using Microsoft.Extensions.Logging;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.Cleansing.Column;

public class ColumnCleansingRuleHandler :
        IRequestHandler<ColumnCleansingRuleCommand, Result<ColumnCleansingRuleResponse>>
{
    private readonly IRulesManager<EnhancedTransformationRule> _ruleManager;
    public ColumnCleansingRuleHandler(
        IProjectService projectService,
        IRulesManager<EnhancedTransformationRule> rulesManager,
        ILogger<ColumnCleansingRuleHandler> logger)
    {
        _ruleManager = rulesManager;
    }

    public async Task<Result<ColumnCleansingRuleResponse>> Handle(ColumnCleansingRuleCommand request, CancellationToken cancellationToken)
    {
        var cleaningRules = new EnhancedCleaningRules
        {
            Id = Guid.NewGuid(),
            ProjectId = request.ProjectId,
            DataSourceId = request.DataSourceId,
        };
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

        var schema = await _ruleManager.GetOutputSchemaAsync(cleaningRules);

        return Result<ColumnCleansingRuleResponse>.Success(new ColumnCleansingRuleResponse(schema));
    }
}
