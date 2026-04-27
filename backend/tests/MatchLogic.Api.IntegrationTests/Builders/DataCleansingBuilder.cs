using MatchLogic.Api.Handlers.Cleansing;
using MatchLogic.Api.Handlers.Cleansing.Create;
using MatchLogic.Application.Features.CleansingAndStandardization.DTOs;
using MatchLogic.Application.Features.Project;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.CleansingAndStandaradization;
using MatchLogic.Domain.Import;
using MatchLogic.Api.Handlers.Cleansing.Create;
using MatchLogic.Application.Features.CleansingAndStandardization.DTOs;
using MatchLogic.Application.Features.Project;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.CleansingAndStandaradization;
using Microsoft.Extensions.DependencyInjection;
namespace MatchLogic.Api.IntegrationTests.Builders;
public class CreateCleansingRuleCommandBuilder
{
    private Guid _projectId = Guid.NewGuid();
    private Guid _dataSourceId = Guid.NewGuid();
    private List<CleaningRuleDto> _standardRules = [];
    private List<ExtendedCleaningRuleDto> _extendedRules = [];
    private List<MappingRuleDto> _mappingRules = [];

    private IProjectService _projectService;

    public CreateCleansingRuleCommandBuilder(IServiceProvider serviceProvider)
    {
        _projectService = serviceProvider.GetRequiredService<IProjectService>();
    }

    public CreateCleansingRuleCommandBuilder WithProjectId(Guid projectId)
    {
        _projectId = projectId;
        return this;
    }

    public CreateCleansingRuleCommandBuilder WithDataSourceId(Guid dataSourceId)
    {
        _dataSourceId = dataSourceId;
        return this;
    }

    public CreateCleansingRuleCommandBuilder WithStandardRule(CleaningRuleDto rule)
    {
        _standardRules.Add(rule);
        return this;
    }

    public CreateCleansingRuleCommandBuilder WithStandardRules(IEnumerable<CleaningRuleDto> rules)
    {
        _standardRules.AddRange(rules);
        return this;
    }

    public CreateCleansingRuleCommandBuilder WithExtendedRule(ExtendedCleaningRuleDto rule)
    {
        _extendedRules.Add(rule);
        return this;
    }

    public CreateCleansingRuleCommandBuilder WithExtendedRules(IEnumerable<ExtendedCleaningRuleDto> rules)
    {
        _extendedRules.AddRange(rules);
        return this;
    }

    public CreateCleansingRuleCommandBuilder WithMappingRule(MappingRuleDto rule)
    {
        _mappingRules.Add(rule);
        return this;
    }

    public CreateCleansingRuleCommandBuilder WithMappingRules(IEnumerable<MappingRuleDto> rules)
    {
        _mappingRules.AddRange(rules);
        return this;
    }

    public CreateCleansingRuleCommand BuildDomain()
    {
        return new CreateCleansingRuleCommand
        {
            ProjectId = _projectId,
            DataSourceId = _dataSourceId,
            StandardRules = _standardRules,
            ExtendedRules = _extendedRules,
            MappingRules = _mappingRules
        };
    }


    public async Task BuildAsync()
    {

        var request = BuildDomain();
        // Create a new cleaning rules instance
        var cleaningRules = new EnhancedCleaningRules
        {
            Id = Guid.NewGuid(),
            ProjectId = request.ProjectId,
            DataSourceId = request.DataSourceId,
        };

        // Add rules
        AddRulesToInstance(cleaningRules, request);

        // Persist changes
        await _projectService.AddCleaningRules(request.ProjectId, request.DataSourceId, cleaningRules);
        //await PersistCleaningRules(request, cleaningRules);

        // Start a new run
        var queuedRun = await StartNewRun(request);

    }

    protected void AddRulesToInstance(EnhancedCleaningRules cleaningRules, CreateCleansingRuleCommand request)
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



    protected async Task<ProjectRun> StartNewRun(CreateCleansingRuleCommand request)
    {
        var stepInformation = new List<StepConfiguration>();
        var dataSourceIds = new[] { request.DataSourceId }.ToArray();

        // Add cleanse step
        stepInformation.Add(new StepConfiguration(
            StepType.Cleanse,
            dataSourceIds
        ));

        var queuedRun = await _projectService.StartNewRun(request.ProjectId, stepInformation);
        //Logger.LogInformation("{HandlerName}: Started JobRun : {queuedRun}",
        //    GetType().Name, queuedRun);

        return queuedRun;
    }
}

