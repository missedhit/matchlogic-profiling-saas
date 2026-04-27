using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.MergeAndSurvivorship;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using System;
using MatchLogic.Application.Common;

namespace MatchLogic.Api.Handlers.Survivorship.SaveOverwriteRules;

public class SaveOverwriteRulesHandler : IRequestHandler<SaveOverwriteRulesRequest, Result<SaveOverwriteRulesResponse>>
{
    private readonly IFieldOverwriteRuleSetRepository _ruleSetRepository;
    private readonly IGenericRepository<Domain.Project.DataSource, Guid> _dataSourceRepository;
    private readonly ILogger<SaveOverwriteRulesHandler> _logger;

    public SaveOverwriteRulesHandler(
        IFieldOverwriteRuleSetRepository ruleSetRepository,
        IGenericRepository<Domain.Project.DataSource, Guid> dataSourceRepository,
        ILogger<SaveOverwriteRulesHandler> logger)
    {
        _ruleSetRepository = ruleSetRepository;
        _dataSourceRepository = dataSourceRepository;
        _logger = logger;
    }

    public async Task<Result<SaveOverwriteRulesResponse>> Handle(
        SaveOverwriteRulesRequest request,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation("Saving {Count} master rules for project {ProjectId}",
            request.Rules?.Count ?? 0, request.ProjectId);

        try
        {
            if (request.Rules == null || !request.Rules.Any())
            {
                return Result<SaveOverwriteRulesResponse>.Error("No rules provided");
            }

            // Get data sources for name-to-ID mapping
            var dataSources = await _dataSourceRepository.QueryAsync(
                ds => ds.ProjectId == request.ProjectId,
                Constants.Collections.DataSources);

            var nameToIdMap = dataSources.ToDictionary(ds => ds.Name, ds => ds.Id);

            // Get or create rule set        
            FieldOverwriteRuleSet ruleSet;
            try
            {
                ruleSet = await _ruleSetRepository.GetActiveRuleSetAsync(request.ProjectId);
            }
            catch
            {
                ruleSet = null;
            }

            // Explicit null check
            if (ruleSet == null)
            {
                ruleSet = new FieldOverwriteRuleSet(request.ProjectId)
                {
                    Id = Guid.NewGuid(),
                    IsActive = true,
                    Rules = new List<FieldOverwriteRule>()
                };
            }

            ruleSet.Rules.Clear();

            if (request.Rules != null)
            {
                foreach (var dto in request.Rules)
                {
                    var rule = ConvertToDomain(dto, ruleSet.Id, nameToIdMap);
                    ruleSet.Rules.Add(rule);
                }
            }

            // Save
            await _ruleSetRepository.SaveWithRulesAsync(ruleSet);

            _logger.LogInformation("Saved {Count} master rules for project {ProjectId}",
                ruleSet.Rules.Count, request.ProjectId);

            return Result<SaveOverwriteRulesResponse>.Success(new SaveOverwriteRulesResponse
            {
                Success = true,
                Message = "Rules saved successfully",
                RulesSaved = ruleSet.Rules.Count
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error saving master rules for project {ProjectId}", request.ProjectId);
            return Result<SaveOverwriteRulesResponse>.Error($"Error saving rules: {ex.Message}");
        }
    }

    private FieldOverwriteRule ConvertToDomain(
           OverwriteRuleDto dto,
           Guid ruleSetId,
           Dictionary<string, Guid> nameToIdMap)
    {
        // Parse operation
        if (!Enum.TryParse<OverwriteOperation>(dto.Operation, true, out var operation))
        {
            _logger.LogWarning(
                "Invalid operation '{Operation}', defaulting to Longest",
                dto.Operation);
            operation = OverwriteOperation.Longest;
        }

        if (!Enum.TryParse<OverwriteCondition>(dto.OverwriteIf, true, out var overwriteIf))
        {
            _logger.LogWarning(
                "Invalid operation '{Operation}', defaulting to NoCondition",
                dto.OverwriteIf);
            overwriteIf = OverwriteCondition.NoCondition;
        }
        if (!Enum.TryParse<OverwriteCondition>(dto.DonotOverwriteIf, true, out var donotOverwriteIf))
        {
            _logger.LogWarning(
                "Invalid operation '{Operation}', defaulting to NoCondition",
                dto.DonotOverwriteIf);
            donotOverwriteIf = OverwriteCondition.NoCondition;
        }
        // Map DataSources dictionary to SelectedDataSourceIds
        // Only include data sources where the checkbox is true
        var selectedDataSourceIds = new List<Guid>();

        if (dto.DataSources != null)
        {
            foreach (var kvp in dto.DataSources)
            {
                if (kvp.Value && nameToIdMap.TryGetValue(kvp.Key, out var dsId))
                {
                    selectedDataSourceIds.Add(dsId);
                }
            }
        }

        // Parse rule ID or generate new one
        Guid ruleId;
        if (string.IsNullOrEmpty(dto.Id) || !Guid.TryParse(dto.Id, out ruleId))
        {
            ruleId = Guid.NewGuid();
        }

        var rule = new FieldOverwriteRule
        {
            Id = ruleId,
            RuleSetId = ruleSetId,
            Order = dto.Order,
            LogicalFieldName = dto.FieldName,
            Operation = operation,
            IsActive = dto.Activated,
            DataSourceFilters = selectedDataSourceIds,
            DoNotOverwriteIf = donotOverwriteIf,
            OverwriteIf = overwriteIf,
            Configuration = dto.Configuration,            
        };

        return rule;
    }
}
