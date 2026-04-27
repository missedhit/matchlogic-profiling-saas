using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.MergeAndSurvivorship;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using System;
using MatchLogic.Application.Common;

namespace MatchLogic.Api.Handlers.Survivorship.SaveMasterRules;

public class SaveMasterRulesHandler : IRequestHandler<SaveMasterRulesRequest, Result<SaveMasterRulesResponse>>
{
    private readonly IMasterRecordRuleSetRepository _ruleSetRepository;
    private readonly IGenericRepository<Domain.Project.DataSource, Guid> _dataSourceRepository;
    private readonly ILogger<SaveMasterRulesHandler> _logger;

    public SaveMasterRulesHandler(
        IMasterRecordRuleSetRepository ruleSetRepository,
        IGenericRepository<Domain.Project.DataSource, Guid> dataSourceRepository,
        ILogger<SaveMasterRulesHandler> logger)
    {
        _ruleSetRepository = ruleSetRepository;
        _dataSourceRepository = dataSourceRepository;
        _logger = logger;
    }

    public async Task<Result<SaveMasterRulesResponse>> Handle(
        SaveMasterRulesRequest request,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation("Saving {Count} master rules for project {ProjectId}",
            request.Rules?.Count ?? 0, request.ProjectId);

        try
        {
            if (request.Rules == null || !request.Rules.Any())
            {
                return Result<SaveMasterRulesResponse>.Error("No rules provided");
            }

            // Get data sources for name-to-ID mapping
            var dataSources = await _dataSourceRepository.QueryAsync(
                ds => ds.ProjectId == request.ProjectId,
                Constants.Collections.DataSources);

            var nameToIdMap = dataSources.ToDictionary(ds => ds.Name, ds => ds.Id);

            // Get or create rule set        
            MasterRecordRuleSet ruleSet;
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
                ruleSet = new MasterRecordRuleSet(request.ProjectId)
                {
                    Id = Guid.NewGuid(),
                    IsActive = true,
                    Rules = new List<MasterRecordRule>()
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

            return Result<SaveMasterRulesResponse>.Success(new SaveMasterRulesResponse
            {
                Success = true,
                Message = "Rules saved successfully",
                RulesSaved = ruleSet.Rules.Count
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error saving master rules for project {ProjectId}", request.ProjectId);
            return Result<SaveMasterRulesResponse>.Error($"Error saving rules: {ex.Message}");
        }
    }

    private MasterRecordRule ConvertToDomain(
           MasterRuleDto dto,
           Guid ruleSetId,
           Dictionary<string, Guid> nameToIdMap)
    {
        // Parse operation
        if (!Enum.TryParse<MasterRecordOperation>(dto.Operation, true, out var operation))
        {
            _logger.LogWarning(
                "Invalid operation '{Operation}', defaulting to Longest",
                dto.Operation);
            operation = MasterRecordOperation.Longest;
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

        var rule = new MasterRecordRule
        {
            Id = ruleId,
            RuleSetId = ruleSetId,
            Order = dto.Order,
            LogicalFieldName = dto.FieldName,
            Operation = operation,
            IsActive = dto.Activated,
            SelectedDataSourceIds = selectedDataSourceIds
        };

        // For PreferDataSource operation, set PreferredDataSourceId from the first selected
        if (operation == MasterRecordOperation.PreferDataSource && selectedDataSourceIds.Any())
        {
            rule.PreferredDataSourceId = selectedDataSourceIds.First();
        }

        return rule;
    }
}
