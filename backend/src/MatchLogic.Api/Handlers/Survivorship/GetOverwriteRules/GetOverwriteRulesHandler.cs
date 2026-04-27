using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.MergeAndSurvivorship;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading;
using System;
using System.Linq;
using MatchLogic.Application.Common;

namespace MatchLogic.Api.Handlers.Survivorship.GetOverwriteRules;

public class GetOverwriteRulesHandler : IRequestHandler<GetOverwriteRulesRequest, Result<GetOverwriteRulesResponse>>
{
    private readonly IFieldOverwriteRuleSetRepository _ruleSetRepository;
    private readonly IGenericRepository<Domain.Project.DataSource, Guid> _dataSourceRepository;
    private readonly ILogger<GetOverwriteRulesHandler> _logger;

    public GetOverwriteRulesHandler(
        IFieldOverwriteRuleSetRepository ruleSetRepository,
        IGenericRepository<Domain.Project.DataSource, Guid> dataSourceRepository,
        ILogger<GetOverwriteRulesHandler> logger)
    {
        _ruleSetRepository = ruleSetRepository;
        _dataSourceRepository = dataSourceRepository;
        _logger = logger;
    }

    public async Task<Result<GetOverwriteRulesResponse>> Handle(
        GetOverwriteRulesRequest request,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation("Getting master rules for project {ProjectId}", request.ProjectId);

        try
        {
            var response = new GetOverwriteRulesResponse();

            // Get data sources for ID-to-name mapping
            var dataSources = await _dataSourceRepository.QueryAsync(
                ds => ds.ProjectId == request.ProjectId,
                Constants.Collections.DataSources);

            var dataSourceMap = dataSources.ToDictionary(ds => ds.Id, ds => ds.Name);

            // Get existing rule set
            FieldOverwriteRuleSet? ruleSet = null;
            try
            {
                ruleSet = await _ruleSetRepository.GetActiveRuleSetAsync(request.ProjectId);
            }
            catch
            {
                // No rules exist yet - return empty
            }

            if (ruleSet?.Rules == null || !ruleSet.Rules.Any())
            {
                _logger.LogInformation("No existing master rules for project {ProjectId}", request.ProjectId);
                return Result<GetOverwriteRulesResponse>.Success(response);
            }

            // Convert to DTOs
            response.Rules = ruleSet.Rules
                .OrderBy(r => r.Order)
                .Select(rule => ConvertToDto(rule, dataSourceMap, dataSources))
                .ToList();

            _logger.LogInformation("Retrieved {Count} master rules for project {ProjectId}",
                response.Rules.Count, request.ProjectId);

            return Result<GetOverwriteRulesResponse>.Success(response);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting master rules for project {ProjectId}", request.ProjectId);
            return Result<GetOverwriteRulesResponse>.Error($"Error retrieving rules: {ex.Message}");
        }
    }

    private OverwriteRuleDto ConvertToDto(
            FieldOverwriteRule rule,
            Dictionary<Guid, string> idToNameMap,
            List<Domain.Project.DataSource> dataSources)
    {
        // Build DataSources dictionary from SelectedDataSourceIds
        var dataSourcesDict = new Dictionary<string, bool>();

        foreach (var ds in dataSources)
        {
            var isSelected = rule.DataSourceFilters?.Contains(ds.Id) ?? false;
            dataSourcesDict[ds.Name] = isSelected;
        }       

        return new OverwriteRuleDto
        {
            Id = rule.Id.ToString(),
            FieldName = rule.LogicalFieldName,
            DataSources = dataSourcesDict,
            Operation = rule.Operation.ToString(),
            Activated = rule.IsActive,
            Order = rule.Order,
            DonotOverwriteIf = rule.DoNotOverwriteIf.ToString(),
            OverwriteIf = rule.OverwriteIf.ToString(),
        };
    }
}
