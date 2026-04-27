using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Cleansing;
using MatchLogic.Application.Interfaces.LiveSearch;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.CleansingAndStandaradization;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.LiveSearch;

public class LiveCleansingService : ILiveCleansingService
{
    private readonly IGenericRepository<EnhancedCleaningRules, Guid> _rulesRepository;
    private readonly IEnhancedRuleFactory _ruleFactory;
    private readonly IRulesManager<EnhancedTransformationRule> _rulesManager;
    private readonly ILogger<LiveCleansingService> _logger;
    private readonly ConcurrentDictionary<Guid, DataSourceCleansingContext> _contexts = new();
    private readonly HashSet<Guid> _loadedProjects = new();

    public LiveCleansingService(
        IGenericRepository<EnhancedCleaningRules, Guid> rulesRepository,
        IEnhancedRuleFactory ruleFactory,
        IRulesManager<EnhancedTransformationRule> rulesManager,
        ILogger<LiveCleansingService> logger)
    {
        _rulesRepository = rulesRepository ?? throw new ArgumentNullException(nameof(rulesRepository));
        _ruleFactory = ruleFactory ?? throw new ArgumentNullException(nameof(ruleFactory));
        _rulesManager = rulesManager ?? throw new ArgumentNullException(nameof(rulesManager));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task LoadProjectRulesAsync(
        Guid projectId,
        IEnumerable<Guid> dataSourceIds,
        CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Loading cleansing rules for project {ProjectId}", projectId);

        foreach (var dsId in dataSourceIds)
        {
            var rulesQuery = await _rulesRepository.QueryAsync(
                x => x.ProjectId == projectId && x.DataSourceId == dsId,
                Constants.Collections.CleaningRules);

            var rules = rulesQuery.FirstOrDefault();

            var context = new DataSourceCleansingContext
            {
                DataSourceId = dsId,
                Rules = rules,
                IsConfigured = rules != null && rules.GetTotalRuleCount() > 0
            };

            if (context.IsConfigured)
            {
                context.RulesManager = _rulesManager; ;
                await context.RulesManager.LoadRulesFromConfigAsync(rules);
                context.DerivedColumns = ExtractDerivedColumns(rules);

                _logger.LogInformation(
                    "Loaded {RuleCount} cleansing rules for data source {DataSourceId}, {DerivedColumns} derived columns",
                    rules.GetTotalRuleCount(),
                    dsId,
                    context.DerivedColumns.Count);
            }
            else
            {
                _logger.LogInformation("No cleansing rules for data source {DataSourceId}", dsId);
            }

            _contexts[dsId] = context;
        }

        _loadedProjects.Add(projectId);
    }

    public IDictionary<string, object> CleanseRecord(Guid dataSourceId, IDictionary<string, object> record)
    {
        if (!_contexts.TryGetValue(dataSourceId, out var context))
            return record;

        if (!context.IsConfigured)
            return record;

        try
        {
            var batch = RecordBatch.FromDictionaries(new[] { record });
            context.RulesManager.ApplyRules(batch);
            return batch.ToDictionaries()[0];
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error cleansing record for data source {DataSourceId}", dataSourceId);
            return record; // Return original on error
        }
    }

    public bool HasCleansingRules(Guid dataSourceId)
    {
        return _contexts.TryGetValue(dataSourceId, out var context) && context.IsConfigured;
    }

    public IReadOnlyList<string> GetDerivedColumns(Guid dataSourceId)
    {
        if (_contexts.TryGetValue(dataSourceId, out var context))
            return context.DerivedColumns.AsReadOnly();

        return new List<string>().AsReadOnly();
    }

    public bool IsProjectLoaded(Guid projectId)
    {
        return _loadedProjects.Contains(projectId);
    }

    private List<string> ExtractDerivedColumns(EnhancedCleaningRules rules)
    {
        var derivedColumns = new HashSet<string>();

        foreach (var rule in rules.ExtendedRules)
        {
            if (rule.OperationType == OperationType.Mapping)
            {
                foreach (var mapping in rule.ColumnMappings)
                {
                    if (!string.IsNullOrEmpty(mapping.TargetColumn))
                        derivedColumns.Add(mapping.TargetColumn);

                    derivedColumns.UnionWith(mapping.OutputColumns);
                }
            }
        }

        foreach (var mappingRule in rules.MappingRules)
        {
            derivedColumns.UnionWith(mappingRule.OutputColumns);
        }

        return derivedColumns.ToList();
    }

    private class DataSourceCleansingContext
    {
        public Guid DataSourceId { get; set; }
        public EnhancedCleaningRules Rules { get; set; }
        public IRulesManager<EnhancedTransformationRule> RulesManager { get; set; }
        public List<string> DerivedColumns { get; set; } = new();
        public bool IsConfigured { get; set; }
    }
}
