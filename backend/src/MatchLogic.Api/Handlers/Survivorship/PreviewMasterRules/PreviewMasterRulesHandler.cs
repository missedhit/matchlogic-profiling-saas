using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.MergeAndSurvivorship;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using System;
using MatchLogic.Application.Common;
using MatchLogic.Api.Handlers.Survivorship.SaveMasterRules;
using MatchLogic.Application.Interfaces.MergeAndSurvivorship;
using MatchLogic.Application.Extensions;
using MatchLogic.Application.Features.MergeAndSurvivorship;
using MatchLogic.Domain.Project;
using MatchLogic.Domain.Entities;
using MatchLogic.Domain.Entities.Common;

namespace MatchLogic.Api.Handlers.Survivorship.PreviewMasterRules;

public class PreviewMasterRulesHandler : IRequestHandler<PreviewMasterRulesRequest, Result<IEnumerable<IDictionary<string, object>>>>
{
    private readonly IMasterRecordRuleSetRepository _ruleSetRepository;
    private readonly IGenericRepository<Domain.Project.DataSource, Guid> _dataSourceRepository;
    private readonly IMasterRecordDeterminationService _masterRecordDeterminationService;
    private readonly IDataStore _dataStore;
    private readonly ILogger<PreviewMasterRulesHandler> _logger;
    private const int PreviewGroupLimit = 3;

    public PreviewMasterRulesHandler(
        IMasterRecordRuleSetRepository ruleSetRepository,
        IGenericRepository<Domain.Project.DataSource, Guid> dataSourceRepository,
        IMasterRecordDeterminationService masterRecordDeterminationService,
        IDataStore dataStore,
        ILogger<PreviewMasterRulesHandler> logger)
    {
        _ruleSetRepository = ruleSetRepository;
        _dataSourceRepository = dataSourceRepository;
        _masterRecordDeterminationService = masterRecordDeterminationService;
        _dataStore = dataStore;
        _logger = logger;
    }

    public async Task<Result<IEnumerable<IDictionary<string, object>>>> Handle(
        PreviewMasterRulesRequest request,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation("Previewing {Count} master rules for project {ProjectId} (limited to {Limit} groups)",
            request.Rules?.Count ?? 0, request.ProjectId, PreviewGroupLimit);

        try
        {
            if (request.Rules == null || !request.Rules.Any())
            {
                return Result<IEnumerable<IDictionary<string, object>>>.Error("No rules provided");
            }

            // Step 1: Get data sources for name-to-ID mapping
            var dataSources = await _dataSourceRepository.QueryAsync(
                ds => ds.ProjectId == request.ProjectId,
                Constants.Collections.DataSources);

            var nameToIdMap = dataSources.ToDictionary(ds => ds.Name, ds => ds.Id);

            // Step 2: Create rule set from request
            var ruleSet = new MasterRecordRuleSet(request.ProjectId)
            {
                Id = Guid.NewGuid(),
                IsActive = true,
                Rules = new List<MasterRecordRule>()
            };

            foreach (var dto in request.Rules)
            {
                var rule = ConvertToDomain(dto, ruleSet.Id, nameToIdMap);
                ruleSet.Rules.Add(rule);
            }

            _logger.LogInformation("Created preview rule set with {Count} rules", ruleSet.Rules.Count);

            // Step 3: Get collection name
            var normalizedProjectId = GuidCollectionNameConverter.ToValidCollectionName(request.ProjectId);
            var collectionName = $"groups_{normalizedProjectId}";

            // Step 4: Stream groups from database with original master tracking
            var groupsWithOriginalMaster = LoadGroupsWithOriginalMasterAsync(
                collectionName,
                cancellationToken);

            // Step 5: Process groups through master determination service and track changes
            var processedGroupsStream = ProcessGroupsWithMasterChangeTrackingAsync(
                groupsWithOriginalMaster,
                ruleSet,
                request.ProjectId,
                cancellationToken);

            // Step 6: Convert results to dictionaries with change information
            var results = new List<IDictionary<string, object>>();

            await foreach (var groupResult in processedGroupsStream.WithCancellation(cancellationToken))
            {
                var groupDict = CreatePreviewResultDictionary(groupResult);
                results.Add(groupDict);
            }

            var changedCount = results.Count(r => r.ContainsKey("MasterRecordChanged") && (bool)r["MasterRecordChanged"]);

            _logger.LogInformation("Preview completed with {Count} processed groups, {Changed} with master changes",
                results.Count, changedCount);

            return Result<IEnumerable<IDictionary<string, object>>>.Success(results);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error previewing master rules for project {ProjectId}", request.ProjectId);
            return Result<IEnumerable<IDictionary<string, object>>>.Error($"Error previewing rules: {ex.Message}");
        }
    }

    /// <summary>
    /// Streams groups from database, capturing which record is currently master
    /// </summary>
    private async IAsyncEnumerable<GroupWithOriginalMaster> LoadGroupsWithOriginalMasterAsync(
        string collectionName,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        _logger.LogInformation("Streaming groups from collection {Collection} (limit: {Limit})",
            collectionName, PreviewGroupLimit);

        int loadedCount = 0;

        await foreach (var groupDoc in _dataStore.StreamDataAsync(collectionName, cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (loadedCount >= PreviewGroupLimit)
            {
                _logger.LogInformation("Reached preview limit of {Limit} groups", PreviewGroupLimit);
                yield break;
            }

            var group = MatchGroupConverter.ConvertDocumentToMatchGroup(groupDoc);

            // Find which record index is currently marked as master (IsMaster = true)
            var originalMasterIndex = FindMasterRecordIndex(group);

            loadedCount++;

            yield return new GroupWithOriginalMaster
            {
                Group = group,
                OriginalMasterRecordIndex = originalMasterIndex
            };
        }

        _logger.LogInformation("Loaded {Count} groups from {Collection} for preview",
            loadedCount, collectionName);
    }

    /// <summary>
    /// Process groups and track which record becomes the master
    /// </summary>
    private async IAsyncEnumerable<GroupPreviewResult> ProcessGroupsWithMasterChangeTrackingAsync(
        IAsyncEnumerable<GroupWithOriginalMaster> groupsWithOriginal,
        MasterRecordRuleSet ruleSet,
        Guid projectId,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // Store original master indices by GroupId
        var originalMasterMap = new Dictionary<int, int>();

        // First pass: collect original master indices
        var groupsList = new List<MatchGroup>();
        await foreach (var groupWithOriginal in groupsWithOriginal)
        {
            originalMasterMap[groupWithOriginal.Group.GroupId] = groupWithOriginal.OriginalMasterRecordIndex;
            groupsList.Add(groupWithOriginal.Group);
        }

        // Convert list to async enumerable for processing
        var groupsEnumerable = groupsList.ToAsyncEnumerable();

        // Process through master determination
        var processedGroups = _masterRecordDeterminationService.DetermineAsync(
            groupsEnumerable,
            ruleSet,
            projectId,
            cancellationToken);

        // Track changes
        await foreach (var processedGroup in processedGroups.WithCancellation(cancellationToken))
        {
            var originalMasterIndex = originalMasterMap.GetValueOrDefault(processedGroup.GroupId, -1);
            var newMasterIndex = FindMasterRecordIndex(processedGroup);

            var masterChanged = originalMasterIndex != newMasterIndex;

            yield return new GroupPreviewResult
            {
                Group = processedGroup,
                OriginalMasterRecordIndex = originalMasterIndex,
                NewMasterRecordIndex = newMasterIndex,
                MasterRecordChanged = masterChanged
            };
        }
    }

    /// <summary>
    /// Finds the index of the record marked as master (IsMaster = true)
    /// Returns -1 if no master is found
    /// </summary>
    private int FindMasterRecordIndex(MatchGroup group)
    {
        if (group?.Records == null || !group.Records.Any())
            return -1;

        for (int i = 0; i < group.Records.Count; i++)
        {
            var record = group.Records[i];
            if (record.TryGetValue(RecordSystemFieldNames.IsMasterRecord, out var isMasterValue) &&
                isMasterValue is bool isMaster &&
                isMaster)
            {
                return i;
            }
        }

        // If no record is marked as master, return -1
        return -1;
    }

    /// <summary>
    /// Creates the preview result dictionary with master change tracking
    /// </summary>
    private IDictionary<string, object> CreatePreviewResultDictionary(GroupPreviewResult previewResult)
    {
        var baseDict = MatchGroupConverter.ConvertMatchGroupToDocument(previewResult.Group);

        // Add master change tracking information
        baseDict["MasterRecordChanged"] = previewResult.MasterRecordChanged;
        baseDict["OriginalMasterRecordIndex"] = previewResult.OriginalMasterRecordIndex;
        baseDict["NewMasterRecordIndex"] = previewResult.NewMasterRecordIndex;

        // Optionally add human-readable information about which records
        if (previewResult.OriginalMasterRecordIndex >= 0 &&
            previewResult.OriginalMasterRecordIndex < previewResult.Group.Records.Count)
        {
            var originalMasterRecord = previewResult.Group.Records[previewResult.OriginalMasterRecordIndex];
            baseDict["OriginalMasterRecordId"] = GetRecordId(originalMasterRecord);
        }

        if (previewResult.NewMasterRecordIndex >= 0 &&
            previewResult.NewMasterRecordIndex < previewResult.Group.Records.Count)
        {
            var newMasterRecord = previewResult.Group.Records[previewResult.NewMasterRecordIndex];
            baseDict["NewMasterRecordId"] = GetRecordId(newMasterRecord);
        }

        return baseDict;
    }

    /// <summary>
    /// Gets the record ID from a record dictionary, checking both _id and RecordId fields
    /// </summary>
    private object GetRecordId(IDictionary<string, object> record)
    {
        if (record.TryGetValue("_id", out var id) && id != null)
            return id;

        if (record.TryGetValue("RecordId", out var recordId) && recordId != null)
            return recordId;

        return null;
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

// Helper classes for master change tracking
internal class GroupWithOriginalMaster
{
    public MatchGroup Group { get; set; }
    public int OriginalMasterRecordIndex { get; set; }
}

internal class GroupPreviewResult
{
    public MatchGroup Group { get; set; }
    public int OriginalMasterRecordIndex { get; set; }
    public int NewMasterRecordIndex { get; set; }
    public bool MasterRecordChanged { get; set; }
}