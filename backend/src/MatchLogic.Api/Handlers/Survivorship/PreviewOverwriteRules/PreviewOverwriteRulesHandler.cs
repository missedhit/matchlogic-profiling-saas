using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.MergeAndSurvivorship;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using System;
using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.MergeAndSurvivorship;
using MatchLogic.Application.Features.MergeAndSurvivorship;
using MatchLogic.Domain.Project;
using MatchLogic.Domain.Entities;
using MatchLogic.Domain.Entities.Common;
using MatchLogic.Application.Extensions;

namespace MatchLogic.Api.Handlers.Survivorship.PreviewOverwriteRules;

public class PreviewOverwriteRulesHandler : IRequestHandler<PreviewOverwriteRulesRequest, Result<IEnumerable<IDictionary<string, object>>>>
{
    private readonly IGenericRepository<Domain.Project.DataSource, Guid> _dataSourceRepository;
    private readonly IGenericRepository<MappedFieldsRow, Guid> _mappedFieldRowsRepository;
    private readonly IFieldOverwriteService _fieldOverwriteService;
    private readonly ILogicalFieldResolver _fieldResolver;
    private readonly IDataStore _dataStore;
    private readonly ILogger<PreviewOverwriteRulesHandler> _logger;
    private const int PreviewGroupLimit = 3;

    public PreviewOverwriteRulesHandler(
        IGenericRepository<Domain.Project.DataSource, Guid> dataSourceRepository,
        IGenericRepository<MappedFieldsRow, Guid> mappedFieldRowsRepository,
        IFieldOverwriteService fieldOverwriteService,
        ILogicalFieldResolver fieldResolver,
        IDataStore dataStore,
        ILogger<PreviewOverwriteRulesHandler> logger)
    {
        _dataSourceRepository = dataSourceRepository;
        _mappedFieldRowsRepository = mappedFieldRowsRepository;
        _fieldOverwriteService = fieldOverwriteService;
        _fieldResolver = fieldResolver;
        _dataStore = dataStore;
        _logger = logger;
    }

    public async Task<Result<IEnumerable<IDictionary<string, object>>>> Handle(
        PreviewOverwriteRulesRequest request,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation("Previewing {Count} overwrite rules for project {ProjectId} (limited to {Limit} groups)",
            request.Rules?.Count ?? 0, request.ProjectId, PreviewGroupLimit);

        try
        {
            if (request.Rules == null || !request.Rules.Any())
            {
                return Result<IEnumerable<IDictionary<string, object>>>.Error("No rules provided");
            }

            // Step 1: Load field mappings (required by FieldOverwriteService)
            var dataSources = await _dataSourceRepository.QueryAsync(
                ds => ds.ProjectId == request.ProjectId,
                Constants.Collections.DataSources);

            var mappedFieldsRows = await _mappedFieldRowsRepository.QueryAsync(
                mf => mf.ProjectId == request.ProjectId,
                Constants.Collections.MappedFieldRows);

            var fieldMappings = await _fieldResolver.ResolveLogicalFieldsAsync(
                mappedFieldsRows?.FirstOrDefault()?.MappedFields,
                dataSources.ToList());

            if (fieldMappings == null || !fieldMappings.Any())
            {
                return Result<IEnumerable<IDictionary<string, object>>>.Error("No field mappings found for project");
            }

            // Step 2: Create name-to-ID mapping for data sources
            var nameToIdMap = dataSources.ToDictionary(ds => ds.Name, ds => ds.Id);

            // Step 3: Create rule set from request
            var ruleSet = new FieldOverwriteRuleSet(request.ProjectId)
            {
                Id = Guid.NewGuid(),
                IsActive = true,
                Rules = new List<FieldOverwriteRule>()
            };

            foreach (var dto in request.Rules)
            {
                var rule = ConvertToDomain(dto, ruleSet.Id, nameToIdMap);
                ruleSet.Rules.Add(rule);
            }

            _logger.LogInformation("Created preview rule set with {Count} rules", ruleSet.Rules.Count);

            // Step 4: Get collection name
            var normalizedProjectId = GuidCollectionNameConverter.ToValidCollectionName(request.ProjectId);
            var collectionName = $"groups_{normalizedProjectId}";

            // Step 5: Stream groups from database with original field values captured
            var groupsWithOriginalValues = LoadGroupsWithOriginalValuesAsync(
                collectionName,
                ruleSet,
                fieldMappings,
                dataSources.ToList(),
                cancellationToken);

            // Step 6: Process groups through field overwrite service and track changes
            var processedGroupsStream = ProcessGroupsWithFieldChangeTrackingAsync(
                groupsWithOriginalValues,
                ruleSet,
                fieldMappings,
                dataSources.ToList(),
                cancellationToken);

            // Step 7: Convert results to dictionaries with change information
            var results = new List<IDictionary<string, object>>();

            await foreach (var groupResult in processedGroupsStream.WithCancellation(cancellationToken))
            {
                var groupDict = CreatePreviewResultDictionary(groupResult);
                results.Add(groupDict);
            }

            var groupsWithChanges = results.Count(r =>
                r.ContainsKey("FieldChanges") &&
                r["FieldChanges"] is List<object> changes &&
                changes.Any());

            _logger.LogInformation("Preview completed with {Count} processed groups, {Changed} groups with field changes",
                results.Count, groupsWithChanges);

            return Result<IEnumerable<IDictionary<string, object>>>.Success(results);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error previewing overwrite rules for project {ProjectId}", request.ProjectId);
            return Result<IEnumerable<IDictionary<string, object>>>.Error($"Error previewing rules: {ex.Message}");
        }
    }

    /// <summary>
    /// Streams groups from database, capturing original field values for fields in the rules
    /// </summary>
    private async IAsyncEnumerable<GroupWithOriginalValues> LoadGroupsWithOriginalValuesAsync(
        string collectionName,
        FieldOverwriteRuleSet ruleSet,
        List<LogicalFieldMapping> fieldMappings,
        List<Domain.Project.DataSource> dataSources,
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

            // Capture original field values for fields that have rules
            var originalValues = CaptureOriginalFieldValues(group, ruleSet, fieldMappings, dataSources);

            loadedCount++;

            yield return new GroupWithOriginalValues
            {
                Group = group,
                OriginalFieldValues = originalValues
            };
        }

        _logger.LogInformation("Loaded {Count} groups from {Collection} for preview",
            loadedCount, collectionName);
    }

    /// <summary>
    /// Captures the original field values for all fields that have rules defined
    /// </summary>
    private Dictionary<int, Dictionary<string, object>> CaptureOriginalFieldValues(
        MatchGroup group,
        FieldOverwriteRuleSet ruleSet,
        List<LogicalFieldMapping> fieldMappings,
        List<Domain.Project.DataSource> dataSources)
    {
        var originalValues = new Dictionary<int, Dictionary<string, object>>();

        // Get logical field names from rules
        var logicalFieldNames = ruleSet.Rules.Select(r => r.LogicalFieldName).Distinct().ToList();

        // For each record in the group
        for (int recordIndex = 0; recordIndex < group.Records.Count; recordIndex++)
        {
            var record = group.Records[recordIndex];
            var recordValues = new Dictionary<string, object>();

            var dataSourceId = ExtractDataSourceId(record);

            // Capture value for each field that has a rule
            foreach (var logicalFieldName in logicalFieldNames)
            {
                var fieldValue = _fieldResolver.GetFieldValue(
                    record,
                    logicalFieldName,
                    dataSourceId,
                    fieldMappings);

                recordValues[logicalFieldName] = fieldValue;
            }

            originalValues[recordIndex] = recordValues;
        }

        return originalValues;
    }

    /// <summary>
    /// Process groups through field overwrite service and track field changes
    /// </summary>
    private async IAsyncEnumerable<GroupFieldChangeResult> ProcessGroupsWithFieldChangeTrackingAsync(
        IAsyncEnumerable<GroupWithOriginalValues> groupsWithOriginal,
        FieldOverwriteRuleSet ruleSet,
        List<LogicalFieldMapping> fieldMappings,
        List<Domain.Project.DataSource> dataSources,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // Store original values by GroupId
        var originalValuesMap = new Dictionary<int, Dictionary<int, Dictionary<string, object>>>();

        // First pass: collect original values
        var groupsList = new List<MatchGroup>();
        await foreach (var groupWithOriginal in groupsWithOriginal)
        {
            originalValuesMap[groupWithOriginal.Group.GroupId] = groupWithOriginal.OriginalFieldValues;
            groupsList.Add(groupWithOriginal.Group);
        }

        // Convert list to async enumerable for processing
        var groupsEnumerable = groupsList.ToAsyncEnumerable();

        // Process through field overwrite service using OverwriteForSingleGroupAsync
        // We can't use the streaming OverwriteAsync because it initializes field mappings internally
        foreach (var group in groupsList)
        {
            var processedGroup = await _fieldOverwriteService.OverwriteForSingleGroupAsync(
                group,
                ruleSet,
                fieldMappings,
                cancellationToken);

            var originalValues = originalValuesMap.GetValueOrDefault(processedGroup.GroupId);

            // Compare and track changes
            var fieldChanges = CompareFieldValues(
                processedGroup,
                originalValues,
                ruleSet,
                fieldMappings,
                dataSources);

            yield return new GroupFieldChangeResult
            {
                Group = processedGroup,
                FieldChanges = fieldChanges
            };
        }
    }

    /// <summary>
    /// Compares original and new field values to identify changes
    /// </summary>
    private List<FieldChangeDetail> CompareFieldValues(
        MatchGroup group,
        Dictionary<int, Dictionary<string, object>> originalValues,
        FieldOverwriteRuleSet ruleSet,
        List<LogicalFieldMapping> fieldMappings,
        List<Domain.Project.DataSource> dataSources)
    {
        var changes = new List<FieldChangeDetail>();

        if (originalValues == null)
            return changes;

        var logicalFieldNames = ruleSet.Rules.Select(r => r.LogicalFieldName).Distinct().ToList();

        // For each record
        for (int recordIndex = 0; recordIndex < group.Records.Count; recordIndex++)
        {
            var record = group.Records[recordIndex];
            var dataSourceId = ExtractDataSourceId(record);

            if (!originalValues.TryGetValue(recordIndex, out var recordOriginalValues))
                continue;

            // Check each field that has a rule
            foreach (var logicalFieldName in logicalFieldNames)
            {
                var originalValue = recordOriginalValues.GetValueOrDefault(logicalFieldName);

                var newValue = _fieldResolver.GetFieldValue(
                    record,
                    logicalFieldName,
                    dataSourceId,
                    fieldMappings);

                // Check if value changed
                if (!AreValuesEqual(originalValue, newValue))
                {
                    var physicalFieldName = _fieldResolver.GetPhysicalFieldName(
                        logicalFieldName,
                        dataSourceId,
                        fieldMappings);

                    changes.Add(new FieldChangeDetail
                    {
                        RecordIndex = recordIndex,
                        RecordId = GetRecordId(record),
                        LogicalFieldName = logicalFieldName,
                        PhysicalFieldName = physicalFieldName,
                        OriginalValue = originalValue,
                        NewValue = newValue,
                        DataSourceId = dataSourceId
                    });
                }
            }
        }

        return changes;
    }

    /// <summary>
    /// Compares two values for equality (handles null, DBNull, and string comparisons)
    /// </summary>
    private bool AreValuesEqual(object value1, object value2)
    {
        // Handle nulls and DBNull
        var isNull1 = value1 == null || value1 == DBNull.Value;
        var isNull2 = value2 == null || value2 == DBNull.Value;

        if (isNull1 && isNull2) return true;
        if (isNull1 || isNull2) return false;

        // String comparison
        return value1.Equals(value2) || value1.ToString() == value2.ToString();
    }

    /// <summary>
    /// Creates the preview result dictionary with field change tracking
    /// </summary>
    private IDictionary<string, object> CreatePreviewResultDictionary(GroupFieldChangeResult changeResult)
    {
        var baseDict = MatchGroupConverter.ConvertMatchGroupToDocument(changeResult.Group);

        // Add field change tracking information
        baseDict["HasFieldChanges"] = changeResult.FieldChanges.Any();
        baseDict["TotalFieldChanges"] = changeResult.FieldChanges.Count;

        // Add detailed field changes
        baseDict["FieldChanges"] = changeResult.FieldChanges.Select(fc => new Dictionary<string, object>
        {
            ["RecordIndex"] = fc.RecordIndex,
            ["RecordId"] = fc.RecordId,
            ["LogicalFieldName"] = fc.LogicalFieldName,
            ["PhysicalFieldName"] = fc.PhysicalFieldName ?? fc.LogicalFieldName,
            ["OriginalValue"] = fc.OriginalValue,
            ["NewValue"] = fc.NewValue,
            ["DataSourceId"] = fc.DataSourceId
        }).ToList();

        return baseDict;
    }

    /// <summary>
    /// Extracts data source ID from record
    /// </summary>
    private Guid ExtractDataSourceId(IDictionary<string, object> record)
    {
        if (record.TryGetValue("_metadata", out var metadataObj) &&
            metadataObj is IDictionary<string, object> metadata)
        {
            if (metadata.TryGetValue("DataSourceId", out var dsId))
            {
                if (dsId is Guid guid) return guid;
                if (Guid.TryParse(dsId.ToString(), out var parsedGuid)) return parsedGuid;
            }
        }

        if (record.TryGetValue("DataSourceId", out var directDsId))
        {
            if (directDsId is Guid guid) return guid;
            if (Guid.TryParse(directDsId.ToString(), out var parsedGuid)) return parsedGuid;
        }

        return Guid.Empty;
    }

    /// <summary>
    /// Gets the record ID from a record dictionary
    /// </summary>
    private object GetRecordId(IDictionary<string, object> record)
    {
        if (record.TryGetValue("_id", out var id) && id != null)
            return id;

        if (record.TryGetValue("RecordId", out var recordId) && recordId != null)
            return recordId;

        return null;
    }

    private FieldOverwriteRule ConvertToDomain(
        OverwriteRuleDto dto,
        Guid ruleSetId,
        Dictionary<string, Guid> nameToIdMap)
    {
        // Parse operation
        if (!Enum.TryParse<OverwriteOperation>(dto.Operation, true, out var operation))
        {
            _logger.LogWarning("Invalid operation '{Operation}', defaulting to Longest", dto.Operation);
            operation = OverwriteOperation.Longest;
        }

        if (!Enum.TryParse<OverwriteCondition>(dto.OverwriteIf, true, out var overwriteIf))
        {
            _logger.LogWarning("Invalid OverwriteIf '{Condition}', defaulting to NoCondition", dto.OverwriteIf);
            overwriteIf = OverwriteCondition.NoCondition;
        }

        if (!Enum.TryParse<OverwriteCondition>(dto.DonotOverwriteIf, true, out var donotOverwriteIf))
        {
            _logger.LogWarning("Invalid DonotOverwriteIf '{Condition}', defaulting to NoCondition", dto.DonotOverwriteIf);
            donotOverwriteIf = OverwriteCondition.NoCondition;
        }

        // Map DataSources dictionary to data source IDs
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
        Guid ruleId = string.IsNullOrEmpty(dto.Id) || !Guid.TryParse(dto.Id, out var parsedId)
            ? Guid.NewGuid()
            : parsedId;

        return new FieldOverwriteRule
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
            Configuration = dto.Configuration
        };
    }
}

// Helper classes for field change tracking
internal class GroupWithOriginalValues
{
    public MatchGroup Group { get; set; }
    // Dictionary: RecordIndex -> (LogicalFieldName -> Value)
    public Dictionary<int, Dictionary<string, object>> OriginalFieldValues { get; set; }
}

internal class GroupFieldChangeResult
{
    public MatchGroup Group { get; set; }
    public List<FieldChangeDetail> FieldChanges { get; set; }
}

internal class FieldChangeDetail
{
    public int RecordIndex { get; set; }
    public object RecordId { get; set; }
    public string LogicalFieldName { get; set; }
    public string PhysicalFieldName { get; set; }
    public object OriginalValue { get; set; }
    public object NewValue { get; set; }
    public Guid DataSourceId { get; set; }
}