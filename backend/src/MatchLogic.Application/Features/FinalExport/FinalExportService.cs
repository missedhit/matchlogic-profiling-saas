using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Linq;
using System;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using System.Threading;
using MatchLogic.Application.Interfaces.Export;
using MatchLogic.Application.Features.Export;
using MatchLogic.Application.Extensions;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Application.Interfaces.FinalExport;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.Entities;
using MatchLogic.Domain.FinalExport;
using MatchLogic.Domain.Project;
using MatchLogic.Application.Common;
using MatchLogic.Domain.Import;

namespace MatchLogic.Application.Features.FinalExport;

public class FinalExportService : IFinalExportService
{
    private readonly IDataStore _dataStore;
    private readonly IGenericRepository<DataSource, Guid> _dataSourceRepository;
    private readonly IGenericRepository<MappedFieldsRow, Guid> _mappedFieldsRepository;
    private readonly IGenericRepository<FinalExportResult, Guid> _exportResultRepository;
    private readonly IGenericRepository<FinalExportSettings, Guid> _exportSettingsRepository;
    private readonly IGenericRepository<MatchDefinitionCollection, Guid> _matchDefinitionRepository;
    private readonly IJobEventPublisher _jobEventPublisher;
    private readonly ILogger<FinalExportService> _logger;
    private readonly IExportDataWriterStrategyFactory _exportDataWriterStrategyFactory;
    private const int BatchSize = 1000;
    private const int ProgressReportInterval = 10000;
    private const int MaxPreviewRecords = 500;
    private readonly IExportFilePathHelper _exportFilePathHelper;

    public FinalExportService(
        IDataStore dataStore,
        IGenericRepository<DataSource, Guid> dataSourceRepository,
        IGenericRepository<MappedFieldsRow, Guid> mappedFieldsRepository,
        IGenericRepository<FinalExportResult, Guid> exportResultRepository,
        IGenericRepository<FinalExportSettings, Guid> exportSettingsRepository,
        IJobEventPublisher jobEventPublisher,
        IExportDataWriterStrategyFactory exportDataWriterStrategyFactory,
        IExportFilePathHelper exportFilePathHelper,
        IGenericRepository<MatchDefinitionCollection, Guid> matchDefinitionRepository,
        ILogger<FinalExportService> logger)
    {
        _dataStore = dataStore;
        _dataSourceRepository = dataSourceRepository;
        _mappedFieldsRepository = mappedFieldsRepository;
        _exportResultRepository = exportResultRepository;
        _exportSettingsRepository = exportSettingsRepository;
        _jobEventPublisher = jobEventPublisher;
        _exportDataWriterStrategyFactory = exportDataWriterStrategyFactory;
        _logger = logger;
        _exportFilePathHelper = exportFilePathHelper;
        _matchDefinitionRepository = matchDefinitionRepository;
    }

    #region Execute Export

    public async Task<FinalExportResult> ExecuteExportAsync(
        Guid projectId,
        FinalExportSettings settings,
        BaseConnectionInfo? connectionInfo = null,  // NEW PARAMETER
        int? maxGroups = null,
        ICommandContext? context = null,
        CancellationToken cancellationToken = default)
    {
        var startTime = DateTime.UtcNow;
        var stepId = context?.StepId ?? Guid.Empty;
        var isPreview = maxGroups.HasValue;
        var isExportToDestination = connectionInfo != null;

        _logger.LogInformation(
       "Starting {Mode} export for project {ProjectId} with ExportAction={ExportAction}, SelectedAction={SelectedAction}, MaxGroups={MaxGroups}, Destination={Destination}",
       isPreview ? "preview" : "full",
       projectId,
       settings.ExportAction,
       settings.SelectedAction,
       maxGroups,
       connectionInfo?.Type.ToString() ?? "LiteDB");

        IExportDataStrategy? writer = null;
        string? tempFilePath = null;
        string? finalFilePath = null;
        bool exportSucceeded = false;

        try
        {
            // Step 1: Load data sources (same order as matching - no OrderBy)
            var dataSources = (await _dataSourceRepository.QueryAsync(
                ds => ds.ProjectId == projectId,
                Constants.Collections.DataSources)).ToList();

            if (!dataSources.Any())
                throw new InvalidOperationException("No data sources found for project");

            // Step 2: Initialize DataSetsToInclude defaults
            if (settings.DataSetsToInclude.Count == 0)
            {
                foreach (var ds in dataSources)
                    settings.DataSetsToInclude[ds.Id] = true;
            }

            // Step 3: Load mapped fields for column projection
            var mappedFieldsRow = (await _mappedFieldsRepository.QueryAsync(
                mf => mf.ProjectId == projectId,
                Constants.Collections.MappedFieldRows)).FirstOrDefault();

            // Step 4: Validate fields exist
            var missingFields = ValidateFieldsExist(mappedFieldsRow, dataSources, settings);
            if (missingFields.Any())
            {
                _logger.LogWarning("Missing fields detected: {Fields}",
                    string.Join(", ", missingFields.Select(f => $"{f.DataSourceName}.{f.FieldName}")));
            }

            var matchDefinitionCollections = await _matchDefinitionRepository.QueryAsync(
              mdc => mdc.ProjectId == projectId,
              Constants.Collections.MatchDefinitionCollection);

            // Step 5: Setup progress tracking (only for full export)
            IStepProgressTracker? loadGroupsStep = null;
            IStepProgressTracker? processStep = null;
            IStepProgressTracker? finalizeStep = null;

            if (!isPreview && context != null)
            {
                loadGroupsStep = _jobEventPublisher.CreateStepTracker(stepId, "Loading Groups", 1, 3);
                processStep = _jobEventPublisher.CreateStepTracker(stepId, "Processing Records", 2, 3);
                finalizeStep = _jobEventPublisher.CreateStepTracker(stepId, "Finalizing", 3, 3);
            }

            // Step 6: Build absolute index map (same order as matching)
            var absoluteStartIndexes = BuildAbsoluteStartIndexes(dataSources);

            // Step 6b: Pre-compute mapped physical field names per data source
            // Used by LoadGroupsAsync to extract only export-relevant fields from group records
            var mappedFieldNamesByDataSource = BuildMappedFieldNamesByDataSource(mappedFieldsRow, dataSources);

            // Step 7: Load groups - read stored data (limited for preview)
            if (loadGroupsStep != null)
                await loadGroupsStep.StartStepAsync(0, cancellationToken);

            var groupsLookup = await LoadGroupsAsync(
                projectId, dataSources, absoluteStartIndexes, mappedFieldNamesByDataSource, maxGroups, cancellationToken);

            if (loadGroupsStep != null)
                await loadGroupsStep.CompleteStepAsync($"Loaded {groupsLookup.GroupCount} groups, {groupsLookup.Records.Count} grouped records");

            // Step 8: Discover score columns from actual stored data
            var scoreColumns = DiscoverScoreColumns(matchDefinitionCollections.FirstOrDefault());
            _logger.LogInformation("Discovered {Count} score columns", scoreColumns.Count);


            if (processStep != null)
                await processStep.StartStepAsync(dataSources.Count, cancellationToken);

            // Step 9: Setup writer
            var exportCollectionName = isPreview
                 ? GetPreviewCollectionName(projectId)
                 : GetExportCollectionName(projectId);

            string? generatedFilePath = null;

            if (connectionInfo != null && _exportFilePathHelper.IsFileBasedExport(connectionInfo.Type))
            {
                // Check if user provided explicit path (override)
                bool hasUserPath = connectionInfo.Parameters.TryGetValue("FilePath", out var userPath)
                                   && !string.IsNullOrWhiteSpace(userPath);

                if (hasUserPath)
                {
                    // User override - use their path directly (no temp)
                    finalFilePath = userPath;
                    writer = _exportDataWriterStrategyFactory.GetStrategy(connectionInfo);

                    _logger.LogInformation(
                        "Using user-specified export path: {Path}", finalFilePath);
                }
                else
                {
                    // No user path - use temp → finalize pattern
                    tempFilePath = _exportFilePathHelper.GetTemporaryExportPath(projectId, connectionInfo.Type);

                    var tempConnectionInfo = new BaseConnectionInfo
                    {
                        Type = connectionInfo.Type,
                        Parameters = new Dictionary<string, string>(connectionInfo.Parameters)
                        {
                            ["FilePath"] = tempFilePath
                        }
                    };

                    writer = _exportDataWriterStrategyFactory.GetStrategy(tempConnectionInfo);

                    _logger.LogInformation(
                        "Using temporary file path for export: {TempPath}", tempFilePath);
                }
            }
            else if (connectionInfo != null)
            {
                // External export (SQL Server, Excel, etc.)
                writer = _exportDataWriterStrategyFactory.GetStrategy(connectionInfo);
                _logger.LogInformation("Using external writer: {Type}", connectionInfo.Type);
            }
            else
            {
                // LiteDB preview or internal storage
                writer = _exportDataWriterStrategyFactory.CreatePreviewWriter(exportCollectionName);
                _logger.LogInformation("Using LiteDB writer for collection: {Collection}", exportCollectionName);
            }

            //Old await _dataStore.DeleteCollection(exportCollectionName);

            // Step 10: Build export schema (headers)
            var exportHeaders = BuildExportHeaders(settings, scoreColumns, mappedFieldsRow, dataSources);
            var schema = ExportSchemaBuilder.BuildSchema(
            settings,
            scoreColumns,
            mappedFieldsRow,
            dataSources);

            await writer.InitializeAsync(schema, cancellationToken);

            _logger.LogInformation(
                "Export schema built with {Columns} columns, types: {Types}",
                schema.Columns.Count,
                string.Join(", ", schema.Columns.Take(5).Select(c => $"{c.Name}:{c.DataType}")));

            var statistics = new FinalExportStatistics
            {
                TotalGroupsAvailable = groupsLookup.TotalGroupsAvailable,
                IsLimited = isPreview && groupsLookup.GroupCount < groupsLookup.TotalGroupsAvailable
            };

            var exportBatch = new List<IDictionary<string, object>>(BatchSize);
            long progressCount = 0;
            var maxRecords = isPreview ? MaxPreviewRecords : long.MaxValue;

            // Step 11: Process each data source
            foreach (var dataSource in dataSources)
            {
                if (!settings.DataSetsToInclude.GetValueOrDefault(dataSource.Id, true))
                {
                    _logger.LogInformation("Skipping excluded data source: {Name}", dataSource.Name);
                    continue;
                }

                var absoluteStart = absoluteStartIndexes[dataSource.Id];
                var sourceCollection = await GetSourceCollectionName(dataSource);
                long dsExported = 0;

                _logger.LogInformation("Processing data source {Name} from {Collection}", dataSource.Name, sourceCollection);

                long streamIndex = 0;
                await foreach (var sourceRecord in _dataStore.StreamDataAsync(sourceCollection, cancellationToken))
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    // Check max records limit for preview
                    if (statistics.RecordsExported >= maxRecords)
                        break;

                    statistics.TotalRecordsProcessed++;
                    progressCount++;

                    // Get row number with fallback to stream index
                    var rowNumber = GetRowNumber(sourceRecord, streamIndex);
                    var absoluteIndex = absoluteStart + rowNumber;
                    streamIndex++;

                    // Check if record is in a group
                    var hasGroupRecord = groupsLookup.Records.TryGetValue(absoluteIndex, out var groupRecord);

                    // For preview mode, skip records not in sampled groups (unless non-duplicate)
                    if (isPreview && !hasGroupRecord && !groupsLookup.NonGroupedIndexesInScope.Contains(absoluteIndex))
                    {
                        // Include some non-grouped records for preview
                        if (statistics.UniqueRecordsExported < 50)
                        {
                            groupsLookup.NonGroupedIndexesInScope.Add(absoluteIndex);
                        }
                        else
                        {
                            continue;
                        }
                    }

                    var isDuplicate = hasGroupRecord && !(groupRecord!.IsNotDuplicate);

                    // Apply filters (SelectedAction + ExportAction)
                    if (!PassesFilters(settings, hasGroupRecord, isDuplicate, groupRecord))
                    {
                        statistics.RecordsSkipped++;
                        continue;
                    }

                    // Build export row - pure projection from stored data
                    var exportRow = ProjectToExportRow(
                        sourceRecord,
                        dataSource,
                        rowNumber,
                        hasGroupRecord,
                        isDuplicate,
                        groupRecord,
                        settings,
                        scoreColumns,
                        mappedFieldsRow);

                    exportBatch.Add(exportRow);
                    statistics.RecordsExported++;
                    dsExported++;

                    // Update statistics
                    if (isDuplicate)
                        statistics.DuplicateRecordsExported++;
                    else
                        statistics.UniqueRecordsExported++;

                    if (groupRecord?.IsMaster == true)
                        statistics.MasterRecordsExported++;

                    if (groupRecord?.IsSelected == true)  // ADD THIS
                        statistics.SelectedRecordsExported++;

                    if (groupRecord?.IsCrossReference == true)
                        statistics.CrossReferenceRecordsExported++;

                    // Flush batch
                    if (exportBatch.Count >= BatchSize)
                    {
                        await writer.WriteBatchAsync(exportBatch, cancellationToken);
                        exportBatch.Clear();
                    }

                    // Report progress (full export only)
                    if (!isPreview && progressCount % ProgressReportInterval == 0 && processStep != null)
                    {
                        await processStep.UpdateProgressAsync(ProgressReportInterval, $"Processed {progressCount} records");
                    }
                }

                statistics.RecordsByDataSource[dataSource.Id] = dsExported;

                if (processStep != null)
                    await processStep.UpdateProgressAsync(1, $"Completed {dataSource.Name}: {dsExported} records exported");

                // Check max records limit for preview
                if (statistics.RecordsExported >= maxRecords)
                    break;
            }

            // Final batch flush
            if (exportBatch.Count > 0)
            {
                await writer.WriteBatchAsync(exportBatch, cancellationToken);
            }

            var writeResult = await writer.FinalizeAsync(cancellationToken);

            if (processStep != null)
                await processStep.CompleteStepAsync($"Processed {statistics.TotalRecordsProcessed} records, exported {statistics.RecordsExported}");

            exportSucceeded = true;
            // Step 11: Finalize
            if (finalizeStep != null)
                await finalizeStep.StartStepAsync(1, cancellationToken);

            statistics.GroupsProcessed = groupsLookup.GroupCount;
            statistics.ProcessingTime = DateTime.UtcNow - startTime;

            if (!string.IsNullOrEmpty(tempFilePath))
            {
                // Move temp to final location (cleaning up old files)
                finalFilePath = _exportFilePathHelper.FinalizeExportFile(
                    tempFilePath, projectId, connectionInfo!.Type);

                _logger.LogInformation(
                    "Export file finalized: {TempPath} -> {FinalPath}",
                    tempFilePath, finalFilePath);
            }
            else if (writeResult.FilePath != null)
            {
                // User-provided path or LiteDB (no temp)
                finalFilePath = writeResult.FilePath;
            }

            var result = new FinalExportResult
            {
                ProjectId = projectId,
                StepJobId = stepId,
                CollectionName = exportCollectionName,
                ExportFilePath = finalFilePath,  // NEW: For file exports
                SettingsHash = ComputeSettingsHash(settings),
                ExportAction = settings.ExportAction,
                SelectedAction = settings.SelectedAction,
                IsPreview = isPreview,
                Statistics = statistics,
                CreatedAt = DateTime.UtcNow
            };

            await _exportResultRepository.DeleteAllAsync(x => x.ProjectId == projectId, Constants.Collections.FinalExportResults);
            // Save export metadata
            await _exportResultRepository.InsertAsync(result, Constants.Collections.FinalExportResults);

            if (finalizeStep != null)
                await finalizeStep.CompleteStepAsync("Export completed");

            _logger.LogInformation(
            "{Mode} export completed: {Exported}/{Total} records in {Time}ms. Groups={Groups}/{TotalGroups}, Duplicates={Dups}, Unique={Unique}, Destination={Dest}",
            isPreview ? "Preview" : "Full",
            statistics.RecordsExported,
            statistics.TotalRecordsProcessed,
            statistics.ProcessingTime.TotalMilliseconds,
            statistics.GroupsProcessed,
            statistics.TotalGroupsAvailable,
            statistics.DuplicateRecordsExported,
            statistics.UniqueRecordsExported,
            writer.Name);

            return result;
        }
        catch (Exception ex)
        {
            exportSucceeded = false;
            _logger.LogError(ex, "Export failed for project {ProjectId}", projectId);
            throw;
        }
        finally
        {
            writer?.Dispose();

            if (!exportSucceeded && !string.IsNullOrEmpty(tempFilePath))
            {
                _exportFilePathHelper.CleanupTemporaryFile(tempFilePath);
                _logger.LogWarning("Export failed - cleaned up temp file: {Path}", tempFilePath);
            }
        }
    }

    #endregion

    #region Load Groups

    private async Task<GroupsLookup> LoadGroupsAsync(
        Guid projectId,
        List<DataSource> dataSources,
        Dictionary<Guid, long> absoluteStartIndexes,
        Dictionary<string, HashSet<string>> mappedFieldNamesByDataSource,
        int? maxGroups,
        CancellationToken cancellationToken)
    {
        var lookup = new GroupsLookup();
        var groupsCollection = GetGroupsCollectionName(projectId);

        _logger.LogInformation("Loading groups from {Collection}, limit={Limit}",
            groupsCollection, maxGroups?.ToString() ?? "none");

        int groupsLoaded = 0;

        await foreach (var groupDoc in _dataStore.StreamDataAsync(groupsCollection, cancellationToken))
        {
            lookup.TotalGroupsAvailable++;

            // For preview, only process up to maxGroups
            if (maxGroups.HasValue && groupsLoaded >= maxGroups.Value)
                continue;

            var groupId = Convert.ToInt32(groupDoc["GroupId"]);
            var records = groupDoc["Records"] as IList<object>;

            if (records == null || records.Count == 0)
                continue;

            lookup.GroupCount++;
            groupsLoaded++;
            var groupDataSources = new HashSet<Guid>();

            foreach (var recordObj in records.OfType<IDictionary<string, object>>())
            {
                var dataSourceId = GetDataSourceId(recordObj);
                if (dataSourceId == Guid.Empty)
                    continue;

                if (!absoluteStartIndexes.TryGetValue(dataSourceId, out var startIndex))
                    continue;

                var rowNumber = GetRowNumberFromGroupRecord(recordObj);
                var absoluteIndex = startIndex + rowNumber;

                // Create stored group record - direct reads, no calculation
                var dsName = dataSources.FirstOrDefault(ds => ds.Id == dataSourceId)?.Name ?? "";
                var storedRecord = new StoredGroupRecord
                {
                    GroupId = groupId,
                    DataSourceId = dataSourceId,
                    DataSourceName = dsName,
                    RowNumber = rowNumber,

                    // Read stored flags directly
                    IsMaster = GetBool(recordObj, RecordSystemFieldNames.IsMasterRecord),
                    IsSelected = GetBool(recordObj, RecordSystemFieldNames.Selected),
                    IsNotDuplicate = GetBool(recordObj, RecordSystemFieldNames.NotDuplicate),
                    MaxScore = GetDouble(recordObj, RecordSystemFieldNames.GroupAvgScore),

                    // Read score data for flattening
                    ScoresByDefinition = ExtractScoresByDefinition(recordObj),
                    MatchedDefinitions = ExtractMatchedDefinitions(recordObj),

                    // Extract data field values (post overwrite/master rules) - mapped fields only
                    DataFields = ExtractDataFields(recordObj, dsName, mappedFieldNamesByDataSource)
                };

                // Store in lookup (warn on duplicate)
                if (lookup.Records.ContainsKey(absoluteIndex))
                {
                    _logger.LogWarning(
                        "Duplicate absolute index {Index} in groups. Existing GroupId={Existing}, New GroupId={New}",
                        absoluteIndex, lookup.Records[absoluteIndex].GroupId, groupId);
                }
                else
                {
                    lookup.Records[absoluteIndex] = storedRecord;
                }

                groupDataSources.Add(dataSourceId);
            }

            // Mark as cross-reference if group spans 2+ data sources
            if (groupDataSources.Count >= 2)
            {
                lookup.CrossReferenceGroups.Add(groupId);
            }
        }

        // Set cross-reference flag on individual records
        foreach (var record in lookup.Records.Values)
        {
            record.IsCrossReference = lookup.CrossReferenceGroups.Contains(record.GroupId);
        }

        _logger.LogInformation(
            "Loaded {Groups}/{Total} groups, {Records} grouped records, {CrossRef} cross-reference groups",
            lookup.GroupCount, lookup.TotalGroupsAvailable, lookup.Records.Count, lookup.CrossReferenceGroups.Count);

        return lookup;
    }

    private static Dictionary<string, object> ExtractDataFields(
        IDictionary<string, object> record,
        string dataSourceName,
        Dictionary<string, HashSet<string>> mappedFieldNamesByDataSource)
    {
        var result = new Dictionary<string, object>();

        if (mappedFieldNamesByDataSource.TryGetValue(dataSourceName, out var fieldNames) && fieldNames.Count > 0)
        {
            // Extract only mapped fields
            foreach (var fieldName in fieldNames)
            {
                if (record.TryGetValue(fieldName, out var value))
                    result[fieldName] = value;
            }
        }
        else
        {
            // Fallback: no mapped fields configured — extract all non-system fields
            foreach (var kvp in record)
            {
                if (!kvp.Key.StartsWith("_") && kvp.Key != "_id")
                    result[kvp.Key] = kvp.Value;
            }
        }

        return result;
    }

    private Dictionary<int, StoredDefinitionScores> ExtractScoresByDefinition(IDictionary<string, object> record)
    {
        var result = new Dictionary<int, StoredDefinitionScores>();

        if (!record.TryGetValue(RecordSystemFieldNames.GroupMatchDetails, out var detailsObj) ||
            detailsObj is not IList<object> details)
            return result;

        foreach (var detail in details.OfType<IDictionary<string, object>>())
        {
            if (!detail.TryGetValue("ScoresByDefinition", out var scoresObj) ||
                scoresObj is not IDictionary<string, object> scoresByDef)
                continue;

            foreach (var defKvp in scoresByDef)
            {
                if (!int.TryParse(defKvp.Key, out var defIndex))
                    continue;

                if (defKvp.Value is not IDictionary<string, object> scoreDetail)
                    continue;

                var stored = new StoredDefinitionScores
                {
                    DefinitionIndex = defIndex,
                    FinalScore = GetDouble(scoreDetail, "FinalScore"),
                    WeightedScore = GetDouble(scoreDetail, "WeightedScore")
                };

                // Read FieldScores exactly as stored
                if (scoreDetail.TryGetValue("FieldScores", out var fsObj) &&
                    fsObj is IDictionary<string, object> fieldScores)
                {
                    foreach (var fs in fieldScores)
                    {
                        stored.FieldScores[fs.Key] = Convert.ToDouble(fs.Value);
                    }
                }

                if (!result.ContainsKey(defIndex))
                    result[defIndex] = stored;
            }
        }

        return result;
    }

    private string ExtractMatchedDefinitions(IDictionary<string, object> record)
    {
        var indices = new HashSet<int>();

        if (record.TryGetValue(RecordSystemFieldNames.GroupMatchDetails, out var detailsObj) &&
            detailsObj is IList<object> details)
        {
            foreach (var detail in details.OfType<IDictionary<string, object>>())
            {
                if (detail.TryGetValue("MatchDefinitionIndices", out var indObj) &&
                    indObj is IList<object> indList)
                {
                    foreach (var idx in indList)
                    {
                        indices.Add(Convert.ToInt32(idx) + 1); // 1-based for display
                    }
                }
            }
        }

        // Legacy format: "123" for definitions 1, 2, 3
        return string.Join("", indices.OrderBy(x => x));
    }

    #endregion

    #region Discover Score Columns

    private List<ScoreColumn> DiscoverScoreColumns(MatchDefinitionCollection matchDefinitionCollection)
    {
        var columns = new List<ScoreColumn>();

        if (matchDefinitionCollection?.Definitions == null || matchDefinitionCollection.Definitions.Count == 0)
            return columns;

        // Group by UIDefinitionIndex — multiple MatchDefinition entries can share the same
        // UIDefinitionIndex (one per DataSourcePair), but their Criteria are identical.
        // UIDefinitionIndex is 0-based and maps directly to the "0","1" keys in ScoresByDefinition.
        var definitionsByIndex = matchDefinitionCollection.Definitions
            .GroupBy(d => d.UIDefinitionIndex)
            .OrderBy(g => g.Key)
            .ToList();

        var defCount = definitionsByIndex.Count;

        if (defCount == 0)
            return columns;

        // 1. Single overall "Score" column — always present
        columns.Add(new ScoreColumn
        {
            ColumnName = "Score",
            DefinitionIndex = -1,
            ColumnType = ScoreColumnType.MaxScore
        });

        // 2. Per-definition columns — one group per UIDefinitionIndex
        foreach (var defGroup in definitionsByIndex)
        {
            var defIndex = defGroup.Key;          // 0-based — matches ScoresByDefinition key "0","1"
            var defNumber = defIndex + 1;         // 1-based — for display column names only

            // All entries in this group have identical Criteria (differ only by DataSourcePair)
            var criteria = defGroup.First().Criteria;

            // 2a. Per-criteria field score columns
            foreach (var criterion in criteria)
            {
                // FieldKey must exactly match what the engine stores in FieldScores.
                // From real data: "City_City", "Contact First Name_Contact First Name"
                // Format: {FieldMappings[0].FieldName}_{FieldMappings[1].FieldName}
                // For same-source matching both sides share the same field name.
                var fieldKey = BuildFieldKey(criterion);

                // Display name is just the left-side field name (no duplication)
                var displayName = criterion.FieldMappings.FirstOrDefault()?.FieldName
                                  ?? criterion.FieldName
                                  ?? $"criterion_{defIndex}";

                // Column name mirrors legacy: "City score" (1 def) or "City score 1" (multi-def)
                var colName = defCount > 1
                    ? $"{displayName} score {defNumber}"
                    : $"{displayName} score";

                columns.Add(new ScoreColumn
                {
                    ColumnName = colName,
                    DefinitionIndex = defIndex,
                    FieldKey = fieldKey,
                    ColumnType = ScoreColumnType.FieldScore
                });
            }

            // 2b. Total score for this definition — e.g. "score 1", "score 2"
            columns.Add(new ScoreColumn
            {
                ColumnName = $"score {defNumber}",
                DefinitionIndex = defIndex,
                ColumnType = ScoreColumnType.TotalScore
            });

            // 2c. Threshold indicator — e.g. "s 1", "s 2"
            columns.Add(new ScoreColumn
            {
                ColumnName = $"s {defNumber}",
                DefinitionIndex = defIndex,
                ColumnType = ScoreColumnType.ThresholdIndicator
            });
        }

        _logger.LogInformation(
            "Built {Count} score columns from {Defs} match definitions (UIDefinitionIndices: {Indices})",
            columns.Count,
            defCount,
            string.Join(", ", definitionsByIndex.Select(g => g.Key)));

        return columns;
    }

    /// <summary>
    /// Builds the FieldScores key exactly as the matching engine stores it.
    /// Format from stored data: "City_City", "Contact First Name_Contact First Name"
    /// i.e. {leftFieldName}_{rightFieldName}
    /// </summary>
    private string BuildFieldKey(MatchCriteria criterion)
    {
        var mappings = criterion.FieldMappings;

        if (mappings == null || mappings.Count == 0)
            return criterion.FieldName ?? "";   // last resort fallback

        var leftField = mappings[0].FieldName;

        // Two mappings = cross-field (potentially different names e.g. "FirstName_First Name")
        // One mapping = same field both sides (e.g. "City_City")
        var rightField = mappings.Count >= 2
            ? mappings[1].FieldName
            : mappings[0].FieldName;

        return $"{leftField}_{rightField}";
    }

    #endregion

    #region Filters

    private bool PassesFilters(
        FinalExportSettings settings,
        bool hasGroupRecord,
        bool isDuplicate,
        StoredGroupRecord? groupRecord)
    {
        // Apply SelectedAction filter first
        var isSelected = groupRecord?.IsSelected ?? false;
        switch (settings.SelectedAction)
        {
            case SelectedAction.SuppressSelected when isSelected:
                return false;
            case SelectedAction.ShowSelectedOnly when !isSelected:
                return false;
        }

        // Apply ExportAction filter
        return settings.ExportAction switch
        {
            ExportAction.AllRecordsAndFlagDuplicates => true,
            ExportAction.SuppressAllDuplicateRecords => !isDuplicate,
            ExportAction.NonDupsAndMasterRecordRemaining => !isDuplicate || (groupRecord?.IsMaster ?? false),
            ExportAction.DuplicatesOnly => isDuplicate,
            ExportAction.CrossReference => isDuplicate && (groupRecord?.IsCrossReference ?? false),
            _ => true
        };
    }

    #endregion

    #region Project to Export Row

    private IDictionary<string, object> ProjectToExportRow(
        IDictionary<string, object> sourceRecord,
        DataSource dataSource,
        long rowNumber,
        bool hasGroupRecord,
        bool isDuplicate,
        StoredGroupRecord? groupRecord,
        FinalExportSettings settings,
        List<ScoreColumn> scoreColumns,
        MappedFieldsRow? mappedFieldsRow)
    {
        var row = new Dictionary<string, object>();

        // 1. System fields - direct projection from stored data
        if (settings.IncludeSystemFields)
        {
            row[ExportFieldNames.GroupId] = groupRecord?.GroupId ?? 0;
            row[ExportFieldNames.DataSourceName] = dataSource.Name;
            row[ExportFieldNames.Record] = rowNumber;
            row[ExportFieldNames.Master] = groupRecord?.IsMaster ?? false;
            row[ExportFieldNames.Selected] = groupRecord?.IsSelected ?? false;
            row[ExportFieldNames.NotDuplicate] = groupRecord?.IsNotDuplicate ?? false;
            row[ExportFieldNames.MdHits] = groupRecord?.MatchedDefinitions ?? "";
        }

        // 2. Score fields - flatten stored nested data (no calculation)
        if (settings.IncludeScoreFields && scoreColumns.Count > 0)
        {
            foreach (var col in scoreColumns)
            {
                object value = col.ColumnType switch
                {
                    ScoreColumnType.MaxScore => (groupRecord?.MaxScore ?? 0) * 100,
                    ScoreColumnType.FieldScore => GetFieldScore(groupRecord, col.DefinitionIndex, col.FieldKey),
                    ScoreColumnType.TotalScore => GetTotalScore(groupRecord, col.DefinitionIndex),
                    ScoreColumnType.ThresholdIndicator => GetThresholdIndicator(groupRecord, col.DefinitionIndex),
                    _ => 0.0
                };
                row[col.ColumnName] = value;
            }
        }

        // 3. Data fields
        // In-group records: use transformed values (post master/overwrite rules) from group document
        // Non-grouped records: use raw/cleansed source record
        if (hasGroupRecord && groupRecord!.DataFields.Count > 0)
            AddDataFields(row, groupRecord.DataFields, dataSource.Name, mappedFieldsRow);
        else
            AddDataFields(row, sourceRecord, dataSource.Name, mappedFieldsRow);

        return row;
    }

    private double GetFieldScore(StoredGroupRecord? record, int defIndex, string fieldKey)
    {
        if (record?.ScoresByDefinition == null)
            return 0;
        if (!record.ScoresByDefinition.TryGetValue(defIndex, out var scores))
            return 0;
        if (!scores.FieldScores.TryGetValue(fieldKey, out var score))
            return 0;

        return score * 100; // Convert to percentage for display
    }

    private double GetTotalScore(StoredGroupRecord? record, int defIndex)
    {
        if (record?.ScoresByDefinition == null)
            return 0;
        if (!record.ScoresByDefinition.TryGetValue(defIndex, out var scores))
            return 0;

        return scores.FinalScore * 100; // Convert to percentage
    }

    private bool GetThresholdIndicator(StoredGroupRecord? record, int defIndex)
    {
        // No group data at all — non-grouped record
        if (record?.ScoresByDefinition == null || record.ScoresByDefinition.Count == 0)
            return false;

        // This specific definition didn't produce any score for this record —
        // it cannot be the winner, no need to compute max
        if (!record.ScoresByDefinition.TryGetValue(defIndex, out var scores))
            return false;

        // This definition matched but scored zero — not a real match
        if (scores.FinalScore <= 0)
            return false;

        // Mirror legacy DefineMatchDefinitionWithMaxScore EXACTLY:
        // Iterate definitions in ascending index order (0, 1, 2...) — same as legacy
        // for (matchDefId = 0; matchDefId < Count; matchDefId++)
        // Use strict > so on equal scores the LOWER index wins and is never displaced.
        // Definitions absent from ScoresByDefinition are treated as score=0 implicitly
        // because we only stored definitions that actually matched (score > 0),
        // and Double.MinValue as starting max ensures any real score beats nothing.
        double runningMax = double.MinValue;
        int winningDefIndex = -1;

        foreach (var kvp in record.ScoresByDefinition.OrderBy(k => k.Key))
        {
            if (kvp.Value.FinalScore > runningMax) // strict > mirrors legacy
            {
                runningMax = kvp.Value.FinalScore;
                winningDefIndex = kvp.Key;
            }
        }

        return defIndex == winningDefIndex;
    }

    private void AddDataFields(
        Dictionary<string, object> row,
        IDictionary<string, object> sourceRecord,
        string dataSourceName,
        MappedFieldsRow? mappedFieldsRow)
    {
        var includedFields = mappedFieldsRow?.MappedFields?.Where(mf => mf.Include).ToList();

        if (includedFields != null && includedFields.Any())
        {
            // Use mapped fields configuration
            foreach (var mappedField in includedFields)
            {
                var canonicalHeader = mappedField.GetAllFields()
                .Where(f => f != null && !string.IsNullOrEmpty(f.FieldName))
                .Select(f => f.FieldName)
                .FirstOrDefault();

                if (string.IsNullOrEmpty(canonicalHeader))
                    continue;

                var fieldMapping = mappedField[dataSourceName];
                if (fieldMapping == null)
                    continue;

                if (sourceRecord.TryGetValue(fieldMapping.FieldName, out var value))
                {
                    row[canonicalHeader] = value;
                }
                else if (!row.ContainsKey(canonicalHeader))
                {
                    row[canonicalHeader] = null;  // Ensure column exists even if null
                }
            }
        }
        else
        {
            // Fallback: include all non-internal fields
            foreach (var kvp in sourceRecord)
            {
                if (!kvp.Key.StartsWith("_") && kvp.Key != "_id" && !row.ContainsKey(kvp.Key))
                {
                    row[kvp.Key] = kvp.Value;
                }
            }
        }
    }

    #endregion

    #region Get Export Data

    public async Task<(IEnumerable<IDictionary<string, object>> Data, int TotalCount)> GetExportDataAsync(
        Guid projectId,
        bool isPreview,
        int pageNumber = 1,
        int pageSize = 100,
        string? filterText = null,
        string? sortColumn = null,
        bool ascending = true,
        string? filters = null,
        CancellationToken cancellationToken = default)
    {
        var collectionName = isPreview
            ? GetPreviewCollectionName(projectId)
            : GetExportCollectionName(projectId);

        var hasFiltersOrSort = !string.IsNullOrEmpty(filterText) ||
                               !string.IsNullOrEmpty(sortColumn) ||
                               !string.IsNullOrEmpty(filters);

        if (hasFiltersOrSort)
        {
            return await _dataStore.GetPagedJobWithSortingAndFilteringDataAsync(
                collectionName,
                pageNumber,
                pageSize,
                filterText,
                sortColumn,
                ascending,
                filters);
        }

        return await _dataStore.GetPagedDataAsync(collectionName, pageNumber, pageSize);
    }

    #endregion

    #region Validation

    public async Task<ExportValidationResult> ValidateExportAsync(
        Guid projectId,
        CancellationToken cancellationToken = default)
    {
        var result = new ExportValidationResult { IsValid = true };

        // Check data sources exist
        var dataSources = (await _dataSourceRepository.QueryAsync(
            ds => ds.ProjectId == projectId,
            Constants.Collections.DataSources)).ToList();

        if (!dataSources.Any())
        {
            result.IsValid = false;
            result.ValidationErrors.Add("No data sources found for project.");
            return result;
        }

        // Check groups collection exists
        var groupsCollection = GetGroupsCollectionName(projectId);
        var groupsCollectionExists = await _dataStore.CollectionExistsAsync(groupsCollection);

        if (!groupsCollectionExists)
        {
            result.HasResults = false;
            _logger.LogInformation("No match results found for project {ProjectId}. All records will be exported as non-duplicates.", projectId);
        }
        else
        {
            // Collection exists - matching was run
            result.HasResults = true;

            var (_, groupCount) = await _dataStore.GetPagedDataAsync(groupsCollection, 1, 1);
            if (groupCount == 0)
            {
                _logger.LogInformation("No groups found. All records will be exported as non-duplicates.");
            }
        }

        // Check if saved settings exist
        var savedSettings = (await _exportSettingsRepository.QueryAsync(
            s => s.ProjectId == projectId,
            Constants.Collections.FinalExportSettings))
            .FirstOrDefault();

        result.HasSavedSettings = savedSettings != null;

        // Check if export preview exists
        var previewCollection = GetPreviewCollectionName(projectId);
        try
        {
            var (_, previewCount) = await _dataStore.GetPagedDataAsync(previewCollection, 1, 1);
            result.HasPreview = previewCount > 0;
        }
        catch
        {
            result.HasPreview = false;
        }

        result.ResultsInSync = result.HasResults;
        return result;
    }

    private List<MissingFieldInfo> ValidateFieldsExist(
        MappedFieldsRow? mappedFieldsRow,
        List<DataSource> dataSources,
        FinalExportSettings settings)
    {
        var missing = new List<MissingFieldInfo>();

        if (mappedFieldsRow?.MappedFields == null)
            return missing;

        foreach (var mappedField in mappedFieldsRow.MappedFields.Where(mf => mf.Include))
        {
            foreach (var ds in dataSources.Where(ds => settings.DataSetsToInclude.GetValueOrDefault(ds.Id, true)))
            {
                var fieldMapping = mappedField[ds.Name];
                if (fieldMapping == null)
                    continue;

                var exists = ds.Configuration?.ColumnMappings?.ContainsKey(fieldMapping.FieldName) ?? false;
                if (!exists)
                {
                    missing.Add(new MissingFieldInfo
                    {
                        DataSourceId = ds.Id,
                        DataSourceName = ds.Name,
                        FieldName = fieldMapping.FieldName
                    });
                }
            }
        }

        return missing;
    }

    #endregion

    #region Helpers

    /// <summary>
    /// Build ordered headers list for export schema
    /// </summary>
    private List<string> BuildExportHeaders(
        FinalExportSettings settings,
        List<ScoreColumn> scoreColumns,
        MappedFieldsRow? mappedFieldsRow,
        List<DataSource> dataSources)
    {
        var headers = new List<string>();

        // 1. System fields
        if (settings.IncludeSystemFields)
        {
            headers.AddRange(new[]
            {
            ExportFieldNames.GroupId,
            ExportFieldNames.DataSourceName,
            ExportFieldNames.Record,
            ExportFieldNames.Master,
            ExportFieldNames.Selected,
            ExportFieldNames.NotDuplicate,
            ExportFieldNames.MdHits,
        });
        }

        // 2. Score fields
        if (settings.IncludeScoreFields && scoreColumns.Count > 0)
        {
            headers.AddRange(scoreColumns.Select(c => c.ColumnName));
        }

        // 3. Data fields (from mapped fields)
        var includedFields = mappedFieldsRow?.MappedFields?.Where(mf => mf.Include).ToList();

        if (includedFields != null && includedFields.Any())
        {
            // Get field names from all included data sources
            foreach (var mappedField in includedFields)
            {
                foreach (var ds in dataSources.Where(d => settings.DataSetsToInclude.GetValueOrDefault(d.Id, true)))
                {
                    var canonicalHeader = mappedField.GetAllFields()
                                           .Where(f => f != null && !string.IsNullOrEmpty(f.FieldName))
                                           .Select(f => f.FieldName)
                                           .FirstOrDefault();
                    if (!string.IsNullOrEmpty(canonicalHeader) && !headers.Contains(canonicalHeader))
                    {
                        headers.Add(canonicalHeader);
                    }
                }
            }
        }

        return headers;
    }
    private Dictionary<Guid, long> BuildAbsoluteStartIndexes(List<DataSource> dataSources)
    {
        var indexes = new Dictionary<Guid, long>();
        long current = 0;

        foreach (var ds in dataSources)
        {
            indexes[ds.Id] = current;
            current += ds.RecordCount;
        }

        return indexes;
    }

    private Dictionary<string, HashSet<string>> BuildMappedFieldNamesByDataSource(
        MappedFieldsRow? mappedFieldsRow,
        List<DataSource> dataSources)
    {
        var result = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);

        var includedFields = mappedFieldsRow?.MappedFields?.Where(mf => mf.Include).ToList();
        if (includedFields == null || !includedFields.Any())
            return result; // empty = fallback to all non-system fields

        foreach (var ds in dataSources)
        {
            var fieldNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var mappedField in includedFields)
            {
                var fieldMapping = mappedField[ds.Name];
                if (fieldMapping != null && !string.IsNullOrEmpty(fieldMapping.FieldName))
                    fieldNames.Add(fieldMapping.FieldName);
            }
            result[ds.Name] = fieldNames;
        }

        return result;
    }

    private Guid GetDataSourceId(IDictionary<string, object> record)
    {
        if (!record.TryGetValue(RecordSystemFieldNames.Metadata, out var metaObj) ||
            metaObj is not IDictionary<string, object> meta)
            return Guid.Empty;

        if (!meta.TryGetValue(MetadataFieldNames.DataSourceId, out var dsId))
            return Guid.Empty;

        return dsId switch
        {
            Guid g => g,
            string s when Guid.TryParse(s, out var parsed) => parsed,
            _ => Guid.Empty
        };
    }

    private long GetRowNumberFromGroupRecord(IDictionary<string, object> record)
    {
        if (!record.TryGetValue(RecordSystemFieldNames.Metadata, out var metaObj) ||
            metaObj is not IDictionary<string, object> meta)
            return 0;

        return GetLong(meta, MetadataFieldNames.RowNumber);
    }

    private long GetRowNumber(IDictionary<string, object> record, long fallbackIndex)
    {
        if (record.TryGetValue(RecordSystemFieldNames.Metadata, out var metaObj) &&
            metaObj is IDictionary<string, object> meta &&
            meta.ContainsKey(MetadataFieldNames.RowNumber))
        {
            return GetLong(meta, MetadataFieldNames.RowNumber);
        }

        return fallbackIndex;
    }

    private bool GetBool(IDictionary<string, object> dict, string key)
    {
        if (!dict.TryGetValue(key, out var v) || v is null)
            return false;

        return v switch
        {
            bool b => b,
            int i => i != 0,
            long l => l != 0,
            string s when bool.TryParse(s, out var sb) => sb,
            _ => false
        };
    }

    private double GetDouble(IDictionary<string, object> dict, string key)
    {
        if (!dict.TryGetValue(key, out var v) || v is null)
            return 0;

        return v switch
        {
            double d => d,
            int i => i,
            long l => l,
            float f => f,
            decimal dec => (double)dec,
            string s when double.TryParse(s, out var sd) => sd,
            _ => 0
        };
    }

    private long GetLong(IDictionary<string, object> dict, string key)
    {
        if (!dict.TryGetValue(key, out var v) || v is null)
            return 0;

        return v switch
        {
            long l => l,
            int i => i,
            double d => (long)d,
            string s when long.TryParse(s, out var sl) => sl,
            _ => 0
        };
    }

    private string ComputeSettingsHash(FinalExportSettings settings)
    {
        var json = JsonSerializer.Serialize(new
        {
            settings.ExportAction,
            settings.SelectedAction,
            DataSets = settings.DataSetsToInclude.OrderBy(k => k.Key),
            settings.IncludeScoreFields,
            settings.IncludeSystemFields
        });

        using var sha = SHA256.Create();
        return Convert.ToBase64String(sha.ComputeHash(Encoding.UTF8.GetBytes(json)));
    }

    private string GetExportCollectionName(Guid projectId) =>
        $"export_{GuidCollectionNameConverter.ToValidCollectionName(projectId)}";

    private string GetPreviewCollectionName(Guid projectId) =>
        $"export_preview_{GuidCollectionNameConverter.ToValidCollectionName(projectId)}";

    private string GetGroupsCollectionName(Guid projectId) =>
        $"groups_{GuidCollectionNameConverter.ToValidCollectionName(projectId)}";

    private async Task<string> GetSourceCollectionName(DataSource ds)
    {
        var cleanseCol = $"{StepType.Cleanse.ToString().ToLower()}_{GuidCollectionNameConverter.ToValidCollectionName(ds.Id)}";
        var importCol = DatasetNames.SnapshotRows(ds.ActiveSnapshotId.GetValueOrDefault()); ;
        var (_, count) = await _dataStore.GetPagedDataAsync(cleanseCol, 1, 1);
        return count > 0 ? cleanseCol : importCol;
    }

    #endregion
}

#region Internal Models

internal class GroupsLookup
{
    public int GroupCount { get; set; }
    public int TotalGroupsAvailable { get; set; }
    public Dictionary<long, StoredGroupRecord> Records { get; } = new();
    public HashSet<int> CrossReferenceGroups { get; } = new();
    public HashSet<long> NonGroupedIndexesInScope { get; } = new();
}

internal class StoredGroupRecord
{
    public int GroupId { get; set; }
    public Guid DataSourceId { get; set; }
    public string DataSourceName { get; set; } = "";
    public long RowNumber { get; set; }

    public bool IsMaster { get; set; }
    public bool IsSelected { get; set; }
    public bool IsNotDuplicate { get; set; }
    public bool IsCrossReference { get; set; }
    public double MaxScore { get; set; }
    public string MatchedDefinitions { get; set; } = "";
    public Dictionary<int, StoredDefinitionScores> ScoresByDefinition { get; set; } = new();

    // Data field values from group record (post overwrite/master rules).
    // Contains only mapped fields to minimize memory. Empty = use sourceRecord fallback.
    public Dictionary<string, object> DataFields { get; set; } = new();
}

internal class StoredDefinitionScores
{
    public int DefinitionIndex { get; set; }
    public double FinalScore { get; set; }
    public double WeightedScore { get; set; }
    public Dictionary<string, double> FieldScores { get; set; } = new();
}

public class ScoreColumn
{
    public string ColumnName { get; set; } = "";
    public int DefinitionIndex { get; set; }
    public string FieldKey { get; set; } = "";
    public ScoreColumnType ColumnType { get; set; }
}

public enum ScoreColumnType
{
    MaxScore,
    FieldScore,
    TotalScore,
    ThresholdIndicator
}

#endregion