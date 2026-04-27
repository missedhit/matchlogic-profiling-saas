using MatchLogic.Application.Interfaces.Scheduling;
using MatchLogic.Application.Common;
using MatchLogic.Application.Extensions;
using MatchLogic.Application.Interfaces.Common;
using MatchLogic.Application.Interfaces.Import;
using MatchLogic.Application.Interfaces.MatchConfiguration;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Application.Interfaces.Security;
using MatchLogic.Domain.Analytics;
using MatchLogic.Domain.CleansingAndStandaradization;
using MatchLogic.Domain.Entities;
using MatchLogic.Domain.Entities.Common;
using MatchLogic.Domain.Export;
using MatchLogic.Domain.FinalExport;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.MatchConfiguration;
using MatchLogic.Domain.MergeAndSurvivorship;
using MatchLogic.Domain.Project;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MatchLogic.Application.Interfaces.Events;

namespace MatchLogic.Application.Features.Project;
public class ProjectService : IProjectService
{
    private readonly IGenericRepository<MatchLogic.Domain.Project.Project, Guid> _projectRepository;
    private readonly IGenericRepository<ProjectRun, Guid> _projectRunRepository;
    private readonly IGenericRepository<DataSource, Guid> _dataSourceRepository;
    private readonly IGenericRepository<Domain.Entities.MatchDefinition, Guid> _matchDefinitionRepository;
    private readonly IGenericRepository<EnhancedCleaningRules, Guid> _cleaningRuleRepository;
    private readonly IGenericRepository<Domain.MergeAndSurvivorship.MergeRules, Guid> _mergeRulesRepository;
    private readonly IGenericRepository<Domain.Export.ExportSettings, Guid> _exportSettingsRepository;
    private readonly IGenericRepository<Domain.Project.StepJob, Guid> _stepJobRepository;
    private readonly IDataStore _dataStore;
    private readonly ILogger<ProjectService> _logger;
    private readonly IJobEventPublisher _jobEventPublisher;
    private readonly IScheduler _scheduler;

    private readonly ISecureParameterHandler _secureParameterHandler;

    private readonly IGenericRepository<MatchingDataSourcePairs, Guid> _pairsRepository;
    private readonly IGenericRepository<MatchDefinitionCollection, Guid> _matchDefinitionCollectionRepository;
    private readonly IFieldOverwriteRuleSetRepository _fieldOverwriteRepository;
    private readonly IMasterRecordRuleSetRepository _masterRecordRepository;
    private readonly IGenericRepository<FinalExportSettings, Guid> _finalExportSettingsRepository;
    private readonly IGenericRepository<MappedFieldsRow, Guid> _mappedFieldRowsRepository;

    private readonly IAutoMappingService _autoMappingService;
    private readonly IFieldMappingService _fieldMappingService;
    private readonly IOAuthTokenService _oAuthTokenService;

    public ProjectService(IGenericRepository<MatchLogic.Domain.Project.Project, Guid> projectRepository,
            IGenericRepository<ProjectRun, Guid> projectRunRepository,
            IGenericRepository<DataSource, Guid> dataSourceRepository,
            IGenericRepository<MatchLogic.Domain.Entities.MatchDefinition, Guid> matchDefinitionRepository,
            IGenericRepository<EnhancedCleaningRules, Guid> cleaningRuleRepository,
            IGenericRepository<Domain.MergeAndSurvivorship.MergeRules, Guid> mergeRulesRepository,
            IGenericRepository<Domain.Export.ExportSettings, Guid> exportSettingsRepository,
            IGenericRepository<Domain.Project.StepJob, Guid> stepJobRepository,
            IDataStore dataStore,            
                IGenericRepository<MatchDefinitionCollection, Guid> matchDefinitionCollectionRepository, // NEW
    IGenericRepository<MatchingDataSourcePairs, Guid> pairsRepository, // NEW    
    IFieldOverwriteRuleSetRepository fieldOverwriteRepository, // NEW
    IMasterRecordRuleSetRepository masterRecordRepository, // NEW
    IGenericRepository<FinalExportSettings, Guid> finalExportSettingsRepository, // NEW
            ISecureParameterHandler secureParameterHandler,
             IAutoMappingService autoMappingService, // NEW
    IFieldMappingService fieldMappingService, // NEW
    IGenericRepository<MappedFieldsRow, Guid> mappedFieldRowsRepository,
            IOAuthTokenService oAuthTokenService, // NEW
    IScheduler scheduler,
    IJobEventPublisher jobEventPublisher,
            ILogger<ProjectService> logger
        )
    {
        _projectRepository = projectRepository;
        _projectRunRepository = projectRunRepository;
        _dataSourceRepository = dataSourceRepository;
        _matchDefinitionRepository = matchDefinitionRepository;
        _cleaningRuleRepository = cleaningRuleRepository;
        _mergeRulesRepository = mergeRulesRepository;
        _exportSettingsRepository = exportSettingsRepository;
        _stepJobRepository = stepJobRepository;        
        _dataStore = dataStore;
        _logger = logger;
        _secureParameterHandler = secureParameterHandler;
        _autoMappingService = autoMappingService; // NEW
        _fieldMappingService = fieldMappingService; // NEW
        _matchDefinitionCollectionRepository = matchDefinitionCollectionRepository; // NEW
        _pairsRepository = pairsRepository; // NEW        
        _fieldOverwriteRepository = fieldOverwriteRepository; // NEW
        _masterRecordRepository = masterRecordRepository; // NEW
        _finalExportSettingsRepository = finalExportSettingsRepository; // NEW
        _mappedFieldRowsRepository = mappedFieldRowsRepository;
        _oAuthTokenService = oAuthTokenService; // NEW
        _scheduler = scheduler;
        _jobEventPublisher = jobEventPublisher;
    }
    public async Task<Domain.Project.Project> CreateProject(string name, string description, int retentionRuns = 2)
    {
        Domain.Project.Project project = new()
        {
            Name = name,
            Description = description,
            RetentionRuns = retentionRuns
        };

        await _projectRepository.InsertAsync(project, Constants.Collections.Projects);

        var projectScoreBand = new ScoreBandCollection()
        {
            ScoreBands = Constants.bands,
            ProjectId = project.Id,
        };
        await _dataStore.InsertAsync(projectScoreBand, Constants.Collections.ScoreBand);

        return project;
    }
    public async Task<Domain.Project.Project> GetProjectById(Guid projectId)
    {
        var project = await _projectRepository.GetByIdAsync(projectId, Constants.Collections.Projects);
        return project ?? throw new InvalidOperationException($"Project with ID {projectId} not found");
    }
    public async Task<Domain.Project.Project> UpdateProject(Guid ProjectId, string name, string description, int retentionRuns = 2)
    {
        var project = await _projectRepository.GetByIdAsync(ProjectId, Constants.Collections.Projects);
        if (project == null)
        {
            throw new InvalidOperationException($"Project with ID {ProjectId} not found");
        }
        project.Name = name;
        project.Description = description;
        project.RetentionRuns = retentionRuns;

        await _projectRepository.UpdateAsync(project, Constants.Collections.Projects);

        return project;
    }

    public async Task DeleteProject(Guid projectId)
    {
        // Remove all the project data and data persistested in execution of each step
        var projectRunsIds = (await _projectRunRepository.QueryAsync(e => e.ProjectId == projectId, Constants.Collections.ProjectRuns)).Select(e => e.Id);

        var dataCollections = (await _stepJobRepository.QueryAsync(e => projectRunsIds.Contains(e.RunId), Constants.Collections.StepJobs))
            .SelectMany(e => e.StepData)
            .Select(e => e.CollectionName)
            .Distinct();
        foreach (var collection in dataCollections)
        {
            if (collection == null)
                continue;
            await _dataStore.DeleteCollection(collection);
        }
        await _dataSourceRepository.DeleteAllAsync(e => e.ProjectId == projectId, Constants.Collections.DataSources);
        await _matchDefinitionRepository.DeleteAllAsync(e => e.ProjectId == projectId, Constants.Collections.MatchDefinition);
        await _cleaningRuleRepository.DeleteAllAsync(e => e.ProjectId == projectId, Constants.Collections.CleaningRules);
        await _mergeRulesRepository.DeleteAllAsync(e => e.ProjectId == projectId, Constants.Collections.MergeRules);
        await _exportSettingsRepository.DeleteAllAsync(e => e.ProjectId == projectId, Constants.Collections.ExportSettings);
        await _projectRunRepository.DeleteAllAsync(e => e.ProjectId == projectId, Constants.Collections.ProjectRuns);
        await _projectRepository.DeleteAsync(projectId, Constants.Collections.Projects);
        await _dataStore.DeleteAllAsync<ScoreBandCollection>(x=>x.ProjectId == projectId, Constants.Collections.ScoreBand);
    }

    public async Task<List<Domain.Project.Project>> GetAllProjects()
    {
        return await _projectRepository.GetAllAsync(Constants.Collections.Projects);
    }
    public async Task AddCleaningRules(Guid projectId, Guid dataSourceId, EnhancedCleaningRules cleaningRules)
    {
        cleaningRules.ProjectId = projectId;
        await _cleaningRuleRepository.InsertAsync(cleaningRules, Constants.Collections.CleaningRules);
    }

    public async Task AddDataSource(Guid projectId, List<DataSource> dataSource)
    {
        foreach (DataSource dataSourceItem in dataSource)
        {
            dataSourceItem.ProjectId = projectId;
            // Encrypt sensitive parameters before saving
            if (dataSourceItem.ConnectionDetails?.Parameters != null)
            {

                var parameters = await _secureParameterHandler
                    .EncryptSensitiveParametersAsync(
                        dataSourceItem.ConnectionDetails.Parameters,
                        dataSourceItem.Id);

                dataSourceItem.ConnectionDetails = new BaseConnectionInfo()
                {
                    Parameters = parameters,
                    Type = dataSourceItem.ConnectionDetails.Type
                };
            }
            await _dataSourceRepository.InsertAsync(dataSourceItem, Constants.Collections.DataSources);

        }

    }

    /// <summary>
    /// Renames a DataSource and synchronizes all dependent data structures
    /// This is a cross-cutting operation that updates:
    /// - FieldMapping entries (DataSourceName property)
    /// - MappedFieldRows (dictionary re-keying)
    /// - MatchDefinitions (field mappings in criteria)
    /// - DataSource entity itself
    /// </summary>
    public async Task<Domain.Project.DataSource> RenameDataSourceAsync(
        Guid dataSourceId,
        string newName)
    {
        _logger.LogInformation(
            "Starting rename operation for DataSource {DataSourceId} to '{NewName}'",
            dataSourceId, newName);

        try
        {
            // ✅ STEP 1: Get current DataSource
            var dataSource = await _dataSourceRepository.GetByIdAsync(
                dataSourceId,
                Constants.Collections.DataSources);

            if (dataSource == null)
            {
                throw new InvalidOperationException(
                    $"DataSource with ID {dataSourceId} not found");
            }

            var oldName = dataSource.Name;

            // Early return if no change
            if (oldName.Equals(newName, StringComparison.OrdinalIgnoreCase))
            {
                _logger.LogDebug(
                    "DataSource name unchanged: '{Name}'",
                    oldName);
                return dataSource;
            }

            _logger.LogInformation(
                "Renaming DataSource from '{OldName}' to '{NewName}'",
                oldName, newName);

            // ✅ STEP 2: Update FieldMapping entries
            await UpdateFieldMappingForRenameAsync(dataSourceId, oldName, newName);

            // ✅ STEP 3: Update MappedFieldRows
            await UpdateMappedFieldRowsForRenameAsync(
                dataSource.ProjectId,
                oldName,
                newName);

            // ✅ STEP 4: Update MatchDefinitions
            await UpdateMatchDefinitionsForRenameAsync(
                dataSource.ProjectId,
                oldName,
                newName);

            // ✅ STEP 5: Update DataSource entity
            dataSource.Name = newName;
            await _dataSourceRepository.UpdateAsync(
                dataSource,
                Constants.Collections.DataSources);

            _logger.LogInformation(
                "Successfully renamed DataSource {DataSourceId} from '{OldName}' to '{NewName}'",
                dataSourceId, oldName, newName);

            return dataSource;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Error renaming DataSource {DataSourceId} to '{NewName}'",
                dataSourceId, newName);
            throw;
        }
    }

    /// <summary>
    /// Update all FieldMapping entries for renamed DataSource
    /// </summary>
    private async Task UpdateFieldMappingForRenameAsync(
        Guid dataSourceId,
        string oldName,
        string newName)
    {
        var fieldMappings = await _dataStore.QueryAsync<FieldMappingEx>(
            x => x.DataSourceId == dataSourceId,
            Constants.Collections.FieldMapping);

        if (!fieldMappings.Any())
        {
            _logger.LogDebug(
                "No FieldMappings found for DataSource {DataSourceId}",
                dataSourceId);
            return;
        }

        // Update DataSourceName for all fields
        foreach (var field in fieldMappings)
        {
            field.DataSourceName = newName;
            field.UpdatedAt = DateTime.UtcNow;
        }

        // Bulk update
        await _dataStore.BulkUpsertByFieldsAsync(
            fieldMappings,
            Constants.Collections.FieldMapping,
            new System.Linq.Expressions.Expression<Func<FieldMappingEx, object>>[]
            {
            x => x.Id
            });

        _logger.LogInformation(
            "Updated {Count} FieldMapping entries: '{OldName}' → '{NewName}'",
            fieldMappings.Count, oldName, newName);
    }

    /// <summary>
    /// Update MappedFieldRows - re-key dictionary entries
    /// CRITICAL: Dictionary keys use lowercased DataSource names
    /// </summary>
    private async Task UpdateMappedFieldRowsForRenameAsync(
        Guid projectId,
        string oldName,
        string newName)
    {
        var mappedFieldsRows = await _mappedFieldRowsRepository.QueryAsync(
            x => x.ProjectId == projectId,
            Constants.Collections.MappedFieldRows);

        var mappedFieldsRow = mappedFieldsRows.FirstOrDefault();

        if (mappedFieldsRow == null || !mappedFieldsRow.MappedFields.Any())
        {
            _logger.LogDebug(
                "No MappedFieldRows found for project {ProjectId}",
                projectId);
            return;
        }

        var oldKey = oldName.ToLower();
        var newKey = newName.ToLower();
        int updatedRowCount = 0;

        foreach (var row in mappedFieldsRow.MappedFields)
        {
            // Check if this row has a field from the renamed DataSource
            if (row.FieldByDataSource.TryGetValue(oldKey, out var fieldMapping))
            {
                // Remove old key
                row.FieldByDataSource.Remove(oldKey);

                // Update field's DataSourceName property
                fieldMapping.DataSourceName = newName;

                // Add with new key
                row.FieldByDataSource[newKey] = fieldMapping;

                updatedRowCount++;
            }
        }

        if (updatedRowCount > 0)
        {
            await _mappedFieldRowsRepository.UpdateAsync(
                mappedFieldsRow,
                Constants.Collections.MappedFieldRows);

            _logger.LogInformation(
                "Updated {Count} MappedFieldRows: re-keyed from '{OldKey}' to '{NewKey}'",
                updatedRowCount, oldKey, newKey);
        }
    }

    /// <summary>
    /// Update MatchDefinitions - update DataSourceName in field mappings
    /// </summary>
    private async Task UpdateMatchDefinitionsForRenameAsync(
        Guid projectId,
        string oldName,
        string newName)
    {
        var matchDefCollections = await _matchDefinitionCollectionRepository.QueryAsync(
            x => x.ProjectId == projectId,
            Constants.Collections.MatchDefinitionCollection);

        var collection = matchDefCollections.FirstOrDefault();

        if (collection == null || !collection.Definitions.Any())
        {
            _logger.LogDebug(
                "No MatchDefinitions found for project {ProjectId}",
                projectId);
            return;
        }

        int updatedCriteriaCount = 0;

        foreach (var matchDef in collection.Definitions)
        {
            if (matchDef.Criteria == null) continue;

            foreach (var criteria in matchDef.Criteria)
            {
                if (criteria.FieldMappings == null) continue;

                foreach (var fieldMapping in criteria.FieldMappings)
                {
                    if (fieldMapping.DataSourceName.Equals(
                        oldName,
                        StringComparison.OrdinalIgnoreCase))
                    {
                        fieldMapping.DataSourceName = newName;
                        updatedCriteriaCount++;
                    }
                }
            }
        }

        if (updatedCriteriaCount > 0)
        {
            await _matchDefinitionCollectionRepository.UpdateAsync(
                collection,
                Constants.Collections.MatchDefinitionCollection);

            _logger.LogInformation(
                "Updated {Count} criteria in MatchDefinitions: '{OldName}' → '{NewName}'",
                updatedCriteriaCount, oldName, newName);
        }
    }
    public void AddExportSettings(Guid projectId, ExportSettings exportSettings)
    {
        throw new NotImplementedException();
    }

    public void AddMatchDefinition(Guid projectId, Domain.Entities.MatchDefinition matchDefinition)
    {
        throw new NotImplementedException();
    }

    public void AddMergeRules(Guid projectId, Guid dataSourceId, List<MergeRule> mergeRules)
    {
        throw new NotImplementedException();
    }
    /// <summary>
    /// Complete a step and enqueue next step type if ready
    /// Legacy equivalent: ProjectProcessor step completion + status updates
    /// </summary>
    public async Task CompleteStep(
        StepJob step,
        StepData stepData,
        RunStatus runStatus = RunStatus.Completed,
        FlowStatistics statistics = null,
        string errorMessage = null)
    {
        var run = await _projectRunRepository.GetByIdAsync(step.RunId,
           Constants.Collections.ProjectRuns);

        var stepId = step.Id;
        if (run == null)
            throw new InvalidOperationException($"Run not found for step {stepId}");

        var currentStep = await _stepJobRepository.GetByIdAsync(stepId, Constants.Collections.StepJobs);
        currentStep.Status = runStatus;
        currentStep.EndTime = DateTime.UtcNow;

        // Store step results
        currentStep.StepData.Add(stepData);

        // ✅ Store step statistics in Configuration for later aggregation
        if (statistics != null)
        {
            currentStep.Configuration["RecordsProcessed"] = statistics.RecordsProcessed;
            currentStep.Configuration["BatchesProcessed"] = statistics.BatchesProcessed;
            currentStep.Configuration["ErrorCount"] = statistics.ErrorRecords;
            currentStep.Configuration["Duration"] = statistics.Duration;
            currentStep.Configuration["OperationType"] = statistics.OperationType;
        }
        if (errorMessage != null)
        {
            currentStep.Configuration["ErrorMessage"] = errorMessage;
        }
        await _stepJobRepository.UpdateAsync(currentStep, Constants.Collections.StepJobs);

        // Get all steps for this run
        var allSteps = await _stepJobRepository.QueryAsync(
            e => e.RunId == run.Id,
            Constants.Collections.StepJobs);

        // ✅ If step succeeded, try to enqueue next step type
        if (runStatus == RunStatus.Completed)
        {
            await TryEnqueueNextStepTypeAsync(run, currentStep, allSteps.ToList());
        }
        else if (runStatus == RunStatus.Failed)
        {
            // ★ FIX: When a step fails, cancel all NotStarted steps.
            // Without this, downstream steps stay NotStarted forever and
            // the run is never finalized — ScheduledTaskExecution stuck InProgress.
            await CancelRemainingStepsAsync(allSteps, run.Id);

            // Refresh list after cancellation so finalization check sees correct statuses
            allSteps = (await _stepJobRepository.QueryAsync(
                e => e.RunId == run.Id, Constants.Collections.StepJobs)).ToList();
        }
        // Check if ALL steps are complete
        var allStepsList = allSteps.ToList();
        if (!allStepsList.Any(s => s.Status == RunStatus.NotStarted || s.Status == RunStatus.InProgress))
        {
            // Determine final run status
            run.Status = allStepsList.Any(s => s.Status == RunStatus.Failed)
                ? RunStatus.Failed
                : RunStatus.Completed;
            run.EndTime = DateTime.UtcNow;

            // Update project
            var project = await _projectRepository.GetByIdAsync(run.ProjectId, Constants.Collections.Projects);
            project.LastRunStep = currentStep.Type;
            await _projectRepository.UpdateAsync(project, Constants.Collections.Projects);

            // ✅ Notify scheduler if this was a scheduled run
            await PublishRunCompletedEventAsync(run, allStepsList);
        }

        await _projectRunRepository.UpdateAsync(run, Constants.Collections.ProjectRuns);
    }

    private async Task CancelRemainingStepsAsync(List<StepJob> allSteps, Guid runId)
    {
        var stepsToCancel = allSteps
            .Where(s => s.Status == RunStatus.NotStarted)
            .ToList();

        if (!stepsToCancel.Any()) return;

        foreach (var step in stepsToCancel)
        {
            step.Status = RunStatus.Cancelled;
            step.EndTime = DateTime.UtcNow;
            await _stepJobRepository.UpdateAsync(step, Constants.Collections.StepJobs);
        }

        _logger.LogWarning(
            "Cancelled {Count} NotStarted step(s) for run {RunId} due to upstream failure: {StepTypes}",
            stepsToCancel.Count,
            runId,
            string.Join(", ", stepsToCancel.Select(s => s.Type).Distinct()));
    }
    /// <summary>
    /// Try to enqueue the next step type when all steps of current type are complete
    /// </summary>
    private async Task TryEnqueueNextStepTypeAsync(
        ProjectRun run,
        StepJob completedStep,
        List<StepJob> allSteps)
    {
        var currentType = completedStep.Type;

        // Get all steps of the SAME type
        var stepsOfSameType = allSteps.Where(s => s.Type == currentType).ToList();

        // Check if ALL steps of this type are complete
        bool allOfTypeComplete = stepsOfSameType.All(s =>
            s.Status == RunStatus.Completed || s.Status == RunStatus.Failed);

        if (!allOfTypeComplete)
        {
            _logger.LogInformation(
                "Step {StepId} completed, but {Remaining} step(s) of type {StepType} still pending",
                completedStep.Id,
                stepsOfSameType.Count(s => s.Status == RunStatus.NotStarted || s.Status == RunStatus.InProgress),
                currentType);
            return;
        }

        // Check if any step of this type failed
        if (stepsOfSameType.Any(s => s.Status == RunStatus.Failed))
        {
            _logger.LogError(
                "Not enqueueing next steps - {FailedCount} step(s) of type {StepType} failed",
                stepsOfSameType.Count(s => s.Status == RunStatus.Failed),
                currentType);
            return;
        }

        // ✅ Get next step type from configured workflow (enum order)
        var nextStepType = GetNextStepType(currentType, allSteps);

        if (nextStepType == null)
        {
            _logger.LogInformation(
                "Step type {StepType} was the final configured step for run {RunId}. Workflow complete.",
                currentType,
                run.Id);
            return;
        }

        // Find all NotStarted steps of the next type
        var nextSteps = allSteps
            .Where(s => s.Type == nextStepType.Value && s.Status == RunStatus.NotStarted)
            .ToList();

        if (!nextSteps.Any())
        {
            _logger.LogWarning(
                "Next step type {NextStepType} expected but no jobs found. " +
                "Possible data inconsistency.",
                nextStepType.Value);
            return;
        }

        // Enqueue all jobs of the next step type
        foreach (var step in nextSteps)
        {
            await QueueStepJob(run, step);
        }

        _logger.LogInformation(
            "✅ Completed all {CompletedType} steps. " +
            "Enqueued {Count} job(s) of type {NextType}",
            currentType,
            nextSteps.Count,
            nextStepType.Value);
    }

    /// <summary>
    /// Get next step type based on enum order (not hardcoded rules)
    /// </summary>
    private StepType? GetNextStepType(StepType currentType, List<StepJob> allSteps)
    {
        // Get distinct step types ordered by enum value
        var configuredStepTypes = allSteps
            .OrderBy(s => s.Type)  // Orders by enum value (10, 20, 30...)
            .Select(s => s.Type)
            .Distinct()
            .ToList();

        var currentIndex = configuredStepTypes.IndexOf(currentType);

        return currentIndex >= 0 && currentIndex < configuredStepTypes.Count - 1
            ? configuredStepTypes[currentIndex + 1]
            : null;
    }

    /// <summary>
    /// Publish ProjectRunCompletedEvent via MediatR
    /// Event handler (ProjectRunCompletedEventHandler) will update scheduler statistics
    /// This breaks the circular dependency
    /// </summary>
    private async Task PublishRunCompletedEventAsync(ProjectRun run, List<StepJob> allSteps)
    {
        try
        {
            // Determine error message if failed
            string errorMessage = null;
            var failedStepIds = new List<Guid>();

            if (run.Status == RunStatus.Failed)
            {
                var failedSteps = allSteps.Where(s => s.Status == RunStatus.Failed).ToList();
                failedStepIds = failedSteps.Select(s => s.Id).ToList();
                errorMessage = failedSteps.Any()
                    ? $"Failed steps: {string.Join(", ", failedSteps.Select(s => s.Type))}"
                    : "Run failed";
            }

            var runEvent = new ProjectRunCompletedEvent
            {
                RunId = run.Id,
                ProjectId = run.ProjectId,
                Status = run.Status,
                CompletedAt = run.EndTime ?? DateTime.UtcNow,
                ScheduledTaskExecutionId = run.ScheduledTaskExecutionId,
                ErrorMessage = errorMessage,
                FailedStepIds = failedStepIds,
                TotalSteps = allSteps.Count,
                CompletedSteps = allSteps.Count(s => s.Status == RunStatus.Completed)
            };

            // Publish via your existing event infrastructure
            await _jobEventPublisher.PublishRunCompletedAsync(runEvent);

        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Failed to publish ProjectRunCompletedEvent for run {RunId}",
                run.Id);
        }
    }






    public async Task RemoveCleaningRules(Guid projectId, Guid cleansingId)
    {
        var cS = await _cleaningRuleRepository.GetByIdAsync(cleansingId, Constants.Collections.CleaningRules);
        if (cS != null)
        {
            throw new InvalidOperationException($"Cleansing Rule with ID {cleansingId} not found");
        }

        await _cleaningRuleRepository.DeleteAsync(cleansingId, Constants.Collections.CleaningRules);
    }

    /// <summary>
    /// Removes a DataSource and all related data from the project
    /// This is a comprehensive cleanup operation that handles:
    /// - Match system (pairs, definitions, results, groups)
    /// - Field management (FieldMapping, MappedFieldRows)
    /// - Rules (overwrite, master record)
    /// - Export settings
    /// - Data collections and step jobs
    /// </summary>
    public async Task RemoveDataSource(Guid projectId, Guid dataSourceId)
    {
        var dataSource = await _dataSourceRepository.GetByIdAsync(
            dataSourceId,
            Constants.Collections.DataSources);

        if (dataSource == null)
        {
            throw new InvalidOperationException(
                $"DataSource with ID {dataSourceId} not found");
        }

        _logger.LogInformation(
            "Starting deletion of DataSource '{Name}' (ID: {DataSourceId}) from Project {ProjectId}",
            dataSource.Name, dataSourceId, projectId);

        // ✅ CHECK: Is this the last DataSource?
        var allDataSources = await _dataSourceRepository.QueryAsync(
            x => x.ProjectId == projectId,
            Constants.Collections.DataSources);

        bool isLastDataSource = allDataSources.Count == 1;

        if (isLastDataSource)
        {
            _logger.LogWarning(
                "Deleting the LAST DataSource in project {ProjectId}. " +
                "ALL project configuration will be cleaned up.",
                projectId);
        }

        try
        {
            // ✅ STEP 1: Handle Match System
            await RemoveDataSourceFromMatchingSystemAsync(projectId, dataSourceId);

            if (isLastDataSource)
            {
                // ✅ LAST DATASOURCE: Nuclear cleanup of ALL configuration
                await CleanupAllProjectConfigurationAsync(projectId);
            }
            else
            {
                // ✅ NOT LAST: Selective cleanup
                await UpdateFieldOverwriteRulesAsync(projectId, dataSourceId);
                await UpdateMasterRecordRulesAsync(projectId, dataSourceId);
                await UpdateFinalExportSettingsAsync(projectId, dataSourceId);
            }

            // ✅ STEP 2: Get all fields for this DataSource (before deletion)
            var fieldsToDelete = await _dataStore.QueryAsync<FieldMappingEx>(
                x => x.DataSourceId == dataSourceId,
                Constants.Collections.FieldMapping);

            // ✅ STEP 3: Remove from MappedFieldRows (UI state)
            if (fieldsToDelete.Any())
            {
                await _autoMappingService.RemoveFieldsFromMappedRowsAsync(
                    projectId,
                    dataSourceId,
                    fieldsToDelete.Select(f => f.FieldName).ToList());

                _logger.LogInformation(
                    "Removed {Count} fields from MappedFieldRows",
                    fieldsToDelete.Count);
            }

            // ✅ STEP 4: Delete FieldMapping entries
            var deletedFieldCount = await _dataStore.DeleteAllAsync<FieldMappingEx>(
                x => x.DataSourceId == dataSourceId,
                Constants.Collections.FieldMapping);            

            _logger.LogInformation(
                "Deleted {Count} FieldMapping entries",
                deletedFieldCount);

            // ✅ STEP 5: Delete StepJobs and related data collections
            await CleanupStepJobsAndCollectionsAsync(dataSourceId);

            // ✅ STEP 6: Delete CleansingRules and profile/cleansing collections
            await DeleteDataSourceRelatedDataAsync(dataSourceId, projectId);

            await _dataStore.DeleteAllAsync<DataSourceColumnNotes>(x => x.DataSourceId == dataSourceId,
                Constants.Collections.DataSourceColumnNotes);

            // Revoke OAuth tokens for cloud storage providers
            if (dataSource.Type is DataSourceType.GoogleDrive or DataSourceType.Dropbox or DataSourceType.OneDrive)
            {
                try
                {
                    await _oAuthTokenService.RevokeTokensAsync(dataSourceId);
                    _logger.LogInformation("Revoked OAuth tokens for DataSource {DataSourceId}", dataSourceId);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to revoke OAuth tokens for DataSource {DataSourceId}, continuing deletion", dataSourceId);
                }
            }

            var activeSnapShot = await _dataStore.GetByIdAsync<DataSnapshot, Guid>(dataSource.ActiveSnapshotId.GetValueOrDefault(),
                Constants.Collections.DataSnapshots);

            if (activeSnapShot != null)
            {
                if (activeSnapShot.FileImportId.HasValue)
                    await _dataStore.DeleteAsync(activeSnapShot.FileImportId.Value, Constants.Collections.ImportFile);
                if (!string.IsNullOrEmpty(activeSnapShot.StoragePrefix))
                    await _dataStore.DeleteCollection(activeSnapShot.StoragePrefix);
            }

            await _dataStore.DeleteAllAsync<DataSnapshot>(x => x.DataSourceId == dataSourceId, Constants.Collections.DataSnapshots);

            // ✅ STEP 7: Delete the DataSource entity itself
            await _dataSourceRepository.DeleteAsync(
                dataSourceId,
                Constants.Collections.DataSources);

            _logger.LogInformation(
                "Successfully deleted DataSource '{Name}' (ID: {DataSourceId}) and all related data",
                dataSource.Name, dataSourceId);

            if (isLastDataSource)
            {
                _logger.LogWarning(
                    "Project {ProjectId} now has NO DataSources. " +
                    "All configuration has been reset. " +
                    "Project is now empty and ready for new DataSources.",
                    projectId);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Error deleting DataSource '{Name}' (ID: {DataSourceId})",
                dataSource.Name, dataSourceId);
            throw;
        }
    }

    /// <summary>
    /// Clean up ALL project configuration when the last DataSource is deleted
    /// This includes all rules, settings, and collections that become meaningless without DataSources
    /// </summary>
    private async Task CleanupAllProjectConfigurationAsync(Guid projectId)
    {
        _logger.LogWarning(
            "Performing COMPLETE cleanup for project {ProjectId} - last DataSource being deleted",
            projectId);

        // ✅ 1. Delete ALL FieldOverwriteRules
        try
        {
            var deletedOverwriteCount = await _fieldOverwriteRepository.DeleteAllAsync(x => x.ProjectId == projectId, Constants.Collections.FieldOverwriteRuleSets);

            _logger.LogInformation(
                "Deleted ALL FieldOverwriteRules: {Count}",
                deletedOverwriteCount);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error deleting FieldOverwriteRules");
        }

        // ✅ 2. Delete ALL MasterRecordRules
        try
        {
            var deletedMasterCount = await _masterRecordRepository.DeleteAllAsync(
                x => x.ProjectId == projectId,
                Constants.Collections.MasterRecordRuleSets);

            _logger.LogInformation(
                "Deleted ALL MasterRecordRules: {Count}",
                deletedMasterCount);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error deleting MasterRecordRules");
        }

        // ✅ 3. Delete ALL FinalExportSettings
        try
        {
            var deletedExportCount = await _finalExportSettingsRepository.DeleteAllAsync(
                x => x.ProjectId == projectId,
                Constants.Collections.FinalExportSettings);

            _logger.LogInformation(
                "Deleted ALL FinalExportSettings: {Count}",
                deletedExportCount);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error deleting FinalExportSettings");
        }

        // ✅ 4. Delete ALL MappedFieldRows
        try
        {
            var deletedMappedCount = await _mappedFieldRowsRepository.DeleteAllAsync(
                x => x.ProjectId == projectId,
                Constants.Collections.MappedFieldRows);

            _logger.LogInformation(
                "Deleted ALL MappedFieldRows: {Count}",
                deletedMappedCount);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error deleting MappedFieldRows");
        }

        // ✅ 5. Delete ALL MatchDefinitionCollection
        try
        {
            var deletedMatchDefCount = await _matchDefinitionCollectionRepository.DeleteAllAsync(
                x => x.ProjectId == projectId,
                Constants.Collections.MatchDefinitionCollection);

            _logger.LogInformation(
                "Deleted ALL MatchDefinitionCollections: {Count}",
                deletedMatchDefCount);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error deleting MatchDefinitionCollections");
        }

        // ✅ 6. Delete ALL DataSourcePairs
        try
        {
            var deletedPairsCount = await _pairsRepository.DeleteAllAsync(
                x => x.ProjectId == projectId,
                Constants.Collections.MatchDataSourcePairs);

            _logger.LogInformation(
                "Deleted ALL DataSourcePairs: {Count}",
                deletedPairsCount);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error deleting DataSourcePairs");
        }

        // ✅ 7. Delete ALL FieldMapping (for all DataSources in project)
        try
        {
            var deletedFieldCount = await _dataStore.DeleteAllAsync<FieldMappingEx>(
                x => x.DataSourceId != Guid.Empty, // Will be refined by project context
                Constants.Collections.FieldMapping);

            _logger.LogInformation(
                "Deleted ALL FieldMapping entries: {Count}",
                deletedFieldCount);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error deleting all FieldMapping");
        }

        // ✅ 8. Delete match results and groups (already handled in RemoveDataSourceFromMatchingSystemAsync)

        _logger.LogWarning(
            "Completed FULL cleanup for project {ProjectId}. " +
            "Project is now empty and ready for new configuration.",
            projectId);
    }
    /// <summary>
    /// Remove DataSource from matching system:
    /// - Delete DataSourcePairs involving this DataSource
    /// - Delete MatchDefinitions for those pairs
    /// - Delete ALL match results and groups (group integrity)
    /// </summary>
    private async Task RemoveDataSourceFromMatchingSystemAsync(
        Guid projectId,
        Guid dataSourceId)
    {
        _logger.LogInformation(
            "Cleaning up match system for DataSource {DataSourceId}",
            dataSourceId);

        // Get DataSourcePairs collection
        var pairsCollection = await _pairsRepository.QueryAsync(
            x => x.ProjectId == projectId,
            Constants.Collections.MatchDataSourcePairs);

        var pairsWrapper = pairsCollection.FirstOrDefault();

        if (pairsWrapper == null || pairsWrapper.Pairs == null || !pairsWrapper.Pairs.Any())
        {
            _logger.LogInformation(
                "No DataSourcePairs found for project. Skipping match system cleanup.");
            return;
        }

        // Find and collect pairs involving this DataSource
        var pairsToDelete = new List<MatchingDataSourcePair>();
        var pairs = pairsWrapper.Pairs;
        for (int i = pairs.Count - 1; i >= 0; i--)
        {
            var pair = pairs[i];
            if (pair.DataSourceA == dataSourceId || pair.DataSourceB == dataSourceId)
            {
                pairsToDelete.Add(pair);
                pairs.RemoveAt(i);
            }
        }

        if (!pairsToDelete.Any())
        {
            _logger.LogInformation(
                "DataSource {DataSourceId} was not participating in any matching pairs. " +
                "Match results preserved.",
                dataSourceId);
            return;
        }

        _logger.LogInformation(
            "DataSource {DataSourceId} is involved in {Count} matching pairs",
            dataSourceId, pairsToDelete.Count);

        // Delete MatchDefinitions for removed pairs
        await DeleteMatchDefinitionsForPairsAsync(projectId, pairsToDelete);

        // Delete ALL match results and groups (group integrity requirement)
        await DeleteAllMatchResultsAndGroupsAsync(projectId, dataSourceId);

        // Save updated pairs collection
        await _pairsRepository.UpdateAsync(
            pairsWrapper,
            Constants.Collections.MatchDataSourcePairs);

        _logger.LogInformation(
            "Successfully removed DataSource from match system. " +
            "Remaining pairs: {RemainingCount}",
            pairs.Count);
    }

    /// <summary>
    /// Delete MatchDefinitions for the specified pairs
    /// </summary>
    private async Task DeleteMatchDefinitionsForPairsAsync(
        Guid projectId,
        List<MatchingDataSourcePair> deletedPairs)
    {
        var matchDefCollection = await _matchDefinitionCollectionRepository.QueryAsync(
            x => x.ProjectId == projectId,
            Constants.Collections.MatchDefinitionCollection);

        var collection = matchDefCollection?.FirstOrDefault();

        if (collection == null || !collection.Definitions.Any())
        {
            _logger.LogInformation("No MatchDefinitions found to delete");
            return;
        }

        var pairIdsToDelete = deletedPairs.Select(p => p.Id).ToHashSet();

        var removedCount = collection.Definitions.RemoveAll(
            d => pairIdsToDelete.Contains(d.DataSourcePairId));

        if (removedCount > 0)
        {
            await _matchDefinitionCollectionRepository.UpdateAsync(
                collection,
                Constants.Collections.MatchDefinitionCollection);

            _logger.LogInformation(
                "Removed {Count} MatchDefinitions for deleted pairs",
                removedCount);
        }
    }

    /// <summary>
    /// Delete ALL match results and groups for the project
    /// This is required because groups span multiple pairs and cannot be partially deleted
    /// </summary>
    private async Task DeleteAllMatchResultsAndGroupsAsync(
        Guid projectId,
        Guid dataSourceId)
    {
        var pairsCollectionName = $"pairs_{GuidCollectionNameConverter.ToValidCollectionName(projectId)}";
        var groupsCollectionName = $"groups_{GuidCollectionNameConverter.ToValidCollectionName(projectId)}";
        var graphsCollectionName = $"matchgraph_{GuidCollectionNameConverter.ToValidCollectionName(projectId)}";

        bool anyDeleted = false;

        try
        {
            var resultsDeleted = await _dataStore.DeleteCollection(pairsCollectionName);
            if (resultsDeleted)
            {
                _logger.LogWarning(
                    "Deleted match results collection: {CollectionName}",
                    pairsCollectionName);
                anyDeleted = true;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Error deleting match results collection: {CollectionName}",
                pairsCollectionName);
        }

        try
        {
            var groupsDeleted = await _dataStore.DeleteCollection(groupsCollectionName);
            if (groupsDeleted)
            {
                _logger.LogWarning(
                    "Deleted match groups collection: {CollectionName}",
                    groupsCollectionName);
                anyDeleted = true;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Error deleting match groups collection: {CollectionName}",
                groupsCollectionName);
        }
        try
        {
            var graphDeleted = await _dataStore.DeleteCollection(graphsCollectionName);
            if (graphDeleted)
            {
                _logger.LogWarning(
                    "Deleted match groups collection: {CollectionName}",
                    graphsCollectionName);
                anyDeleted = true;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Error deleting match groups collection: {CollectionName}",
                graphsCollectionName);
        }

        if (anyDeleted)
        {
            _logger.LogWarning(
                "Deleted ALL match results and groups for project {ProjectId}. " +
                "Reason: DataSource {DataSourceId} deletion breaks group integrity. " +
                "User must re-run matching to regenerate results.",
                projectId, dataSourceId);
        }
    }

    /// <summary>
    /// Update FieldOverwriteRules - remove deleted DataSource from filters
    /// </summary>
    private async Task UpdateFieldOverwriteRulesAsync(
        Guid projectId,
        Guid dataSourceId)
    {
        var ruleSets = await _fieldOverwriteRepository.GetAllByProjectIdAsync(projectId);

        if (!ruleSets.Any())
        {
            _logger.LogDebug("No FieldOverwriteRules found for project");
            return;
        }

        foreach (var ruleSet in ruleSets)
        {
            bool hasChanges = false;

            foreach (var rule in ruleSet.Rules)
            {
                if (rule.DataSourceFilters?.Remove(dataSourceId) == true)
                {
                    hasChanges = true;

                    _logger.LogInformation(
                        "Removed DataSource from FieldOverwriteRule '{Field}'. " +
                        "Remaining filters: {Count}",
                        rule.LogicalFieldName,
                        rule.DataSourceFilters.Count);
                }
            }

            if (hasChanges)
            {
                await _fieldOverwriteRepository.SaveWithRulesAsync(
                    ruleSet);

                _logger.LogInformation(
                    "Updated FieldOverwriteRuleSet '{Name}'",
                    ruleSet.Name);
            }
        }
    }

    /// <summary>
    /// Update MasterRecordRules - clear preferred DataSource and remove from selections
    /// </summary>
    private async Task UpdateMasterRecordRulesAsync(
        Guid projectId,
        Guid dataSourceId)
    {
        var ruleSets = await _masterRecordRepository.GetAllByProjectIdAsync(projectId);

        if (!ruleSets.Any())
        {
            _logger.LogDebug("No MasterRecordRules found for project");
            return;
        }

        foreach (var ruleSet in ruleSets)
        {
            bool hasChanges = false;

            foreach (var rule in ruleSet.Rules)
            {
                // Clear preferred DataSource if it matches
                if (rule.PreferredDataSourceId == dataSourceId)
                {
                    rule.PreferredDataSourceId = null;
                    rule.IsActive = false;
                    hasChanges = true;

                    _logger.LogWarning(
                        "Deactivated MasterRecordRule '{Field}' - preferred DataSource deleted",
                        rule.LogicalFieldName);
                }

                // Remove from selected DataSources
                if (rule.SelectedDataSourceIds?.Remove(dataSourceId) == true)
                {
                    hasChanges = true;

                    _logger.LogInformation(
                        "Removed DataSource from MasterRecordRule '{Field}'. " +
                        "Remaining selections: {Count}",
                        rule.LogicalFieldName,
                        rule.SelectedDataSourceIds.Count);

                    // Deactivate if no DataSources remain selected
                    if (!rule.SelectedDataSourceIds.Any())
                    {
                        rule.IsActive = false;

                        _logger.LogWarning(
                            "Deactivated MasterRecordRule '{Field}' - no DataSources remaining",
                            rule.LogicalFieldName);
                    }
                }
            }

            if (hasChanges)
            {
                await _masterRecordRepository.SaveWithRulesAsync(
                    ruleSet);

                _logger.LogInformation("Updated MasterRecordRuleSet");
            }
        }
    }

    /// <summary>
    /// Clean up StepJobs and associated data collections
    /// </summary>
    private async Task CleanupStepJobsAndCollectionsAsync(Guid dataSourceId)
    {
        // Get all StepJobs for this DataSource
        var stepJobs = await _stepJobRepository.QueryAsync(
            x => x.Type == StepType.Import && x.DataSourceId == dataSourceId,
            Constants.Collections.StepJobs);

        if (!stepJobs.Any())
        {
            _logger.LogDebug("No StepJobs found for DataSource");
            return;
        }

        var stepIds = stepJobs.Select(x => x.Id).ToList();

        // Delete all collections associated with these steps
        var dataCollections = stepJobs
            .SelectMany(e => e.StepData)
            .Select(e => e.CollectionName)
            .Where(c => !string.IsNullOrEmpty(c))
            .Distinct();

        foreach (var collection in dataCollections)
        {
            try
            {
                await _dataStore.DeleteCollection(collection);
                _logger.LogInformation("Deleted data collection: {CollectionName}", collection);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error deleting collection: {CollectionName}", collection);
            }
        }

        // Collect RunIds to check if they should be deleted
        var runIds = stepJobs.Select(x => (x.RunId, false)).ToList();

        // Check each run - delete if it only has Import steps
        for (int i = 0; i < runIds.Count; i++)
        {
            var runId = runIds[i];
            var otherSteps = await _stepJobRepository.QueryAsync(
                x => x.RunId == runId.Item1 && x.Type != StepType.Import,
                Constants.Collections.StepJobs);

            // Mark for deletion if no other step types exist
            runIds[i] = (runId.Item1, !otherSteps.Any());
        }

        // Delete runs that only had Import steps
        var runsToDelete = runIds.Where(x => x.Item2).Select(x => x.Item1).ToList();

        if (runsToDelete.Any())
        {
            await _projectRunRepository.DeleteAllAsync(
                x => runsToDelete.Contains(x.Id),
                Constants.Collections.ProjectRuns);

            _logger.LogInformation("Deleted {Count} ProjectRuns", runsToDelete.Count);
        }

        // Delete the StepJobs
        await _stepJobRepository.DeleteAllAsync(
            s => stepIds.Contains(s.Id),
            Constants.Collections.StepJobs);

        _logger.LogInformation("Deleted {Count} StepJobs", stepIds.Count);
    }
    /// <summary>
    /// Update FinalExportSettings - remove deleted DataSource from export configuration
    /// </summary>
    private async Task UpdateFinalExportSettingsAsync(
        Guid projectId,
        Guid dataSourceId)
    {
        var exportSettings = await _finalExportSettingsRepository.QueryAsync(
            x => x.ProjectId == projectId,
            Constants.Collections.FinalExportSettings);

        var settings = exportSettings.FirstOrDefault();

        if (settings?.DataSetsToInclude?.Remove(dataSourceId) == true)
        {
            settings.UpdatedAt = DateTime.UtcNow;

            await _finalExportSettingsRepository.UpdateAsync(
                settings,
                Constants.Collections.FinalExportSettings);

            _logger.LogInformation(
                "Removed DataSource from FinalExportSettings. " +
                "Remaining datasets: {Count}",
                settings.DataSetsToInclude.Count);
        }
    }
    /// <summary>
    /// Delete CleansingRules and profile/cleansing collections for the DataSource
    /// </summary>
    private async Task DeleteDataSourceRelatedDataAsync(Guid dataSourceId, Guid projectId)
    {
        var profileCollectionName = $"{StepType.Profile.ToString().ToLower()}_{GuidCollectionNameConverter.ToValidCollectionName(dataSourceId)}";
        var profileCollectionNameRowRef = $"{StepType.Profile.ToString().ToLower()}_{GuidCollectionNameConverter.ToValidCollectionName(dataSourceId)}_RowReferenceDocument";
        var cleansingCollectionName = $"{StepType.Cleanse.ToString().ToLower()}_{GuidCollectionNameConverter.ToValidCollectionName(dataSourceId)}";
        var cleansingCollectionNamePreview = $"{StepType.Cleanse.ToString().ToLower()}_{GuidCollectionNameConverter.ToValidCollectionName(dataSourceId)}_preview";

        // Delete cleansing rules
        var cleansingRules = await _cleaningRuleRepository.QueryAsync(
            x => x.ProjectId == projectId && x.DataSourceId == dataSourceId,
            Constants.Collections.CleaningRules);

        if (cleansingRules.Any())
        {
            var ruleIdsToDelete = cleansingRules.Select(r => r.Id).ToList();

            await _cleaningRuleRepository.DeleteAllAsync(
                x => ruleIdsToDelete.Contains(x.Id),
                Constants.Collections.CleaningRules);

            _logger.LogInformation("Deleted {Count} CleansingRules", cleansingRules.Count);
        }

        // Delete profile collection
        try
        {
            await _dataStore.DeleteCollection(profileCollectionName);
            _logger.LogInformation("Deleted profile collection: {CollectionName}", profileCollectionName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error deleting profile collection: {CollectionName}", profileCollectionName);
        }

        try
        {
            await _dataStore.DeleteCollection(profileCollectionNameRowRef);
            _logger.LogInformation("Deleted profile collection: {CollectionName}", profileCollectionNameRowRef);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error deleting profile collection: {CollectionName}", profileCollectionNameRowRef);
        }
        // Delete cleansing collection
        try
        {
            await _dataStore.DeleteCollection(cleansingCollectionName);
            _logger.LogInformation("Deleted cleansing collection: {CollectionName}", cleansingCollectionName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error deleting cleansing collection: {CollectionName}", cleansingCollectionName);
        }

        try
        {
            await _dataStore.DeleteCollection(cleansingCollectionNamePreview);
            _logger.LogInformation("Deleted cleansing collection: {CollectionName}", cleansingCollectionNamePreview);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error deleting cleansing collection: {CollectionName}", cleansingCollectionNamePreview);
        }
    }
    public void RemoveExportSettings(Guid projectId)
    {
        throw new NotImplementedException();
    }

    public void RemoveMatchDefinition(Guid projectId, Guid matchDefinitionId)
    {
        throw new NotImplementedException();
    }

    public void RemoveMergeRules(Guid projectId, Guid dataSourceId)
    {
        throw new NotImplementedException();
    }

    public async Task<ProjectRun> StartNewRun(Guid projectId, List<StepConfiguration> stepsConfiguration,
        Guid? scheduledTaskExecutionId = null)
    {
        var run = new ProjectRun
        {
            Id = Guid.NewGuid(),
            ProjectId = projectId,
            StartTime = DateTime.UtcNow,
            Status = RunStatus.InProgress,
            ScheduledTaskExecutionId = scheduledTaskExecutionId
        };

        await _projectRunRepository.InsertAsync(run, Constants.Collections.ProjectRuns);

        stepsConfiguration = stepsConfiguration.OrderBy(s => s.Type).ToList();
        List<StepJob> steps = new();

        foreach (var stepConfig in stepsConfiguration)
        {
            foreach (var dataSourceId in stepConfig.DataSourceIds)
            {
                var stepJob = new StepJob
                {
                    Type = stepConfig.Type,
                    RunId = run.Id,
                    Status = RunStatus.NotStarted,
                    StartTime = DateTime.UtcNow,
                    DataSourceId = dataSourceId,
                    Configuration = stepConfig.Configuration ?? new Dictionary<string, object>()
                };

                // Only for Import until refactored
                if (stepConfig.Type == StepType.Import)
                {
                    stepJob.Configuration[Constants.FieldNames.DataSourceId] = dataSourceId;
                }

                steps.Add(stepJob);
                await _stepJobRepository.InsertAsync(stepJob, Constants.Collections.StepJobs);
            }
        }

        if (steps.Any())
        {
            var firstStepType = steps.OrderBy(s => s.Type).First().Type;
            var firstSteps = steps.Where(s => s.Type == firstStepType).ToList();

            foreach (var step in firstSteps)
            {
                await QueueStepJob(run, step);
            }

            _logger.LogInformation(
                "Started run {RunId} with {TotalSteps} total steps. " +
                "Enqueued {EnqueuedCount} job(s) of type {StepType} (first in workflow)",
                run.Id,
                steps.Count,
                firstSteps.Count,
                firstStepType);
        }
        return run;
    }

    public async Task<StepJob> StartStep(Guid runId, StepType stepType, Dictionary<string, object> configuration)
    {
        var run = await _projectRunRepository.GetByIdAsync(runId, Constants.Collections.ProjectRuns);
        var steps = await _stepJobRepository.QueryAsync(s => s.RunId == runId && s.Type == stepType && s.Status == RunStatus.NotStarted,
            Constants.Collections.StepJobs);
        var step = steps.First();

        step.Configuration = configuration ?? new Dictionary<string, object>();
        step.Status = RunStatus.InProgress;

        await _stepJobRepository.UpdateAsync(step, Constants.Collections.StepJobs);
        await QueueStepJob(run, step);
        return step;
    }

    public void UpdateProject(Domain.Project.Project project)
    {
        throw new NotImplementedException();
    }

    public void UpdateRunStatus(Guid runId, RunStatus status)
    {
        throw new NotImplementedException();
    }


    private async Task QueueStepJob(ProjectRun run, StepJob step)
    {
        var jobInfo = new ProjectJobInfo
        {
            JobId = Guid.NewGuid(),
            RunId = run.Id,
            ProjectId = run.ProjectId,
            CurrentStep = step
        };

        // here is jobQueue
        await _scheduler.EnqueueJobAsync(jobInfo);
    }


}
