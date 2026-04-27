using MatchLogic.Application.Common;
using MatchLogic.Application.Extensions;
using MatchLogic.Application.Interfaces.Cleansing;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Application.Interfaces.Import;
using MatchLogic.Application.Interfaces.MatchConfiguration;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.CleansingAndStandaradization;
using MatchLogic.Domain.Entities;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using Microsoft.Extensions.Logging;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DomainDataSource = MatchLogic.Domain.Project.DataSource;

namespace MatchLogic.Infrastructure.Project.Commands;

public class DataCleansingCommand : BaseCommand
{    
    private readonly ICleansingModule _cleansingService;
    private readonly IGenericRepository<EnhancedCleaningRules, Guid> _cleaningRulesRepository;
    private readonly IFieldMappingService _fieldMappingService;
    private readonly IAutoMappingService _autoMappingService;
    private readonly IDataStore _dataStore;
    public DataCleansingCommand(        
        ICleansingModule cleansingModule,
        IProjectService projectService,
        IJobEventPublisher jobEventPublisher,
        IGenericRepository<ProjectRun, Guid> projectRunRepository,
        IGenericRepository<StepJob, Guid> stepJobRepository,
        IGenericRepository<EnhancedCleaningRules, Guid> cleaningRulesRepository,
        IGenericRepository<DomainDataSource, Guid> dataSourceRepository,
        IDataStore dataStore,
        IFieldMappingService fieldMappingService,
        IAutoMappingService autoMappingService,
        ILogger<DataCleansingCommand> logger)
        : base(projectService, jobEventPublisher, projectRunRepository, stepJobRepository,dataSourceRepository, logger)
    {        
        _cleansingService = cleansingModule;
        _cleaningRulesRepository = cleaningRulesRepository;
        _dataStore = dataStore;
        _fieldMappingService = fieldMappingService;
        _autoMappingService = autoMappingService;
    }

    protected override int NumberOfSteps => 1;

    protected override async Task<StepData> ExecCommandAsync(ICommandContext context, StepJob step, CancellationToken cancellationToken = default)
    {

        var dataSourceId = step.DataSourceId;
        var isPreview = false;
        if (step.Configuration?.TryGetValue("IsPreview", out var value) == true)
        {
            isPreview = (bool)value;
        }

        if (!dataSourceId.HasValue)
        {
            throw new InvalidOperationException("DataSourceId is required for cleansing step");
        }
       
        var previousSteps = context.GetStepOutput(StepType.Import,dataSourceId);

        // Get previous step data
        var inputCollection = previousSteps.First().CollectionName;

        var outputCollection = isPreview ? 
            $"{step.Type.ToString().ToLower()}_{GuidCollectionNameConverter.ToValidCollectionName(dataSourceId.GetValueOrDefault())}_preview":
            $"{step.Type.ToString().ToLower()}_{GuidCollectionNameConverter.ToValidCollectionName(dataSourceId.GetValueOrDefault())}";
        
        // Query directly for EnhancedCleaningRules with this dataSourceId
        var cleaningRules = await _cleaningRulesRepository.QueryAsync(
            x => x.ProjectId == context.ProjectId && x.DataSourceId == dataSourceId.Value,
            Constants.Collections.CleaningRules);

        var cleansingRule = cleaningRules.FirstOrDefault();

        if (cleansingRule == null || cleaningRules.Count() == 0)
        {
            throw new InvalidOperationException($"Cleaning rules not found with DataSourceID {dataSourceId}");
        }

        // Process the cleansing operations
        await _cleansingService.ProcessDataAsync(
            inputCollection,
            outputCollection,
           cleansingRule,           
           context,
           isPreview: isPreview);

        if (!isPreview)
        {
            // Get cleansing output
            var schemaInfo = await _cleansingService.GetOutputSchemaAsync(cleansingRule);

            // Get existing fields to calculate max index
            var existingFields = await _dataStore.QueryAsync<FieldMappingEx>(
                x => x.DataSourceId == dataSourceId.Value,
                Constants.Collections.FieldMapping);

            var maxIndex = existingFields.Any()
                ? existingFields.Max(f => f.FieldIndex)
                : -1;

            var newColumns = schemaInfo.OutputColumns
               .Where(x => x.IsNewColumn)
               .Select((col, idx) => new FieldColumnInfo
               {
                   Name = col.Name,
                   Index = maxIndex + 1 + idx,
                   DataType = typeof(string).ToString(),
                   IsNewColumn = true,
                   SourceOperation = col.ProducedBy
               })
               .ToList();

            var dataSource = await _dataStore.GetByIdAsync<DomainDataSource, Guid>(
                dataSourceId.Value,
                Constants.Collections.DataSources);

            // Get existing system-managed fields from cleansing
            var existingSystemFields = await _dataStore.QueryAsync<FieldMappingEx>(
                x => x.DataSourceId == dataSourceId.Value &&
                     x.Origin == FieldOrigin.CleansingOperation &&
                     x.IsSystemManaged == true,
                Constants.Collections.FieldMapping);

            var existingFieldNames = existingSystemFields.Select(f => f.FieldName).ToHashSet();
            var newFieldNames = newColumns.Select(c => c.Name).ToHashSet();

            // STEP 1: Check MatchDefinition FIRST (before touching MappedFieldRows)
            var fieldsToRemove = existingSystemFields
            .Where(f => !newFieldNames.Contains(f.FieldName))
            .ToList();

            // STEP 1: Classify fields (reuse FieldMappingService logic!)
            if (fieldsToRemove.Any())
            {
                var (fieldsUsedInMatch, fieldsSafeToRemove) =
                    await _fieldMappingService.ClassifyFieldsByUsageAsync(
                        fieldsToRemove,
                        context.ProjectId);

                // STEP 1a: Keep used fields in MappedFieldRows but mark inactive
                if (fieldsUsedInMatch.Any())
                {
                    await _autoMappingService.UpdateFieldsInactiveStatusInMappedRowsAsync(
                        context.ProjectId,
                        dataSourceId.Value,
                        fieldsUsedInMatch.Select(f => f.FieldName).ToList(),
                        isActive: false);
                }

                // STEP 1b: Remove safe fields from MappedFieldRows
                if (fieldsSafeToRemove.Any())
                {
                    await _autoMappingService.RemoveFieldsFromMappedRowsAsync(
                        context.ProjectId,
                        dataSourceId.Value,
                        fieldsSafeToRemove.Select(f => f.FieldName).ToList());
                }
            }

            // STEP 2: Sync FieldMapping (handles soft/hard delete internally)
            await _fieldMappingService.SyncSystemGeneratedFieldsAsync(
                dataSourceId: dataSourceId.Value,
                dataSourceName: dataSource.Name,
                currentColumns: newColumns,
                origin: FieldOrigin.CleansingOperation,
                projectId: context.ProjectId,
                sourceOperationId: cleansingRule.Id);

            // STEP 3: Add new fields to MappedFieldRows
            var newlyCreatedFields = await _dataStore.QueryAsync<FieldMappingEx>(
                x => x.DataSourceId == dataSourceId.Value &&
                     x.Origin == FieldOrigin.CleansingOperation &&
                     x.IsActive == true &&
                     newColumns.Select(nc => nc.Name).Contains(x.FieldName),
                Constants.Collections.FieldMapping);

            if (newlyCreatedFields.Any())
            {
                await _autoMappingService.AddSystemGeneratedFieldsToMappedRowsAsync(
                    context.ProjectId,
                    dataSourceId.Value,
                    newlyCreatedFields);
            }
        }

        return new StepData
        {
            Id = Guid.NewGuid(),
            StepJobId = step.Id,
            DataSourceId = dataSourceId.Value,
            CollectionName = outputCollection
        };
    }

    protected override async Task ValidateInputs(ICommandContext context, StepJob step, CancellationToken cancellationToken = default)
    {
        if (!step.DataSourceId.HasValue)
        {
            throw new InvalidOperationException("DataSourceId is required for import step");
        }
        // Fail fast here with a clear message — don't let null reach ProcessDataAsync
        var rules = await _cleaningRulesRepository.QueryAsync(
            x => x.ProjectId == context.ProjectId && x.DataSourceId == step.DataSourceId.Value,
            Constants.Collections.CleaningRules);

        if (rules.Count() == 0)
            throw new InvalidOperationException(
                $"No cleansing rules defined for data source {step.DataSourceId}. " +
                "Define rules before scheduling a cleanse step.");
    }    
}
