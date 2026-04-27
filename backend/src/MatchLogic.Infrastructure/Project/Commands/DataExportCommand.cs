using MatchLogic.Application.Common;
using MatchLogic.Application.Features.Export;
using MatchLogic.Application.Features.Import;
using MatchLogic.Application.Features.Transform;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Application.Interfaces.MatchConfiguration;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.Export;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using MatchLogic.Infrastructure.Export;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Project.Commands;

public class DataExportCommand : BaseCommand
{
    //private Guid? _projectId;
    private Guid? _exportId;

    private readonly IExportDataWriterStrategyFactory _exportDataWriterStrategyFactory;
    private readonly IDataTransformerFactory _transformerFactory;
    private readonly IDataStore _dataStore;
    private readonly IColumnFilter _columnFilter;
    private readonly IGenericRepository<DataExportOptions, Guid> _dataExportRepository;
    private readonly IGenericRepository<Domain.Project.DataSource, Guid> _dataSourceRepository;
    private readonly IAutoMappingService _mappingService;

    private readonly ILogger<DataExportCommand> _logger;

    public DataExportCommand(
        IProjectService projectService, 
        IJobEventPublisher jobEventPublisher,
        IGenericRepository<ProjectRun, Guid> projectRunRepository,
        IGenericRepository<StepJob, Guid> stepJobRepository,
        IGenericRepository<Domain.Project.DataSource, Guid> dataSourceRepository,
        IGenericRepository<DataExportOptions, Guid> dataExportRepository,
        ILogger<DataExportCommand> logger,
        IColumnFilter columnFilter,
        IDataStore dataStore,
        IExportDataWriterStrategyFactory exportDataWriterStrategy,
        IDataTransformerFactory transformerFactory,
        IAutoMappingService mappingService) 
        : base(projectService, jobEventPublisher, projectRunRepository, stepJobRepository, dataSourceRepository, logger)
    {
        _exportDataWriterStrategyFactory = exportDataWriterStrategy;
        _transformerFactory = transformerFactory;
        _dataExportRepository = dataExportRepository;
        _dataStore = dataStore;
        _columnFilter = columnFilter;
        _mappingService = mappingService;
        _dataSourceRepository = dataSourceRepository;
        _logger = logger;
    }

    protected override int NumberOfSteps => 2;

    protected override async Task<StepData> ExecCommandAsync(ICommandContext context, StepJob step, CancellationToken cancellationToken = default)
    {
        if (!_exportId.HasValue)
            throw new InvalidOperationException($"{Constants.FieldNames.ExportId} is required for export step");
        // Get export configuration
        var exportOptions = await _dataExportRepository.GetByIdAsync(_exportId.Value, Constants.Collections.ExportSettings);
        if (exportOptions == null)
            throw new InvalidOperationException($"Data Export Settings {_exportId.Value} not found");

        var exportWriterStrategy = _exportDataWriterStrategyFactory.GetStrategy(exportOptions.ConnectionConfig);

        // Create and configure export module
        var exportModule = new DataExportModule(
            _dataStore,
            exportWriterStrategy,
            _columnFilter,
            _jobEventPublisher,
            _transformerFactory,
            _mappingService,
            _dataSourceRepository,
            _logger as ILogger<DataExportModule>);

        try
        {
            var success = await exportModule.ExportDataAsync(
                exportOptions,
                context,
                CancellationToken.None);

            if (!success)
                throw new Exception($"Export failed for data source {exportOptions.TableName}");

            return new StepData
            {
                Id = Guid.NewGuid(),
                StepJobId = step.Id,
                DataSourceId = _exportId.Value,
                CollectionName = exportOptions.CollectionName,
                DataFormat = "json"
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error exporting data source {DataSourceId}", _exportId.Value);
            throw;
        }
    }


    protected override Task ValidateInputs(ICommandContext context, StepJob step, CancellationToken cancellationToken = default)
    {
        if (step.Configuration?.TryGetValue(Constants.FieldNames.ExportId , out var exportId) == true)
        {   
            _exportId = (Guid)exportId;
        }
        else
        {
            throw new InvalidOperationException($"{Constants.FieldNames.ExportId} is required for export step");
        }
        return Task.CompletedTask;
    }

}
