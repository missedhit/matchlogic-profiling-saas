using MatchLogic.Application.Common;
using MatchLogic.Application.Extensions;
using MatchLogic.Application.Features.Project;
using MatchLogic.Application.Interfaces.DataProfiling;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.DataProfiling;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using MatchLogic.Infrastructure.Project.Commands;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Project.DataProfiling
{
    public class DataProfilingCommand : BaseCommand
    {
        private readonly IDataProfiler _dataProfiler;
        private readonly IDataStore _dataStore;
        private readonly ILogger<DataProfilingCommand> _logger;
        private readonly IGenericRepository<Domain.Project.DataSource, Guid> _dataSourceRepository;

        public DataProfilingCommand(
            IProjectService projectService,
            IJobEventPublisher jobEventPublisher,
            IGenericRepository<ProjectRun, Guid> projectRunRepository,
            IGenericRepository<StepJob, Guid> stepJobRepository,
            IDataStore dataStore,
            IDataProfiler dataProfiler,
            IGenericRepository<Domain.Project.DataSource, Guid> dataSourceRepository,
            ILogger<DataProfilingCommand> logger)
            : base(projectService, jobEventPublisher, projectRunRepository, stepJobRepository,dataSourceRepository, logger)
        {
            _dataProfiler = dataProfiler;
            _dataStore = dataStore;
            _logger = logger;
            _dataSourceRepository = dataSourceRepository;            
        }

        protected override int NumberOfSteps => 2;

        protected override async Task<StepData> ExecCommandAsync(ICommandContext context, StepJob step, CancellationToken cancellationToken = default)
        {
            // Get the data source ID from the step configuration
            if (!step.DataSourceId.HasValue)
            {
                throw new InvalidOperationException("DataSourceId is required for profiling step");
            }

            var dataSourceId = step.DataSourceId.GetValueOrDefault();

            // Get the previous import step data for this data source
            var importSteps = context.GetStepOutput(StepType.Import, dataSourceId);

            if (!importSteps.Any())
            {
                throw new InvalidOperationException("No import step found for the data source. Import data before profiling.");
            }

            // Get the collection name from the import step
            var collectionName = importSteps.First().CollectionName;

            // Create profile collection name as per requirements
            var profileCollectionName = $"{step.Type.ToString().ToLower()}_{GuidCollectionNameConverter.ToValidCollectionName(dataSourceId)}";

            // Get the data source details
            var dataSource = await _dataSourceRepository.GetByIdAsync(dataSourceId, Constants.Collections.DataSources);
            if (dataSource == null)
            {
                throw new InvalidOperationException($"Data source {dataSourceId} not found");
            }            

            try
            {
                // Create step tracker
                var profilingStep = _jobEventPublisher.CreateStepTracker(context.StepId, "Data Profiling", 1, NumberOfSteps);
                var savingStep = _jobEventPublisher.CreateStepTracker(context.StepId, "Saving Profile Results", 2, NumberOfSteps);

                await profilingStep.StartStepAsync(0, CancellationToken.None);
                await savingStep.StartStepAsync(0, CancellationToken.None);

                _logger.LogInformation("Starting profiling of data source {DataSourceId} from collection {CollectionName}",
                    dataSourceId, collectionName);

                // Get data stream from the collection
                var dataStream = _dataStore.StreamDataAsync(collectionName, CancellationToken.None);

                // Profile the data
                var profileResult = await _dataProfiler.ProfileDataAsync(
                    dataStream,
                    dataSource,                    
                    commandContext: context,
                    collectionName: profileCollectionName,
                    cancellationToken: CancellationToken.None);

                await profilingStep.CompleteStepAsync();
                await savingStep.CompleteStepAsync();                


                // Return the step data
                return new StepData
                {
                    Id = Guid.NewGuid(),
                    StepJobId = step.Id,
                    DataSourceId = dataSourceId,
                    CollectionName = profileCollectionName,
                    DataFormat = "json"
                };
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error profiling data source {DataSourceId}", dataSourceId);                
                throw;
            }
        }

        protected override Task ValidateInputs(ICommandContext context, StepJob step, CancellationToken cancellationToken = default)
        {
            if (!step.DataSourceId.HasValue)
            {
                throw new InvalidOperationException("DataSourceId is required for profiling step");
            }

            return Task.CompletedTask;
        }
    }
}
