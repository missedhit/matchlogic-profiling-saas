using MatchLogic.Application.Features.Project;
using MatchLogic.Domain.Entities.Common;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.Project;

public interface IProjectService
{
    Task<Domain.Project.Project> CreateProject(string name, string description, int retentionRuns = 2);
    Task<Domain.Project.Project> GetProjectById(Guid projectId);
    Task DeleteProject(Guid projectId);
    Task<Domain.Project.Project> UpdateProject(Guid ProjectId, string name, string description, int retentionRuns = 2);
    Task<List<Domain.Project.Project>> GetAllProjects();

    Task AddDataSource(Guid projectId, List<DataSource> dataSource);
    Task<DataSource> RenameDataSourceAsync(Guid dataSourceId, string newName);
    Task RemoveDataSource(Guid projectId, Guid dataSourceId);

    Task<ProjectRun> StartNewRun(Guid projectId, List<StepConfiguration> stepsConfiguration, Guid? scheduledTaskExecutionId = null);
    Task<StepJob> StartStep(Guid runId, StepType stepType, Dictionary<string, object> configuration);
    Task CompleteStep(StepJob step, StepData stepData, RunStatus runStatus = RunStatus.Completed, FlowStatistics statistics = null, string errorMessage = null);
}
