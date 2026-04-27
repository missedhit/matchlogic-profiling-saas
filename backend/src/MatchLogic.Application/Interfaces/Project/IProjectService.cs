using MatchLogic.Application.Features.Project;
using MatchLogic.Domain.CleansingAndStandaradization;
using MatchLogic.Domain.Entities;
using MatchLogic.Domain.Export;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.MergeAndSurvivorship;
using MatchLogic.Domain.Project;
using MatchLogic.Application.Features.Project;
using MatchLogic.Domain.CleansingAndStandaradization;
using MatchLogic.Domain.Entities;
using MatchLogic.Domain.Export;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.MergeAndSurvivorship;
using MatchLogic.Domain.Project;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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


    /// <summary>
    /// Renames a DataSource and synchronizes all dependent data structures
    /// (FieldMappings, MappedFieldRows, MatchDefinitions)
    /// </summary>
    /// <param name="dataSourceId">DataSource to rename</param>
    /// <param name="newName">New name for the DataSource</param>
    /// <returns>Updated DataSource with new name</returns>
    Task<Domain.Project.DataSource> RenameDataSourceAsync(Guid dataSourceId, string newName);
    Task RemoveDataSource(Guid projectId, Guid dataSourceId);
    void AddMatchDefinition(Guid projectId, MatchDefinition matchDefinition);
    void RemoveMatchDefinition(Guid projectId, Guid matchDefinitionId);
    Task<ProjectRun> StartNewRun(Guid projectId, List<StepConfiguration> stepsConfiguration, Guid? scheduledTaskExecutionId = null);
    void UpdateRunStatus(Guid runId, RunStatus status);
    Task<StepJob> StartStep(Guid runId, StepType stepType, Dictionary<string, object> configuration);
    Task CompleteStep(StepJob step, StepData stepData, RunStatus runStatus = RunStatus.Completed, FlowStatistics statistics = null, string errorMessage = null);
    Task AddCleaningRules(Guid projectId, Guid dataSourceId, EnhancedCleaningRules cleaningRules);
    Task RemoveCleaningRules(Guid projectId, Guid cleansingId);
    void AddMergeRules(Guid projectId, Guid dataSourceId, List<MergeRule> mergeRules);
    void RemoveMergeRules(Guid projectId, Guid dataSourceId);
    void AddExportSettings(Guid projectId, ExportSettings exportSettings);
    void RemoveExportSettings(Guid projectId);
}
