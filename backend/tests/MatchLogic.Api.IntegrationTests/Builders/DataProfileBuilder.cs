using MatchLogic.Application.Features.Project;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.Import;
using MatchLogic.Application.Features.Project;
using MatchLogic.Application.Interfaces.Project;
using Microsoft.Extensions.DependencyInjection;

namespace MatchLogic.Api.IntegrationTests.Builders;
public class DataProfileBuilder
{
    private readonly IProjectService _projectService;


    private readonly List<Guid> _dataSourceIds = new();
    private Guid _projectId;
    private StepType stepType = StepType.Profile;


    public DataProfileBuilder(IServiceProvider serviceProvider)
    {
        _projectService = serviceProvider.GetRequiredService<IProjectService>();
    }

    public DataProfileBuilder WithProjectId(Guid projectId)
    {
        if (projectId == Guid.Empty)
            throw new ArgumentException("Project ID cannot be empty.", nameof(projectId));
        _projectId = projectId;
        return this;
    }
    public DataProfileBuilder WithDataSourceId(Guid dataSourceId)
    {
        if (dataSourceId == Guid.Empty)
            throw new ArgumentException("Data Source ID cannot be empty.", nameof(dataSourceId));
        _dataSourceIds.Add(dataSourceId);
        return this;
    }
    public DataProfileBuilder WithDataSourceIds(List<Guid> dataSourceIds)
    {
        if (dataSourceIds == null || !dataSourceIds.Any())
            throw new ArgumentException("Data Source IDs cannot be null or empty.", nameof(dataSourceIds));
        _dataSourceIds.AddRange(dataSourceIds);
        return this;
    }

    public DataProfileBuilder EnableAdvanceProfiling()
    {
        stepType = StepType.AdvanceProfile;
        return this;
    }

    public async Task<ProjectRun> BuidAsync()
    {

        if (_projectId == Guid.Empty)
            throw new InvalidOperationException("Project ID is not set.");
        if (_dataSourceIds == null || !_dataSourceIds.Any())
            throw new InvalidOperationException("Data Source IDs are not set.");

        var stepInformation = new List<StepConfiguration>
        {
            // Add Profiling step
            new(stepType, _dataSourceIds.ToArray())
        };

        var queuedRun = await _projectService.StartNewRun(_projectId, stepInformation);
        Task.Delay(1000).Wait();
        return queuedRun;
    }

}
