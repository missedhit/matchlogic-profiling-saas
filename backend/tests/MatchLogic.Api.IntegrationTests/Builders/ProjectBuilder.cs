using MatchLogic.Application.Interfaces.Project;
using Microsoft.Extensions.DependencyInjection;

namespace MatchLogic.Api.IntegrationTests.Builders;
public class ProjectBuilder
{
    private readonly Project _project = new()
    {
        Name = "Test Project",
        Description = "Test Description"
    };
    private readonly IServiceProvider _serviceProvider;
    public ProjectBuilder(IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
    }

    public ProjectBuilder WithValid()
    {
        _project.Name = "Test Project";
        _project.Description = "Test Description";
        return this;
    }

    public ProjectBuilder WithId(Guid id)
    {
        _project.Id = id;
        return this;
    }

    public ProjectBuilder WithName(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Name cannot be null or empty.", nameof(name));

        _project.Name = name;
        return this;
    }

    public ProjectBuilder WithDescription(string description)
    {
        if (string.IsNullOrWhiteSpace(description))
            throw new ArgumentException("Description cannot be null or empty.", nameof(description));

        _project.Description = description;
        return this;
    }

    public Project BuildDomain() => _project;

    public Task<Project> BuildAsync() => _serviceProvider.GetService<IProjectService>().CreateProject(_project.Name, _project.Description);
}