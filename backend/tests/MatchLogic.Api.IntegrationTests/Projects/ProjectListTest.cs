using MatchLogic.Api.Endpoints;
using MatchLogic.Api.Handlers.Project.List;
using MatchLogic.Api.IntegrationTests.Builders;
using MatchLogic.Api.IntegrationTests.Common;
using MatchLogic.Application.Interfaces.Project;

namespace MatchLogic.Api.IntegrationTests.Projects;

[Collection("ListTest Project Tests")]
public class ProjectListTest : BaseApiTest
{
    private readonly IProjectService projectService;
    public ProjectListTest() : base(ProjectEndpoints.PATH)
    {
        projectService = GetService<IProjectService>();
    }
    [Fact]
    public async Task ListProjects_ShouldReturn_Success()
    {
        // Arrange
        //Add New Project
        Project newProject = await new ProjectBuilder(GetServiceProvider())
            .BuildAsync();

        Guid projectId = newProject.Id;
        // Act
        var response = await httpClient.GetAndDeserializeAsync<Result<List<ProjectListResponse>>>(RequestURIPath);
        // Assert
        ProjectListResponse expectedItem = new ProjectListResponse(newProject.Id, newProject.Name, newProject.Description, newProject.CreatedAt, newProject.ModifiedAt);

        AssertBaseSuccessResponse(response);
        response.Value.Contains<ProjectListResponse>(expectedItem);
    }


    [Fact]
    public async Task ListProjects_WhenNoProjectsExist_ShouldReturn_EmptyList()
    {
        //Arrange
        await DeleteAllProjects();
        // Act
        var response = await httpClient.GetAndDeserializeAsync<Result<List<ProjectListResponse>>>(RequestURIPath);
        // Assert
        AssertBaseNotFoundResponseError(response, "No Projects Found");


    }
    private async Task DeleteAllProjects()
    {
        var projects = await projectService.GetAllProjects();
        foreach (var project in projects)
        {
            await projectService.DeleteProject(project.Id);
        }
    }
}
