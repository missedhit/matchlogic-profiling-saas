using MatchLogic.Api.Common;
using MatchLogic.Api.Endpoints;
using MatchLogic.Api.Handlers.Project.ById;
using MatchLogic.Api.IntegrationTests.Builders;
using MatchLogic.Api.IntegrationTests.Common;
using MatchLogic.Application.Interfaces.Project;

namespace MatchLogic.Api.IntegrationTests.Projects;

[Collection("GetByIdTest Project Tests")]
public class ProjectGetByIdTest : BaseApiTest
{
    private readonly IProjectService projectService;

    public ProjectGetByIdTest() : base(ProjectEndpoints.PATH)
    {
        projectService = GetService<IProjectService>();
    }

    [Fact]
    public async Task GetById_ShouldReturnSuccess()
    {
        // Arrange
        Project newProject = await new ProjectBuilder(GetServiceProvider())
            .BuildAsync();
        // Act
        var response = await GetProjectByIdAsync(newProject.Id);
        // Assert
        AssertSuccessResponse(response, newProject);
    }

    [Fact]
    public async Task GetById_ShouldReturnInvalid_ForInvalidId()
    {
        // Arrange
        Guid projectId = Guid.Empty;
        // Act
        var response = await GetProjectByIdAsync(projectId);
        // Assert
        AssertInvalidResponse(response,
        [
            //"'Id' must not be empty.",
            ValidationMessages.Required("Project ID"),
            ValidationMessages.NotExists("Project")
        ]);
    }

    [Fact]
    public async Task GetById_ShouldReturnNotFound_ForNonExistingId()
    {
        // Arrange
        Guid projectId = Guid.NewGuid();
        // Act
        var response = await GetProjectByIdAsync(projectId);
        // Assert
        AssertInvalidResponse(response, new List<string>
        {
            ValidationMessages.NotExists("Project")
        });
    }

    [Fact]
    public async Task GetById_ShouldReturn_HttpRequestException_ForInvalidGuid()
    {
        // Arrange
        string projectId = "InvalidGuid";
        // Act
        var response = await httpClient.GetAndDeserializeAsync<Result>($"{RequestURIPath}/{projectId}");
        // Assert
        AssertBaseExceptionMessage(response, "Failed to bind parameter \"Guid id\" from \"InvalidGuid\".");

    }


    private async Task<Result<GetProjectResponse>> GetProjectByIdAsync(object projectId)
    {
        return await httpClient.GetAndDeserializeAsync<Result<GetProjectResponse>>($"{RequestURIPath}/{projectId}");
    }

    private void AssertSuccessResponse(Result<GetProjectResponse> response, Project newProject)
    {

        AssertBaseSuccessResponse(response);
        response.Value.Id.Should().NotBe(Guid.Empty);
        response.Value.Id.Should().Be(newProject.Id);
        response.Value.Name.Should().NotBeNullOrEmpty();
        response.Value.Name.Should().Be(newProject.Name);
        response.Value.Description.Should().NotBeNullOrEmpty();
        response.Value.Description.Should().Be(newProject.Description);
    }

    private void AssertInvalidResponse(Result<GetProjectResponse> response, List<string> expectedErrorMessages)
    {
        AssertBaseValidationErrors(response, expectedErrorMessages
            .Select(item => ("Id", item))
            .ToList());
    }
}

