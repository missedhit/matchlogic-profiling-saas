using MatchLogic.Api.Common;
using MatchLogic.Api.Endpoints;
using MatchLogic.Api.Handlers.Project.Delete;
using MatchLogic.Api.IntegrationTests.Builders;
using MatchLogic.Api.IntegrationTests.Common;
using MatchLogic.Application.Interfaces.Project;

namespace MatchLogic.Api.IntegrationTests.Projects;

[Collection("Delete Project Tests")]
public class ProjectDeleteTest : BaseApiTest
{
    private readonly IProjectService projectService;
    public ProjectDeleteTest() : base(ProjectEndpoints.PATH)
    {
        projectService = GetService<IProjectService>();
    }

    [Fact]
    public async Task DeleteProject_ShouldReturn_NoContent()
    {
        // Arrange
        var newProject = await new ProjectBuilder(GetServiceProvider())
            .BuildAsync();
        var projectId = newProject.Id;

        // Act
        var response = await DeleteProjectAsync(projectId);
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => projectService.GetProjectById(projectId));

        // Assert
        AssertProjectNotFound(exception, projectId);
        //AssertResponseNoContent(response);
        AssertBaseSuccessResponse(response, ResultStatus.NoContent);
    }

    [Fact]
    public async Task DeleteProject_EmptyGuid_ShouldReturn_InvalidOperationException()
    {
        // Arrange
        var projectId = Guid.Empty;

        // Act
        var response = await DeleteProjectAsync(projectId);
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => projectService.GetProjectById(projectId));

        // Assert
        AssertProjectNotFound(exception, projectId);
        AssertResponseInvalid(response, new List<string>
        {
            //"'Guid' must not be empty.",
            ValidationMessages.Required("Project ID"),
            ValidationMessages.NotExists("Project")
        });
    }

    [Fact]
    public async Task DeleteProject_WhenProjectDoesNotExist_ShouldReturn_InvalidOperationException()
    {
        // Arrange
        var projectId = Guid.NewGuid();

        // Act
        var response = await DeleteProjectAsync(projectId);
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => projectService.GetProjectById(projectId));

        // Assert
        AssertProjectNotFound(exception, projectId);
        AssertResponseInvalid(response, new List<string> { ValidationMessages.NotExists("Project") });
    }

    [Fact]
    public async Task DeleteProject_InvalidGuid_ShouldReturn_HttpRequestException()
    {
        // Arrange
        var projectId = "InvalidGuid";

        // Act
        var response = await httpClient.DeleteAndDeserializeAsync<Result>($"{RequestURIPath}/{projectId}");
        // Assert
        AssertBaseExceptionMessage(response, "Failed to bind parameter \"Guid id\" from \"InvalidGuid\".");

    }


    private async Task<Result<DeleteProjectResponse>> DeleteProjectAsync(Guid projectId)
    {
        return await httpClient.DeleteAndDeserializeAsync<Result<DeleteProjectResponse>>(RequestURIPath + "/" + projectId);
    }


    private void AssertProjectNotFound(InvalidOperationException exception, Guid projectId)
    {
        exception.Message.Should().Be($"Project with ID {projectId} not found");
    }

    private void AssertResponseInvalid(Result<DeleteProjectResponse> response, List<string> expectedErrorMessages)
    {
        AssertBaseValidationErrors(response, expectedErrorMessages
           .Select(item => ("Guid", item))
           .ToList());
    }
}
