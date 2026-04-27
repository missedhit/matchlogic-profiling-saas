using MatchLogic.Api.Common;
using MatchLogic.Api.Endpoints;
using MatchLogic.Api.Handlers.Project.Update;
using MatchLogic.Api.IntegrationTests.Builders;
using MatchLogic.Api.IntegrationTests.Common;

namespace MatchLogic.Api.IntegrationTests.Projects;

[Collection("UpdateTest Project Tests")]
public class ProjectUpdateTest : BaseApiTest
{

    public ProjectUpdateTest() : base(ProjectEndpoints.PATH)
    {
    }

    #region Project Guid Cases
    [Fact]
    public async Task UpdateProject_ProjectId_GuidNotExists_ShouldReturnInvalidNotExists()
    {
        // Arrange
        var request = CreateUpdateProjectRequest(Guid.NewGuid(), "Updated Project Name", "Updated Project Description");
        // Act
        var response = await UpdateProjectAsync(request);
        // Assert
        AssertInvalidResponse(response, "Id", ValidationMessages.NotExists("Project"));
    }

    [Fact]
    public async Task UpdateProject_ProjectId_EmptyGuid_ShouldReturnInvalid()
    {
        // Arrange
        var request = CreateUpdateProjectRequest(Guid.Empty, "Updated Project Name Test", "Updated Project Description");
        // Act
        var response = await UpdateProjectAsync(request);
        // Assert
        AssertInvalidResponse(response,
        [
            //("Id", "'Id' must not be empty."),
            ("Id", ValidationMessages.Required("Project ID")),
            ("Id", ValidationMessages.NotExists("Project"))
        ]);
    }

    [Fact]
    public async Task UpdateProject_ProjectId_InvalidGuid_ShouldReturn_HttpRequestException()
    {
        // Arrange
        string projectId = "InvalidGuid";
        var invalidBody = new { Id = projectId, Name = "Updated Project Name Test", Description = "Updated Project Description" };
        // Act & Assert
        var exception = await Assert.ThrowsAsync<HttpRequestException>(() => httpClient.PutAndDeserializeAsync<Result<UpdateProjectResponse>>($"{RequestURIPath}/{projectId}", StringContentHelpers.FromModelAsJson(invalidBody)));
        AssertHttpRequestException(exception, System.Net.HttpStatusCode.MethodNotAllowed, "Response status code does not indicate success: 405 (Method Not Allowed).");
    }
    #endregion

    #region Project Name Cases
    [Fact]
    public async Task UpdateProject_ProjectName_ShouldReturnSuccess()
    {
        // Arrange
        Guid existingProj = await AddSampleAsync();
        var request = CreateUpdateProjectRequest(existingProj, "Updated Project Name Test", "Updated Project Description");
        // Act
        var response = await UpdateProjectAsync(request);
        // Assert
        AssertSuccessResponse(response, request, existingProj);
    }

    [Fact]
    public async Task UpdateProject_ProjectName_EmptyName_ShouldReturnInvalid()
    {
        // Arrange
        Guid existingProj = await AddSampleAsync();
        var request = CreateUpdateProjectRequest(existingProj, "", "Updated Project Description");
        // Act
        var response = await UpdateProjectAsync(request);
        // Assert
        AssertInvalidResponse(response, "Name", ValidationMessages.Required("Project name"));
    }

    [Theory]
    [InlineData("Project Name", "This is Description")]
    [InlineData("Project name", "This is Description")]
    [InlineData("project name", "This is Description")]
    [InlineData("ProjectName", "This is Description")]
    [InlineData("projectname", "This is Description")]
    public async Task UpdateProject_ProjectName_DifferentNameCasing_ShouldReturn_Success(string Name, string Description)
    {
        // Arrange
        Guid projectId = await AddSampleAsync();
        var request = CreateUpdateProjectRequest(projectId, Name, Description);
        // Act
        var response = await UpdateProjectAsync(request);
        // Assert
        AssertSuccessResponse(response, request, projectId);
    }

    [Fact]
    public async Task UpdateProject_ProjectName_SameNameCasing_ShouldReturn_Success()
    {
        // Arrange
        string projectName = "Test Update Project";
        string projectDescription = "Test Description";
        Project newProject = await new ProjectBuilder(GetServiceProvider())
            .WithName(projectName)
            .WithName(projectDescription)
            .BuildAsync();
        var request = CreateUpdateProjectRequest(newProject.Id, newProject.Name, projectDescription);
        // Act
        var response = await UpdateProjectAsync(request);
        // Assert
        AssertSuccessResponse(response, request, newProject.Id);
    }

    [Fact]
    public async Task UpdateProject_ProjectName_SameNameCasing_ShouldReturn_ProjectNameExists()
    {
        // Arrange
        string projectName = "Test Update Project";
        string projectDescription = "Test Description";

        Project newProject = await new ProjectBuilder(GetServiceProvider())
            .WithName(projectName)
            .WithName(projectDescription)
            .BuildAsync();

        Project newProject1 = await new ProjectBuilder(GetServiceProvider())
            .WithName(projectName)
            .WithName(projectDescription)
            .BuildAsync();
        var request = CreateUpdateProjectRequest(newProject.Id, newProject1.Name, projectDescription);
        // Act
        var response = await UpdateProjectAsync(request);
        // Assert
        AssertInvalidResponse(response, "Name", ValidationMessages.AlreadyExists("Project Name"));
    }

    [Fact]
    public async Task UpdateProject_ProjectName_CharacterBelowOrEqual150Long_ShouldReturn_Success()
    {
        // Arrange
        Guid projectId = await AddSampleAsync();
        var request = CreateUpdateProjectRequest(projectId, new string('A', Application.Common.Constants.FieldLength.NameMaxLength), string.Empty);
        // Act
        var response = await UpdateProjectAsync(request);
        // Assert
        AssertSuccessResponse(response, request, projectId);
    }

    [Fact]
    public async Task UpdateProject_ProjectName_151CharactersLong_ShouldReturn_InvalidError()
    {
        // Arrange
        Guid projectId = await AddSampleAsync();
        var request = CreateUpdateProjectRequest(projectId, new string('A', Application.Common.Constants.FieldLength.NameMaxLength + 1), string.Empty);
        // Act
        var response = await UpdateProjectAsync(request);
        // Assert
        AssertInvalidResponse(response, "Name", ValidationMessages.MaxLength("Project name", Application.Common.Constants.FieldLength.NameMaxLength));
    }
    #endregion

    #region Project Description Cases
    [Fact]
    public async Task UpdateProject_ProjectDescription_Empty_ShouldReturn_Success()
    {
        // Arrange
        Guid projectId = await AddSampleAsync();
        var request = CreateUpdateProjectRequest(projectId, "Test Project", string.Empty);
        // Act
        var response = await UpdateProjectAsync(request);
        // Assert
        AssertSuccessResponse(response, request, projectId);
    }

    [Fact]
    public async Task UpdateProject_ProjectDescription_CharacterBelowOrEqual2000Long_ShouldReturn_Success()
    {
        // Arrange
        Guid projectId = await AddSampleAsync();
        var request = CreateUpdateProjectRequest(projectId, "Project Name Test", new string('A', Application.Common.Constants.FieldLength.DescriptionMaxLength));
        // Act
        var response = await UpdateProjectAsync(request);
        // Assert
        AssertSuccessResponse(response, request, projectId);
    }

    [Fact]
    public async Task UpdateProject_ProjectDescription_2001CharactersLong_ShouldReturn_InvalidError()
    {
        // Arrange
        Guid projectId = await AddSampleAsync();
        var request = CreateUpdateProjectRequest(projectId, "Project Name Test", new string('A', Application.Common.Constants.FieldLength.DescriptionMaxLength + 1));
        // Act
        var response = await UpdateProjectAsync(request);
        // Assert
        AssertInvalidResponse(response, "Description", ValidationMessages.MaxLength("Project description", Application.Common.Constants.FieldLength.DescriptionMaxLength));
    }
    #endregion

    private async Task<Guid> AddSampleAsync()
    {
        Project newProject = await new ProjectBuilder(GetServiceProvider())
            .BuildAsync();
        return newProject.Id;
    }

    private UpdateProjectRequest CreateUpdateProjectRequest(Guid id, string name, string description)
    {
        return new UpdateProjectRequest(id, name, description);
    }
    private async Task<Result<UpdateProjectResponse>> UpdateProjectAsync(UpdateProjectRequest request)
    {
        return await httpClient.PutAndDeserializeAsync<Result<UpdateProjectResponse>>(RequestURIPath, StringContentHelpers.FromModelAsJson(request));
    }

    private void AssertInvalidResponse(Result<UpdateProjectResponse> response, string identifier, string errorMessage)
    {
        AssertBaseSingleValidationError(response, identifier, errorMessage);
    }

    private void AssertInvalidResponse(Result<UpdateProjectResponse> response, List<(string Identifier, string ErrorMessage)> errors)
    {
        AssertBaseValidationErrors(response, errors);
    }

    private void AssertSuccessResponse(Result<UpdateProjectResponse> response, UpdateProjectRequest request, Guid projectId)
    {
        AssertBaseSuccessResponse(response);
        response.Value.Id.Should().NotBe(Guid.Empty);
        response.Value.Id.Should().Be(projectId);
        response.Value.Name.Should().NotBeNullOrEmpty();
        response.Value.Name.Should().Be(request.Name);
        response.Value.Description.Should().Be(request.Description);
        response.Value.ModifiedAt.Should().NotBeNull();
        response.Value.ModifiedAt.Value.Date.Should().Be(DateTime.Now.Date);
    }

    private void AssertHttpRequestException(HttpRequestException exception, System.Net.HttpStatusCode statusCode, string message)
    {
        exception.Message.Should().Be(message);
        exception.StatusCode.Should().Be(statusCode);
    }
}
