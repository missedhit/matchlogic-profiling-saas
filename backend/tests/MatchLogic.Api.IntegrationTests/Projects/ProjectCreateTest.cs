using MatchLogic.Api.Common;
using MatchLogic.Api.Endpoints;
using MatchLogic.Api.Handlers.Project.Create;
using MatchLogic.Api.IntegrationTests.Common;   
using MatchLogic.Application.Interfaces.Project;
namespace MatchLogic.Api.IntegrationTests.Projects;

[Collection("CreateTest Project Tests")]
public class ProjectCreateTest : BaseApiTest
{
    private readonly IProjectService projectService;
    public ProjectCreateTest() : base(ProjectEndpoints.PATH)
    {
        projectService = GetService<IProjectService>();
    }

    #region Project Name Cases
    [Fact]
    public async Task CreateProject_ProjectName_ShouldReturn_Guid()
    {
        // Arrange
        var request = new CreateProjectRequest("Test Project", "Test Description");
        // Act
        var response = await CreateProjectAsync(request);
        // Assert
        AssertSuccessResponse(response, request);


    }

    [Theory]
    [InlineData("Project Name", "This is Description")]
    [InlineData("Project name", "This is Description")]
    [InlineData("project name", "This is Description")]
    [InlineData("ProjectName", "This is Description")]
    [InlineData("projectname", "This is Description")]
    public async Task CreateProject_ProjectName_DifferentNameCasing_ShouldReturn_Success(string name, string description)
    {
        // Arrange
        var request = new CreateProjectRequest(name, description);
        // Act
        var response = await CreateProjectAsync(request);
        // Assert
        AssertSuccessResponse(response, request);
    }

    [Fact]
    public async Task CreateProject_ProjectName_SameNameCasing_ShouldReturn_Invalid_AlreadyExists()
    {
        string projectName = "Project Name Test";
        string projectDescription = "This is Description";

        // Arrange
        var result = await projectService.CreateProject(projectName, projectDescription);
        var request = new CreateProjectRequest(result.Name, result.Description);
        // Act
        var response = await CreateProjectAsync(request);
        // Assert
        AssertInvalidResponse(response, "Name", ValidationMessages.AlreadyExists("Project Name"));
    }

    [Fact]
    public async Task CreateProject_ProjectName_Empty_ShouldReturn_InvalidError()
    {
        // Arrange
        var request = new CreateProjectRequest("", "This is Description");
        // Act
        var response = await CreateProjectAsync(request);
        // Assert
        AssertInvalidResponse(response, "Name", ValidationMessages.Required("Project name"));
    }

    [Fact]
    public async Task CreateProject_ProjectName_CharacterBelowOrEqual150Long_ShouldReturn_Success()
    {
        // Arrange
        var request = new CreateProjectRequest(new string('A', Application.Common.Constants.FieldLength.NameMaxLength), string.Empty);
        // Act
        var response = await CreateProjectAsync(request);
        // Assert
        AssertSuccessResponse(response, request);
    }

    [Fact]
    public async Task CreateProject_ProjectName_151CharactersLong_ShouldReturn_InvalidError()
    {
        // Arrange
        var request = new CreateProjectRequest(new string('A', Application.Common.Constants.FieldLength.NameMaxLength + 1), string.Empty);
        // Act
        var response = await CreateProjectAsync(request);
        // Assert
        AssertInvalidResponse(response, "Name", ValidationMessages.MaxLength("Project name", Application.Common.Constants.FieldLength.NameMaxLength));
    }
    #endregion

    #region Project Description Cases
    [Fact]
    public async Task CreateProject_ProjectDescription_Empty_ShouldNotReturn_InvalidError()
    {
        // Arrange
        var request = new CreateProjectRequest("Test Project", "");
        // Act
        var response = await CreateProjectAsync(request);
        // Assert
        AssertSuccessResponse(response, request);
    }

    [Fact]
    public async Task CreateProject_ProjectDescription_CharacterBelowOrEqual2000Long_ShouldNotReturn_InvalidError()
    {
        // Arrange
        var request = new CreateProjectRequest("Project Name Test", new string('A', Application.Common.Constants.FieldLength.DescriptionMaxLength));
        // Act
        var response = await CreateProjectAsync(request);
        // Assert
        AssertSuccessResponse(response, request);
    }

    [Fact]
    public async Task CreateProject_ProjectDescription_2001CharactersLong_ShouldReturn_InvalidError()
    {
        // Arrange
        var request = new CreateProjectRequest("Project Name Test", new string('A', Application.Common.Constants.FieldLength.DescriptionMaxLength + 1));
        // Act
        var response = await CreateProjectAsync(request);
        // Assert
        AssertInvalidResponse(response, "Description", ValidationMessages.MaxLength("Project description", Application.Common.Constants.FieldLength.DescriptionMaxLength));
    }


    #endregion

    private async Task<Result<CreateProjectResponse>> CreateProjectAsync(CreateProjectRequest request)
    {
        return await httpClient.PostAndDeserializeAsync<Result<CreateProjectResponse>>(RequestURIPath, StringContentHelpers.FromModelAsJson(request));
    }

    private void AssertSuccessResponse(Result<CreateProjectResponse> response, CreateProjectRequest request)
    {
        AssertBaseSuccessResponse(response);
        response.Value.Id.Should().NotBe(Guid.Empty);
        response.Value.Name.Should().NotBeNullOrEmpty();
        response.Value.Name.Should().Be(request.Name);
        response.Value.Description.Should().Be(request.Description);
        response.Value.CreatedAt.Date.Should().Be(DateTime.Now.Date);
        response.Value.ModifiedAt.Should().BeNull();
    }

    private void AssertInvalidResponse(Result<CreateProjectResponse> response, string identifier, string errorMessage)
    {
        AssertBaseSingleValidationError(response, identifier, errorMessage);
    }

}
