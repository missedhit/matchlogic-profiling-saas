using MatchLogic.Api.Handlers.MatchConfiguration.Create;
using MatchLogic.Api.Handlers.MatchConfiguration;
using MatchLogic.Api.IntegrationTests.Builders;
using MatchLogic.Api.IntegrationTests.Common;
using MatchLogic.Api.Endpoints;
using MatchLogic.Api.Common;

namespace MatchLogic.Api.IntegrationTests.MatchConfiguration;

[Collection("MatchConfiguration Tests")]
public class MatchConfigurationCreateTest : BaseApiTest
{
    public MatchConfigurationCreateTest() : base(MatchConfigutaionEndpoints.PATH) {
    }

    #region Positive Test Cases

    [Fact]
    public async Task CreateMatchConfiguration_ValidRequest_ShouldReturn_Success()
    {
        // Arrange
        var project = await CreateTestProjectAsync();
        var dataSources = await CreateTestDataSourceAsync(project, ["DataSourceA", "DataSourceB"]);

        var request = new CreateMatchConfigurationRequest
        {
            ProjectId = project.Id,
            Pairs =
            [
                // Single valid pair
                new BaseDataSourcePairDTO(dataSources[0].Id, dataSources[1].Id) 
            ]
        };

        // Act
        var response = await CreateMatchConfiguration(request);

        // Assert
        AssertMatchConfigurationSuccess(request, response);

    }

    

    [Fact]
    public async Task CreateMatchConfiguration_MultipleValidPairs_ShouldReturn_Success()
    {
        // Arrange
        var project = await CreateTestProjectAsync();
        var dataSources = await CreateTestDataSourceAsync(project, ["DataSourceA", "DataSourceB", "DataSourceC"]);
        var request = new CreateMatchConfigurationRequest
        {
            ProjectId = project.Id,
            Pairs =
            [
                // Multiple valid pairs
                new BaseDataSourcePairDTO(dataSources[0].Id, dataSources[1].Id),
                new BaseDataSourcePairDTO(dataSources[1].Id, dataSources[2].Id)
            ]
        };

        // Act
        var response = await CreateMatchConfiguration(request);

        // Assert
        AssertMatchConfigurationSuccess(request, response);
    }

    #endregion

    #region Negative Test Cases

    [Fact]
    public async Task CreateMatchConfiguration_EmptyProjectId_ShouldReturn_ValidationError()
    {
        // Arrange
        var dsA = Guid.NewGuid();
        var dsB = Guid.NewGuid();

        var request = new CreateMatchConfigurationRequest
        {
            ProjectId = Guid.Empty, // Empty ProjectId
            Pairs =
            [
                new BaseDataSourcePairDTO(dsA, dsB)
            ]
        };

        // Act
        var response = await CreateMatchConfiguration(request);

        // Assert
        AssertBaseValidationErrors(response, [
            ("ProjectId", ValidationMessages.Required("Project ID")),
            ("ProjectId", ValidationMessages.NotExists("Project")),
            ("Pairs[0]", "Each pair must reference valid data source IDs belonging to the specified project."),
            ]);
    }

    [Fact]
    public async Task CreateMatchConfiguration_NonExistentProjectId_ShouldReturn_ValidationError()
    {
        // Arrange
        var dsA = Guid.NewGuid();
        var dsB = Guid.NewGuid();

        var request = new CreateMatchConfigurationRequest
        {
            ProjectId = Guid.NewGuid(), // Non-existent
            Pairs = 
            [
                new BaseDataSourcePairDTO(dsA, dsB)
            ]
        };

        // Act
        var response = await CreateMatchConfiguration(request);

        // Assert
        AssertBaseValidationErrors(response, [
            ("ProjectId", ValidationMessages.NotExists("Project")),
            ("Pairs[0]", "Each pair must reference valid data source IDs belonging to the specified project."),
            ]);
    }

    [Fact]
    public async Task CreateMatchConfiguration_NullPairs_ShouldReturn_ValidationError()
    {
        // Arrange
        var project = await CreateTestProjectAsync();

        var request = new CreateMatchConfigurationRequest
        {
            ProjectId = project.Id,
            Pairs = null // Null Pairs
        };

        // Act
        var response = await CreateMatchConfiguration(request);

        // Assert
        AssertBaseValidationErrors(response, [

            ("Pairs",  ValidationMessages.CannotBeEmpty("Pairs")),
            ("Pairs",  ValidationMessages.CannotBeNull("Pairs")),
            ]);
    }

    [Fact]
    public async Task CreateMatchConfiguration_EmptyPairs_ShouldReturn_ValidationError()
    {
        // Arrange
        var project = await CreateTestProjectAsync();

        var request = new CreateMatchConfigurationRequest
        {
            ProjectId = project.Id,
            Pairs = [] // Empty pairs list
        };

        // Act
        var response = await CreateMatchConfiguration(request);

        // Assert
        AssertBaseSingleValidationError(response, "Pairs", ValidationMessages.CannotBeEmpty("Pairs"));
    }

    [Fact]
    public async Task CreateMatchConfiguration_PairWithEmptyDataSourceA_ShouldReturn_ValidationError()
    {
        // Arrange
        var project = await CreateTestProjectAsync();

        var dataSources = await CreateTestDataSourceAsync(project, ["DataSourceB"]);

        var request = new CreateMatchConfigurationRequest
        {
            ProjectId = project.Id,
            Pairs =
            [
                new BaseDataSourcePairDTO(Guid.Empty, dataSources.First().Id)// Empty DataSourceA
            ]
        };

        // Act
        var response = await CreateMatchConfiguration(request);

        // Assert
        AssertBaseSingleValidationError(response, "Pairs[0]", "Each pair must reference valid data source IDs belonging to the specified project.");
    }

    [Fact]
    public async Task CreateMatchConfiguration_PairWithEmptyDataSourceB_ShouldReturn_ValidationError()
    {
        // Arrange
        var project = await CreateTestProjectAsync();

        var dataSources = await CreateTestDataSourceAsync(project, ["DataSourceA"]);

        var request = new CreateMatchConfigurationRequest
        {
            ProjectId = project.Id,
            Pairs =
            [
                new BaseDataSourcePairDTO(dataSources.First().Id, Guid.Empty)// Empty DataSourceB
            ]
        };

        // Act
        var response = await CreateMatchConfiguration(request);

        // Assert
        AssertBaseSingleValidationError(response, "Pairs[0]", "Each pair must reference valid data source IDs belonging to the specified project.");
    }

    [Fact]
    public async Task CreateMatchConfiguration_PairWithNonExistentDataSourceA_ShouldReturn_ValidationError()
    {
        // Arrange
        var project = await CreateTestProjectAsync();
        var dataSources = await CreateTestDataSourceAsync(project, ["DataSourceB"]);

        var request = new CreateMatchConfigurationRequest
        {
            ProjectId = project.Id,
            Pairs =
            [
                new BaseDataSourcePairDTO(Guid.NewGuid(), dataSources.First().Id) // Non-existent A
            ]
        };

        // Act
        var response = await CreateMatchConfiguration(request);

        // Assert
        AssertBaseSingleValidationError(response, "Pairs[0]", "Each pair must reference valid data source IDs belonging to the specified project.");
    }

    [Fact]
    public async Task CreateMatchConfiguration_PairWithNonExistentDataSourceB_ShouldReturn_ValidationError()
    {
        // Arrange
        var project = await CreateTestProjectAsync();
        var dataSources = await CreateTestDataSourceAsync(project, ["DataSourceA"]);

        var request = new CreateMatchConfigurationRequest
        {
            ProjectId = project.Id,
            Pairs =
            [
                new BaseDataSourcePairDTO(dataSources.First().Id, Guid.NewGuid()) // Non-existent B
            ]
        };

        // Act
        var response = await CreateMatchConfiguration(request);

        // Assert
        AssertBaseSingleValidationError(response, "Pairs[0]", "Each pair must reference valid data source IDs belonging to the specified project.");
    }

    [Fact]
    public async Task CreateMatchConfiguration_PairWithDataSourcesNotBelongingToProject_ShouldReturn_ValidationError()
    {
        // Arrange
        var project = await CreateTestProjectAsync();
        var otherProject = await CreateTestProjectAsync();
        var dataSources = await CreateTestDataSourceAsync(project, ["DataSourceA", "DataSourceB"]);

        var request = new CreateMatchConfigurationRequest
        {
            ProjectId = otherProject.Id, // Other ProjectId does not belong to Pair DataSources
            Pairs =
            [
                new BaseDataSourcePairDTO(dataSources[0].Id, dataSources[1].Id)
            ]
        };

        // Act
        var response = await CreateMatchConfiguration(request);

        // Assert
        AssertBaseSingleValidationError(response, "Pairs[0]", "Each pair must reference valid data source IDs belonging to the specified project.");
    }

    #endregion

    #region Helper Methods

    private async Task<Project> CreateTestProjectAsync()
    {
        // Implement using your builder or test utility
        return await new ProjectBuilder(GetServiceProvider()).WithValid().BuildAsync();
    }

    private async Task<List<DataSource>> CreateTestDataSourceAsync(Project projectId, string[] DataSource)
    {
        // Implement using your builder or test utility
        var dataSource = await new DataSourceBuilder(GetServiceProvider())
            .WithProject(projectId)
            .AddMultipleDataSourcesAsync(DataSource);
        return dataSource.DataSources;
    }
    private void AssertMatchConfigurationSuccess(CreateMatchConfigurationRequest request, Result<BaseMatchConfigurationResponse> response)
    {
        AssertBaseSuccessResponse(response);
        response.Value.Should().NotBeNull();
        response.Value.Pairs.Should().NotBeNull();
        response.Value.Pairs.ProjectId.Should().NotBeEmpty();
        response.Value.Pairs.ProjectId.Should().Be(request.ProjectId);
        response.Value.Pairs.Count.Should().Be(request.Pairs.Count);
        response.Value.Pairs.Id.Should().NotBeEmpty();
        for (int i = 0; i < request.Pairs.Count; i++)
        {
            response.Value.Pairs[i].Should().NotBeNull();
            response.Value.Pairs[i].DataSourceA.Should().NotBeEmpty();
            response.Value.Pairs[i].DataSourceB.Should().NotBeEmpty();

            response.Value.Pairs[i].DataSourceA.Should().Be(request.Pairs[i].DataSourceA);
            response.Value.Pairs[i].DataSourceB.Should().Be(request.Pairs[i].DataSourceB);
        }
    }

    private async Task<Result<BaseMatchConfigurationResponse>> CreateMatchConfiguration(CreateMatchConfigurationRequest request)
    {
        return await httpClient.PostAndDeserializeAsync<Result<BaseMatchConfigurationResponse>>(RequestURIPath, StringContentHelpers.FromModelAsJson(request));
    }
    #endregion
}
