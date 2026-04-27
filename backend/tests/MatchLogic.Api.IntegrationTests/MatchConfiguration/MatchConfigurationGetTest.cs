using MatchLogic.Api.Common;
using MatchLogic.Api.Endpoints;
using MatchLogic.Api.Handlers.MatchConfiguration;
using MatchLogic.Api.IntegrationTests.Builders;
using MatchLogic.Api.IntegrationTests.Common;
using MatchLogic.Domain.MatchConfiguration;

namespace MatchLogic.Api.IntegrationTests.MatchConfiguration;

[Collection("MatchConfiguration Tests")]
public class MatchConfigurationGetTest : BaseApiTest
{
    public MatchConfigurationGetTest() : base(MatchConfigutaionEndpoints.PATH) { }

    #region Positive Test Cases

    [Fact]
    public async Task GetMatchConfiguration_ValidProjectId_ShouldReturn_Success()
    {
        // Arrange
        var project = await CreateTestProjectAsync();
        var dataSources = await CreateTestDataSourceAsync(project, ["DataSourceA", "DataSourceB"]);
        var matchConfig = await CreateTestMatchConfigurationAsync(project, [
                new MatchingDataSourcePair(dataSources[0].Id, dataSources[1].Id)
            ]);

        // Act
        var response = await GetMatchConfigurationAsync(project.Id);

        // Assert
        AssertBaseSuccessResponse(response);
        response.Value.Should().NotBeNull();
        response.Value.Pairs.Should().NotBeNull();
        response.Value.Pairs.ProjectId.Should().NotBeEmpty();
        response.Value.Pairs.ProjectId.Should().Be(project.Id);
        response.Value.Pairs.Count.Should().Be(1);
        response.Value.Pairs.Id.Should().NotBeEmpty();

        response.Value.Pairs[0].Should().NotBeNull();
        response.Value.Pairs[0].DataSourceA.Should().NotBeEmpty();
        response.Value.Pairs[0].DataSourceB.Should().NotBeEmpty();

        response.Value.Pairs[0].DataSourceA.Should().Be(dataSources[0].Id);
        response.Value.Pairs[0].DataSourceB.Should().Be(dataSources[1].Id);
    }

    [Fact]
    public async Task GetMatchConfiguration_ProjectWithNoConfiguration_ShouldReturn_EmptyPairs()
    {
        // Arrange
        var project = await CreateTestProjectAsync();

        // Act
        var response = await GetMatchConfigurationAsync(project.Id);

        // Assert
        AssertBaseSuccessResponse(response);
        response.Value.Should().NotBeNull();
        response.Value.Pairs.Should().BeNull();
    }

    #endregion

    #region Negative Test Cases

    [Fact]
    public async Task GetMatchConfiguration_EmptyProjectId_ShouldReturn_ValidationError()
    {
        // Arrange
        var invalidProjectId = Guid.Empty;

        // Act
        var response = await GetMatchConfigurationAsync(invalidProjectId);

        // Assert
        AssertBaseValidationErrors(response, [
            ("ProjectId", ValidationMessages.Required("Project ID")),
            ("ProjectId", ValidationMessages.NotExists("Project")),
        ]);
    }

    [Fact]
    public async Task GetMatchConfiguration_NonExistentProjectId_ShouldReturn_ValidationError()
    {
        // Arrange
        var invalidProjectId = Guid.NewGuid();

        // Act
        var response = await GetMatchConfigurationAsync(invalidProjectId);

        // Assert
        AssertBaseSingleValidationError(response, "ProjectId", ValidationMessages.NotExists("Project"));
    }

    #endregion

    #region Helper Methods
    
    private async Task<Project> CreateTestProjectAsync()
    {
        return await new ProjectBuilder(GetServiceProvider()).WithValid().BuildAsync();
    }

    private async Task<List<DataSource>> CreateTestDataSourceAsync(Project project, string[] names)
    {
        var dataSource = await new DataSourceBuilder(GetServiceProvider())
            .WithProject(project)
            .AddMultipleDataSourcesAsync(names);
        return dataSource.DataSources;
    }

    private async Task<MatchingDataSourcePairs> CreateTestMatchConfigurationAsync(Project project, List<MatchingDataSourcePair> pairs)
    {
        return await new MatchConfigurationBuilder(GetServiceProvider())
            .WithProjectId(project.Id)
            .WithPairs(pairs)
            .BuildAsync();
    }

    private async Task<Result<BaseMatchConfigurationResponse>> GetMatchConfigurationAsync(Guid projectId)
    {
        // GET: /api/match-configuration/?ProjectId={projectId}
        var url = $"{RequestURIPath}/?ProjectId={projectId}";
        return await httpClient.GetAndDeserializeAsync<Result<BaseMatchConfigurationResponse>>(url);
    }

    #endregion
}
