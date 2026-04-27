using MatchLogic.Api.Common;
using MatchLogic.Api.Endpoints;
using MatchLogic.Api.Handlers.MatchConfiguration.GetDataSources;
using MatchLogic.Api.IntegrationTests.Builders;
using MatchLogic.Api.IntegrationTests.Common;

namespace MatchLogic.Api.IntegrationTests.MatchConfiguration;

[Collection("MatchConfiguration Tests")]
public class MatchConfigurationDataSourcesTest : BaseApiTest
{
    public MatchConfigurationDataSourcesTest() : base(MatchConfigutaionEndpoints.PATH) { }

    #region Positive Test Cases

    [Fact]
    public async Task GetDataSources_ValidProjectId_ShouldReturn_DataSources()
    {
        // Arrange
        var project = await CreateTestProjectAsync();
        var dataSources = await CreateTestDataSourceAsync(project, ["DS1", "DS2", "DS3"]);

        // Act
        var response = await GetDataSourcesAsync(project.Id);

        // Assert
        AssertBaseSuccessResponse(response);
        response.Value.Should().NotBeNull();
        response.Value.Count.Should().Be(dataSources.Count);
        foreach (var ds in dataSources)
        {
            response.Value.Should().Contain(x => x.Id == ds.Id && x.Name == ds.Name);
        }
    }

    [Fact]
    public async Task GetDataSources_ProjectWithNoDataSources_ShouldReturn_EmptyList()
    {
        // Arrange
        var project = await CreateTestProjectAsync();

        // Act
        var response = await GetDataSourcesAsync(project.Id);

        // Assert

        AssertBaseNotFoundResponseError(response, $"No data sources found for project ID: {project.Id}.");
        response.Value.Should().BeNull();
    }

    #endregion

    #region Negative Test Cases

    [Fact]
    public async Task GetDataSources_EmptyProjectId_ShouldReturn_ValidationError()
    {
        // Arrange
        var invalidProjectId = Guid.Empty;

        // Act
        var response = await GetDataSourcesAsync(invalidProjectId);

        // Assert
        AssertBaseValidationErrors(response, [
            ("ProjectId", ValidationMessages.Required("Project ID")),
            ("ProjectId", ValidationMessages.NotExists("Project"))
        ]);
    }

    [Fact]
    public async Task GetDataSources_NonExistentProjectId_ShouldReturn_ValidationError()
    {
        // Arrange
        var invalidProjectId = Guid.NewGuid();

        // Act
        var response = await GetDataSourcesAsync(invalidProjectId);

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

    private async Task<Result<List<MatchConfigDataSourcesResponse>>> GetDataSourcesAsync(Guid projectId)
    {
        var url = $"{RequestURIPath}/DataSources?projectId={projectId}";
        return await httpClient.GetAndDeserializeAsync<Result<List<MatchConfigDataSourcesResponse>>>(url);
    }

    #endregion
}
