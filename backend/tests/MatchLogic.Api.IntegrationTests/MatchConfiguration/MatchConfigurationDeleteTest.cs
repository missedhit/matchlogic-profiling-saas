using MatchLogic.Api.Common;
using MatchLogic.Api.Endpoints;
using MatchLogic.Api.Handlers.MatchConfiguration.Delete;
using MatchLogic.Api.IntegrationTests.Builders;
using MatchLogic.Api.IntegrationTests.Common;
using MatchLogic.Domain.MatchConfiguration;

namespace MatchLogic.Api.IntegrationTests.MatchConfiguration;

[Collection("MatchConfiguration Tests")]
public class MatchConfigurationDeleteTest : BaseApiTest
{
    public MatchConfigurationDeleteTest() : base(MatchConfigutaionEndpoints.PATH) { }

    #region Positive Test Cases

    [Fact]
    public async Task DeleteMatchConfiguration_ValidId_ShouldReturn_Success()
    {
        // Arrange
        var project = await CreateTestProjectAsync();
        var dataSources = await CreateTestDataSourceAsync(project, ["DataSourceA", "DataSourceB"]);
        var matchConfig = await CreateTestMatchConfigurationAsync(project, [
                new MatchingDataSourcePair(dataSources[0].Id, dataSources[1].Id)
            ]);

        // Act
        var response = await DeleteMatchConfigurationAsync(matchConfig.Id);

        // Assert
        AssertBaseSuccessResponse(response);
        response.Value.Id.Should().NotBeEmpty();
        response.Value.Id.Should().Be(matchConfig.Id);
        response.Value.Message.Should().Be("Match configuration deleted successfully.");
    }

    #endregion

    #region Negative Test Cases

    [Fact]
    public async Task DeleteMatchConfiguration_EmptyId_ShouldReturn_ValidationError()
    {
        // Arrange
        var invalidId = Guid.Empty;

        // Act
        var response = await DeleteMatchConfigurationAsync(invalidId);

        // Assert
        AssertBaseValidationErrors(response, [            
            ("MatchConfigurationId", ValidationMessages.CannotBeEmpty("Match configuration ID")),
            ("MatchConfigurationId", ValidationMessages.NotExists("Match configuration ID"))
        ]);
    }

    [Fact]
    public async Task DeleteMatchConfiguration_NonExistentId_ShouldReturn_ValidationError()
    {
        // Arrange
        var invalidId = Guid.NewGuid();

        // Act
        var response = await DeleteMatchConfigurationAsync(invalidId);

        // Assert
        AssertBaseSingleValidationError(response, "MatchConfigurationId", ValidationMessages.NotExists("Match configuration ID"));
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

    private async Task<Result<DeleteMatchConfigurationResponse>> DeleteMatchConfigurationAsync(Guid id)
    {
        return await httpClient.DeleteAndDeserializeAsync<Result<DeleteMatchConfigurationResponse>>($"{RequestURIPath}/{id}");
    }

    #endregion
}
