using MatchLogic.Api.Common;
using MatchLogic.Api.Endpoints;
using MatchLogic.Api.Handlers.MappedFieldRow.Get;
using MatchLogic.Api.Handlers.MappedFieldRow.Update;
using MatchLogic.Api.Handlers.MappedFieldRow.AutoMapping;
using MatchLogic.Api.IntegrationTests.Builders;
using MatchLogic.Api.IntegrationTests.Common;
using MatchLogic.Application.Features.MatchDefinition.DTOs;

namespace MatchLogic.Api.IntegrationTests.MappedFieldRow;

[Collection("MappedFieldRow Tests")]
public class MappedFieldRowGetTest : BaseApiTest
{
    public MappedFieldRowGetTest() : base(MatchDefinitionEndpoints.PATH) { }

    #region Positive Test Cases

    [Fact]
    public async Task GetMappedFieldRow_ValidProjectId_ShouldReturn_Success()
    {
        // Arrange
        var project = await CreateTestProjectAsync();
        var dataSources = await CreateTestDataSourceAsync(project, ["DataSourceA", "DataSourceB"]);

        // Act
        var response = await GetMappedFieldRowAsync(project.Id);

        // Assert
        AssertBaseSuccessResponse(response);
        response.Value.Should().NotBeNull();
        response.Value.MappedFieldsRow.Should().NotBeNull();
    }

    [Fact]
    public async Task GetMappedFieldRow_ProjectWithExistingMappedRows_ShouldReturn_SavedData()
    {
        // Arrange
        var project = await CreateTestProjectAsync();
        var dataSources = await CreateTestDataSourceAsync(project, ["DataSourceA", "DataSourceB"]);
        

        // Act
        var response = await GetMappedFieldRowAsync(project.Id);

        // Assert
        AssertBaseSuccessResponse(response);
        response.Value.Should().NotBeNull();
        response.Value.MappedFieldsRow.Should().NotBeNull();
        response.Value.MappedFieldsRow.Should().NotBeEmpty();
    }

    #endregion

    #region Helper Methods

    private async Task<List<DataSource>> CreateTestDataSourceAsync(Project project, string[] names)
    {
        var dataSource = await new DataSourceBuilder(GetServiceProvider())
            .WithProject(project)
            .AddMultipleDataSourcesAsync(names);
        return dataSource.DataSources;
    }
    private async Task<Project> CreateTestProjectAsync()
    {
        return await new ProjectBuilder(GetServiceProvider()).WithValid().BuildAsync();
    }
    private async Task<Result<MappedFieldRowResponse>> GetMappedFieldRowAsync(Guid projectId)
    {
        var url = $"{RequestURIPath}/MappedRow/{projectId}";
        return await httpClient.GetAndDeserializeAsync<Result<MappedFieldRowResponse>>(url);
    }

    #endregion
}
