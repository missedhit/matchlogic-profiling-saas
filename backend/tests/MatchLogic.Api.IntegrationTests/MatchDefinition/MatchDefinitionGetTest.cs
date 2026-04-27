using MatchLogic.Api.Common;
using MatchLogic.Api.Endpoints;
using MatchLogic.Api.Handlers.MatchDefinition.Get;
using MatchLogic.Api.IntegrationTests.Builders;
using MatchLogic.Api.IntegrationTests.Common;
using MatchLogic.Application.Features.MatchDefinition.DTOs;
using MatchLogic.Domain.MatchConfiguration;
using Mapster;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Api.IntegrationTests.MatchDefinition;
[Collection("MatchDefinition Tests")]
public class MatchDefinitionGetTest : BaseApiTest
{
    public MatchDefinitionGetTest() : base(MatchDefinitionEndpoints.PATH) { }

    #region Positive Test Cases

    [Fact]
    public async Task GetMatchDefinition_ValidProjectId_ShouldReturn_Success()
    {
        // Arrange
        var project = await CreateTestProjectAsync();
        var dataSources = await CreateTestDataSourceAsync(project, ["DataSourceA", "DataSourceB"]);

        var dataSourcePair = await CreateTestMatchConfigurationAsync(project, [
                new MatchingDataSourcePair(dataSources[0].Id, dataSources[1].Id)
            ]);

        // Create match definition first
        await CreateTestMatchDefinitionAsync(project, dataSources);

        // Act
        var response = await GetMatchDefinitionAsync(project.Id);

        // Assert
        AssertBaseSuccessResponse(response);
        response.Value.Should().NotBeNull();
        response.Value.MatchDefinition.Should().NotBeNull();
        response.Value.MatchSetting.Should().NotBeNull();
        response.Value.MatchDefinition.ProjectId.Should().Be(project.Id);
    }

    [Fact]
    public async Task GetMatchDefinition_WithMultipleDefinitions_ShouldReturn_AllDefinitions()
    {
        // Arrange
        var project = await CreateTestProjectAsync();
        var dataSources = await CreateTestDataSourceAsync(project, ["DataSourceA", "DataSourceB", "DataSourceC"]);

        var dataSourcePair = await CreateTestMatchConfigurationAsync(project, [
                new MatchingDataSourcePair(dataSources[0].Id, dataSources[1].Id)
            ]);

        // Create match definition with multiple definitions
        await CreateTestMatchDefinitionWithMultipleDefinitionsAsync(project, dataSources);


        // Act
        var response = await GetMatchDefinitionAsync(project.Id);

        // Assert
        AssertBaseSuccessResponse(response);
        response.Value.Should().NotBeNull();
        response.Value.MatchDefinition.Definitions.Should().NotBeEmpty();
        response.Value.MatchDefinition.Definitions.Count.Should().BeGreaterThan(1);
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

    private async Task<Result<MatchDefinitionResponse>> GetMatchDefinitionAsync(Guid projectId)
    {
        var url = $"{RequestURIPath}/{projectId}";
        return await httpClient.GetAndDeserializeAsync<Result<MatchDefinitionResponse>>(url);
    }

    private async Task<MatchingDataSourcePairs> CreateTestMatchConfigurationAsync(Project project, List<MatchingDataSourcePair> pairs)
    {
        return await new MatchConfigurationBuilder(GetServiceProvider())
            .WithProjectId(project.Id)
            .WithPairs(pairs)
            .BuildAsync();
    }

    public async Task<MatchDefinitionCollectionMappedRowDto> CreateTestMatchDefinitionAsync( Project project, List<DataSource> dataSources)
    {
        return await new MatchDefinitionBuilder(GetServiceProvider())
            .WithProject(project)
            .WithDataSources(dataSources)
            .WithSingleDefinition()
            .BuildAsync();
    }

    public async Task<MatchDefinitionCollectionMappedRowDto> CreateTestMatchDefinitionWithMultipleDefinitionsAsync(Project project, List<DataSource> dataSources)
    {
        return await new MatchDefinitionBuilder(GetServiceProvider())
            .WithProject(project)
            .WithDataSources(dataSources)
            .WithMultipleDefinitions()
            .BuildAsync();
    }
    #endregion
}
