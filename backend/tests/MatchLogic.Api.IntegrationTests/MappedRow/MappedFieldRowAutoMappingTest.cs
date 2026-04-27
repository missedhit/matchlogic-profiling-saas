using MatchLogic.Api.Common;
using MatchLogic.Api.Endpoints;
using MatchLogic.Api.Handlers.MappedFieldRow.AutoMapping;
using MatchLogic.Api.IntegrationTests.Builders;
using MatchLogic.Api.IntegrationTests.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Api.IntegrationTests.MappedRow;
[Collection("MappedFieldRow Tests")]
public class MappedFieldRowAutoMappingTest : BaseApiTest
{
    public MappedFieldRowAutoMappingTest() : base(MatchDefinitionEndpoints.PATH) { }

    #region Positive Test Cases

    [Fact]
    public async Task AutoMapping_ValidProjectWithDefaultMapping_ShouldReturn_Success()
    {
        // Arrange
        var project = await CreateTestProjectAsync();
        var dataSources = await CreateTestDataSourceAsync(project, ["DataSourceA", "DataSourceB"]);

        var command = new AutoMappingCommand(project.Id, MatchDefinitionMappingType.Default);

        // Act
        var response = await AutoMappingAsync(project.Id, command);

        // Assert
        AssertBaseSuccessResponse(response);
        response.Value.Should().NotBeNull();
        response.Value.MappedFieldsRow.Should().NotBeNull();
    }

    [Fact]
    public async Task AutoMapping_ValidProjectWithSequentialMapping_ShouldReturn_Success()
    {
        // Arrange
        var project = await CreateTestProjectAsync();
        var dataSources = await CreateTestDataSourceAsync(project, ["DataSourceA", "DataSourceB"]);

        var command = new AutoMappingCommand(project.Id, MatchDefinitionMappingType.Sequential);

        // Act
        var response = await AutoMappingAsync(project.Id, command);

        // Assert
        AssertBaseSuccessResponse(response);
        response.Value.Should().NotBeNull();
        response.Value.MappedFieldsRow.Should().NotBeNull();
    }

    [Fact]
    public async Task AutoMapping_ProjectWithMultipleDataSources_ShouldReturn_MappedFields()
    {
        // Arrange
        var project = await CreateTestProjectAsync();
        var dataSources = await CreateTestDataSourceAsync(project, ["DataSourceA", "DataSourceB", "DataSourceC"]);

        var command = new AutoMappingCommand(project.Id, MatchDefinitionMappingType.Default);

        // Act
        var response = await AutoMappingAsync(project.Id, command);

        // Assert
        AssertBaseSuccessResponse(response);
        response.Value.Should().NotBeNull();
        response.Value.MappedFieldsRow.Should().NotBeNull();

        // Verify that mappings include all data sources
        if (response.Value.MappedFieldsRow.Any())
        {
            var firstRow = response.Value.MappedFieldsRow.First();
            firstRow.FieldsByDataSource.Should().NotBeEmpty();
        }
    }

    #endregion

    #region Negative Test Cases

    [Fact]
    public async Task AutoMapping_EmptyProjectId_ShouldReturn_ValidationError()
    {
        // Arrange
        var command = new AutoMappingCommand(Guid.Empty, MatchDefinitionMappingType.Default);

        // Act
        var response = await AutoMappingAsync(Guid.Empty, command);

        // Assert
        AssertBaseValidationErrors(response, [
            ("projectId", ValidationMessages.Required("Project ID")),
            ("projectId", ValidationMessages.NotExists("Project"))
        ]);
    }

    [Fact]
    public async Task AutoMapping_NonExistentProjectId_ShouldReturn_ValidationError()
    {
        // Arrange
        var invalidProjectId = Guid.NewGuid();
        var command = new AutoMappingCommand(invalidProjectId, MatchDefinitionMappingType.Default);

        // Act
        var response = await AutoMappingAsync(invalidProjectId, command);

        // Assert
        AssertBaseSingleValidationError(response, "projectId", ValidationMessages.NotExists("Project"));
    }   

    #endregion

    #region Helper Methods

    private async Task<Result<AutoMappingResponse>> AutoMappingAsync(Guid projectId, AutoMappingCommand command)
    {
        var url = $"{RequestURIPath}/MappedRow/AutoMapping/{projectId}";
        return await httpClient.PutAndDeserializeAsync<Result<AutoMappingResponse>>(url, StringContentHelpers.FromModelAsJson(command));
    }

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
    #endregion
}
