using MatchLogic.Api.Common;
using MatchLogic.Api.Endpoints;
using MatchLogic.Api.Handlers.MappedFieldRow.Update;
using MatchLogic.Api.IntegrationTests.Builders;
using MatchLogic.Api.IntegrationTests.Common;
using MatchLogic.Application.Features.MatchDefinition.DTOs;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Api.IntegrationTests.MappedRow;
[Collection("MappedFieldRow Tests")]
public class MappedFieldRowUpdateTest : BaseApiTest
{
    public MappedFieldRowUpdateTest() : base(MatchDefinitionEndpoints.PATH) { }

    #region Positive Test Cases

    [Fact]
    public async Task UpdateMappedFieldRow_ValidRequest_ShouldReturn_Success()
    {
        // Arrange
        var project = await CreateTestProjectAsync();
        var dataSources = await CreateTestDataSourceAsync(project, ["DataSourceA", "DataSourceB"]);
        var mappedFieldRows = CreateValidMappedFieldRows(dataSources);

        var command = new UpdateMappedFieldRowCommand(mappedFieldRows, project.Id);

        // Act
        var response = await UpdateMappedFieldRowAsync(project.Id, command);

        // Assert
        AssertBaseSuccessResponse(response);
        response.Value.Should().NotBeNull();
        response.Value.MappedFieldsRow.Should().NotBeNull();
        response.Value.MappedFieldsRow.Count.Should().Be(mappedFieldRows.Count);
    }

    [Fact]
    public async Task UpdateMappedFieldRow_WithIncludeFlags_ShouldReturn_Success()
    {
        // Arrange
        var project = await CreateTestProjectAsync();
        var dataSources = await CreateTestDataSourceAsync(project, ["DataSourceA", "DataSourceB"]);
        var mappedFieldRows = CreateValidMappedFieldRows(dataSources);

        // Set some rows to not include
        mappedFieldRows[0].Include = false;

        var command = new UpdateMappedFieldRowCommand(mappedFieldRows, project.Id);

        // Act
        var response = await UpdateMappedFieldRowAsync(project.Id, command);

        // Assert
        AssertBaseSuccessResponse(response);
        response.Value.Should().NotBeNull();
        response.Value.MappedFieldsRow[0].Include.Should().BeFalse();
    }

    #endregion

    #region Negative Test Cases

    [Fact]
    public async Task UpdateMappedFieldRow_EmptyProjectId_ShouldReturn_ValidationError()
    {
        // Arrange
        var mappedFieldRows = new List<MappedFieldRowDto>();
        var command = new UpdateMappedFieldRowCommand(mappedFieldRows, Guid.Empty);

        // Act
        var response = await UpdateMappedFieldRowAsync(Guid.Empty, command);

        // Assert
        AssertBaseValidationErrors(response, [
            ("projectId", ValidationMessages.Required("Project ID")),
            ("projectId", ValidationMessages.NotExists("Project")),
            ("mappedFieldRows", ValidationMessages.CannotBeEmpty("Mapped field rows"))            
        ]);
    }

    [Fact]
    public async Task UpdateMappedFieldRow_NonExistentProjectId_ShouldReturn_ValidationError()
    {
        // Arrange
        var invalidProjectId = Guid.NewGuid();
        var mappedFieldRows = new List<MappedFieldRowDto>();
        var command = new UpdateMappedFieldRowCommand(mappedFieldRows, invalidProjectId);

        // Act
        var response = await UpdateMappedFieldRowAsync(invalidProjectId, command);

        // Assert
        AssertBaseValidationErrors(response, [            
            ("projectId", ValidationMessages.NotExists("Project")),
            ("mappedFieldRows", ValidationMessages.CannotBeEmpty("Mapped field rows"))
        ]);
    }

    [Fact]
    public async Task UpdateMappedFieldRow_NullMappedFieldRows_ShouldReturn_ValidationError()
    {
        // Arrange
        var project = await CreateTestProjectAsync();
        var command = new UpdateMappedFieldRowCommand(null, project.Id);

        // Act
        var response = await UpdateMappedFieldRowAsync(project.Id, command);

        // Assert
        AssertBaseValidationErrors(response, [
            ("mappedFieldRows", ValidationMessages.CannotBeEmpty("Mapped field rows")),
            ("mappedFieldRows", ValidationMessages.CannotBeNull("Mapped field rows")),
        ]);
    }

    [Fact]
    public async Task UpdateMappedFieldRow_EmptyMappedFieldRows_ShouldReturn_ValidationError()
    {
        // Arrange
        var project = await CreateTestProjectAsync();
        var command = new UpdateMappedFieldRowCommand(new List<MappedFieldRowDto>(), project.Id);

        // Act
        var response = await UpdateMappedFieldRowAsync(project.Id, command);

        // Assert
        AssertBaseSingleValidationError(response, "mappedFieldRows", ValidationMessages.CannotBeEmpty("Mapped field rows"));
    }

    #endregion

    #region Helper Methods


    private async Task<Project> CreateTestProjectAsync()
    {
        return await new ProjectBuilder(GetServiceProvider()).WithValid().BuildAsync();
    }
    private List<MappedFieldRowDto> CreateValidMappedFieldRows(List<DataSource> dataSources)
    {
        return new List<MappedFieldRowDto>
        {
            new MappedFieldRowDto
            {
                Include = true,
                FieldsByDataSource = new Dictionary<string, FieldDto>
                {
                    [dataSources[0].Name.ToLower()] = new FieldDto
                    {
                        Id = Guid.NewGuid(),
                        Name = "Name",
                        DataSourceId = dataSources[0].Id,
                        DataSourceName = dataSources[0].Name
                    },
                    [dataSources[1].Name.ToLower()] = new FieldDto
                    {
                        Id = Guid.NewGuid(),
                        Name = "FullName",
                        DataSourceId = dataSources[1].Id,
                        DataSourceName = dataSources[1].Name
                    }
                }
            }
        };
    }
    
    private async Task<List<DataSource>> CreateTestDataSourceAsync(Project project, string[] names)
    {
        var dataSource = await new DataSourceBuilder(GetServiceProvider())
            .WithProject(project)
            .AddMultipleDataSourcesAsync(names);
        return dataSource.DataSources;
    }

    private async Task<Result<UpdateMappedFieldRowResponse>> UpdateMappedFieldRowAsync(Guid projectId, UpdateMappedFieldRowCommand command)
    {
        var url = $"{RequestURIPath}/MappedRow/{projectId}";
        return await httpClient.PutAndDeserializeAsync<Result<UpdateMappedFieldRowResponse>>(url, StringContentHelpers.FromModelAsJson(command));
    }

    #endregion
}
