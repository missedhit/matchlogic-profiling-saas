using MatchLogic.Api.Endpoints;
using MatchLogic.Api.Handlers.Cleansing.GetRules;
using MatchLogic.Api.IntegrationTests.Builders;
using MatchLogic.Api.IntegrationTests.Common;
using MatchLogic.Application.Features.CleansingAndStandardization.DTOs;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.CleansingAndStandaradization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Api.IntegrationTests.Cleansing;
public class DataCleansingGetRulesTest : BaseApiTest
{
    public DataCleansingGetRulesTest() : base(DataCleansingEndpoints.PATH)
    {
    }

    [Fact]
    public async Task GetRules_WithValidRequest_ShouldReturn_Success()
    {
        // Arrange
        var tuple = await new DataSourceBuilder(GetServiceProvider())
        .AddSingleDataSourceAsync("DataSource1");

        // Create cleansing rules for the data source
        await new CreateCleansingRuleCommandBuilder(GetServiceProvider())
            .WithProjectId(tuple.DataSource.ProjectId)
            .WithDataSourceId(tuple.DataSource.Id)
            .WithStandardRules(
            [
                new CleaningRuleDto
                {
                    ColumnName = "Name",
                    RuleType = CleaningRuleType.UpperCase,
                }
            ])
            .BuildAsync();

        var request = new GetCleansingRulesRequest(
            DataSourceId: tuple.DataSource.Id,
            ProjectId: tuple.DataSource.ProjectId
        );

        // Act
        var response = await GetRulesAsync(request);

        // Assert
        AssertBaseSuccessResponse(response);
        response.Value.Should().NotBeNull();
    }

    [Fact]
    public async Task GetRules_WithInvalidProjectId_ShouldReturn_Error()
    {
        // Arrange
        var tuple = await new DataSourceBuilder(GetServiceProvider())
        .AddSingleDataSourceAsync("DataSource1");

        var request = new GetCleansingRulesRequest(
            DataSourceId: tuple.DataSource.Id,
            ProjectId: Guid.NewGuid() // Non-existent project
        );

        // Act
        var response = await GetRulesAsync(request);

        // Assert
        AssertBaseSingleValidationError(response, "ProjectId", "Project does not exist.");
    }

    [Fact]
    public async Task GetRules_WithInvalidDataSourceId_ShouldReturn_Error()
    {
        // Arrange
        var project = await CreateTestProject();

        var request = new GetCleansingRulesRequest(
            DataSourceId: Guid.NewGuid(), // Non-existent data source
            ProjectId: project.Id
        );

        // Act
        var response = await GetRulesAsync(request);

        // Assert
        AssertBaseSingleValidationError(response, "DataSourceId", "DataSource does not exist.");
    }

    [Fact]
    public async Task GetRules_WithEmptyProjectId_ShouldReturn_Error()
    {
        // Arrange
        var tuple = await new DataSourceBuilder(GetServiceProvider())
        .AddSingleDataSourceAsync("DataSource1");

        var request = new GetCleansingRulesRequest(
            DataSourceId: tuple.DataSource.Id,
            ProjectId: Guid.Empty // Invalid
        );

        // Act
        var response = await GetRulesAsync(request);

        // Assert
        AssertBaseValidationErrors(response, [
            ("ProjectId", "'Project Id' must not be empty."),
            ("ProjectId", "Project Id is required."),
            ("ProjectId", "Project does not exist."),
            ]);
    }

    [Fact]
    public async Task GetRules_WithEmptyDataSourceId_ShouldReturn_Error()
    {
        // Arrange
        var project = await CreateTestProject();

        var request = new GetCleansingRulesRequest(
            DataSourceId: Guid.Empty, // Invalid
            ProjectId: project.Id
        );

        // Act
        var response = await GetRulesAsync(request);

        // Assert
        AssertBaseValidationErrors(response, [
            ("DataSourceId", "'Data Source Id' must not be empty."),
            ("DataSourceId", "DataSource Id is required."),
            ("DataSourceId", "DataSource does not exist."),
            ]);
    }

    // Helper for creating a test project
    private async Task<Project> CreateTestProject()
    {
        var projectName = $"Test Project {Guid.NewGuid()}";
        var projectService = GetService<IProjectService>();
        return await projectService.CreateProject(projectName, "Test project for get rules operations");
    }


    private async Task<Result<GetCleansingRulesResponse>> GetRulesAsync(GetCleansingRulesRequest request)
    {
       return await httpClient.GetAndDeserializeAsync<Result<GetCleansingRulesResponse>>(
            $"{RequestURIPath}/Rules?DataSourceId={request.DataSourceId}&ProjectId={request.ProjectId}"
        );
    }
}
