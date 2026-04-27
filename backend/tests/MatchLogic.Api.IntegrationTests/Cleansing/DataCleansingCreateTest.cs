using MatchLogic.Api.Endpoints;
using MatchLogic.Api.Handlers.Cleansing.Create;
using MatchLogic.Api.Handlers.Cleansing;
using MatchLogic.Api.IntegrationTests.Common;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.CleansingAndStandaradization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using MatchLogic.Api.IntegrationTests.Builders;

namespace MatchLogic.Api.IntegrationTests.Cleansing;
[Collection("DataCleansing Create Tests")]
public class DataCleansingCreateTest : BaseApiTest
{
    private readonly IProjectService _projectService;

    public DataCleansingCreateTest() : base(DataCleansingEndpoints.PATH)
    {
        _projectService = GetService<IProjectService>();
    }

    [Fact]
    public async Task ApplyRules_WithValidRequest_ShouldReturn_Success()
    {        
        // Arrange
        var tuple = await new DataSourceBuilder(GetServiceProvider())
         .AddSingleDataSourceAsync("DataSource1");

        var request = CreateValidCleansingRuleRequest(tuple.DataSource.ProjectId, tuple.DataSource.Id);

        // Act
        var response = await ApplyRulesAsync(request);

        // Assert
        AssertSuccessResponse(response);
    }

    [Fact]
    public async Task ApplyRules_WithInvalidProjectId_ShouldReturn_Error()
    {
        // Arrange
        var request = CreateValidCleansingRuleRequest(Guid.NewGuid(), Guid.NewGuid());

        // Act
        var response = await ApplyRulesAsync(request);

        // Assert
        AssertFailedResponse(response, "Project does not exist");
    }

    [Fact]
    public async Task ApplyRules_WithInvalidDataSourceId_ShouldReturn_Error()
    {
        // Arrange
        var project = await CreateTestProject();
        var request = CreateValidCleansingRuleRequest(project.Id, Guid.NewGuid());

        // Act
        var response = await ApplyRulesAsync(request);

        // Assert
        AssertFailedResponse(response, "DataSource does not exist");
    }

    [Fact]
    public async Task ApplyRules_WithEmptyColumnOperations_ShouldReturn_Success()
    {
        // Arrange
        var tuple = await new DataSourceBuilder(GetServiceProvider())
         .AddSingleDataSourceAsync("DataSource1");

        var request = new CreateCleansingRule
        {
            ProjectId = tuple.DataSource.ProjectId,
            DataSourceId = tuple.DataSource.Id,
            ColumnOperations = new List<ColumnOperationModel>()
        };

        // Act
        var response = await ApplyRulesAsync(request);

        // Assert
        AssertSuccessResponse(response);
    }

    private async Task<Result<CreateCleansingRuleResponse>> ApplyRulesAsync(CreateCleansingRule request)
    {
        return await httpClient.PostAndDeserializeAsync<Result<CreateCleansingRuleResponse>>(
            $"{RequestURIPath}/ApplyRules",
            StringContentHelpers.FromModelAsJson(request));
    }

    private CreateCleansingRule CreateValidCleansingRuleRequest(Guid projectId, Guid dataSourceId)
    {
        return new CreateCleansingRule
        {
            ProjectId = projectId,
            DataSourceId = dataSourceId,
            ColumnOperations = new List<ColumnOperationModel>
            {
                new ColumnOperationModel
                {
                    ColumnName = "Name",
                    CopyField = true,
                    Operations = new List<OperationModel>
                    {
                        new OperationModel
                        {
                            Type = OperationType.Standard,
                            CleaningType = CleaningRuleType.Trim,
                            Parameters = new Dictionary<string, string>()
                        },
                        new OperationModel
                        {
                            Type = OperationType.Standard,
                            CleaningType = CleaningRuleType.UpperCase,
                            Parameters = new Dictionary<string, string>()
                        }
                    }
                }
            }
        };
    }

    private async Task<Project> CreateTestProject()
    {
        var projectName = $"Test Project {Guid.NewGuid()}";
        return await _projectService.CreateProject(projectName, "Test project for cleansing operations");
    }    

    private void AssertSuccessResponse(Result<CreateCleansingRuleResponse> response)
    {
        AssertBaseSuccessResponse(response);
        response.Value.ProjectRun.Should().NotBeNull();
        response.Value.ProjectRun.Id.Should().NotBe(Guid.Empty);
    }

    private void AssertFailedResponse(Result<CreateCleansingRuleResponse> response, string errorMessageContains)
    {
        response.Should().NotBeNull();
        response.IsSuccess.Should().BeFalse();
        response.ValidationErrors.Should().NotBeNullOrEmpty();
        response.ValidationErrors.Should().Contain(e => e.ErrorMessage.Contains(errorMessageContains));
    }
}
