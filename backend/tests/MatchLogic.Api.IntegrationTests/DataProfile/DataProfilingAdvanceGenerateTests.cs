using MatchLogic.Api.Common;
using MatchLogic.Api.Endpoints;
using MatchLogic.Api.Handlers.DataProfile.GenerateAdvanceProfile;
using MatchLogic.Api.IntegrationTests.Builders;
using MatchLogic.Api.IntegrationTests.Common;

namespace MatchLogic.Api.IntegrationTests.DataProfile;
[Collection("DataProfile Generate Advance Profile Tests")]
public class DataProfilingAdvanceGenerateTests : BaseApiTest
{
    public DataProfilingAdvanceGenerateTests() : base(DataProfilingEndpoints.PATH)
    {
    }

    #region Positive Test Cases

    [Fact]
    public async Task GenerateAdvanceProfiling_ValidRequest_ShouldReturn_Success()
    {
        // Arrange
        var dataSource = await new DataSourceBuilder(GetServiceProvider())
            .AddSingleDataSourceAsync("DataSource1");

        var request = new GenerateAdvanceDataProfileRequest
        {
            ProjectId = dataSource.ProjectRun.ProjectId,
            DataSourceIds = [dataSource.DataSource.Id]
        };

        // Act
        var response = await GenerateAdvanceProfilingAsync(request);

        // Assert
        AssertBaseSuccessResponse(response);
        response.Value.Should().NotBeNull();
        response.Value.ProjectRun.Should().NotBeNull();
        response.Value.ProjectRun.Id.Should().NotBeEmpty();
        response.Value.ProjectRun.ProjectId.Should().Be(request.ProjectId);
        //response.Value.ProjectRun.Id.Should().Contain(request.DataSourceIds.First());
        //response.Value.Result.Status.Should().Be("InProgress");
    }

    #endregion

    #region Negative Test Cases

    [Fact]
    public async Task GenerateAdvanceProfiling_NullProjectId_ShouldReturn_InvalidError()
    {
        var dataSource = await new DataSourceBuilder(GetServiceProvider())
            .AddSingleDataSourceAsync("DataSource1");

        var request = new GenerateAdvanceDataProfileRequest
        {
            ProjectId = Guid.Empty,
            DataSourceIds = [dataSource.DataSource.Id]
        };

        // Act
        var response = await GenerateAdvanceProfilingAsync(request);

        // Assert
        AssertBaseValidationErrors(response,
        [
            //("ProjectId", "'Project Id' must not be empty."),
            ("ProjectId", ValidationMessages.Required("Project ID")),
            ("ProjectId", ValidationMessages.NotExists("Project")),
        ]);
    }

    [Fact]
    public async Task GenerateAdvanceProfiling_NonExistentProject_ShouldReturn_InvalidError()
    {
        var dataSource = await new DataSourceBuilder(GetServiceProvider())
            .AddSingleDataSourceAsync("DataSource1");

        var request = new GenerateAdvanceDataProfileRequest
        {
            ProjectId = Guid.NewGuid(), // Assume this ID does not exist
            DataSourceIds = [dataSource.DataSource.Id]
        };

        // Act
        var response = await GenerateAdvanceProfilingAsync(request);

        // Assert
        AssertBaseValidationErrors(response,
        [
            ("ProjectId", ValidationMessages.NotExists("Project")),
            ("DataSourceIds",ValidationMessages.InvalidForSpecified("Data Sources","Project")),
        ]);
    }

    [Fact]
    public async Task GenerateAdvanceProfiling_EmptyDataSourceIds_ShouldReturn_InvalidError()
    {
        var dataSource = await new DataSourceBuilder(GetServiceProvider())
            .AddSingleDataSourceAsync("DataSource1");

        var request = new GenerateAdvanceDataProfileRequest
        {
            ProjectId = dataSource.ProjectRun.ProjectId,
            DataSourceIds = []
        };

        // Act
        var response = await GenerateAdvanceProfilingAsync(request);

        // Assert
        AssertInvalidResponse(response, "DataSourceIds", "At least one Data Source is required. Data Source list should not be empty.");
    }

    [Fact]
    public async Task GenerateAdvanceProfiling_DuplicateDataSourceIds_ShouldReturn_InvalidError()
    {
        var dataSource = await new DataSourceBuilder(GetServiceProvider())
            .AddSingleDataSourceAsync("DataSource1");

        var duplicateId = dataSource.DataSource.Id;
        var request = new GenerateAdvanceDataProfileRequest
        {
            ProjectId = dataSource.ProjectRun.ProjectId,
            DataSourceIds = [duplicateId, duplicateId]
        };

        // Act
        var response = await GenerateAdvanceProfilingAsync(request);

        // Assert
        AssertBaseValidationErrors(response, [
            ("DataSourceIds", ValidationMessages.MustBeUniqueInList("Data Source Id's")),
            ("DataSourceIds", ValidationMessages.InvalidForSpecified("Data Sources","Project"))
            ]);
    }

    [Fact]
    public async Task GenerateAdvanceProfiling_NonExistentDataSource_ShouldReturn_InvalidError()
    {
        var dataSource = await new DataSourceBuilder(GetServiceProvider())
            .AddSingleDataSourceAsync("DataSource1");

        var request = new GenerateAdvanceDataProfileRequest
        {
            ProjectId = dataSource.ProjectRun.ProjectId,
            DataSourceIds = [Guid.NewGuid()] // Assume this ID does not exist
        };

        // Act
        var response = await GenerateAdvanceProfilingAsync(request);

        // Assert
        AssertInvalidResponse(response, "DataSourceIds", ValidationMessages.InvalidForSpecified("Data Sources", "Project"));
    }

    #endregion

    private async Task<Result<GenerateAdvanceDataProfileResponse>> GenerateAdvanceProfilingAsync(GenerateAdvanceDataProfileRequest request)
    {
        var response = await httpClient.PostAndDeserializeAsync<Result<GenerateAdvanceDataProfileResponse>>(
            $"{RequestURIPath}/GenerateAdvance", StringContentHelpers.FromModelAsJson(request));
        return response;
    }

    private void AssertInvalidResponse(Result<GenerateAdvanceDataProfileResponse> response, string identifier, string errorMessage)
    {
        AssertBaseSingleValidationError(response, identifier, errorMessage);
    }
}
