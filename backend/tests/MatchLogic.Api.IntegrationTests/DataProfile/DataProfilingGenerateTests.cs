using MatchLogic.Api.Common;
using MatchLogic.Api.Endpoints;
using MatchLogic.Api.Handlers.DataProfile.GenerateProfile;
using MatchLogic.Api.IntegrationTests.Builders;
using MatchLogic.Api.IntegrationTests.Common;
using MatchLogic.Domain.Import;
using NPOI.SS.UserModel.Charts;
using System.Text.Json;

namespace MatchLogic.Api.IntegrationTests.DataProfile;
[Collection("DataProfile Generate Profile Tests")]
public class DataProfilingGenerateTests : BaseApiTest
{
    public DataProfilingGenerateTests() : base(DataProfilingEndpoints.PATH)
    {

    }
    #region Positive Test Cases

    [Fact]
    public async Task GenerateProfiling_ValidRequest_ShouldReturn_Success()
    {
        // Arrange
        var dataSource = await new DataSourceBuilder(GetServiceProvider())
            .AddMultipleDataSourcesAsync(["DataSource1", "DataSource2"]);
        var dataSourceIds = dataSource.DataSources.Select(x => x.Id).ToList();
        var request = new GenerateDataProfileRequest
        {
            ProjectId = dataSource.ProjectRun.ProjectId,
            DataSourceIds = dataSourceIds
        };

        // Act
        var response = await GenerateProfilingAsync(request);

        // Assert
        AssertSuccessResponse(response, request);
    }

    [Fact]
    public async Task GenerateProfiling_ValidRequestWithSingleDataSource_ShouldReturn_Success()
    {
        // Arrange
        var dataSource = await new DataSourceBuilder(GetServiceProvider())
            .AddSingleDataSourceAsync("DataSource1");

        var request = new GenerateDataProfileRequest
        {
            ProjectId = dataSource.ProjectRun.ProjectId,
            DataSourceIds = [dataSource.DataSource.Id]
        };

        // Act
        var response = await GenerateProfilingAsync(request);

        // Assert
        AssertSuccessResponse(response, request);
    }

    #endregion

    #region Negative Test Cases

    [Fact]
    public async Task GenerateProfiling_NullProjectId_ShouldReturn_InvalidError()
    {
        var dataSource = await new DataSourceBuilder(GetServiceProvider())
           .AddSingleDataSourceAsync("DataSource1");
        // Arrange
        var request = new GenerateDataProfileRequest
        {
            ProjectId = Guid.Empty,
            DataSourceIds = [dataSource.DataSource.Id]
        };

        // Act
        var response = await GenerateProfilingAsync(request);

        // Assert

        AssertBaseValidationErrors(response,
        [
            //("ProjectId", "'Project Id' must not be empty."),
            ("ProjectId", ValidationMessages.Required("Project ID")),
            ("ProjectId", ValidationMessages.NotExists("Project")),
            //("DataSourceIds", "One or more Data Sources do not exist or are invalid for the specified Project."),
        ]);
    }

    [Fact]
    public async Task GenerateProfiling_NonExistentProject_ShouldReturn_InvalidError()
    {
        // Arrange
        var dataSource = await new DataSourceBuilder(GetServiceProvider())
           .AddSingleDataSourceAsync("DataSource1");

        var request = new GenerateDataProfileRequest
        {
            ProjectId = Guid.NewGuid(), // Assume this ID does not exist in the database
            DataSourceIds = [dataSource.DataSource.Id]
        };

        // Act
        var response = await GenerateProfilingAsync(request);
        // Assert
        AssertBaseValidationErrors(response,
        [
            ("ProjectId", ValidationMessages.NotExists("Project")),
            ("DataSourceIds", ValidationMessages.InvalidForSpecified("Data Sources","Project")),
        ]);
    }

    //[Fact]
    //public async Task GenerateProfiling_NullDataSourceIds_ShouldReturn_InvalidError()
    //{
    //    // Arrange
    //    var request = new GenerateDataProfileRequest
    //    {
    //        ProjectId = Guid.NewGuid(),
    //        DataSourceIds = null
    //    };

    //    // Act
    //    var response = await GenerateProfilingAsync(request);

    //    // Assert
    //    AssertInvalidResponse(response, "DataSourceIds", "At least one Data Source is required. Data Source list should not be empty.");
    //}

    [Fact]
    public async Task GenerateProfiling_EmptyDataSourceIds_ShouldReturn_InvalidError()
    {
        // Arrange
        var dataSource = await new DataSourceBuilder(GetServiceProvider())
           .AddSingleDataSourceAsync("DataSource1");
        var request = new GenerateDataProfileRequest
        {
            ProjectId = dataSource.ProjectRun.ProjectId,
            DataSourceIds = []
        };

        // Act
        var response = await GenerateProfilingAsync(request);

        // Assert
        AssertInvalidResponse(response, "DataSourceIds", "At least one Data Source is required. Data Source list should not be empty.");
    }

    [Fact]
    public async Task GenerateProfiling_DuplicateDataSourceIds_ShouldReturn_InvalidError()
    {
        // Arrange
        var dataSource = await new DataSourceBuilder(GetServiceProvider())
           .AddSingleDataSourceAsync("DataSource1");
        var duplicateId = dataSource.DataSource.Id;
        var request = new GenerateDataProfileRequest
        {
            ProjectId = dataSource.ProjectRun.ProjectId,
            DataSourceIds = [duplicateId, duplicateId]
        };


        // Act
        var response = await GenerateProfilingAsync(request);

        // Assert
        AssertInvalidResponse(response, "DataSourceIds", ValidationMessages.MustBeUniqueInList("Data Source Id's"));
    }

    [Fact]
    public async Task GenerateProfiling_NonExistentDataSource_ShouldReturn_InvalidError()
    {
        // Arrange
        var dataSource = await new DataSourceBuilder(GetServiceProvider())
           .AddSingleDataSourceAsync("DataSource1");
        var request = new GenerateDataProfileRequest
        {
            ProjectId = dataSource.ProjectRun.ProjectId,
            DataSourceIds = [Guid.NewGuid()] // Assume this ID does not exist in the database
        };

        // Act
        var response = await GenerateProfilingAsync(request);

        // Assert
        AssertInvalidResponse(response, "DataSourceIds", ValidationMessages.InvalidForSpecified("Data Sources", "Project"));
    }

    #endregion
    private async Task<Result<GenerateDataProfileResponse>> GenerateProfilingAsync(GenerateDataProfileRequest request)
    {
        System.Diagnostics.Debug.WriteLine("Start : GenerateDataProfileRequest {0}", DateTime.Now.ToString());
        var watch = System.Diagnostics.Stopwatch.StartNew();

        var response = await httpClient.PostAndDeserializeAsync<Result<GenerateDataProfileResponse>>($"{RequestURIPath}/Generate", StringContentHelpers.FromModelAsJson(request));

        watch.Stop();

        // Get the elapsed time as a TimeSpan value.
        TimeSpan ts = watch.Elapsed;

        // Format and display the TimeSpan value.
        string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}",
            ts.Hours, ts.Minutes, ts.Seconds,
            ts.Milliseconds / 10);
        System.Diagnostics.Debug.WriteLine("End : GenerateDataProfileRequest {0} : ElapsedTime {1}", DateTime.Now.ToString(), elapsedTime);

        return response;
    }

    private void AssertSuccessResponse(Result<GenerateDataProfileResponse> response, GenerateDataProfileRequest request)
    {
        AssertBaseSuccessResponse(response);
        response.Value.Should().NotBeNull();
        response.Value.ProjectRun.Should().NotBeNull();
        response.Value.ProjectRun.Id.Should().NotBeEmpty();
        response.Value.ProjectRun.ProjectId.Should().NotBeEmpty();
        response.Value.ProjectRun.ProjectId.Should().Be(request.ProjectId);
        response.Value.ProjectRun.Status.Should().Be(RunStatus.InProgress);
    }

    private void AssertInvalidResponse(Result<GenerateDataProfileResponse> response, string identifier, string errorMessage)
    {
        AssertBaseSingleValidationError(response, identifier, errorMessage);
    }
}
