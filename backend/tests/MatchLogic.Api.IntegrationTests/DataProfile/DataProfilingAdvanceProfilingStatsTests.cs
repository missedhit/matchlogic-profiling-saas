using MatchLogic.Api.Common;
using MatchLogic.Api.Endpoints;
using MatchLogic.Api.Handlers.DataProfile.AdvanceStatisticAnalysis;
using MatchLogic.Api.IntegrationTests.Builders;
using MatchLogic.Api.IntegrationTests.Common;

namespace MatchLogic.Api.IntegrationTests.DataProfile;
public class DataProfilingAdvanceProfilingStatsTests : BaseApiTest
{
    public DataProfilingAdvanceProfilingStatsTests() : base(DataProfilingEndpoints.PATH)
    {
    }

    [Fact]
    public async Task GetStatisticalResponse_ValidDataSourceId_ShouldReturn_Success()
    {

        // Arrange
        var dataSource = await new DataSourceBuilder(GetServiceProvider())
            .AddSingleDataSourceAsync("DataSource1");
        //Generate Data Profile
        var projectRun = await new DataProfileBuilder(GetServiceProvider())
            .WithProjectId(dataSource.DataSource.ProjectId)
            .WithDataSourceId(dataSource.DataSource.Id)
            .EnableAdvanceProfiling()
            .BuidAsync();

        var request = dataSource.DataSource.Id;

        // Act
        var response = await GetStatisticalResponseAsync(request);

        // Assert
        AssertSuccessResponse(response, request);
    }

    [Fact]
    public async Task GetStatisticalResponse_NoDataProfile_ShouldReturn_InvalidError()
    {
        // Arrange
        var dataSource = await new DataSourceBuilder(GetServiceProvider())
            .AddSingleDataSourceAsync("DataSource1");
        var request = dataSource.DataSource.Id;// No profile data generated for this data source

        // Act
        var response = await GetStatisticalResponseAsync(request);

        // Assert
        AssertBaseNotFoundResponseError(response, $"No profile data found for DataSourceId: {dataSource.DataSource.Id}");
    }

    [Fact]
    public async Task GetStatisticalResponse_NullDataSourceId_ShouldReturn_InvalidError()
    {
        // Arrange
        var request = Guid.Empty; // Empty Guid

        // Act
        var response = await GetStatisticalResponseAsync(request);

        // Assert
        AssertBaseValidationErrors(response,
        [
            //("DataSourceId", "'Data Source Id' must not be empty."),
            ("DataSourceId", ValidationMessages.Required("DataSource Id")),
            ("DataSourceId", ValidationMessages.NotExists("DataSource")),
        ]);
    }

    [Fact]
    public async Task GetStatisticalResponse_NonExistentDataSourceId_ShouldReturn_NotFoundError()
    {
        // Arrange
        var request = Guid.NewGuid(); // Assume this ID does not exist in the database

        // Act
        var response = await GetStatisticalResponseAsync(request);

        // Assert
        AssertInvalidResponse(response, "DataSourceId", ValidationMessages.NotExists("DataSource"));
    }

    #region Helper Methods
    private async Task<Result<AdvanceStatisticAnalysisResponse>> GetStatisticalResponseAsync(Guid Id)
    {
        return await httpClient.GetAndDeserializeAsync<Result<AdvanceStatisticAnalysisResponse>>($"{RequestURIPath}/AdvanceAnalytics?dataSourceId={Id}");
    }

    private void AssertSuccessResponse(Result<AdvanceStatisticAnalysisResponse> response, Guid request)
    {
        AssertBaseSuccessResponse(response);
        response.Value.Should().NotBeNull();
        response.Value.ProfileResult.Should().NotBeNull();
        response.Value.ProfileResult.DataSourceId.Should().Be(request);
        response.Value.ProfileResult.AdvancedColumnProfiles.Should().NotBeNull();
        response.Value.ProfileResult.AdvancedColumnProfiles.Should().NotBeEmpty();
        response.Value.ProfileResult.AdvancedColumnProfiles.Should().HaveCountGreaterThan(0);
    }

    private void AssertInvalidResponse(Result<AdvanceStatisticAnalysisResponse> response, string identifier, string errorMessage)
    {
        AssertBaseSingleValidationError(response, identifier, errorMessage);
    }
    #endregion
}
