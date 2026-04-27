using MatchLogic.Api.Endpoints;
using MatchLogic.Api.Handlers.JobInfo;
using MatchLogic.Api.IntegrationTests.Builders;
using MatchLogic.Api.IntegrationTests.Common;
using MatchLogic.Domain.Import;

namespace MatchLogic.Api.IntegrationTests.JonInfo;
[Collection("Job Run Info Tests")]
public class JobRunStatusTest : BaseApiTest
{
    public JobRunStatusTest() : base(JobStatusEndpoint.PATH)
    {
    }


    [Fact]
    public async Task GetJobRunStatus_SingleDataSource_ValidRunId_ReturnsSuccess()
    {
        // Arrange
        var touple = await new DataSourceBuilder(GetServiceProvider())
            .AddSingleDataSourceAsync("DataSource1");
        var runId = touple.ProjectRun.Id;
        // Act
        //Task.Delay(1000).Wait();
        var response = await httpClient.GetAndDeserializeAsync<Result<JobRunStatusResponse>>($"{RequestURIPath}/Status/{runId}");
        // Assert        
        AssertBaseSuccessResponse(response);
        response.Value.JobStatuses.Count.Should().Be(1);
        response.Value.JobStatuses.First().DataSourceName.Should().Be("DataSource1");
        response.Value.JobStatuses.First().Error.Should().BeNullOrEmpty();
    }
    [Fact]
    public async Task GetJobRunStatus_MultipleDataSource_ValidRunId_ReturnsSuccess()
    {
        // Arrange
        var touple = await new DataSourceBuilder(GetServiceProvider())
            .AddMultipleDataSourcesAsync(["DataSource1", "DataSource2"]);
        var runId = touple.ProjectRun.Id;
        // Act
        //Task.Delay(1000).Wait();
        var response = await httpClient.GetAndDeserializeAsync<Result<JobRunStatusResponse>>($"{RequestURIPath}/Status/{runId}");
        // Assert        
        AssertBaseSuccessResponse(response);
        response.Value.JobStatuses.Count.Should().Be(2);
        response.Value.JobStatuses.First().Error.Should().BeNullOrEmpty();
        response.Value.JobStatuses.Last().Error.Should().BeNullOrEmpty();
    }


    [Fact]
    public async Task GetJobRunStatus_InvalidRunId_ReturnsError()
    {
        // Arrange
        var runId = Guid.NewGuid(); // Assuming Invalid RunId
        // Act
        var response = await httpClient.GetAndDeserializeAsync<Result<JobRunStatusResponse>>($"{RequestURIPath}/Status/{runId}");
        // Assert
        AssertBaseInvalidResponse(response);
    }


    [Fact]
    public async Task GetJobRunStatus_EmptyRunId_ReturnsError()
    {
        // Arrange
        var runId = Guid.Empty; // Assuming Invalid RunId
        // Act
        var response = await httpClient.GetAndDeserializeAsync<Result<JobRunStatusResponse>>($"{RequestURIPath}/Status/{runId}");
        // Assert
        AssertBaseInvalidResponse(response);
    }
}
