using MatchLogic.Api.Common;
using MatchLogic.Api.Handlers.DataSource.List;
using MatchLogic.Api.IntegrationTests.DataImport.DataSource.Base;

namespace MatchLogic.Api.IntegrationTests.DataImport.DataSource;
[Collection("DataImport List of DataSources  Tests")]
public class DataImportListDataSourceTest : BaseDataSourceTest
{
    private async Task<Result<List<ListDataSourceResponse>>> ListDataSources(ListDataSourceRequest request)
    {
        var response = await httpClient.GetAndDeserializeAsync<Result<List<ListDataSourceResponse>>>($"{RequestURIPath}/DataSource/?ProjectId={request.ProjectId}");
        return response;
    }

    [Fact]
    public async Task ListDataSources_ValidRequest_ShouldReturn_Success()
    {
        // Arrange
        var dataSource = await AddTestSingleDataSource();
        var request = new ListDataSourceRequest(ProjectId: dataSource.ProjectId);

        // Act
        var response = await ListDataSources(request);

        // Assert
        AssertSuccessResponse(response, request);
    }

    [Fact]
    public async Task ListDataSources_InvalidProjectId_ShouldReturn_Error()
    {
        // Arrange
        var request = new ListDataSourceRequest(ProjectId: Guid.NewGuid()); // Assuming this Project ID does not exist

        // Act
        var response = await ListDataSources(request);

        // Assert
        AssertDataSourceInvalidResponse(response, "ProjectId", ValidationMessages.NotExists("Project"));
    }

    [Fact]
    public async Task ListDataSources_EmptyProjectId_ShouldReturn_Error()
    {
        // Arrange
        var request = new ListDataSourceRequest(ProjectId: Guid.Empty); // Empty Guid

        // Act
        var response = await ListDataSources(request);

        // Assert
        AssertBaseValidationErrors(response, [
            //("ProjectId", "'Project Id' must not be empty."),
            ("ProjectId", ValidationMessages.Required("Project ID")),
            ("ProjectId", ValidationMessages.NotExists("Project"))
            ]);
    }

    private void AssertSuccessResponse(Result<List<ListDataSourceResponse>> response, ListDataSourceRequest request)
    {
        AssertBaseSuccessResponse(response);
        response.Value.Should().NotBeNullOrEmpty();
    }

    private void AssertDataSourceInvalidResponse(Result<List<ListDataSourceResponse>> response, string identifier, string errorMessage)
    {
        AssertBaseSingleValidationError(response, identifier, errorMessage);
    }
}
