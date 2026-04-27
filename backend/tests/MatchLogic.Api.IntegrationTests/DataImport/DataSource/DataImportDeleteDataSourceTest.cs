using MatchLogic.Api.Common;
using MatchLogic.Api.Handlers.DataSource.Delete;
using MatchLogic.Api.IntegrationTests.DataImport.DataSource.Base;

namespace MatchLogic.Api.IntegrationTests.DataImport.DataSource;
[Collection("DataImport Deleted DataSources  Tests")]
public class DataImportDeleteDataSourceTest : BaseDataSourceTest
{
    private async Task<Result<DeleteDataSourceResponse>> DeleteDataSource(Guid id)
    {
        var response = await httpClient.DeleteAndDeserializeAsync<Result<DeleteDataSourceResponse>>($"{RequestURIPath}/DataSource/{id}");
        return response;
    }

    [Fact]
    public async Task DeleteDataSource_ValidId_ShouldReturn_Success()
    {
        // Arrange
        var dataSource = await AddTestSingleDataSource();
        // Act
        var response = await DeleteDataSource(dataSource.Id);
        // Assert
        //AssertSuccessResponse(response, dataSource.Id);
        AssertBaseSuccessResponse(response, ResultStatus.NoContent);
    }

    [Fact]
    public async Task DeleteDataSource_NullId_ShouldReturn_Error()
    {
        // Arrange
        var id = Guid.Empty; // Empty Guid
        // Act
        var response = await DeleteDataSource(id);
        // Assert
        AssertBaseValidationErrors(response, [
            //("Id","'Id' must not be empty."),
            ("Id", ValidationMessages.Required("DataSource Id")),
            ("Id", ValidationMessages.NotExists("DataSource"))
            ]);
    }

    [Fact]
    public async Task DeleteDataSource_InvalidId_ShouldReturn_Error()
    {
        // Arrange
        var id = Guid.NewGuid(); // Assuming this ID does not exist
        // Act
        var response = await DeleteDataSource(id);
        // Assert
        AssertDataSourceInvalidResponse(response, "Id", ValidationMessages.NotExists("DataSource"));
    }

   
    private void AssertSuccessResponse(Result<DeleteDataSourceResponse> response, Guid id)
    {
        AssertBaseSuccessResponse(response,ResultStatus.NoContent);
    }

    private void AssertDataSourceInvalidResponse(Result<DeleteDataSourceResponse> response, string identifier, string errorMessage)
    {
        AssertBaseSingleValidationError(response, identifier, errorMessage);
    }
/*    [Fact]
    public async Task DeleteDataSource_InUse_ShouldReturn_Error()
    {
        // Arrange
        var dataSource = await AddTestSingleDataSource();
        // Simulate the data source being in use
        await UseDataSource(dataSource.Id);
        // Act
        var response = await DeleteDataSource(dataSource.Id);
        // Assert
        AssertDataSourceInvalidResponse(response, "Id", "DataSource is currently in use and cannot be deleted.");
    }
    private async Task UseDataSource(Guid id)
    {
        // Simulate the data source being in use
        // This method should contain logic to mark the data source as in use
        // For example, by creating a dependency or a reference to it
    }*/
}
