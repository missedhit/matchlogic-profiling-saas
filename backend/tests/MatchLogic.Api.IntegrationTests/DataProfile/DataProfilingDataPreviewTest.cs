using MatchLogic.Api.Endpoints;
using MatchLogic.Api.IntegrationTests.Common;
using MatchLogic.Api.Handlers.DataProfile.DataPreview;
using MatchLogic.Domain.DataProfiling;
using MatchLogic.Api.IntegrationTests.Builders;
using MatchLogic.Domain.Import;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Api.Common;

namespace MatchLogic.Api.IntegrationTests.DataProfile;

[Collection("DataProfiling Tests")]
public class DataProfilingDataPreviewTest : BaseApiTest
{
    public DataProfilingDataPreviewTest() : base(DataProfilingEndpoints.PATH)
    {
    }

    #region Positive Test Cases

    [Fact]
    public async Task DataPreview_ValidRequest_ShouldReturn_Success()
    {

        // Arrange
        var dataSource = await new DataSourceBuilder(GetServiceProvider())
            .AddSingleDataSourceAsync("DataSource1");
        //Generate Data Profile
        var projectRun = await new DataProfileBuilder(GetServiceProvider())
            .WithProjectId(dataSource.DataSource.ProjectId)
            .WithDataSourceId(dataSource.DataSource.Id)
            .BuidAsync();
        //await Task.Delay(5000);
        var profileResult = await GetProfileResultData(dataSource.DataSource.Id);
        var docId = profileResult.ColumnProfiles["Name"]
            .CharacteristicRowDocumentIds[ProfileCharacteristic.DistinctValue];
        var request = new DataPreviewRequest
        {
            DocumentId = docId,
            DataSourceId = dataSource.DataSource.Id
        };

        // Act
        var response = await DataPreviewAsync(request);

        // Assert
        AssertSuccessResponse(response, request);
    }

    #endregion

    #region Negative Test Cases

    [Fact]
    public async Task DataPreview_InvalidDataSourceId_ShouldReturn_NotFound()
    {
        // Arrange

        var dataSource = await new DataSourceBuilder(GetServiceProvider())
            .AddSingleDataSourceAsync("DataSource1");
        //Generate Data Profile
        var projectRun = await new DataProfileBuilder(GetServiceProvider())
            .WithProjectId(dataSource.DataSource.ProjectId)
            .WithDataSourceId(dataSource.DataSource.Id)
            .BuidAsync();

        var profileResult = await GetProfileResultData(dataSource.DataSource.Id);
        var docId = profileResult.ColumnProfiles["Name"]
            .CharacteristicRowDocumentIds[ProfileCharacteristic.DistinctValue];
        var request = new DataPreviewRequest
        {
            DocumentId = docId,
            DataSourceId = Guid.Empty // Empty DataSourceId
        };

        // Act
        var response = await DataPreviewAsync(request);

        // Assert
        AssertBaseValidationErrors(response, [
            //("DataSourceId", "'Data Source Id' must not be empty."),
            ("DataSourceId", ValidationMessages.Required("DataSource Id")),
            ("DataSourceId", ValidationMessages.NotExists("DataSource"))]);
    }

    [Fact]
    public async Task DataPreview_EmptyDocumentId_ShouldReturn_InvalidError()
    {
        // Arrange
        var dataSource = await new DataSourceBuilder(GetServiceProvider())
            .AddSingleDataSourceAsync("DataSource1");
        //Generate Data Profile
        var projectRun = await new DataProfileBuilder(GetServiceProvider())
            .WithProjectId(dataSource.DataSource.ProjectId)
            .WithDataSourceId(dataSource.DataSource.Id)
            .BuidAsync();
        var request = new DataPreviewRequest
        {
            DocumentId = Guid.Empty, // Empty DocumentId
            DataSourceId = dataSource.DataSource.Id
        };

        // Act
        var response = await DataPreviewAsync(request);

        // Assert
        AssertBaseValidationErrors(response, [
            ("DocumentId", "'Document Id' must not be empty."),
            ("DocumentId", ValidationMessages.Required("Document Id"))]);
    }

    [Fact]
    public async Task DataPreview_InvalidDocumentId_ShouldReturn_InvalidError()
    {
        // Arrange
        var dataSource = await new DataSourceBuilder(GetServiceProvider())
            .AddSingleDataSourceAsync("DataSource1");
        // Generate Data Profile
        var projectRun = await new DataProfileBuilder(GetServiceProvider())
            .WithProjectId(dataSource.DataSource.ProjectId)
            .WithDataSourceId(dataSource.DataSource.Id)
            .BuidAsync();
        var request = new DataPreviewRequest
        {
            DocumentId = Guid.NewGuid(), // Invalid DocumentId
            DataSourceId = dataSource.DataSource.Id
        };

        // Act
        var response = await DataPreviewAsync(request);

        // Assert
        //AssertBaseNotFoundResponseError(response, $"No row reference data found for the specified document ID: {request.DocumentId}.");
        AssertBaseNotFoundResponseError(response, "No row reference data found");
    }

    [Fact]
    public async Task DataPreview_NonExistentDataSource_ShouldReturn_NotFound()
    {
        // Arrange
        var dataSource = await new DataSourceBuilder(GetServiceProvider())
            .AddSingleDataSourceAsync("DataSource1");
        //Generate Data Profile
        var projectRun = await new DataProfileBuilder(GetServiceProvider())
            .WithProjectId(dataSource.DataSource.ProjectId)
            .WithDataSourceId(dataSource.DataSource.Id)
            .BuidAsync();

        var profileResult = await GetProfileResultData(dataSource.DataSource.Id);
        var docId = profileResult.ColumnProfiles["Name"]
            .CharacteristicRowDocumentIds[ProfileCharacteristic.DistinctValue];
        var request = new DataPreviewRequest
        {
            DocumentId = docId,
            DataSourceId = Guid.NewGuid() // Non-existent DataSourceId
        };

        // Act
        var response = await DataPreviewAsync(request);

        // Assert
        AssertInvalidResponse(response, "DataSourceId", ValidationMessages.NotExists("DataSource"));
    }

    #endregion

    #region Helper Methods

    private async Task<Result<DataPreviewResponse>> DataPreviewAsync(DataPreviewRequest request)
    {
        return await httpClient.GetAndDeserializeAsync<Result<DataPreviewResponse>>($"{RequestURIPath}/Data?DataSourceId={request.DataSourceId}&DocumentId={request.DocumentId}");
    }

    private void AssertSuccessResponse(Result<DataPreviewResponse> response, DataPreviewRequest request)
    {
        AssertBaseSuccessResponse(response);
        response.Value.rowReferences.Should().NotBeNull();
        response.Value.rowReferences.Should().BeOfType<List<RowReference>>();
    }

    private void AssertInvalidResponse(Result<DataPreviewResponse> response, string identifier, string errorMessage)
    {
        AssertBaseSingleValidationError(response, identifier, errorMessage);
    }


    private async Task<ProfileResult> GetProfileResultData(Guid dataSourceId)
    {
        //var dataSourceId = request.DataSourceId;
        var profileCollectionName = StepType.Profile.ToCollectionName(dataSourceId);
        // Query the profile result using the data source ID
        var profileResultRepository = GetService<IGenericRepository<ProfileResult, Guid>>();
        var profileList = await profileResultRepository.QueryAsync(x => x.DataSourceId == dataSourceId, profileCollectionName);
        return profileList.First();

    }

    #endregion
}

