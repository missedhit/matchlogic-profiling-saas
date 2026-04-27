using MatchLogic.Api.Common;
using MatchLogic.Api.Handlers.DataSource.Update;
using MatchLogic.Api.IntegrationTests.DataImport.DataSource.Base;
namespace MatchLogic.Api.IntegrationTests.DataImport.DataSource;

[Collection("DataImport Update DataSource Tests")]
public class DataImportUpdateDataSourceTest : BaseDataSourceTest
{
    private async Task<Result<UpdateDataSourceResponse>> UpdateDataSource(UpdateDataSourceRequest request) 
    {
       /* var str= await httpClient.PatchAsync($"{RequestURIPath}", StringContentHelpers.FromModelAsJson(request));
        Stream receiveStream = await str.Content.ReadAsStreamAsync();
        StreamReader readStream = new StreamReader(receiveStream, Encoding.UTF8);
        string stt = readStream.ReadToEnd();*/
        var response = await httpClient.PatchAndDeserializeAsync<Result<UpdateDataSourceResponse>>($"{RequestURIPath}/DataSource", StringContentHelpers.FromModelAsJson(request));
        return response;
    }
    [Fact]
    public async Task UpdateDataSource_ValidRequest_ShouldReturn_Success()
    {
        // Arrange
        var dataSource = await AddTestSingleDataSource();
        var request = new UpdateDataSourceRequest(Id : dataSource.Id, Name : "UpdatedDataSource");
        // Act
        var response = await UpdateDataSource(request);
        // Assert
        AssertSuccessResponse(response,request);
    }
    [Fact]
    public async Task UpdateDataSource_NullId_ShouldReturn_Error()
    {
        // Arrange
        var request = new UpdateDataSourceRequest(
            Id: Guid.Empty,//Empty Guid
            Name: "UpdatedDataSource");
        // Act
        var response = await UpdateDataSource(request);
        // Assert
        AssertBaseValidationErrors(response, [
            //("Id","'Id' must not be empty."),
            ("Id", ValidationMessages.Required("DataSource Id")),
            ("Id", ValidationMessages.NotExists("DataSource")),
            ("Name",ValidationMessages.AlreadyExists("DataSource name"))
            ]);
    }

    [Fact]
    public async Task UpdateDataSource_InvalidId_ShouldReturn_Error()
    {
        // Arrange
        var request = new UpdateDataSourceRequest(
            Id: Guid.NewGuid() // Assuming this ID does not exist
            ,Name: "UpdatedDataSource");
        // Act
        var response = await UpdateDataSource(request);
        // Assert
        AssertBaseValidationErrors(response, [
            ("Id", ValidationMessages.NotExists("DataSource")),
            ("Name", ValidationMessages.AlreadyExists("DataSource name"))
            ]);
    }

    [Fact]
    public async Task UpdateDataSource_DuplicateName_ShouldReturn_Error()
    {
        // Arrange
        var testDataSource = await AddTestMultipleDataSource();
        var request = new UpdateDataSourceRequest(
            Id : testDataSource[1].Id,//Second Data Source ID
            Name : "DataSource1"); // Duplicate name as First Data Source

        // Act
        var response = await UpdateDataSource(request);
        // Assert
        AssertDataSourceInvalidResponse(response, "Name", ValidationMessages.AlreadyExists("DataSource name"));
    }
    [Fact]
    public async Task UpdateDataSource_InvalidName_ShouldReturn_Error()
    {
        // Arrange
        var dataSource = await AddTestSingleDataSource();
        var request = new UpdateDataSourceRequest(
            Id: dataSource.Id,
            Name: string.Empty); // Invalid name (empty string)

        // Act
        var response = await UpdateDataSource(request);

        // Assert
        AssertDataSourceInvalidResponse(response, "Name", ValidationMessages.Required("DataSource name"));
    }


    [Fact]
    public async Task UpdateDataSource_Name_CharacterBelowOrEqual150Long_ShouldReturn_Success()
    {
        // Arrange
        var dataSource = await AddTestSingleDataSource();
        var request = new UpdateDataSourceRequest(
            Id: dataSource.Id,
            Name: new string('A', Application.Common.Constants.FieldLength.NameMaxLength)); //Valid Length

        // Act
        var response = await UpdateDataSource(request);

        // Assert
        AssertSuccessResponse(response, request);
        //AssertDataSourceInvalidResponse(response, "Name", "DataSource name is required.");
    }

    [Fact]
    public async Task UpdateDataSource_Name_151CharactersLong_ShouldReturn_InvalidError()
    {
        // Arrange
        var dataSource = await AddTestSingleDataSource();
        var request = new UpdateDataSourceRequest(
            Id: dataSource.Id,
            Name: new string('A', Application.Common.Constants.FieldLength.NameMaxLength + 1)); // Invalid Length

        // Act
        var response = await UpdateDataSource(request);

        // Assert
        AssertDataSourceInvalidResponse(response, "Name",ValidationMessages.MaxLength("DataSource name", Application.Common.Constants.FieldLength.NameMaxLength));
    }
    private void AssertSuccessResponse(Result<UpdateDataSourceResponse> response, UpdateDataSourceRequest request)
    {
        AssertBaseSuccessResponse(response);
        response.Value.Should().NotBeNull();
        response.Value.Id.Should().Be(request.Id);
        response.Value.Name.Should().Be(request.Name);
        response.Value.ModifiedAt.Should().NotBeNull();
        response.Value.ModifiedAt.Value.Date.Should().Be(DateTime.UtcNow.Date);

    }

    private void AssertDataSourceInvalidResponse(Result<UpdateDataSourceResponse> response, string identifier, string errorMessage)
    {
        AssertBaseSingleValidationError(response, identifier, errorMessage);
    }
}
