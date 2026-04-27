using MatchLogic.Api.Common;
using MatchLogic.Api.Handlers.DataSource.Create;
using MatchLogic.Api.Handlers.DataSource.Data;
using MatchLogic.Api.IntegrationTests.Builders;
using MatchLogic.Api.IntegrationTests.DataImport.DataSource.Base;
using MatchLogic.Domain.Import;

namespace MatchLogic.Api.IntegrationTests.DataImport.DataSource;
[Collection("DataImport List Imported Data Tests")]
public class DataImportListPreviewDataDataSourceTest : BaseDataSourceTest
{
    private async Task<Result<PreviewDataSourceResponse>> ListPreviewData(PreviewDataSourceRequest request)
    {
        return await httpClient.GetAndDeserializeAsync<Result<PreviewDataSourceResponse>>(
            $"{RequestURIPath}/DataSource/Data?id={request.Id}&pageNumber={request.PageNumber}&pageSize={request.PageSize}&filterText={request.FilterText}&sortColumn={request.SortColumn}&ascending={request.Ascending}");
    }

    [Fact]
    public async Task ListPreviewData_ValidRequest_ShouldReturn_Success()
    {
        // Arrange
        var dataSource = await AddTestSingleDataSource();
        var request = new PreviewDataSourceRequest
        {
            Id = dataSource.Id,
            PageNumber = 1,
            PageSize = 10,
            FilterText = null,
            SortColumn = null,
            Ascending = true
        };

        // Act
        var response = await ListPreviewData(request);

        // Assert
        AssertSuccessResponse(response, request);
    }

    [Fact]
    public async Task ListPreviewData_SearchOn_ValidColumnNames_ShouldReturn_Data()
    {
        // Arrange
        var project = await new ProjectBuilder(GetServiceProvider()).BuildAsync();
        var file = await new FileImportBuilder(GetServiceProvider())
            .WithProjectId(project.Id)
            .CreateValidColumnNamesTestExcelFile()
            .BuildAsync();

        var handerRequest = new CreateDataSourceRequest
                        (
                            ProjectId: project.Id,
                            Connection: new BaseConnectionInfo
                            {
                                Type = DataSourceType.Excel,
                                Parameters =
                                {
                                    ["FileId"] = file.Id.ToString(),
                                    ["FilePath"] = file.FilePath,
                                    ["HasHeaders"] = "true"
                                }
                            },
                            DataSources: // Duplicate Data Source Name
                            [
                                new DataSourceRequest (Name : "DataSource1", TableName : "Sheet1", []),
                            ]
                        );
        var dataSource = await AddTestSingleDataSource(handerRequest);
        var request = new PreviewDataSourceRequest
        {
            Id = dataSource.Id,
            PageNumber = 1,
            PageSize = 10,
            FilterText = "Abagail Manuel", //Search Text
            SortColumn = null,
            Ascending = true
        };

        // Act
        var response = await ListPreviewData(request);

        // Assert
        
        AssertSuccessResponse(response, request);
        response.Value.TotalCount.Should().Be(1);    
    }

    [Fact]
    public async Task ListPreviewData_InvalidId_ShouldReturn_Error()
    {
        // Arrange
        var request = new PreviewDataSourceRequest
        {
            Id = Guid.NewGuid(), // Assuming this ID does not exist
            PageNumber = 1,
            PageSize = 10,
            FilterText = null,
            SortColumn = null,
            Ascending = true
        };

        // Act
        var response = await ListPreviewData(request);

        // Assert
        AssertDataSourceInvalidResponse(response, "Id", ValidationMessages.NotExists("DataSource"));
    }

    [Fact]
    public async Task ListPreviewData_EmptyId_ShouldReturn_Error()
    {
        // Arrange
        var request = new PreviewDataSourceRequest
        {
            Id = Guid.Empty, // Empty Guid
            PageNumber = 1,
            PageSize = 10,
            FilterText = null,
            SortColumn = null,
            Ascending = true
        };

        // Act
        var response = await ListPreviewData(request);

        // Assert
        AssertBaseValidationErrors(response, new List<(string Identifier, string ErrorMessage)>
        {
            //("Id", "'Id' must not be empty."),
            ("Id", ValidationMessages.Required("DataSource Id")),
            ("Id", ValidationMessages.NotExists("DataSource"))
        });
    }

    private void AssertSuccessResponse(Result<PreviewDataSourceResponse> response, PreviewDataSourceRequest request)
    {
        AssertBaseSuccessResponse(response);
        response.Value.Should().NotBeNull();
        response.Value.Data.Should().NotBeNullOrEmpty();
        response.Value.TotalCount.Should().BeGreaterThan(0);
    }

    private void AssertDataSourceInvalidResponse(Result<PreviewDataSourceResponse> response, string identifier, string errorMessage)
    {
        AssertBaseSingleValidationError(response, identifier, errorMessage);
    }

    [Fact]
    public async Task ListPreviewData_PageNumberZero_ShouldReturn_Error()
    {
        // Arrange
        var dataSource = await AddTestSingleDataSource();
        var request = new PreviewDataSourceRequest
        {
            Id = dataSource.Id,
            PageNumber = 0, // Invalid PageNumber
            PageSize = 10,
            FilterText = null,
            SortColumn = null,
            Ascending = true
        };
        // Act
        var response = await ListPreviewData(request);
        // Assert
        AssertDataSourceInvalidResponse(response, "PageNumber", "PageNumber must be greater than zero.");
    }
    [Fact]
    public async Task ListPreviewData_PageSizeZero_ShouldReturn_Error()
    {
        // Arrange
        var dataSource = await AddTestSingleDataSource();
        var request = new PreviewDataSourceRequest
        {
            Id = dataSource.Id,
            PageNumber = 1, 
            PageSize = 0,// Invalid PageSize
            FilterText = null,
            SortColumn = null,
            Ascending = true
        };
        // Act
        var response = await ListPreviewData(request);
        // Assert
        AssertDataSourceInvalidResponse(response, "PageSize", "PageSize must be greater than zero.");
    }
    [Fact]
    public async Task ListPreviewData_WithFilterText_ShouldReturn_FilteredResults()
    {
        // Arrange
        var dataSource = await AddTestSingleDataSource();
        var request = new PreviewDataSourceRequest
        {
            Id = dataSource.Id,
            PageNumber = 1,
            PageSize = 10,
            FilterText = "John", // Assuming "John" is a valid filter text
            SortColumn = null,
            Ascending = true
        };

        // Act
        var response = await ListPreviewData(request);
        // Assert
        AssertSuccessResponse(response, request);
        response.Value.Data.Should().AllSatisfy(item => item.Values.Contains("John"));
    }

    [Fact]
    public async Task ListPreviewData_WithSortColumn_ShouldReturn_SortedResults()
    {
        // Arrange
        var dataSource = await AddTestSingleDataSource();
        var request = new PreviewDataSourceRequest
        {
            Id = dataSource.Id,
            PageNumber = 1,
            PageSize = 10,
            FilterText = null,
            SortColumn = "Name", // Assuming "Name" is a valid sort column
            Ascending = true
        };

        // Act
        var response = await ListPreviewData(request);

        // Assert
        AssertSuccessResponse(response, request);
        var sortedData = response.Value.Data
       .OrderBy(item => item["Name"].ToString()) 
       .ToList();
        response.Value.Data.Should().Equal(sortedData);
    }

    [Fact]
    public async Task ListPreviewData_WithDescendingOrder_ShouldReturn_DescendingSortedResults()
    {
        // Arrange
        var dataSource = await AddTestSingleDataSource();
        var request = new PreviewDataSourceRequest
        {
            Id = dataSource.Id,
            PageNumber = 1,
            PageSize = 10,
            FilterText = null,
            SortColumn = "Name", // Assuming "Name" is a valid sort column
            Ascending = false
        };

        // Act
        var response = await ListPreviewData(request);

        // Assert
        AssertSuccessResponse(response, request);
        var sortedData = response.Value.Data
        .OrderByDescending(item => item["Name"].ToString()) 
        .ToList();
        response.Value.Data.Should().Equal(sortedData);
    }

    [Fact]
    public async Task ListPreviewData_WithDescendingOrder_ShouldReturn_AscendingSortedResults()
    {
        // Arrange
        var dataSource = await AddTestSingleDataSource();
        var request = new PreviewDataSourceRequest
        {
            Id = dataSource.Id,
            PageNumber = 1,
            PageSize = 10,
            FilterText = null,
            SortColumn = "Name", // Assuming "Name" is a valid sort column
            Ascending = true
        };

        // Act
        var response = await ListPreviewData(request);

        // Assert
        AssertSuccessResponse(response, request);
        var sortedData = response.Value.Data
        .OrderBy(item => item["Name"].ToString())
        .ToList();
        response.Value.Data.Should().Equal(sortedData);
    }
}
