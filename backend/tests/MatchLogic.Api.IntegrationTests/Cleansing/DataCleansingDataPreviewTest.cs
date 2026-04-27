using MatchLogic.Api.Endpoints;
using MatchLogic.Api.Handlers.Cleansing.DataPreview;
using MatchLogic.Api.IntegrationTests.Builders;
using MatchLogic.Api.IntegrationTests.Common;
using MatchLogic.Application.Features.CleansingAndStandardization.DTOs;
using MatchLogic.Domain.CleansingAndStandaradization;


namespace MatchLogic.Api.IntegrationTests.Cleansing;
[Collection("DataCleansing DataPreview Tests")]
public class DataCleansingDataPreviewTest : BaseApiTest
{
    public DataCleansingDataPreviewTest() : base(DataCleansingEndpoints.PATH) // Adjust path as needed
    {
    }

    [Fact]
    public async Task DataPreview_WithValidRequest_ShouldReturn_Success()
    {
        // Arrange
        var tuple = await new DataSourceBuilder(GetServiceProvider())
            .AddSingleDataSourceAsync("DataSource1");
        // Create cleansing rules for the data source
        await new CreateCleansingRuleCommandBuilder(GetServiceProvider())
            .WithProjectId(tuple.DataSource.ProjectId)
            .WithDataSourceId(tuple.DataSource.Id)
            .WithStandardRules(new List<CleaningRuleDto>
            {
                new CleaningRuleDto
                {
                    ColumnName = "Name",
                    RuleType = CleaningRuleType.UpperCase,
                }
            })
            .BuildAsync();

        var request = new DataPreviewCleansingRequest()
        {
            DataSourceId = Guid.NewGuid(), // Non-existent
            PageNumber = 1,
            PageSize = 10,
            FilterText = "",
            SortColumn = "",
            Ascending = true,
        };

        // Act
        var response = await GetPreviewData(request);

        // Assert
        AssertBaseSuccessResponse(response);
    }

    [Fact]
    public async Task DataPreview_WithInvalidDataSourceId_ShouldReturn_Error()
    {
        // Arrange
        var request = new DataPreviewCleansingRequest()
        {
            DataSourceId = Guid.NewGuid(), // Non-existent
            PageNumber = 1,
            PageSize = 10,
            FilterText = "",
            SortColumn = "",
            Ascending = true,
        };

        // Act
        var response = await GetPreviewData(request);

        // Assert
        AssertBaseSingleValidationError(response, "DataSourceId", "DataSource does not exist.");
    }

    [Fact]
    public async Task DataPreview_WithInvalidPageNumber_ShouldReturn_Error()
    {
        // Arrange
        var tuple = await new DataSourceBuilder(GetServiceProvider())
            .AddSingleDataSourceAsync("DataSource1");

        var request = new DataPreviewCleansingRequest()
        {
            DataSourceId = Guid.NewGuid(), // Non-existent
            PageNumber = 0,
            PageSize = 10,
            FilterText = "",
            SortColumn = "",
            Ascending = true,
        };

        // Act
        var response = await GetPreviewData(request);

        // Assert
        AssertBaseSingleValidationError(response, "PageNumber", "PageNumber must be greater than zero.");
    }

    [Fact]
    public async Task DataPreview_WithInvalidPageSize_ShouldReturn_Error()
    {
        // Arrange
        var tuple = await new DataSourceBuilder(GetServiceProvider())
            .AddSingleDataSourceAsync("DataSource1");

        var request = new DataPreviewCleansingRequest()
        {
            DataSourceId = Guid.NewGuid(), // Non-existent
            PageNumber = 1,
            PageSize = 0,
            FilterText = "",
            SortColumn = "",
            Ascending = true,
        };

        // Act
        var response = await GetPreviewData(request);

        // Assert
        AssertBaseSingleValidationError(response, "PageSize", "PageSize must be greater than zero.");
    }

    private async Task<Result<DataPreviewCleansingResponse>> GetPreviewData(DataPreviewCleansingRequest request)
    {
        return await httpClient.GetAndDeserializeAsync<Result<DataPreviewCleansingResponse>>(
            $"{RequestURIPath}/Preview?DataSourceId={request.DataSourceId}&PageNumber={request.PageNumber}&PageSize={request.PageSize}"
        );
    }
}
