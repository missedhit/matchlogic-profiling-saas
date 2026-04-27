using MatchLogic.Api.Endpoints;
using MatchLogic.Api.Handlers.DictionaryCategory.AddItems;
using MatchLogic.Api.IntegrationTests.Common;
using MatchLogic.Api.IntegrationTests.Builders;
using MatchLogic.Api.Common;

namespace MatchLogic.Api.IntegrationTests.DictionaryCategory;

[Collection("DictionaryCategory Tests")]
public class AddItemsToDictionaryCategoryTest : BaseApiTest
{
    public AddItemsToDictionaryCategoryTest() : base(DictionaryCategoryEndpoints.PATH)
    {
    }

    #region Positive Test Cases

    [Fact]
    public async Task AddItemsToDictionaryCategory_ValidRequest_ShouldReturn_Success()
    {
        // Arrange
        var existingCategory = await CreateTestDictionaryCategoryAsync();
        var itemsToAdd = new List<string> { "NewItem1", "NewItem2" };

        // Act
        var response = await AddItemsToDictionaryCategoryAsync(existingCategory.Id, itemsToAdd);

        // Assert
        AssertSuccessResponse(response, existingCategory.Id, itemsToAdd);
    }

    #endregion

    #region Negative Test Cases

    [Fact]
    public async Task AddItemsToDictionaryCategory_InvalidId_ShouldReturn_InvalidError()
    {
        // Arrange
        var invalidId = Guid.NewGuid();
        var itemsToAdd = new List<string> { "NewItem1", "NewItem2" };

        // Act
        var response = await AddItemsToDictionaryCategoryAsync(invalidId, itemsToAdd);

        // Assert
        //AssertInvalidResponse(response, "Id", "Dictionary does not exist.");
        AssertBaseValidationErrors(response, [
            ("Id", ValidationMessages.NotExists("Dictionary")),
            ("Id", ValidationMessages.ModificationNotAllowed("System Dictionary"))
            ]);
    }

    [Fact]
    public async Task AddItemsToDictionaryCategory_EmptyItems_ShouldReturn_InvalidError()
    {
        // Arrange
        var existingCategory = await CreateTestDictionaryCategoryAsync();
        var itemsToAdd = new List<string>();

        // Act
        var response = await AddItemsToDictionaryCategoryAsync(existingCategory.Id, itemsToAdd);

        // Assert
        AssertInvalidResponse(response, "Items", ValidationMessages.CannotBeEmpty("Items list"));
    }

    #endregion

    #region Helper Methods

    private async Task<Result<AddItemsToDictionaryCategoryResponse>> AddItemsToDictionaryCategoryAsync(Guid id, List<string> items)
    {
        return await httpClient.PostAndDeserializeAsync<Result<AddItemsToDictionaryCategoryResponse>>(
            $"{RequestURIPath}/{id}/items",
            StringContentHelpers.FromModelAsJson(items));
    }

    private async Task<Domain.Dictionary.DictionaryCategory> CreateTestDictionaryCategoryAsync()
    {
        return await new DictionaryCategoryBuilder(GetServiceProvider())
            .WithValid()
            .BuildAsync();
    }

    private void AssertSuccessResponse(Result<AddItemsToDictionaryCategoryResponse> response, Guid id, List<string> items)
    {
        AssertBaseSuccessResponse(response);
        response.Value.Id.Should().Be(id);
        response.Value.Items.Should().Contain(items);
    }

    private void AssertInvalidResponse(Result<AddItemsToDictionaryCategoryResponse> response, string identifier, string errorMessage)
    {
        AssertBaseSingleValidationError(response, identifier, errorMessage);
    }

    #endregion
}
