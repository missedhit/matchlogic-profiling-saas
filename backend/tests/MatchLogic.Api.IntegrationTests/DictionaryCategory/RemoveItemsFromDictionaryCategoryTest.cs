using MatchLogic.Api.Endpoints;
using MatchLogic.Api.Handlers.DictionaryCategory.RemoveItems;
using MatchLogic.Api.IntegrationTests.Common;
using MatchLogic.Api.IntegrationTests.Builders;
using System.Net.Http.Json;
using MatchLogic.Api.Common;
using MatchLogic.Api.Handlers.DictionaryCategory.Update;

namespace MatchLogic.Api.IntegrationTests.DictionaryCategory;

[Collection("DictionaryCategory Tests")]
public class RemoveItemsFromDictionaryCategoryTest : BaseApiTest
{
    public RemoveItemsFromDictionaryCategoryTest() : base(DictionaryCategoryEndpoints.PATH)
    {
    }

    #region Positive Test Cases

    [Fact]
    public async Task RemoveItemsFromDictionaryCategory_ValidRequest_ShouldReturn_Success()
    {
        // Arrange
        var initialItems = new List<string> { "Item1", "Item2", "Item3", "Item4" };
        var existingCategory = await CreateTestDictionaryCategoryAsync(initialItems);
        var itemsToRemove = new List<string> { "Item2", "Item4" };

        // Act
        var response = await RemoveItemsFromDictionaryCategoryAsync(existingCategory.Id, itemsToRemove);

        // Assert
        AssertSuccessResponse(response, existingCategory.Id, initialItems.Except(itemsToRemove).ToList());
    }

    #endregion

    #region Negative Test Cases

    [Fact]
    public async Task RemoveItemsFromDictionaryCategory_InvalidId_ShouldReturn_NotFound()
    {
        // Arrange
        var invalidId = Guid.NewGuid();
        var itemsToRemove = new List<string> { "Item1", "Item2" };

        // Act
        var response = await RemoveItemsFromDictionaryCategoryAsync(invalidId, itemsToRemove);

        // Assert
        AssertBaseValidationErrors(response, [
            ("Id", ValidationMessages.NotExists("Dictionary")),
            ("Id", ValidationMessages.ModificationNotAllowed("System Dictionary"))
        ]);
    }

    [Fact]
    public async Task RemoveItemsFromDictionaryCategory_EmptyItems_ShouldReturn_InvalidError()
    {
        // Arrange
        var existingCategory = await CreateTestDictionaryCategoryAsync();
        var itemsToRemove = new List<string>();

        // Act
        var response = await RemoveItemsFromDictionaryCategoryAsync(existingCategory.Id, itemsToRemove);

        // Assert
        AssertInvalidResponse(response, "Items", ValidationMessages.CannotBeEmpty("Items list"));
    }

    [Fact]
    public async Task RemoveItemsFromDictionaryCategory_ItemsNotInCategory_ShouldReturn_Success()
    {
        // Arrange
        var initialItems = new List<string> { "Item1", "Item2" };
        var existingCategory = await CreateTestDictionaryCategoryAsync(initialItems);
        var itemsToRemove = new List<string> { "Item3", "Item4" }; // Items not in category

        // Act
        var response = await RemoveItemsFromDictionaryCategoryAsync(existingCategory.Id, itemsToRemove);

        // Assert
        AssertSuccessResponse(response, existingCategory.Id, initialItems); // Category should remain unchanged
    }

    #endregion

    #region Helper Methods

    private async Task<Result<RemoveItemsFromDictionaryCategoryResponse>> RemoveItemsFromDictionaryCategoryAsync(Guid id, List<string> items)
    {

        var requestUri = $"{RequestURIPath}/{id}/items";
        var request = new HttpRequestMessage(HttpMethod.Delete, requestUri)
        {
            Content = JsonContent.Create(items)
        };

        var httpResponse = await httpClient.SendAsync(request);
        var result = await httpResponse.Content.ReadFromJsonAsync<Result<RemoveItemsFromDictionaryCategoryResponse>>();
        return result;

    }

    private async Task<Domain.Dictionary.DictionaryCategory> CreateTestDictionaryCategoryAsync(List<string> items = null)
    {
        var builder = new DictionaryCategoryBuilder(GetServiceProvider())
            .WithValid();

        if (items != null)
        {
            builder.WithItems(items);
        }

        return await builder.BuildAsync();
    }

    private void AssertSuccessResponse(Result<RemoveItemsFromDictionaryCategoryResponse> response, Guid id, List<string> expectedRemainingItems)
    {
        AssertBaseSuccessResponse(response);
        response.Value.Id.Should().Be(id);
        response.Value.Items.Should().BeEquivalentTo(expectedRemainingItems);
    }

    private void AssertInvalidResponse(Result<RemoveItemsFromDictionaryCategoryResponse> response, string identifier, string errorMessage)
    {
        AssertBaseSingleValidationError(response, identifier, errorMessage);
    }

    #endregion
}
