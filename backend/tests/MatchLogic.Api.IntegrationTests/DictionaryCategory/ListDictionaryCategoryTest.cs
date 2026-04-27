using MatchLogic.Api.Endpoints;
using MatchLogic.Api.IntegrationTests.Common;
using MatchLogic.Api.IntegrationTests.Builders;

namespace MatchLogic.Api.IntegrationTests.DictionaryCategory;

[Collection("DictionaryCategory Tests")]
public class ListDictionaryCategoryTest : BaseApiTest
{
    private const int DEAULT_DICTIONARY_COUNT = 4;
    public ListDictionaryCategoryTest() : base(DictionaryCategoryEndpoints.PATH)
    {
    }

    #region Positive Test Cases

    [Fact]
    public async Task ListDictionaryCategory_ShouldReturn_AllCategories()
    {
        // Arrange
        await CreateTestDictionaryCategoryAsync("Category 1");
        await CreateTestDictionaryCategoryAsync("Category 2");

        // Act
        var response = await ListDictionaryCategoryAsync();

        // Assert
        AssertSuccessResponse(response);
        response.Value.Should().HaveCount(DEAULT_DICTIONARY_COUNT + 2);
        response.Value.Select(c => c.Name).Should().Contain(["Category 1", "Category 2"]);
    }

    [Fact]
    public async Task ListDictionaryCategory_NoCategories_ShouldReturn_EmptyList()
    {
        // Act
        var response = await ListDictionaryCategoryAsync();

        // Assert
        //AssertBaseNotFoundResponseError(response, "No dictionary category found.");
        AssertSuccessResponse(response);
        response.Value.Count.Should().Be(DEAULT_DICTIONARY_COUNT);
    }

    #endregion

    #region Helper Methods

    private async Task<Result<List<BaseDictionaryCategoryDTO>>> ListDictionaryCategoryAsync()
    {
        return await httpClient.GetAndDeserializeAsync<Result<List<BaseDictionaryCategoryDTO>>>(RequestURIPath);
    }

    private async Task<Domain.Dictionary.DictionaryCategory> CreateTestDictionaryCategoryAsync(string name)
    {
        return await new DictionaryCategoryBuilder(GetServiceProvider())
            .WithValid()
            .WithName(name)
            .BuildAsync();
    }

    private void AssertSuccessResponse(Result<List<BaseDictionaryCategoryDTO>> response)
    {
        AssertBaseSuccessResponse(response);
        response.Value.Should().NotBeNull();
    }

    #endregion
}
