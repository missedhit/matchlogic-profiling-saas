using MatchLogic.Api.Endpoints;
using MatchLogic.Api.Handlers.DictionaryCategory.Delete;
using MatchLogic.Api.IntegrationTests.Common;
using MatchLogic.Api.IntegrationTests.Builders;
using MatchLogic.Api.Common;

namespace MatchLogic.Api.IntegrationTests.DictionaryCategory;

[Collection("DictionaryCategory Tests")]
public class DeleteDictionaryCategoryTest : BaseApiTest
{
    public DeleteDictionaryCategoryTest() : base(DictionaryCategoryEndpoints.PATH)
    {
    }

    #region Positive Test Cases

    [Fact]
    public async Task DeleteDictionaryCategory_ValidId_ShouldReturn_Success()
    {
        // Arrange
        var existingCategory = await CreateTestDictionaryCategoryAsync();

        // Act
        var response = await DeleteDictionaryCategoryAsync(existingCategory.Id);

        // Assert
        AssertSuccessResponse(response, existingCategory.Id);
    }

    #endregion

    #region Negative Test Cases

    [Fact]
    public async Task DeleteDictionaryCategory_EmptyId_ShouldReturn_Error()
    {
        // Arrange
        var invalidId = Guid.Empty;

        // Act
        var response = await DeleteDictionaryCategoryAsync(invalidId);

        // Assert
        AssertBaseValidationErrors(response, [
            ("Id", ValidationMessages.Required("Id")),
            ("Id", ValidationMessages.NotExists("Dictionary")),
            ("Id", "System Dictionary cannot be deleted.")
            ]);
    }

    [Fact]
    public async Task DeleteDictionaryCategory_InvalidId_ShouldReturn_Error()
    {
        // Arrange
        var invalidId = Guid.NewGuid(); // Non-existent ID

        // Act
        var response = await DeleteDictionaryCategoryAsync(invalidId);

        // Assert
        AssertBaseValidationErrors(response, [
            ("Id", ValidationMessages.NotExists("Dictionary")),
            ("Id", "System Dictionary cannot be deleted.")
            ]);
    }


    [Fact]
    public async Task DeleteDictionaryCategory_SystemPattern_ShouldReturn_Error()
    {
        // Arrange
        var existingCategory = await new DictionaryCategoryBuilder(GetServiceProvider())
            .WithValid()
            .WithIsSystem(true)
            .BuildAsync();

        // Act
        var response = await DeleteDictionaryCategoryAsync(existingCategory.Id);

        // Assert
        AssertBaseSingleValidationError(response, "Id", "System Dictionary cannot be deleted.");
    }
    #endregion

    #region Helper Methods

    private async Task<Result<DeleteDictionaryCategoryResponse>> DeleteDictionaryCategoryAsync(Guid id)
    {
        return await httpClient.DeleteAndDeserializeAsync<Result<DeleteDictionaryCategoryResponse>>($"{RequestURIPath}/{id}");
    }

    private async Task<Domain.Dictionary.DictionaryCategory> CreateTestDictionaryCategoryAsync()
    {
        return await new DictionaryCategoryBuilder(GetServiceProvider())
            .WithValid()
            .BuildAsync();
    }

    private void AssertSuccessResponse(Result<DeleteDictionaryCategoryResponse> response, Guid id)
    {
        AssertBaseSuccessResponse(response);
        response.Value.Id.Should().Be(id);
        response.Value.Message.Should().Be("Dictionary category deleted successfully.");
    }

    #endregion
}
