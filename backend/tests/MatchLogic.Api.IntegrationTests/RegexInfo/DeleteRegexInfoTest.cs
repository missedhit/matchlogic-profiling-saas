using Ardalis.HttpClientTestExtensions;
using MatchLogic.Api.Common;
using MatchLogic.Api.Endpoints;
using MatchLogic.Api.Handlers.RegexInfo.Delete;
using MatchLogic.Api.IntegrationTests.Builders;
using MatchLogic.Api.IntegrationTests.Common;
using FluentAssertions;
using System;
using System.Threading.Tasks;
using Xunit;
using DomainRegex = MatchLogic.Domain.Regex.RegexInfo;

namespace MatchLogic.Api.IntegrationTests.RegexInfo;

[Collection("RegexInfo Tests")]
public class DeleteRegexInfoTest : BaseApiTest
{
    public DeleteRegexInfoTest() : base(RegexInfoEndpoints.PATH)
    {
    }

    #region Positive Test Cases

    [Fact]
    public async Task DeleteRegex_ValidId_ShouldReturn_Success()
    {
        // Arrange
        var existingRegex = await CreateTestRegexAsync();

        // Act
        var response = await DeleteRegexAsync(existingRegex.Id);

        // Assert
        AssertSuccessResponse(response, existingRegex.Id);
    }

    #endregion

    #region Negative Test Cases

    [Fact]
    public async Task DeleteRegex_EmptyId_ShouldReturn_NotFound()
    {
        // Arrange
        var invalidId = Guid.Empty;

        // Act
        var response = await DeleteRegexAsync(invalidId);

        // Assert
        AssertInvalidResponse(response, "Id", ValidationMessages.Required("ID"));
    }

    [Fact]
    public async Task DeleteRegex_InvalidId_ShouldReturn_NotFound()
    {
        // Arrange
        var invalidId = Guid.NewGuid(); // Non-existent ID

        // Act
        var response = await DeleteRegexAsync(invalidId);

        // Assert
        //AssertBaseNotFoundResponseError(response);
        AssertInvalidResponse(response, "Id", "Regex pattern not found.");
    }

    [Fact]
    public async Task DeleteRegex_SystemPattern_ShouldReturn_Error()
    {
        // Arrange
        var existingRegex = await CreateTestRegexAsync(isSystem: true);

        // Act
        var response = await DeleteRegexAsync(existingRegex.Id);

        // Assert
        AssertInvalidResponse(response, "Id", "System regex patterns cannot be deleted.");
    }

    #endregion

    #region Helper Methods

    private async Task<Result<DeleteRegexInfoResponse>> DeleteRegexAsync(Guid id)
    {
        return await httpClient.DeleteAndDeserializeAsync<Result<DeleteRegexInfoResponse>>($"{RequestURIPath}/{id}");
    }

    private async Task<DomainRegex> CreateTestRegexAsync(string name = "Test Regex", bool isSystem = false)
    {
        return await new RegexInfoBuilder(GetServiceProvider())
            .WithValid()
            .WithName(name)
            .WithIsSystem(isSystem)
            .BuildAsync();
    }

    private void AssertSuccessResponse(Result<DeleteRegexInfoResponse> response, Guid id)
    {
        AssertBaseSuccessResponse(response);
        response.Value.IsDeleted.Should().BeTrue();
        response.Value.Message.Should().Be("Regex pattern deleted successfully.");
    }

    private void AssertInvalidResponse(Result<DeleteRegexInfoResponse> response, string identifier, string errorMessage)
    {
        AssertBaseSingleValidationError(response, identifier, errorMessage);
    }

    #endregion
}
