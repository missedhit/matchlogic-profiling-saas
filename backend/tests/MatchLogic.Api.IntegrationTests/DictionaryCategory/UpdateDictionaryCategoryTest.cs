using MatchLogic.Api.Endpoints;
using MatchLogic.Api.Handlers.DictionaryCategory.Update;
using MatchLogic.Api.IntegrationTests.Common;
using MatchLogic.Api.IntegrationTests.Builders;
using FluentAssertions;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;
using DomainModel = MatchLogic.Domain.Dictionary.DictionaryCategory;
using MatchLogic.Api.Common;

namespace MatchLogic.Api.IntegrationTests.DictionaryCategory;

[Collection("DictionaryCategory Tests")]
public class UpdateDictionaryCategoryTest : BaseApiTest
{
    public UpdateDictionaryCategoryTest() : base(DictionaryCategoryEndpoints.PATH)
    {
    }

    #region Positive Test Cases

    [Fact]
    public async Task UpdateDictionaryCategory_ValidRequest_ShouldReturn_Success()
    {
        // Arrange
        var existingCategory = await CreateTestDictionaryCategoryAsync();
        var request = new UpdateDictionaryCategoryRequest
        {
            Id = existingCategory.Id,
            Name = "Updated Dictionary Category",
            Description = "This is an updated dictionary category.",
            Items = new List<string> { "UpdatedItem1", "UpdatedItem2" }
        };

        // Act
        var response = await UpdateDictionaryCategoryAsync(request);

        // Assert
        AssertSuccessResponse(response, request);
    }

    #endregion

    #region Negative Test Cases

    [Fact]
    public async Task UpdateDictionaryCategory_InvalidId_ShouldReturn_NotFound()
    {
        // Arrange
        var request = new UpdateDictionaryCategoryRequest
        {
            Id = Guid.NewGuid(), // Non-existent ID
            Name = "Updated Dictionary Category",
            Description = "This is an updated dictionary category.",
            Items = ["UpdatedItem1", "UpdatedItem2"]
        };

        // Act
        var response = await UpdateDictionaryCategoryAsync(request);

        // Assert
        AssertBaseValidationErrors(response, [
            ("Id", ValidationMessages.NotExists("Dictionary")),
            ("Id", "System Dictionary cannot be updated.")
        ]);
    }

    [Fact]
    public async Task UpdateDictionaryCategory_EmptyName_ShouldReturn_InvalidError()
    {
        // Arrange
        var existingCategory = await CreateTestDictionaryCategoryAsync();
        var request = new UpdateDictionaryCategoryRequest
        {
            Id = existingCategory.Id,
            Name = "",
            Description = "This is an updated dictionary category.",
            Items = new List<string> { "UpdatedItem1", "UpdatedItem2" }
        };

        // Act
        var response = await UpdateDictionaryCategoryAsync(request);

        // Assert
        AssertInvalidResponse(response, "Name", ValidationMessages.Required("Name"));
    }

    #endregion

    #region Helper Methods

    private async Task<Result<DomainModel>> UpdateDictionaryCategoryAsync(UpdateDictionaryCategoryRequest request)
    {
        return await httpClient.PutAndDeserializeAsync<Result<DomainModel>>(
            RequestURIPath,
            StringContentHelpers.FromModelAsJson(request));
    }

    private async Task<DomainModel> CreateTestDictionaryCategoryAsync()
    {
        return await new DictionaryCategoryBuilder(GetServiceProvider())
            .WithValid()
            .BuildAsync();
    }

    private void AssertSuccessResponse(Result<DomainModel> response, UpdateDictionaryCategoryRequest request)
    {
        AssertBaseSuccessResponse(response);
        response.Value.Id.Should().Be(request.Id);
        response.Value.Name.Should().Be(request.Name);
        response.Value.Description.Should().Be(request.Description);
        response.Value.Items.Should().BeEquivalentTo(request.Items);
    }

    private void AssertInvalidResponse(Result<DomainModel> response, string identifier, string errorMessage)
    {
        AssertBaseSingleValidationError(response, identifier, errorMessage);
    }

    #endregion
}
