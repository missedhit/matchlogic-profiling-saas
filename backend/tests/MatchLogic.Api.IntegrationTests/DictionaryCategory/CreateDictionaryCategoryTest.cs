using MatchLogic.Api.Common;
using MatchLogic.Api.Endpoints;
using MatchLogic.Api.Handlers.DictionaryCategory.Create;
using MatchLogic.Api.IntegrationTests.Common;

namespace MatchLogic.Api.IntegrationTests.DictionaryCategory;

[Collection("DictionaryCategory Tests")]
public class CreateDictionaryCategoryTest : BaseApiTest
{
    public CreateDictionaryCategoryTest() : base(DictionaryCategoryEndpoints.PATH)
    {
    }

    #region Positive Test Cases

    [Fact]
    public async Task CreateDictionaryCategory_ValidRequest_ShouldReturn_Success()
    {
        // Arrange
        var request = new CreateDictionaryCategoryRequest
        {
            Name = "Valid Dictionary Category",
            Description = "This is a valid dictionary category.",
            Items = new List<string> { "Item1", "Item2", "Item3" },
        };

        // Act
        var response = await CreateDictionaryCategoryAsync(request);

        // Assert
        AssertSuccessResponse(response, request);
    }

    [Fact]
    public async Task CreateDictionaryCategory_WithEmptyItems_ShouldReturn_Invalid()
    {
        // Arrange
        var request = new CreateDictionaryCategoryRequest
        {
            Name = "Dictionary Category Without Items",
            Description = "This is a dictionary category without items.",
            Items = [], // Empty Items 
        };

        // Act
        var response = await CreateDictionaryCategoryAsync(request);

        // Assert

        AssertBaseSingleValidationError(response, "Items", "Items list cannot be empty.");
    }

    #endregion

    #region Negative Test Cases

    [Fact]
    public async Task CreateDictionaryCategory_EmptyName_ShouldReturn_InvalidError()
    {
        // Arrange
        var request = new CreateDictionaryCategoryRequest
        {
            Name = "",
            Description = "This is a valid dictionary category.",
            Items = new List<string> { "Item1", "Item2", "Item3" },
        };

        // Act
        var response = await CreateDictionaryCategoryAsync(request);

        // Assert
        AssertInvalidResponse(response, "Name", ValidationMessages.Required("Name"));
    }

    [Fact]
    public async Task CreateDictionaryCategory_NameExceedsMaxLength_ShouldReturn_InvalidError()
    {
        // Arrange
        var request = new CreateDictionaryCategoryRequest
        {
            Name = new string('A', Application.Common.Constants.FieldLength.NameMaxLength + 1), // Exceeds max length
            Description = "This is a valid dictionary category.",
            Items = new List<string> { "Item1", "Item2", "Item3" },
        };

        // Act
        var response = await CreateDictionaryCategoryAsync(request);

        // Assert
        AssertInvalidResponse(response, "Name", ValidationMessages.MaxLength("Name", Application.Common.Constants.FieldLength.NameMaxLength));
    }

    [Fact]
    public async Task CreateDictionaryCategory_DescriptionExceedsMaxLength_ShouldReturn_InvalidError()
    {
        // Arrange
        var request = new CreateDictionaryCategoryRequest
        {
            Name = "Valid Name",
            Description = new string('A', Application.Common.Constants.FieldLength.DescriptionMaxLength + 1), // Exceeds max length
            Items = new List<string> { "Item1", "Item2", "Item3" },
        };

        // Act
        var response = await CreateDictionaryCategoryAsync(request);

        // Assert
        AssertInvalidResponse(response, "Description", ValidationMessages.MaxLength("Description", Application.Common.Constants.FieldLength.DescriptionMaxLength));
    }

    [Fact]
    public async Task CreateDictionaryCategory_ItemWithEmptyValue_ShouldReturn_InvalidError()
    {
        // Arrange
        var request = new CreateDictionaryCategoryRequest
        {
            Name = "Dictionary Category With Empty Item",
            Description = "This is a dictionary category with an empty item.",
            Items = ["Item1", "", "Item3"],
        };

        // Act
        var response = await CreateDictionaryCategoryAsync(request);

        // Assert
        AssertInvalidResponse(response, "Items", ValidationMessages.CannotContainEmptyOrWhitespace("Items list"));
    }

    [Fact]
    public async Task CreateDictionaryCategory_DuplicateName_ShouldReturn_InvalidError()
    {
        // Arrange
        var request = new CreateDictionaryCategoryRequest
        {
            Name = "Duplicate Dictionary Category",
            Description = "This is a duplicate dictionary category.",
            Items = new List<string> { "Item1", "Item2", "Item3" },
        };

        // Create the first dictionary category
        await CreateDictionaryCategoryAsync(request);

        // Act
        var response = await CreateDictionaryCategoryAsync(request);

        // Assert
        AssertInvalidResponse(response, "Name", ValidationMessages.AlreadyExists("dictionary category"));
    }

    #endregion

    #region Helper Methods

    private async Task<Result<CreateDictionaryCategoryResponse>> CreateDictionaryCategoryAsync(CreateDictionaryCategoryRequest request)
    {
        return await httpClient.PostAndDeserializeAsync<Result<CreateDictionaryCategoryResponse>>(
            RequestURIPath,
            StringContentHelpers.FromModelAsJson(request));
    }

    private void AssertSuccessResponse(Result<CreateDictionaryCategoryResponse> response, CreateDictionaryCategoryRequest request)
    {
        AssertBaseSuccessResponse(response);
        response.Value.Id.Should().NotBe(Guid.Empty);
        response.Value.Name.Should().Be(request.Name);
        response.Value.Description.Should().Be(request.Description);
        response.Value.Items.Should().BeEquivalentTo(request.Items);
        //response.Value.IsDefault.Should().Be(request.IsDefault);
        response.Value.IsSystem.Should().BeFalse();
        //response.Value.IsSystemDefault.Should().BeFalse();
        //response.Value.IsDeleted.Should().BeFalse();
        response.Value.Version.Should().BeGreaterThan(0);
    }

    private void AssertInvalidResponse(Result<CreateDictionaryCategoryResponse> response, string identifier, string errorMessage)
    {
        AssertBaseSingleValidationError(response, identifier, errorMessage);
    }

    #endregion
}
