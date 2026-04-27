using MatchLogic.Api.Common;
using MatchLogic.Api.Endpoints;
using MatchLogic.Api.Handlers.RegexInfo.Create;
using MatchLogic.Api.IntegrationTests.Builders;
using MatchLogic.Api.IntegrationTests.Common;

namespace MatchLogic.Api.IntegrationTests.RegexInfo;

[Collection("RegexInfo Tests")]
public class CreateRegexInfoTest : BaseApiTest
{
    public CreateRegexInfoTest() : base(RegexInfoEndpoints.PATH)
    {
    }

    #region Positive Test Cases

    [Fact]
    public async Task CreateRegex_ValidRequest_ShouldReturn_Success()
    {
        // Arrange
        var request = new CreateRegexInfoRequest
        {
            Name = "Valid Regex",
            Description = "This is a valid regex pattern.",
            RegexExpression = "^[a-zA-Z0-9]+$",
            IsDefault = true
        };

        // Act
        var response = await CreateRegexAsync(request);

        // Assert
        AssertSuccessResponse(response, request);
    }

    [Fact]
    public async Task CreateRegex_MinimalValidRequest_ShouldReturn_Success()
    {
        // Arrange
        var request = new CreateRegexInfoRequest
        {
            Name = "Minimal Regex",
            RegexExpression = "^[000-9000]+$"
        };

        // Act
        var response = await CreateRegexAsync(request);

        // Assert
        AssertSuccessResponse(response, request);
    }


    #endregion

    #region Negative Test Cases
    [Fact]
    public async Task CreateRegex_NullName_ShouldReturn_InvalidError()
    {
        // Arrange
        var request = new CreateRegexInfoRequest
        {
            Name = null,
            Description = "This is a valid regex pattern.",
            RegexExpression = "^[a-zA-Z0-9]+$",
            IsDefault = true
        };

        // Act
        var response = await CreateRegexAsync(request);

        // Assert
        AssertInvalidResponse(response, "Name", "'Name' must not be empty.");
    }
    [Fact]
    public async Task CreateRegex_WhitespaceName_ShouldReturn_InvalidError()
    {
        // Arrange
        var request = new CreateRegexInfoRequest
        {
            Name = "   ",
            Description = "This is a valid regex pattern.",
            RegexExpression = "^[a-zA-Z0-9]+$",
            IsDefault = true
        };

        // Act
        var response = await CreateRegexAsync(request);

        // Assert
        AssertInvalidResponse(response, "Name", ValidationMessages.Required("Name"));
    }

    [Fact]
    public async Task CreateRegex_EmptyName_ShouldReturn_InvalidError()
    {
        // Arrange
        var request = new CreateRegexInfoRequest
        {
            Name = "",
            Description = "This is a valid regex pattern.",
            RegexExpression = "^[a-zA-Z0-9]+$",
            IsDefault = true
        };

        // Act
        var response = await CreateRegexAsync(request);

        // Assert
        AssertInvalidResponse(response, "Name", ValidationMessages.Required("Name"));
    }

    [Fact]
    public async Task CreateRegex_DescriptionExceedsMaxLength_ShouldReturn_InvalidError()
    {
        // Arrange
        var request = new CreateRegexInfoRequest
        {
            Name = "Valid Name", 
            Description = new string('A', Application.Common.Constants.FieldLength.DescriptionMaxLength + 1),// Exceeds max length of 2000
            RegexExpression = "^[a-zA-Z0-9]+$",
            IsDefault = true
        };

        // Act
        var response = await CreateRegexAsync(request);
        // Assert
        AssertInvalidResponse(response, "Description", ValidationMessages.MaxLength("Description", Application.Common.Constants.FieldLength.DescriptionMaxLength));
    }
    [Fact]
    public async Task CreateRegex_NameExceedsMaxLength_ShouldReturn_InvalidError()
    {
        // Arrange
        var request = new CreateRegexInfoRequest
        {
            Name = new string('A', Api.Common.ApiConstants.RegexFieldLength.NameMaxLength + 1), // Exceeds max length of 200
            Description = "This is a valid regex pattern.",
            RegexExpression = "^[a-zA-Z0-9]+$",
            IsDefault = true
        };

        // Act
        var response = await CreateRegexAsync(request);
        // Assert
        AssertInvalidResponse(response, "Name", ValidationMessages.MaxLength("Name", ApiConstants.RegexFieldLength.NameMaxLength));
    }

    [Fact]
    public async Task CreateRegex_InvalidRegexExpression_ShouldReturn_InvalidError()
    {
        // Arrange
        var request = new CreateRegexInfoRequest
        {
            Name = "Invalid Regex",
            Description = "This is an invalid regex pattern.",
            RegexExpression = "[a-zA-Z0-9", // Invalid regex
            IsDefault = true
        };

        // Act
        var response = await CreateRegexAsync(request);

        // Assert
        AssertInvalidResponse(response, "RegexExpression", ValidationMessages.Invalid("regex expression"));
    }

    [Fact]
    public async Task CreateRegex_DuplicateName_ShouldReturn_InvalidError()
    {
        // Arrange

        var existingRegexInfo = await new RegexInfoBuilder(GetServiceProvider())
            .WithValid()
            .WithName("Duplicate Regex Name")
            .WithRegexExpression("^[a-zA-Z]+$")
            .BuildAsync();

        var request = new CreateRegexInfoRequest
        {
            Name = "Duplicate Regex Name",
            Description = "This is a duplicate regex pattern.",
            RegexExpression = "^[a-zA-Z0-9]+$",
            IsDefault = true
        };
        // Act
        var response = await CreateRegexAsync(request);

        // Assert
        AssertInvalidResponse(response, "Name", ValidationMessages.AlreadyExists("regex pattern"));
    }
    [Fact]
    public async Task CreateRegex_EmptyRegexExpression_ShouldReturn_InvalidError()
    {
        // Arrange
        var request = new CreateRegexInfoRequest
        {
            Name = "Empty Regex Expression",
            Description = "This is an invalid regex pattern.",
            RegexExpression = "",
            IsDefault = true
        };

        // Act
        var response = await CreateRegexAsync(request);

        // Assert
        AssertInvalidResponse(response, "RegexExpression", ValidationMessages.Required("Regex expression"));
    }
    [Fact]
    public async Task CreateRegex_WhitespaceRegexExpression_ShouldReturn_InvalidError()
    {
        // Arrange
        var request = new CreateRegexInfoRequest
        {
            Name = "Whitespace Regex Expression",
            Description = "This is an invalid regex pattern.",
            RegexExpression = "   ",
            IsDefault = true
        };

        // Act
        var response = await CreateRegexAsync(request);

        // Assert
        AssertInvalidResponse(response, "RegexExpression", ValidationMessages.Required("Regex expression"));
    }

    
    [Fact]
    public async Task CreateRegex_RegexExpressionTooLong_ShouldReturn_InvalidError()
    {
        // Arrange
        var request = new CreateRegexInfoRequest
        {
            Name = "Long Regex Expression",
            Description = "This regex expression exceeds the maximum length.",
            RegexExpression = new string('a', Api.Common.ApiConstants.RegexFieldLength.RegexMaxLength + 1),
            IsDefault = true
        };

        // Act
        var response = await CreateRegexAsync(request);

        // Assert
        AssertInvalidResponse(response, "RegexExpression", ValidationMessages.MaxLength("RegexExpression", ApiConstants.RegexFieldLength.RegexMaxLength));
    }

    [Fact]
    public async Task CreateRegex_DuplicateRegex_ShouldReturn_InvalidError()
    {
        // Arrange
        // Create the first default regex
        var existingRegexInfo = await new RegexInfoBuilder(GetServiceProvider())
            .WithValid()
            .WithName("Duplicate Regex")
            .WithRegexExpression("^[a-zA-Z]+$")
            .BuildAsync();

        var newDefaultRequest = new CreateRegexInfoRequest
        {
            Name = "New Default Regex",
            Description = "This is another default regex pattern.",
            RegexExpression = "^[a-zA-Z]+$",    // Same as previous one
            IsDefault = true
        };

        // Act
        var response = await CreateRegexAsync(newDefaultRequest);

        // Assert
        AssertInvalidResponse(response, "RegexExpression", ValidationMessages.AlreadyExists("regex expression"));
    }


    #endregion

    #region Helper Methods

    private async Task<Result<CreateRegexInfoResponse>> CreateRegexAsync(CreateRegexInfoRequest request)
    {
        return await httpClient.PostAndDeserializeAsync<Result<CreateRegexInfoResponse>>(RequestURIPath, StringContentHelpers.FromModelAsJson(request));
    }

    private void AssertSuccessResponse(Result<CreateRegexInfoResponse> response, CreateRegexInfoRequest request)
    {
        AssertBaseSuccessResponse(response);
        response.Value.Id.Should().NotBe(Guid.Empty);
        response.Value.Name.Should().Be(request.Name);
        response.Value.Description.Should().Be(request.Description);
        response.Value.RegexExpression.Should().Be(request.RegexExpression);
        response.Value.IsDefault.Should().Be(request.IsDefault);
        response.Value.IsSystem.Should().BeFalse();
        response.Value.IsSystemDefault.Should().BeFalse();
        //response.Value.IsDeleted.Should().BeFalse();
        response.Value.Version.Should().Be(-1);
    }

    private void AssertInvalidResponse(Result<CreateRegexInfoResponse> response, string identifier, string errorMessage)
    {
        AssertBaseSingleValidationError(response, identifier, errorMessage);
    }

    #endregion
}
