using MatchLogic.Api.Common;
using MatchLogic.Api.Endpoints;
using MatchLogic.Api.Handlers.RegexInfo.Update;
using MatchLogic.Api.IntegrationTests.Builders;
using MatchLogic.Api.IntegrationTests.Common;
using DomainRegex = MatchLogic.Domain.Regex.RegexInfo;

namespace MatchLogic.Api.IntegrationTests.RegexInfo;

[Collection("RegexInfo Tests")]
public class UpdateRegexInfoTest : BaseApiTest
{
    public UpdateRegexInfoTest() : base(RegexInfoEndpoints.PATH)
    {
    }

    #region Positive Test Cases

    [Fact]
    public async Task UpdateRegex_ValidRequest_ShouldReturn_Success()
    {
        // Arrange
        var existingRegex = await CreateTestRegexAsync();
        var request = new UpdateRegexInfoRequest
        {
            Id = existingRegex.Id,
            Name = "Updated Regex",
            Description = "This is an updated regex pattern.",
            //RegexExpression = "^[a-zA-Z]+$",
            RegexExpression = RandomRegex(),
            IsDefault = false
        };

        // Act
        var response = await UpdateRegexAsync(request);

        // Assert
        AssertSuccessResponse(response, request);
    }

    #endregion

    #region Negative Test Cases

    [Fact]
    public async Task UpdateRegex_InvalidId_ShouldReturn_NotFound()
    {
        // Arrange
        var request = new UpdateRegexInfoRequest
        {
            Id = Guid.NewGuid(), // Non-existent ID
            Name = "Updated Regex",
            Description = "This is an updated regex pattern.",
            RegexExpression = RandomRegex(), //"^[a-zA-Z]+$",
            IsDefault = false
        };

        // Act
        var response = await UpdateRegexAsync(request);

        // Assert
        AssertInvalidResponse(response, "Id", ValidationMessages.NotExists("Regex pattern"));

    }

    [Fact]
    public async Task UpdateRegex_EmptyName_ShouldReturn_InvalidError()
    {
        // Arrange
        var existingRegex = await CreateTestRegexAsync();
        var request = new UpdateRegexInfoRequest
        {
            Id = existingRegex.Id,
            Name = "",
            Description = "This is an updated regex pattern.",
            RegexExpression = existingRegex.RegexExpression,
            IsDefault = false
        };

        // Act
        var response = await UpdateRegexAsync(request);

        // Assert
        AssertInvalidResponse(response, "Name", ValidationMessages.Required("Name"));
    }

    [Fact]
    public async Task UpdateRegex_NameExceedsMaxLength_ShouldReturn_InvalidError()
    {
        // Arrange
        var existingRegex = await CreateTestRegexAsync();
        var request = new UpdateRegexInfoRequest
        {
            Id = existingRegex.Id,
            Name = new string('A', Api.Common.ApiConstants.RegexFieldLength.NameMaxLength + 1), // Exceeds max length
            Description = "This is an updated regex pattern.",
            RegexExpression = existingRegex.RegexExpression,
            IsDefault = false
        };

        // Act
        var response = await UpdateRegexAsync(request);

        // Assert
        AssertInvalidResponse(response, "Name", ValidationMessages.MaxLength("Name", ApiConstants.RegexFieldLength.NameMaxLength));
    }


    [Fact]
    public async Task UpdateRegex_DescriptionExceedsMaxLength_ShouldReturn_InvalidError()
    {
        // Arrange
        var existingRegex = await CreateTestRegexAsync();
        var request = new UpdateRegexInfoRequest
        {
            Id = existingRegex.Id,
            Name = "Valid Regex Name",
            Description = new string('A', Application.Common.Constants.FieldLength.DescriptionMaxLength + 1), // Exceeds max length
            RegexExpression = existingRegex.RegexExpression,
            IsDefault = false
        };

        // Act
        var response = await UpdateRegexAsync(request);

        // Assert
        AssertInvalidResponse(response, "Description", ValidationMessages.MaxLength("Description", Application.Common.Constants.FieldLength.DescriptionMaxLength));
    }

    [Fact]
    public async Task UpdateRegex_InvalidRegexExpression_ShouldReturn_InvalidError()
    {
        // Arrange
        var existingRegex = await CreateTestRegexAsync();
        var request = new UpdateRegexInfoRequest
        {
            Id = existingRegex.Id,
            Name = "Updated Regex",
            Description = "This is an updated regex pattern.",
            RegexExpression = "[a-zA-Z", // Invalid regex
            IsDefault = false
        };

        // Act
        var response = await UpdateRegexAsync(request);

        // Assert
        AssertInvalidResponse(response, "RegexExpression", ValidationMessages.Invalid("regex expression"));
    }

    [Fact]
    public async Task UpdateRegex_DuplicateName_ShouldReturn_InvalidError()
    {
        // Arrange
        var existingRegex1 = await CreateTestRegexAsync("Regex 1");
        var existingRegex2 = await CreateTestRegexAsync("Regex 2");

        var request = new UpdateRegexInfoRequest
        {
            Id = existingRegex2.Id,
            Name = existingRegex1.Name, // Duplicate name
            Description = "This is an updated regex pattern.",
            RegexExpression = RandomRegex(),
            IsDefault = false
        };

        // Act
        var response = await UpdateRegexAsync(request);

        // Assert
        AssertInvalidResponse(response, "Name", ValidationMessages.AlreadyExists("regex pattern"));
    }

    [Fact]
    public async Task UpdateRegex_SystemPattern_ShouldReturn_Error()
    {
        // Arrange
        var existingRegex = await CreateTestRegexAsync(isSystem: true);
        var request = new UpdateRegexInfoRequest
        {
            Id = existingRegex.Id,
            Name = "Updated Regex",
            Description = "This is an updated regex pattern.",
            RegexExpression = existingRegex.RegexExpression,
            IsDefault = false
        };

        // Act
        var response = await UpdateRegexAsync(request);

        // Assert
        AssertInvalidResponse(response, "Id", "System regex patterns cannot be modified.");
    }

    [Fact]
    public async Task UpdateRegex__RegexExpressionTooLong_ShouldReturn_InvalidError()
    {
        // Arrange
        var existingRegex = await CreateTestRegexAsync(isSystem: false);
        var request = new UpdateRegexInfoRequest
        {
            Id = existingRegex.Id,
            Name = "Long Regex Expression",
            Description = "This regex expression exceeds the maximum length.",
            RegexExpression = new string('a', Api.Common.ApiConstants.RegexFieldLength.RegexMaxLength + 1),
            IsDefault = true
        };

        // Act
        var response = await UpdateRegexAsync(request);

        // Assert
        AssertInvalidResponse(response, "RegexExpression", ValidationMessages.MaxLength("RegexExpression", ApiConstants.RegexFieldLength.RegexMaxLength));
    }
    [Fact]
    public async Task UpdateRegex_DuplicateRegex_ShouldReturn_InvalidError()
    {
        // Arrange
        // Create the first default regex
        var firstRegexInfo = await new RegexInfoBuilder(GetServiceProvider())
            .WithValid()
            .WithName("Duplicate Regex")
            .WithRegexExpression("^[a-zA-Z]+$")
            .BuildAsync();

        var secondRegexInfo = await new RegexInfoBuilder(GetServiceProvider())
            .WithValid()
            .WithName("Duplicate Regex")
            .WithRegexExpression("^[a-zA-Z0-9]+$")
            .BuildAsync();

        var newDefaultRequest = new UpdateRegexInfoRequest
        {
            Id = secondRegexInfo.Id,
            Name = "Updated Duplicate Regex",
            Description = "This is duplicate regex pattern.",
            RegexExpression = "^[a-zA-Z]+$",    // Same as first one
            IsDefault = true
        };

        // Act
        var response = await UpdateRegexAsync(newDefaultRequest);

        // Assert
        AssertInvalidResponse(response, "RegexExpression", ValidationMessages.AlreadyExists("regex expression"));
    }
    #endregion

    #region Helper Methods

    private async Task<Result<UpdateRegexInfoResponse>> UpdateRegexAsync(UpdateRegexInfoRequest request)
    {
        return await httpClient.PutAndDeserializeAsync<Result<UpdateRegexInfoResponse>>(RequestURIPath, StringContentHelpers.FromModelAsJson(request));
    }

    private async Task<DomainRegex> CreateTestRegexAsync(string name = "Test Regex", bool isSystem = false)
    {
        return await new RegexInfoBuilder(GetServiceProvider())
            .WithValid()
            .WithName(name)
            .WithIsSystem(isSystem)
            .BuildAsync();

        /*var request = new CreateRegexInfoRequest
        {
            Name = name,
            Description = "This is a test regex pattern.",
            RegexExpression = "^[a-zA-Z0-9]+$",
            IsDefault = false
        };

        var response = await httpClient.PostAndDeserializeAsync<Result<CreateRegexInfoResponse>>(RequestURIPath, StringContentHelpers.FromModelAsJson(request));
        response.Should().NotBeNull();
        response.IsSuccess.Should().BeTrue();

        var createdRegex = response.Value;
        createdRegex.Should().NotBeNull();
        createdRegex.IsSystem = isSystem; // Simulate system pattern if needed
        return createdRegex;*/
    }

    private void AssertSuccessResponse(Result<UpdateRegexInfoResponse> response, UpdateRegexInfoRequest request)
    {
        AssertBaseSuccessResponse(response);
        response.Value.Id.Should().Be(request.Id);
        response.Value.Name.Should().Be(request.Name);
        response.Value.Description.Should().Be(request.Description);
        response.Value.RegexExpression.Should().Be(request.RegexExpression);
        response.Value.IsDefault.Should().Be(request.IsDefault);
    }

    private void AssertInvalidResponse(Result<UpdateRegexInfoResponse> response, string identifier, string errorMessage)
    {
        AssertBaseSingleValidationError(response, identifier, errorMessage);
    }
    private string RandomRegex()
    {
        // Generate a unique random regex pattern
        var random = new Random();
        var length = random.Next(5, 15);
        var chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        var pattern = new char[length];
        for (int i = 0; i < length; i++)
        {
            // Randomly choose between a character or a character class
            if (random.NextDouble() < 0.7)
            {
                pattern[i] = chars[random.Next(chars.Length)];
            }
            else
            {
                // Add a simple character class
                pattern[i] = '.';
            }
        }
        var uniqueRegex = $"^{new string(pattern)}$";
        return uniqueRegex;
    }
    #endregion
}
