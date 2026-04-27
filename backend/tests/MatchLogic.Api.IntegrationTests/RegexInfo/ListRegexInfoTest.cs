using Ardalis.HttpClientTestExtensions;
using MatchLogic.Api.Endpoints;
using MatchLogic.Api.Handlers.RegexInfo;
using MatchLogic.Api.Handlers.RegexInfo.List;
using MatchLogic.Api.IntegrationTests.Builders;
using MatchLogic.Api.IntegrationTests.Common;
using FluentAssertions;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;
using DomainRegex = MatchLogic.Domain.Regex.RegexInfo;

namespace MatchLogic.Api.IntegrationTests.RegexInfo;

[Collection("RegexInfo Tests")]
public class ListRegexInfoTest : BaseApiTest
{
    private const int DEAULT_REGEX_COUNT = 10;
    public ListRegexInfoTest() : base(RegexInfoEndpoints.PATH)
    {
    }

    #region Positive Test Cases

    [Fact]
    public async Task ListRegex_ValidRequest_ShouldReturn_Success()
    {
        // Arrange
        await CreateTestRegexAsync("Regex 1");
        await CreateTestRegexAsync("Regex 2");

        // Act
        var response = await ListRegexAsync();

        // Assert
        AssertSuccessResponse(response, DEAULT_REGEX_COUNT + 2);
    }
    [Fact]
    public async Task ListRegex_NoRegexPatterns_ShouldReturn_EmptyList()
    {
        // Act
        var response = await ListRegexAsync();
        var customRegex = response.Value.Where(r => r.IsSystem!= true).ToList();
        // Assert
        response.Should().NotBeNull();
        response.Status.Should().Be(ResultStatus.Ok);
        response.IsSuccess.Should().BeTrue();
        response.ValidationErrors.Should().BeNullOrEmpty();
        response.Value.Should().NotBeNull();
        response.Value.Count.Should().Be(DEAULT_REGEX_COUNT);
        customRegex.Count.Should().Be(0);
    }
    /*[Fact]
    public async Task ListRegex_NoRegexPatterns_ShouldReturn_EmptyList()
    {
        // Act
        var response = await ListRegexAsync();

        // Assert
        response.Should().NotBeNull();
        response.Status.Should().Be(ResultStatus.NotFound);
        response.IsSuccess.Should().BeFalse();
        response.ValidationErrors.Should().BeNullOrEmpty();
        response.Errors.Should().NotBeNullOrEmpty();
        response.Errors.First().Equals("No regex patterns found.");
    }*/

    #endregion

    #region Negative Test Cases

   /* [Fact]
    public async Task ListRegex_InvalidEndpoint_ShouldReturn_NotFound()
    {
        // Act
        var response = await httpClient.GetAsync($"{RequestURIPath}/invalid-endpoint");

        // Assert
        response.StatusCode.Should().Be(System.Net.HttpStatusCode.NotFound);
    }*/

    #endregion

    #region Helper Methods

    private async Task<Result<List<RegexInfoDTO>>> ListRegexAsync()
    {
        return await httpClient.GetAndDeserializeAsync<Result<List<RegexInfoDTO>>>(RequestURIPath);
    }

    private async Task<DomainRegex> CreateTestRegexAsync(string name)
    {
        return await new RegexInfoBuilder(GetServiceProvider())
            .WithValid()
            .WithName(name)
            .BuildAsync();
    }

    private void AssertSuccessResponse(Result<List<RegexInfoDTO>> response, int expectedCount)
    {
        AssertBaseSuccessResponse(response);
        response.Value.Should().NotBeNull();
        response.Value.Count.Should().Be(expectedCount);
    }

    #endregion
}
