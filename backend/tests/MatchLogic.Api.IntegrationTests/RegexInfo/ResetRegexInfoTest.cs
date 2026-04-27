using MatchLogic.Api.Endpoints;
using MatchLogic.Api.Handlers.RegexInfo.ResetSystemDefaults;
using MatchLogic.Api.Handlers.RegexInfo.Update;
using MatchLogic.Api.IntegrationTests.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Api.IntegrationTests.RegexInfo;
[Collection("RegexInfo Tests")]
public class ResetRegexInfoTest : BaseApiTest
{
    public ResetRegexInfoTest() : base(RegexInfoEndpoints.PATH)
    {
    }
    [Fact]
    public async Task ResetRegex_ValidRequest_ShouldReturn_Success()
    {
        // Arrange
        var request = new ResetRegexRequest();
        // Act
        var response = await httpClient.PostAndDeserializeAsync<Result<ResetRegexResponse>>($"{RequestURIPath}/Reset", StringContentHelpers.FromModelAsJson(request));
        // Assert
        AssertBaseSuccessResponse(response);
        response.Value.IsReseted.Should().BeTrue();
        response.Value.Message.Should().Be("Successfully reset Regex Info to system defaults");
    }
}
