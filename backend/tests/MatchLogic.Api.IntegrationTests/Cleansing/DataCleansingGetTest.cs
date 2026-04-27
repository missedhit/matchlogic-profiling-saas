using MatchLogic.Api.Endpoints;
using MatchLogic.Api.Handlers.Cleansing.Get;
using MatchLogic.Api.IntegrationTests.Common;
using MatchLogic.Domain.CleansingAndStandaradization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Api.IntegrationTests.Cleansing;
[Collection("DataCleansing Get Tests")]
public class DataCleansingGetTest : BaseApiTest
{
    public DataCleansingGetTest() : base(DataCleansingEndpoints.PATH)
    {
    }

    [Fact]
    public async Task GetCleansingRules_ShouldReturn_Success()
    {
        // Act
        var response = await GetCleansingRulesAsync();

        // Assert
        AssertSuccessResponse(response);
    }

    private async Task<Result<CleansingRuleResponse>> GetCleansingRulesAsync()
    {
        return await httpClient.GetAndDeserializeAsync<Result<CleansingRuleResponse>>($"{RequestURIPath}/");
    }

    private void AssertSuccessResponse(Result<CleansingRuleResponse> response)
    {
        AssertBaseSuccessResponse(response);

        // Verify operation type metadata
        response.Value.OperationType.Should().NotBeNull();
        response.Value.OperationType.Should().ContainKey(OperationType.Standard);
        response.Value.OperationType.Should().ContainKey(OperationType.Mapping);

        // Verify cleansing type metadata
        response.Value.CleansingType.Should().NotBeNull();
        response.Value.CleansingType.Should().ContainKey(CleaningRuleType.Trim);
        response.Value.CleansingType.Should().ContainKey(CleaningRuleType.UpperCase);

        // Verify mapping operation type metadata
        response.Value.MappingOperationType.Should().NotBeNull();
        response.Value.MappingOperationType.Should().ContainKey(MappingOperationType.WordSmith);

        // Verify parameter definitions
        response.Value.CleansingTypeParameters.Should().NotBeNull();
        response.Value.CleansingTypeParameters.Should().ContainKey(CleaningRuleType.Replace);
        response.Value.CleansingTypeParameters[CleaningRuleType.Replace].Should().NotBeEmpty();

        // Verify mapping parameters
        response.Value.MappingTypeParameters.Should().NotBeNull();
        response.Value.MappingTypeParameters.Should().ContainKey(MappingOperationType.WordSmith);

        // Verify mapping requirements
        response.Value.MappingTypeRequirements.Should().NotBeNull();
        response.Value.MappingTypeRequirements.Should().ContainKey(MappingOperationType.WordSmith);
        response.Value.MappingTypeRequirements[MappingOperationType.WordSmith].RequiresSourceColumns.Should().BeTrue();
    }
}
