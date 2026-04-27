using MatchLogic.Application.Interfaces.MatchConfiguration;
using MatchLogic.Domain.MatchConfiguration;
using Microsoft.Extensions.DependencyInjection;

namespace MatchLogic.Api.IntegrationTests.Builders;
public class MatchConfigurationBuilder(IServiceProvider serviceProvider)
{
    private readonly IServiceProvider _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));

    private Guid _projectId = Guid.NewGuid();
    private List<MatchingDataSourcePair> _pairs = [];

    public MatchConfigurationBuilder WithProjectId(Guid projectId)
    {
        if (projectId == Guid.Empty)
            throw new ArgumentException("Project ID cannot be empty.", nameof(projectId));
        _projectId = projectId;
        return this;
    }
    public MatchConfigurationBuilder WithPairs(List<MatchingDataSourcePair> pairs)
    {
        _pairs = pairs;
        return this;
    }
    
    public async Task<MatchingDataSourcePairs> BuildAsync()
    {
        IMatchConfigurationService matchConfigurationService = _serviceProvider.GetRequiredService<IMatchConfigurationService>();
        var result = await matchConfigurationService.CreateDataSourcePairsAsync(_projectId, new MatchingDataSourcePairs(_pairs) { ProjectId = _projectId });
        return result;
    }
}
