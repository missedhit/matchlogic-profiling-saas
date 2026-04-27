using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Application.Interfaces.DataMatching;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching;
public class MatchGroupingServiceFactory
{
    private readonly IOptions<RecordLinkageOptions> _options;

    public MatchGroupingServiceFactory(IOptions<RecordLinkageOptions> options)
    {
        _options = options;
    }

    public IMatchGroupingService CreateMatchGroupingService(bool similarRecordsInGroup)
    {
        if (similarRecordsInGroup)
            return new MatchGroupingServiceWithSimilarRecordsInGroups(_options);
        return new MatchGroupingService(_options);
    }
}
