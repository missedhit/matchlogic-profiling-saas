using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.Import;
public record ImportResponse
{
    public Guid JobId { get; init; }

    public ImportResponse(Guid jobId)
    {
        JobId = jobId;
    }
}
