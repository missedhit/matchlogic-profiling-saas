using MatchLogic.Domain.Entities.Common;
using System.Collections.Generic;
using System;
using MatchLogic.Domain.Entities.Common;
using MatchLogic.Domain.Import;

namespace MatchLogic.Api.Handlers.JobInfo;

public class JobRunStatusResponse
{
    public List<Domain.Entities.Common.JobStatus> JobStatuses { get; set; }
    public String RunStatus { get; set; }
}
