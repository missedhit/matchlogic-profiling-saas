using MatchLogic.Domain.Project;
using System;

namespace MatchLogic.Domain.Entities.Common;

public class ProjectJobInfo
{
    public Guid JobId { get; set; }
    public Guid RunId { get; set; }
    public Guid ProjectId { get; set; }
    public StepJob CurrentStep { get; set; }
}
