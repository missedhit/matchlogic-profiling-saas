using MatchLogic.Domain.Project;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Domain.Entities.Common;

public class MatchJobInfo
{
    public Guid JobId { get; set; }
    public bool MergeOverlappingGroups { get; set; }
    public bool IsProbabilistic { get; set; }
    public List<MatchCriteria> Criteria { get; set; }
}

public class ProjectJobInfo
{
    public Guid JobId { get; set; }
    public Guid RunId { get; set; }
    public Guid ProjectId { get; set; }
    public StepJob CurrentStep { get; set; }
}