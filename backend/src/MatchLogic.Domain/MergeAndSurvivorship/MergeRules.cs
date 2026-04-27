using MatchLogic.Domain.Entities.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Domain.MergeAndSurvivorship;
public class MergeRules : IEntity
{
    //public Guid Id { get; set; }
    public Guid ProjectId { get; set; }
    public Guid ProjectRunId { get; set; }
    public List<MergeRule> Rules { get; set; } = new();
}

public class MergeRule : IEntity
{
    //public Guid Id { get; set; }
    public Guid ColumnName { get; set; }
    public MergeRule RuleType { get; set; }
    public Dictionary<string, string> Arguments { get; set; } = new();

}

public enum MergeRuleType : byte
{
    None = 1,
    Longest = 2
}
