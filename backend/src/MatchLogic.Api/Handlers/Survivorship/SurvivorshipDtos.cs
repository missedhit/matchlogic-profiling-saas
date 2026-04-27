using System.Collections.Generic;

namespace MatchLogic.Api.Handlers.Survivorship;

/// <summary>
/// DTO for master record rule
/// </summary>
public class MasterRuleDto
{
    public string Id { get; set; } = string.Empty;
    public string FieldName { get; set; } = string.Empty;
    public Dictionary<string, bool> DataSources { get; set; } = new();
    public string Operation { get; set; } = "Longest";
    public bool Activated { get; set; }
    public int Order { get; set; }
}

public class OverwriteRuleDto : MasterRuleDto
{
    public string OverwriteIf { get; set; } = "No Condition";
    public string DonotOverwriteIf { get; set; } = "No Condition";
    public Dictionary<string,object> Configuration { get; set; } = new();
}