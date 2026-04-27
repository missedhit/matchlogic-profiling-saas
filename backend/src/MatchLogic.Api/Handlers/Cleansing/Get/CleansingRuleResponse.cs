using MatchLogic.Domain.CleansingAndStandaradization;
using System.Collections.Generic;

namespace MatchLogic.Api.Handlers.Cleansing.Get;

public class CleansingRuleResponse
{
    public Dictionary<OperationType, string> OperationType { get; set; }
    public Dictionary<CleaningRuleType, string> CleansingType { get; set; }
    public Dictionary<MappingOperationType, string> MappingOperationType { get; set; }

    public Dictionary<CleaningRuleType, List<OperationParameter>> CleansingTypeParameters { get; set; }
    public Dictionary<MappingOperationType, List<OperationParameter>> MappingTypeParameters { get; set; }
    public Dictionary<MappingOperationType, MappingRequirements> MappingTypeRequirements { get; set; }
}
