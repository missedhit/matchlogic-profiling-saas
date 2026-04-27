using MatchLogic.Domain.CleansingAndStandaradization;
using System.Collections.Generic;

namespace MatchLogic.Api.Handlers.Cleansing.GetRules;
public record GetCleansingRulesResponse(List<EnhancedCleaningRules> rules);
