using MatchLogic.Domain.Project;

namespace MatchLogic.Api.Handlers.Cleansing.Create;

public record CreateCleansingRuleResponse(ProjectRun ProjectRun) : ICleansingRuleResponse;
