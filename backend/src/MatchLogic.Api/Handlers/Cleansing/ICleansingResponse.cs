using MatchLogic.Domain.Project;

namespace MatchLogic.Api.Handlers.Cleansing;

public interface ICleansingRuleResponse
{
    ProjectRun ProjectRun { get; }
}
