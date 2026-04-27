using MatchLogic.Domain.Project;
using System;

namespace MatchLogic.Api.Handlers.Cleansing.Update;

public record class UpdateCleansingRuleResponse(ProjectRun ProjectRun) : ICleansingRuleResponse;
