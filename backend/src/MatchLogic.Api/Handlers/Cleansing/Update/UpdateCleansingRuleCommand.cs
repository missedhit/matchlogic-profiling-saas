using System.Collections.Generic;
using System;
using MatchLogic.Application.Features.CleansingAndStandardization.DTOs;
using MatchLogic.Api.Handlers.Cleansing.Create;

namespace MatchLogic.Api.Handlers.Cleansing.Update;

public class UpdateCleansingRuleCommand : BaseCleansingRuleCommand<UpdateCleansingRuleResponse>
{
    public Guid Id { get; set; }
}

public class UpdateCleansingRule: BaseCleansingRule
{
    public Guid Id { get; set; }
}
