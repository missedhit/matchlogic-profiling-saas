
using MatchLogic.Application.Features.MatchDefinition.DTOs;
using MatchLogic.Domain.Entities;
using System;
using System.Collections.Generic;


namespace MatchLogic.Api.Handlers.MatchDefinition.Update;
public class UpdateMatchDefinitionResponse
{
    public MatchDefinitionCollectionMappedRowDto MatchDefinition { get; set; }
    public MatchSettings MatchSetting { get; set; }
}
