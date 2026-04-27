using System;

namespace MatchLogic.Api.Handlers.MappedFieldRow.AutoMapping;
public record AutoMappingCommand(Guid projectId, MatchDefinitionMappingType mappingType) : IRequest<Result<AutoMappingResponse>>;

public enum MatchDefinitionMappingType
{
    Default,
    Sequential
}

