using MatchLogic.Application.Features.MatchDefinition.DTOs;
using System;
using System.Collections.Generic;

namespace MatchLogic.Api.Handlers.MappedFieldRow.Update;
public record UpdateMappedFieldRowCommand(List<MappedFieldRowDto> mappedFieldRows, Guid projectId) : IRequest<Result<UpdateMappedFieldRowResponse>>;

