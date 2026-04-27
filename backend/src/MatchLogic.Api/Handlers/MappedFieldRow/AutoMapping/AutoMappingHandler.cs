using System;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using MatchLogic.Application.Interfaces.Persistence;
using Microsoft.Extensions.Logging;
using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.MatchConfiguration;
using System.Collections.Generic;
using MatchLogic.Domain.Entities;
using DomainMappedFieldRow = MatchLogic.Domain.Entities.MappedFieldRow;
using Mapster;
using MatchLogic.Application.Features.MatchDefinition.DTOs;
namespace MatchLogic.Api.Handlers.MappedFieldRow.AutoMapping;

public class AutoMappingHandler : IRequestHandler<AutoMappingCommand, Result<AutoMappingResponse>>
{
    private readonly ILogger<AutoMappingHandler> _logger;
    private readonly IAutoMappingService _autoMappingService;

    public AutoMappingHandler(IAutoMappingService autoMappingService, ILogger<AutoMappingHandler> logger)
    {
        _autoMappingService = autoMappingService;
        _logger = logger;
    }
    async Task<Result<AutoMappingResponse>> IRequestHandler<AutoMappingCommand, Result<AutoMappingResponse>>.Handle(AutoMappingCommand request, CancellationToken cancellationToken)
    {
        var projectId = request.projectId;
        var mappingType = request.mappingType;

        List<DomainMappedFieldRow> mappedFieldRows = null;

        var fieldExMapping = await _autoMappingService.GetExtendedFieldsAsync(projectId);

        bool isFieldExMappingRequired = fieldExMapping.Values.Any(x => x == null || x?.Count == 0);

        if (isFieldExMappingRequired)
        {
            await _autoMappingService.InsertExtendedFieldsByProjectAsync(projectId);
        }

        if (mappingType == MatchDefinitionMappingType.Default)
        {
            mappedFieldRows = await _autoMappingService.PerformAutoMappingAsync(projectId);
        }
        else
        {
            mappedFieldRows = await _autoMappingService.PerformSequentialMappingAsync(projectId);
        }

        var mappedFieldRowDtos = mappedFieldRows?.Select(row => new MappedFieldRowDto
        {
            Include = row.Include,
            FieldsByDataSource = row.GetAllFields().ToDictionary(
                field => field.DataSourceName.ToLower(),
                field => new FieldDto
                {
                    Id = field.Id,
                    Name = field.FieldName,
                    DataSourceId = field.DataSourceId,
                    DataSourceName = field.DataSourceName
                }
            )
        }).ToList();

        var response = new AutoMappingResponse
        {
            MappedFieldsRow = mappedFieldRowDtos
        };

        _logger.LogInformation("MappedFieldRow found successfully. projectId: {projectId}", projectId);

        return new Result<AutoMappingResponse>(response);
    }
}
