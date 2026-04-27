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
namespace MatchLogic.Api.Handlers.MappedFieldRow.Get;

public class MappedFieldRowHandler : IRequestHandler<MappedFieldRowRequest, Result<MappedFieldRowResponse>>
{
    private readonly ILogger<MappedFieldRowHandler> _logger;
    private readonly IAutoMappingService _autoMappingService;

    public MappedFieldRowHandler(IAutoMappingService autoMappingService, ILogger<MappedFieldRowHandler> logger)
    {
        _autoMappingService = autoMappingService;
        _logger = logger;
    }
    async Task<Result<MappedFieldRowResponse>> IRequestHandler<MappedFieldRowRequest, Result<MappedFieldRowResponse>>.Handle(MappedFieldRowRequest request, CancellationToken cancellationToken)
    {
        var projectId = request.projectId;
        
        List<DomainMappedFieldRow> mappedFieldRows = await _autoMappingService.GetSavedMappedFieldRowsAsync(projectId);

        var fieldExMapping = await _autoMappingService.GetExtendedFieldsAsync(projectId,activeOnly: true);

        bool isFieldExMappingRequired = fieldExMapping.Values.Any(x => x == null || x?.Count == 0);

        if (isFieldExMappingRequired)
        {
            await _autoMappingService.InsertExtendedFieldsByProjectAsync(projectId);
        }

        if (mappedFieldRows == null || mappedFieldRows?.Count == 0)
        {
            mappedFieldRows = await _autoMappingService.PerformAutoMappingAsync(projectId);
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
                    DataSourceName = field.DataSourceName,
                    Origin = field.Origin.ToString(),        // Send origin to Angular
                    IsActive = field.IsActive,               // Send status to Angular
                    IsSystemManaged = field.IsSystemManaged
                }
            )
        }).ToList();

        var response = new MappedFieldRowResponse
        {
            MappedFieldsRow = mappedFieldRowDtos
        };

        _logger.LogInformation("MappedFieldRow found successfully. projectId: {projectId}", projectId);

        return new Result<MappedFieldRowResponse>(response);
    }
}
