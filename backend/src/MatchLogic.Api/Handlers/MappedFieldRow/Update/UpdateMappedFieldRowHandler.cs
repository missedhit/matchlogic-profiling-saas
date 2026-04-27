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
namespace MatchLogic.Api.Handlers.MappedFieldRow.Update;

public class UpdateMappedFieldRowHandler : IRequestHandler<UpdateMappedFieldRowCommand, Result<UpdateMappedFieldRowResponse>>
{
    private readonly ILogger<UpdateMappedFieldRowHandler> _logger;
    private readonly IAutoMappingService _autoMappingService;

    public UpdateMappedFieldRowHandler(IAutoMappingService autoMappingService, ILogger<UpdateMappedFieldRowHandler> logger)
    {
        _autoMappingService = autoMappingService;
        _logger = logger;
    }
    async Task<Result<UpdateMappedFieldRowResponse>> IRequestHandler<UpdateMappedFieldRowCommand, Result<UpdateMappedFieldRowResponse>>.Handle(UpdateMappedFieldRowCommand request, CancellationToken cancellationToken)
    {
        var projectId = request.projectId;
        var mappedFieldRows = request.mappedFieldRows;

        var mappedFieldRow = mappedFieldRows?.Select(row => new DomainMappedFieldRow
        {
            Include = row.Include,
            FieldByDataSource = row.FieldsByDataSource.Where(x=>x.Value !=null).ToDictionary(
                field => field.Key.ToLower(),
                field => field.Value == null ? null: new FieldMappingEx
                {
                    Id = field.Value.Id,
                    FieldName = field.Value.Name,
                    DataSourceId = field.Value.DataSourceId,
                    DataSourceName = field.Value.DataSourceName
                }
            )
        }).ToList();

         await _autoMappingService.UpdateMappedFieldRowsAsync(projectId, mappedFieldRow);

        var response = new UpdateMappedFieldRowResponse
        {
            MappedFieldsRow = mappedFieldRows
        };

        _logger.LogInformation("MappedFieldRow found successfully. projectId: {projectId}", projectId);

        return new Result<UpdateMappedFieldRowResponse>(response);
    }
}
