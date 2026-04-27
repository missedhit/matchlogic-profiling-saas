using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Entities.Common;
using System.Threading.Tasks;
using System.Threading;
using System;
using System.Linq;

namespace MatchLogic.Api.Handlers.ColumnNotes.Update;

public class UpsertColumnNotesCommand : IRequest<DataSourceColumnNotesDto>
{
    public Guid DataSourceId { get; set; }
    public UpsertColumnNotesRequest Request { get; set; }
}

public class UpsertColumnNotesHandler : IRequestHandler<UpsertColumnNotesCommand, DataSourceColumnNotesDto>
{
    private readonly IGenericRepository<DataSourceColumnNotes, Guid> _repository;
    private const string CollectionName = "DataSourceColumnNotes";

    public UpsertColumnNotesHandler(IGenericRepository<DataSourceColumnNotes, Guid> repository)
    {
        _repository = repository;
    }

    public async Task<DataSourceColumnNotesDto> Handle(UpsertColumnNotesCommand request, CancellationToken cancellationToken)
    {
        var results = await _repository.QueryAsync(
            x => x.DataSourceId == request.DataSourceId,
            CollectionName);

        var existing = results.FirstOrDefault();

        if (existing == null)
        {
            // Create new
            var newEntity = new DataSourceColumnNotes
            {
                Id = Guid.NewGuid(),
                DataSourceId = request.DataSourceId,
                ColumnNotes = request.Request.ColumnNotes,
            };

            await _repository.InsertAsync(newEntity, CollectionName);

            return new DataSourceColumnNotesDto
            {
                Id = newEntity.Id,
                DataSourceId = newEntity.DataSourceId,
                ColumnNotes = newEntity.ColumnNotes
            };
        }
        else
        {
            // Update existing - replace entire dictionary
            existing.ColumnNotes = request.Request.ColumnNotes;

            await _repository.UpdateAsync(existing, CollectionName);

            return new DataSourceColumnNotesDto
            {
                Id = existing.Id,
                DataSourceId = existing.DataSourceId,
                ColumnNotes = existing.ColumnNotes
            };
        }
    }
}
