using MatchLogic.Application.Interfaces.Persistence;
using System.Threading.Tasks;
using System.Threading;
using System;
using MatchLogic.Domain.Entities.Common;
using System.Linq;

namespace MatchLogic.Api.Handlers.ColumnNotes.Get;

public class GetColumnNotesQuery : IRequest<DataSourceColumnNotesDto>
{
    public Guid DataSourceId { get; set; }
}

public class GetColumnNotesHandler : IRequestHandler<GetColumnNotesQuery, DataSourceColumnNotesDto>
{
    private readonly IGenericRepository<DataSourceColumnNotes, Guid> _repository;
    private const string CollectionName = "DataSourceColumnNotes";

    public GetColumnNotesHandler(IGenericRepository<DataSourceColumnNotes, Guid> repository)
    {
        _repository = repository;
    }

    public async Task<DataSourceColumnNotesDto> Handle(GetColumnNotesQuery request, CancellationToken cancellationToken)
    {
        var results = await _repository.QueryAsync(
            x => x.DataSourceId == request.DataSourceId,
            CollectionName);

        var entity = results.FirstOrDefault();

        if (entity == null)
        {
            return new DataSourceColumnNotesDto
            {
                Id = Guid.Empty,
                DataSourceId = request.DataSourceId,
                ColumnNotes = new()
            };
        }

        return new DataSourceColumnNotesDto
        {
            Id = entity.Id,
            DataSourceId = entity.DataSourceId,
            ColumnNotes = entity.ColumnNotes
        };
    }
}
