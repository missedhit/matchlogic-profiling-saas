using MatchLogic.Application.Interfaces.Common;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Application.Interfaces.Import;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.Project;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.Import;

public class ImportModuleFactory : IImportModuleFactory
{
    private readonly IDataStore _dataStore;
    private readonly ILoggerFactory _loggerFactory;
    private readonly IRecordHasher _recordHasher;
    private readonly IJobEventPublisher _jobEventPublisher;
    private readonly IColumnFilter _columnFilter;

    public ImportModuleFactory(
        IDataStore dataStore,
        ILoggerFactory loggerFactory,
        IRecordHasher recordHasher,
        IJobEventPublisher jobEventPublisher,
        IColumnFilter columnFilter)
    {
        _dataStore = dataStore;
        _loggerFactory = loggerFactory;
        _recordHasher = recordHasher;
        _jobEventPublisher = jobEventPublisher;
        _columnFilter = columnFilter;
    }

    public IImportModule Create(
        IConnectionReaderStrategy reader,
        ICommandContext context,
        Guid dataSourceId,
        Dictionary<string, ColumnMapping>? columnMappings)
    {
        // Standardise on OrderedDataImportModule (your current “production” module)
        return new OrderedDataImportModule(
            reader,
            _dataStore,
            _loggerFactory.CreateLogger<OrderedDataImportModule>(),
            _recordHasher,
            _jobEventPublisher,
            context,
            columnMappings ?? new Dictionary<string, ColumnMapping>(),
            _columnFilter,
            dataSourceId);
    }
}
