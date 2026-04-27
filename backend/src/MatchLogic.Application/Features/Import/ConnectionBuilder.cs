using MatchLogic.Application.Interfaces.Import;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Linq;

namespace MatchLogic.Application.Features.Import;

public interface IConnectionBuilder
{
    IConnectionBuilder WithArgs(DataSourceType type, Dictionary<string, string> args, DataSourceConfiguration? sourceConfiguration = null);
    IConnectionReaderStrategy Build();
}

public class ConnectionBuilder : IConnectionBuilder
{
    private IConnectionReaderStrategy readerStrategy;
    private ConnectionConfig config;

    private readonly ConnectionConfigFactory _configFactory;
    private readonly ConnectionReaderStrategyFactory _strategyFactory;
    private readonly ILogger _logger;
    public ConnectionBuilder(ConnectionConfigFactory config, ConnectionReaderStrategyFactory connectionReader, ILogger<ConnectionBuilder> logger)
    {
        _configFactory = config;
        _strategyFactory = connectionReader;
        _logger = logger;
    }
    public IConnectionReaderStrategy Build()
    {
        return readerStrategy;
    }

    public IConnectionBuilder WithArgs(DataSourceType type, Dictionary<string, string> args, DataSourceConfiguration? sourceConfiguration = null)
    {
        config = _configFactory.CreateFromArgs(type,args,sourceConfiguration);
        readerStrategy = _strategyFactory.GetStrategy(config, _logger);
        return this;
    }
}

public class ColumMappingHelper
{
    /// <summary>
    /// Returns only the headers that will actually be imported,
    /// respecting column mappings when present.
    /// </summary>
    public static IEnumerable<string> GetImportedHeaders(IConnectionReaderStrategy dataReader, Dictionary<string, ColumnMapping> columnMappings)
    {
        var allHeaders = dataReader.GetHeaders();

        if (columnMappings == null || !columnMappings.Any())
            return allHeaders;

        // Only count headers that exist in mappings AND are marked as included
        return allHeaders
            .Where(h => columnMappings.TryGetValue(h, out var mapping) && mapping.Include);
    }
}