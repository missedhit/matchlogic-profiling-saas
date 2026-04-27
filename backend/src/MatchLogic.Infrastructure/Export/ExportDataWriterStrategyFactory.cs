using MatchLogic.Application.Features.Export;
using MatchLogic.Application.Features.Import;
using MatchLogic.Application.Interfaces.Export;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using MatchLogic.Infrastructure.Export.Writers;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Export
{
    public class ExportDataWriterStrategyFactory : IExportDataWriterStrategyFactory
    {
        private readonly IDataStore _dataStore;
        private readonly Dictionary<DataSourceType, Type> _strategyMap;
        private readonly ILogger _logger;
        private readonly ConnectionConfigFactory _configFactory;
        public ExportDataWriterStrategyFactory(IDataStore dataStore, ILogger<ExportDataWriterStrategyFactory> logger, ConnectionConfigFactory configFactory)
        {
            _strategyMap = DiscoverStrategyTypes();
            _logger = logger;
            _dataStore = dataStore;
            _configFactory = configFactory;
        }

        public IExportDataStrategy CreatePreviewWriter(string collectionName)
        {
            return new LiteDbExportDataWriter(_dataStore, collectionName, _logger);
        }
        private static Dictionary<DataSourceType, Type> DiscoverStrategyTypes()
        {
#pragma warning disable CS8619 // Nullability of reference types in value doesn't match target type.
            return
                Assembly.Load("MatchLogic.Infrastructure")
                .GetTypes()
                .Where(t => t.IsClass && !t.IsAbstract && typeof(IExportDataStrategy).IsAssignableFrom(t))
                .Select(t => new
                {
                    StrategyType = t,
                    Attribute = t.GetCustomAttribute<HandlesExportWriter>(),
                })
                .Where(x => x.Attribute != null)
                .ToDictionary(
                    x => x.Attribute.DataSourceType,
                    x => x.StrategyType
                );
#pragma warning restore CS8619 // Nullability of reference types in value doesn't match target type.
        }


        public IExportDataStrategy GetStrategy(BaseConnectionInfo connectionInfo)
        {
            if (!_strategyMap.TryGetValue(connectionInfo.Type, out var strategyType))
            {
                throw new ArgumentException($"No strategy found for DataSourceType: {connectionInfo.Type}");
            }
            var conn = _configFactory.CreateFromArgs(connectionInfo.Type, connectionInfo.Parameters);

            return (IExportDataStrategy)Activator.CreateInstance(strategyType, conn, _logger);
        }
    }
}
