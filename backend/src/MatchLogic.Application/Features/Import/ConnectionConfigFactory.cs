using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using System;
using System.Collections.Generic;
using System.Linq;

namespace MatchLogic.Application.Features.Import
{
    public class ConnectionConfigFactory
    {
        private readonly IEnumerable<ConnectionConfig> _configPrototypes;
        public ConnectionConfigFactory()
        {
           _configPrototypes = AppDomain.CurrentDomain.GetAssemblies()
                .SelectMany(assembly => assembly.GetTypes())
                .Where(type => typeof(ConnectionConfig).IsAssignableFrom(type) && type.IsClass && !type.IsAbstract)
                .Select(type => (ConnectionConfig)Activator.CreateInstance(type)!)
                .ToList();
        }

        public ConnectionConfig CreateFromArgs(DataSourceType type, Dictionary<string, string> args,DataSourceConfiguration? sourceConfiguration = null)
        {
            var config = _configPrototypes
                .FirstOrDefault(c => c.CanCreateFromArgs(type));
            
            return config == null
                ? throw new ArgumentException($"No suitable configuration found for the provided data source type: {type}")
                : config.CreateFromArgs(type, args ,sourceConfiguration);
        }
    }
}
