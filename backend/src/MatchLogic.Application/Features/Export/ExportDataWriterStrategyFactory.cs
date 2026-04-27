using MatchLogic.Application.Features.Import;
using MatchLogic.Application.Interfaces.Export;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace MatchLogic.Application.Features.Export
{
    public interface IExportDataWriterStrategyFactory
    {
        /// <summary>
        /// Creates a LiteDB writer for preview or default export storage.
        /// Uses the application's configured IDataStore.
        /// </summary>
        IExportDataStrategy CreatePreviewWriter(string collectionName);
        IExportDataStrategy GetStrategy(BaseConnectionInfo connectionInfo);

    }    
}
