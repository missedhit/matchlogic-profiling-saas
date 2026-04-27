using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.Project;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.Import;

public interface IImportModuleFactory
{
    IImportModule Create(
        IConnectionReaderStrategy reader,
        ICommandContext context,
        Guid dataSourceId,
        Dictionary<string, ColumnMapping>? columnMappings);
}
