using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.Import;
public interface IImportModule
{
    Task<Guid> ImportDataAsync(string collectionName = "", CancellationToken cancellationToken = default);
}
