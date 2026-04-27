using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.Export;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.Export;

public interface IExportModule
{
    Task<bool> ExportDataAsync(
        DataExportOptions options,
        ICommandContext commandContext,
        CancellationToken cancellationToken = default);
}