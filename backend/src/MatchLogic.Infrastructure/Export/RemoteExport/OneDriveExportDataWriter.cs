using MatchLogic.Application.Features.Export;
using MatchLogic.Domain.Import;
using Microsoft.Extensions.Logging;

namespace MatchLogic.Infrastructure.Export.RemoteExport;

[HandlesExportWriter(DataSourceType.OneDrive)]
public class OneDriveExportDataWriter : BaseRemoteExportDataWriter
{
    public override DataSourceType Type => DataSourceType.OneDrive;

    public OneDriveExportDataWriter(ConnectionConfig connectionConfig, ILogger logger)
        : base(connectionConfig, logger) { }
}
