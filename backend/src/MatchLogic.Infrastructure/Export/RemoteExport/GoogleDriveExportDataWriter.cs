using MatchLogic.Application.Features.Export;
using MatchLogic.Domain.Import;
using Microsoft.Extensions.Logging;

namespace MatchLogic.Infrastructure.Export.RemoteExport;

[HandlesExportWriter(DataSourceType.GoogleDrive)]
public class GoogleDriveExportDataWriter : BaseRemoteExportDataWriter
{
    public override DataSourceType Type => DataSourceType.GoogleDrive;

    public GoogleDriveExportDataWriter(ConnectionConfig connectionConfig, ILogger logger)
        : base(connectionConfig, logger) { }
}
