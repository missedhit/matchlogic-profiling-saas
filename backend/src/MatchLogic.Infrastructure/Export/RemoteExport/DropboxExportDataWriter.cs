using MatchLogic.Application.Features.Export;
using MatchLogic.Domain.Import;
using Microsoft.Extensions.Logging;

namespace MatchLogic.Infrastructure.Export.RemoteExport;

[HandlesExportWriter(DataSourceType.Dropbox)]
public class DropboxExportDataWriter : BaseRemoteExportDataWriter
{
    public override DataSourceType Type => DataSourceType.Dropbox;

    public DropboxExportDataWriter(ConnectionConfig connectionConfig, ILogger logger)
        : base(connectionConfig, logger) { }
}
