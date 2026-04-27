using MatchLogic.Application.Features.Export;
using MatchLogic.Domain.Import;
using Microsoft.Extensions.Logging;

namespace MatchLogic.Infrastructure.Export.RemoteExport;

[HandlesExportWriter(DataSourceType.SFTP)]
public class SftpExportDataWriter : BaseRemoteExportDataWriter
{
    public override DataSourceType Type => DataSourceType.SFTP;

    public SftpExportDataWriter(ConnectionConfig connectionConfig, ILogger logger)
        : base(connectionConfig, logger) { }
}
