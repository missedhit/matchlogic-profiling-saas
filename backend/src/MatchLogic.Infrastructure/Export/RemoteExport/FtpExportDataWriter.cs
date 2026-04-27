using MatchLogic.Application.Features.Export;
using MatchLogic.Domain.Import;
using Microsoft.Extensions.Logging;

namespace MatchLogic.Infrastructure.Export.RemoteExport;

[HandlesExportWriter(DataSourceType.FTP)]
public class FtpExportDataWriter : BaseRemoteExportDataWriter
{
    public override DataSourceType Type => DataSourceType.FTP;

    public FtpExportDataWriter(ConnectionConfig connectionConfig, ILogger logger)
        : base(connectionConfig, logger) { }
}
