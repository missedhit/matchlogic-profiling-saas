using MatchLogic.Application.Features.Export;
using MatchLogic.Domain.Import;
using Microsoft.Extensions.Logging;

namespace MatchLogic.Infrastructure.Export.RemoteExport;

[HandlesExportWriter(DataSourceType.AzureBlob)]
public class AzureBlobExportDataWriter : BaseRemoteExportDataWriter
{
    public override DataSourceType Type => DataSourceType.AzureBlob;

    public AzureBlobExportDataWriter(ConnectionConfig connectionConfig, ILogger logger)
        : base(connectionConfig, logger) { }
}
