using MatchLogic.Application.Interfaces.Storage;
using MatchLogic.Domain.Project;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.DataSource.Base;

// Replaces the BaseConnectionInfoHandler inheritance pattern. For file-based
// data sources where parameters carry a FileId, asks the resolver for a local
// file path (downloads from S3 to /tmp if needed) and returns a lease that
// must be disposed by the caller (typically via `await using`).
public static class ConnectionInfoConfigurator
{
    public static async Task<(BaseConnectionInfo connection, IAsyncDisposable lease)> ConfigureAsync(
        BaseConnectionInfo source,
        IFileSourceResolver fileSourceResolver,
        CancellationToken cancellationToken = default)
    {
        var parameters = new Dictionary<string, string>(source.Parameters);

        if (parameters.TryGetValue("FileId", out var fileIdRaw)
            && Guid.TryParse(fileIdRaw, out var fileId))
        {
            var fileLease = await fileSourceResolver.ResolveAsync(fileId, cancellationToken);
            parameters["FilePath"] = fileLease.LocalPath;

            var connection = new BaseConnectionInfo
            {
                Type = source.Type,
                Parameters = parameters
            };
            return (connection, fileLease);
        }

        return (source, NoopLease.Instance);
    }

    private sealed class NoopLease : IAsyncDisposable
    {
        public static readonly NoopLease Instance = new();
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
