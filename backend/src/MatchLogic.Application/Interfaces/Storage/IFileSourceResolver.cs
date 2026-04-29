using System;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.Storage;

public interface IFileSourceResolver
{
    Task<IFileSourceLease> ResolveAsync(Guid fileImportId, CancellationToken cancellationToken = default);
}

public interface IFileSourceLease : IAsyncDisposable
{
    string LocalPath { get; }
}
