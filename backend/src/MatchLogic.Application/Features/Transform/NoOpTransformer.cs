using MatchLogic.Application.Interfaces.Transform;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.Transform;

/// <summary>
/// Identity transformer: yields rows unchanged.
/// Zero allocation, zero cost - used when no transformation is needed.
/// Still supports column projection (renaming) via base class.
/// </summary>
[HandlesDataTransformer("none")]
internal class NoOpTransformer : BaseDataTransformer
{
    public override string Name => "no-op";

    public NoOpTransformer(TransformerConfiguration configuration)
        : base(configuration, logger: null)
    {
    }

    /// <summary>
    /// No-op transformation: return row as-is.
    /// Column projection (if configured) is applied by base class.
    /// </summary>
    protected override Task<IEnumerable<IDictionary<string, object>>> TransformRowInternalAsync(
        IDictionary<string, object> row,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult<IEnumerable<IDictionary<string, object>>>(new List<IDictionary<string, object>> { row });
    }
}