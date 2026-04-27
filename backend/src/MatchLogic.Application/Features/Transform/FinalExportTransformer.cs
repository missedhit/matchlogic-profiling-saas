using MatchLogic.Application.Interfaces.Transform;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.Transform;
[HandlesDataTransformer("finalExport")]
internal class FinalExportTransformer : BaseDataTransformer
{
    public override string Name => "finalExport";

    public FinalExportTransformer(TransformerConfiguration configuration)
        : base(configuration, logger: null)
    {
    }

    /// <summary>
    /// Final export transformation: return row as-is.
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


