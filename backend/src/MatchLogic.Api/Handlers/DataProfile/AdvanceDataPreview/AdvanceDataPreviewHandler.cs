using MatchLogic.Api.Common;
using MatchLogic.Application.Interfaces.DataProfiling;
using MatchLogic.Domain.Import;
using Microsoft.Extensions.Logging;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.DataProfile.AdvanceDataPreview;

public class AdvanceDataPreviewHandler(IProfileService profileService,ILogger<AdvanceDataPreviewHandler> logger) : IRequestHandler<AdvanceDataPreviewRequest, Result<AdvanceDataPreviewResponse>>
{
    public async Task<Result<AdvanceDataPreviewResponse>> Handle(AdvanceDataPreviewRequest request, CancellationToken cancellationToken)
    {
        var collectionName = StepType.AdvanceProfile.ToCollectionName(request.DataSourceId);
        // Get the collection name for row references
        string rowReferenceCollectionName = $"{collectionName}_RowReferenceDocument";

        // Get RowReferenceCollection from Service
        var response  = await profileService.GetRowReferencesByDocumentIdAsync(request.DocumentId, rowReferenceCollectionName);

        if (response == null || response.Count == 0)
        {
            return Result<AdvanceDataPreviewResponse>.NotFound($"No row reference data found for the specified document ID: {request.DocumentId}.");
        }
        
        return Result<AdvanceDataPreviewResponse>.Success(new AdvanceDataPreviewResponse() { rowReferences = response });
    }
}
