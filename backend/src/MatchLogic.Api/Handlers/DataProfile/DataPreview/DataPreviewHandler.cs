using MatchLogic.Api.Common;
using MatchLogic.Application.Interfaces.DataProfiling;
using MatchLogic.Domain.Import;
using Microsoft.Extensions.Logging;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.DataProfile.DataPreview;

public class DataPreviewHandler(IProfileService profileService,ILogger<DataPreviewHandler> logger) : IRequestHandler<DataPreviewRequest, Result<DataPreviewResponse>>
{
    public async Task<Result<DataPreviewResponse>> Handle(DataPreviewRequest request, CancellationToken cancellationToken)
    {
        var collectionName = StepType.Profile.ToCollectionName(request.DataSourceId);
        // Get the collection name for row references
        string rowReferenceCollectionName = $"{collectionName}_RowReferenceDocument";

        // Get RowReferenceCollection from Service
        var response  = await profileService.GetRowReferencesByDocumentIdAsync(request.DocumentId, rowReferenceCollectionName);

        if (response == null || response.Count == 0)
        {
            return Result<DataPreviewResponse>.NotFound($"No row reference data found");
        }

        return Result<DataPreviewResponse>.Success(new DataPreviewResponse() { rowReferences = response });
    }
}
