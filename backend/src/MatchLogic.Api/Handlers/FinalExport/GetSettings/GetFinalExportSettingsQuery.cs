using System;

namespace MatchLogic.Api.Handlers.FinalExport.GetSettings;

public class GetFinalExportSettingsQuery : IRequest<Result<GetFinalExportSettingsResponse>>
{
    public Guid ProjectId { get; set; }
}