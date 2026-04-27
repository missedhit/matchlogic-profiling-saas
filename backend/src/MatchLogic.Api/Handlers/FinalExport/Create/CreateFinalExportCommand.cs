using MatchLogic.Domain.Project;
using System;

namespace MatchLogic.Api.Handlers.FinalExport.Create;

public class CreateFinalExportCommand : IRequest<Result<CreateFinalExportResponse>>
{
    public Guid ProjectId { get; set; }
    public Guid? SettingsId { get; set; }  // Optional: use specific settings, else use saved/default

    public BaseConnectionInfo ConnectionInfo { get; set; }
}