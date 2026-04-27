using System;

namespace MatchLogic.Api.Handlers.FinalExport.SaveSettings;

public record SaveFinalExportSettingsResponse(Guid SettingsId, bool Success);