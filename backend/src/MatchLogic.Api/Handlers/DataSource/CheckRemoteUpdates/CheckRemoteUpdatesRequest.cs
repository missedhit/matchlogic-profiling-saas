using MatchLogic.Application.Interfaces.Import;
using MatchLogic.Domain.Project;
using System;
using System.Collections.Generic;

namespace MatchLogic.Api.Handlers.DataSource.CheckRemoteUpdates;

public record CheckRemoteUpdatesRequest(Guid ProjectId) : IRequest<Result<List<CheckRemoteUpdateResult>>>;

public record CheckRemoteUpdateResult(
    Guid DataSourceId,
    string DataSourceName,
    int DataSourceType,
    bool HasUpdates,
    RemoteFileMetadata? CurrentMetadata,
    StoredFileMetadata? StoredMetadata,
    string? Error
);
