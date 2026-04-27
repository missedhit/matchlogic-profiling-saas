using System;
using System.Collections.Generic;

namespace MatchLogic.Api.Handlers.LiveSearch.Status;

public record StatusResponse
{
    public bool IsHealthy { get; init; }
    public string NodeType { get; init; }
    public NodeStatistics Statistics { get; init; }
    public List<ProjectIndexInfo> IndexedProjects { get; init; }
}

public record NodeStatistics
{
    public long TotalMemoryMB { get; init; }
    public long UsedMemoryMB { get; init; }
    public int AvailableCores { get; init; }
    public TimeSpan Uptime { get; init; }
}

public record ProjectIndexInfo
{
    public Guid ProjectId { get; init; }
    public string ProjectName { get; init; }
    public int TotalRecords { get; init; }
    public int IndexedFields { get; init; }
    public DateTime IndexCreatedAt { get; init; }
    public int DataSourceCount { get; init; }
}