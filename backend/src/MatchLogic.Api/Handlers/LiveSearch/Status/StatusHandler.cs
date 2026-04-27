using MatchLogic.Application.Features.LiveSearch;
using MatchLogic.Application.Interfaces.LiveSearch;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Infrastructure.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.LiveSearch.Status;

public class StatusHandler(
    IOptions<ApplicationOperationConfig> operationConfig,
    IOptions<LiveSearchConfiguration> liveSearchConfig,
    IQGramIndexManager indexManager,
    IGenericRepository<Domain.Project.Project, Guid> projectRepo,
    ILogger<StatusHandler> logger)
    : IRequestHandler<StatusRequest, Result<StatusResponse>>
{
    private static readonly DateTime _startTime = DateTime.UtcNow;

    public async Task<Result<StatusResponse>> Handle(
        StatusRequest request,
        CancellationToken cancellationToken)
    {
        try
        {
            var config = liveSearchConfig.Value;
            var opConfig = operationConfig.Value;

            // Get memory statistics
            using var process = Process.GetCurrentProcess();
            var totalMemoryMB = GC.GetTotalMemory(false) / 1024 / 1024;
            var workingSetMB = process.WorkingSet64 / 1024 / 1024;

            // Get index information using existing IQGramIndexManager from Core
            var indexedProjects = new System.Collections.Generic.List<ProjectIndexInfo>();

            if (opConfig.Mode == OperationMode.LiveSearch)
            {
                try
                {
                    var index = await indexManager.GetIndexAsync(config.ProjectId, cancellationToken);

                    if (index != null)
                    {
                        var project = await projectRepo.GetByIdAsync(
                            config.ProjectId,
                            "Projects");

                        indexedProjects.Add(new ProjectIndexInfo
                        {
                            ProjectId = config.ProjectId,
                            ProjectName = project?.Name ?? "Unknown",
                            TotalRecords = index.TotalRecords,
                            IndexedFields = index.GlobalFieldIndex.Count,
                            IndexCreatedAt = index.CreatedAt,
                            DataSourceCount = index.DataSourceStats.Count
                        });
                    }
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "Could not retrieve index information");
                }
            }

            var response = new StatusResponse
            {
                IsHealthy = true,
                NodeType = config.IsIndexingNode ? "Indexing" : "Query",
                Statistics = new NodeStatistics
                {
                    TotalMemoryMB = totalMemoryMB,
                    UsedMemoryMB = workingSetMB,
                    AvailableCores = Environment.ProcessorCount,
                    Uptime = DateTime.UtcNow - _startTime
                },
                IndexedProjects = indexedProjects
            };

            return Result<StatusResponse>.Success(response);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Status check failed");

            return Result<StatusResponse>.Error("Status check failed");
        }
    }
}