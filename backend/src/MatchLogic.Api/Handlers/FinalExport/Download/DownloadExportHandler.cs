using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.FinalExport;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using Microsoft.Extensions.Logging;
using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.FinalExport.Download;

/// <summary>
/// Handler for downloading exported files.
/// </summary>
public sealed class DownloadExportHandler(
    IGenericRepository<ProjectRun, Guid> runRepository,
    IGenericRepository<FinalExportResult, Guid> exportResultRepository,
    ILogger<DownloadExportHandler> logger)
    : IRequestHandler<DownloadExportQuery, Result<FileDownloadResult>>
{
    public async Task<Result<FileDownloadResult>> Handle(
        DownloadExportQuery request,
        CancellationToken cancellationToken)
    {
        try
        {
            // 1. Get project run
            var run = await runRepository.GetByIdAsync(
                request.RunId,
                Constants.Collections.ProjectRuns);

            if (run == null)
            {
                return Result.NotFound("Export job not found.");
            }

            // 2. Validate run status
            if (run.Status != RunStatus.Completed)
            {
                return Result.Error(
                    $"Export job status is {run.Status}. Download is only available when status is Completed.");
            }

            // 3. Resolve export file path
            var exportResult = (await exportResultRepository.QueryAsync(
                r => r.ProjectId == run.ProjectId,
                Constants.Collections.FinalExportResults))
                .FirstOrDefault();

            var outputFilePath = exportResult?.ExportFilePath;

            if (string.IsNullOrWhiteSpace(outputFilePath))
            {
                return Result.Error(
                    "This export was configured for a database destination. No file is available for download.");
            }

            // 4. Validate file existence
            if (!System.IO.File.Exists(outputFilePath))
            {
                logger.LogWarning(
                    "Export file not found at path: {FilePath}",
                    outputFilePath);

                return Result.NotFound(
                    "The exported file no longer exists on the server.");
            }

            // 5. Prepare file stream
            var fileName = Path.GetFileName(outputFilePath);
            var contentType = GetContentType(outputFilePath);

            var stream = new FileStream(
                outputFilePath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read);

            logger.LogInformation(
                "Serving export file download: {FileName} for run {RunId}",
                fileName,
                request.RunId);

            // 6. Return file payload
            return Result.Success(
                new FileDownloadResult(stream, contentType, fileName));
        }
        catch (Exception ex)
        {
            logger.LogError(
                ex,
                "Error downloading export for run {RunId}",
                request.RunId);

            return Result.Error(
                $"Failed to download export: {ex.Message}");
        }
    }

    private static string GetContentType(string filePath)
    {
        var extension = Path.GetExtension(filePath)?.ToLowerInvariant();

        return extension switch
        {
            ".csv" => "text/csv",
            ".xlsx" => "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ".xls" => "application/vnd.ms-excel",
            ".json" => "application/json",
            ".xml" => "application/xml",
            ".parquet" => "application/octet-stream",
            _ => "application/octet-stream"
        };
    }
}
