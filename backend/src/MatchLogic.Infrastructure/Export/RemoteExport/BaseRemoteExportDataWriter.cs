using MatchLogic.Application.Features.Import;
using MatchLogic.Application.Interfaces.Import;
using MatchLogic.Domain.Export;
using MatchLogic.Domain.Import;
using MatchLogic.Infrastructure.Export;
using MatchLogic.Infrastructure.Export.Writers;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Export.RemoteExport;

/// <summary>
/// Base class for all remote storage export writers.
/// Flow: Initialize local temp writer (CSV or Excel) → Write batches to temp file → Upload to remote → Cleanup.
/// </summary>
public abstract class BaseRemoteExportDataWriter : BaseExportDataWriter
{
    protected readonly RemoteFileConnectionConfig _config;
    private readonly RemoteFileConnectorFactory _connectorFactory;
    private BaseExportDataWriter? _localWriter;
    private IRemoteFileConnector? _connector;
    private string? _tempFilePath;
    private string? _tempDir;

    public override string Name => $"Remote ({Type}) Export Writer";

    protected BaseRemoteExportDataWriter(ConnectionConfig connectionConfig, ILogger logger)
        : base(logger, 1000)
    {
        _config = connectionConfig as RemoteFileConnectionConfig
            ?? throw new ArgumentException($"Invalid configuration type for {GetType().Name}");
        _connectorFactory = new RemoteFileConnectorFactory();
    }

    public override async Task InitializeAsync(ExportSchema schema, CancellationToken ct = default)
    {
        await base.InitializeAsync(schema, ct);

        // Create temp directory
        _tempDir = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
            "MatchLogic", "temp", "export", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);

        // Determine export format and create local writer
        var format = _config.ExportFormat.ToUpperInvariant();
        var remotePath = _config.RemotePath;
        var fileName = string.IsNullOrEmpty(remotePath)
            ? $"export_{DateTime.UtcNow:yyyyMMdd_HHmmss}"
            : Path.GetFileNameWithoutExtension(remotePath);

        if (format == "EXCEL" || format == "XLSX")
        {
            _tempFilePath = Path.Combine(_tempDir, $"{fileName}.xlsx");
            var excelConfig = new ExcelConnectionConfig();
            excelConfig.CreateFromArgs(DataSourceType.Excel, new Dictionary<string, string>
            {
                ["FilePath"] = _tempFilePath
            });
            _localWriter = new ExcelExportDataWriter(excelConfig, _logger);
        }
        else
        {
            // Default to CSV
            _tempFilePath = Path.Combine(_tempDir, $"{fileName}.csv");
            var csvConfig = new CSVConnectionConfig();
            csvConfig.CreateFromArgs(DataSourceType.CSV, new Dictionary<string, string>
            {
                ["FilePath"] = _tempFilePath
            });
            _localWriter = new CsvExportDataWriter(csvConfig, _logger);
        }

        await _localWriter.InitializeAsync(schema, ct);
        _logger.LogInformation("Remote export writer initialized. Temp file: {Path}, Format: {Format}", _tempFilePath, format);
    }

    public override async Task WriteBatchAsync(IReadOnlyList<IDictionary<string, object>> batch, CancellationToken ct = default)
    {
        await base.WriteBatchAsync(batch, ct);

        if (_localWriter == null)
            throw new InvalidOperationException("Writer not initialized");

        await _localWriter.WriteBatchAsync(batch, ct);
        _rowsWritten += batch.Count;
    }

    protected override async Task WriteBatchAsync(
        List<IDictionary<string, object>> batch,
        ExportContext context,
        CancellationToken cancellationToken)
    {
        if (_localWriter == null)
            throw new InvalidOperationException("Writer not initialized");

        // Delegate to the local writer's public WriteBatchAsync (List<T> implements IReadOnlyList<T>)
        await _localWriter.WriteBatchAsync(batch, cancellationToken);
        _rowsWritten += batch.Count;
    }

    public override async Task<ExportWriteResult> FinalizeAsync(CancellationToken ct = default)
    {
        try
        {
            // Step 1: Finalize local file
            if (_localWriter != null)
            {
                await _localWriter.FinalizeAsync(ct);
                _logger.LogInformation("Local temp file finalized: {Path} ({Size} bytes)",
                    _tempFilePath, _tempFilePath != null ? new FileInfo(_tempFilePath).Length : 0);
            }

            // Step 2: Upload to remote
            if (_tempFilePath != null && File.Exists(_tempFilePath))
            {
                _connector = _connectorFactory.Create(_config.RemoteType, _config, _logger);

                var remotePath = _config.RemotePath;
                var tempFileName = Path.GetFileName(_tempFilePath);
                if (string.IsNullOrEmpty(remotePath))
                {
                    // No path specified — upload to root with auto-generated filename
                    remotePath = tempFileName;
                }
                else if (string.IsNullOrEmpty(Path.GetExtension(remotePath)))
                {
                    // Path has no file extension — treat as directory, append filename
                    remotePath = remotePath.TrimEnd('/', '\\') + "/" + tempFileName;
                }

                // Check if file exists and handle overwrite policy
                if (!_config.OverwriteExisting)
                {
                    var exists = await _connector.FileExistsAsync(remotePath, ct);
                    if (exists)
                    {
                        AddError($"Remote file already exists and OverwriteExisting is false: {remotePath}");
                        return await base.FinalizeAsync(ct);
                    }
                }

                _logger.LogInformation("Uploading export file to remote: {RemotePath}", remotePath);
                await _connector.UploadFileAsync(_tempFilePath, remotePath, progress: null, ct: ct);
                _logger.LogInformation("Remote upload complete: {RemotePath}", remotePath);
            }

            return await base.FinalizeAsync(ct);
        }
        finally
        {
            Cleanup();
        }
    }

    private void Cleanup()
    {
        _localWriter?.Dispose();
        _connector?.Dispose();

        try
        {
            if (_tempDir != null && Directory.Exists(_tempDir))
            {
                Directory.Delete(_tempDir, recursive: true);
                _logger.LogDebug("Cleaned up temp export directory: {Dir}", _tempDir);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to cleanup temp export directory: {Dir}", _tempDir);
        }
    }

    public override void Dispose()
    {
        if (!_disposed)
        {
            Cleanup();
        }
        base.Dispose();
    }
}
