using MatchLogic.Application.Features.Import;
using MatchLogic.Application.Interfaces.Import;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Import;

/// <summary>
/// Reader strategy for all remote file connectors (FTP, SFTP, S3, Azure Blob, Google Drive, Dropbox, OneDrive).
/// Downloads the remote file to a local temp path, then delegates to CsvDataReaderOptimized or ExcelDataReader.
/// </summary>
[HandlesConnectionConfig(typeof(RemoteFileConnectionConfig))]
public class RemoteFileReaderStrategy : BaseReaderStrategy
{
    private readonly RemoteFileConnectionConfig _config;
    private readonly RemoteFileConnectorFactory _connectorFactory;
    private IRemoteFileConnector? _connector;
    private IConnectionReaderStrategy? _delegateReader;
    private string? _localTempPath;
    private bool _disposed;

    public override string Name => $"Remote ({_config.RemoteType}) Reader";
    public override long RowCount => _delegateReader?.RowCount ?? 0;
    public override long DuplicateHeaderCount => _delegateReader?.DuplicateHeaderCount ?? 0;

    public RemoteFileReaderStrategy(ConnectionConfig config, ILogger logger)
        : base(config, logger)
    {
        _config = config as RemoteFileConnectionConfig
            ?? throw new ArgumentException("Invalid configuration type for RemoteFileReaderStrategy");
        _connectorFactory = new RemoteFileConnectorFactory();
    }

    public override IEnumerable<string> GetHeaders()
    {
        EnsureDelegateReaderInitialized().GetAwaiter().GetResult();
        return _delegateReader!.GetHeaders();
    }

    protected override async Task<IEnumerable<IDictionary<string, object>>> ReadBatchAsync(CancellationToken cancellationToken)
    {
        await EnsureDelegateReaderInitialized();
        // The delegate reader handles batch reading internally via its own ReadRowsAsync
        // This won't be called directly — ReadRowsAsync is overridden below
        return Enumerable.Empty<IDictionary<string, object>>();
    }

    public override async Task<IAsyncEnumerable<IDictionary<string, object>>> ReadRowsAsync(
        int maxDegreeOfParallelism = 4, CancellationToken cancellationToken = default)
    {
        await EnsureDelegateReaderInitialized();
        return await _delegateReader!.ReadRowsAsync(maxDegreeOfParallelism, cancellationToken);
    }

    public override async Task<IEnumerable<IDictionary<string, object>>> ReadPreviewBatchAsync(
        DataImportOptions options, IColumnFilter columnFilter, CancellationToken cancellationToken)
    {
        await EnsureDelegateReaderInitialized();
        return await _delegateReader!.ReadPreviewBatchAsync(options, columnFilter, cancellationToken);
    }

    public override async Task<bool> TestConnectionAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            using var connector = _connectorFactory.Create(_config.RemoteType, _config, _logger);
            return await connector.TestConnectionAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Remote connection test failed for {RemoteType}", _config.RemoteType);
            return false;
        }
    }

    public override async Task<List<TableInfo>> GetAvailableTables()
    {
        // For remote files, a "table" is essentially the file itself (or sheets within an Excel file)
        await EnsureDelegateReaderInitialized();
        return await _delegateReader!.GetAvailableTables();
    }

    public override async Task<TableSchema> GetTableSchema(string tableName)
    {
        await EnsureDelegateReaderInitialized();
        return await _delegateReader!.GetTableSchema(tableName);
    }

    private async Task EnsureDelegateReaderInitialized()
    {
        if (_delegateReader != null) return;

        _logger.LogInformation("Initializing remote file reader for {RemoteType}: {RemotePath}",
            _config.RemoteType, _config.RemotePath);

        // Step 1: Download file from remote
        _connector = _connectorFactory.Create(_config.RemoteType, _config, _logger);

        var tempDir = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
            "MatchLogic", "temp", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDir);

        _localTempPath = await _connector.DownloadFileAsync(
            _config.RemotePath, tempDir, progress: null, ct: default);

        _logger.LogInformation("Downloaded remote file to: {LocalPath} ({Size} bytes)",
            _localTempPath, new FileInfo(_localTempPath).Length);

        // Step 2: Detect file type and create appropriate internal config
        var extension = Path.GetExtension(_localTempPath).ToLowerInvariant();
        ConnectionConfig internalConfig;
        DataSourceType internalType;

        if (extension == ".csv" || extension == ".txt" || extension == ".tsv")
        {
            internalType = DataSourceType.CSV;
            var csvConfig = new CSVConnectionConfig();
            var csvArgs = new Dictionary<string, string>
            {
                ["FilePath"] = _localTempPath,
                ["HasHeaders"] = "true"
            };
            if (extension == ".tsv")
                csvArgs["Delimiter"] = "\t";

            csvConfig.CreateFromArgs(DataSourceType.CSV, csvArgs, _config.SourceConfig);
            internalConfig = csvConfig;
        }
        else if (extension == ".xlsx" || extension == ".xls")
        {
            internalType = DataSourceType.Excel;
            var excelConfig = new ExcelConnectionConfig();
            var excelArgs = new Dictionary<string, string>
            {
                ["FilePath"] = _localTempPath
            };
            excelConfig.CreateFromArgs(DataSourceType.Excel, excelArgs, _config.SourceConfig);
            internalConfig = excelConfig;
        }
        else
        {
            throw new NotSupportedException($"Unsupported file extension for remote import: {extension}");
        }

        // Step 3: Create the delegate reader via the strategy factory
        var strategyFactory = new ConnectionReaderStrategyFactory();
        _delegateReader = strategyFactory.GetStrategy(internalConfig, _logger);

        _logger.LogInformation("Remote file reader delegating to {ReaderType} for {Extension}",
            _delegateReader.GetType().Name, extension);
    }

    public override void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        _delegateReader?.Dispose();
        _connector?.Dispose();

        // Cleanup temp file
        if (_localTempPath != null)
        {
            try
            {
                if (File.Exists(_localTempPath))
                    File.Delete(_localTempPath);

                var tempDir = Path.GetDirectoryName(_localTempPath);
                if (tempDir != null && Directory.Exists(tempDir) &&
                    !Directory.EnumerateFileSystemEntries(tempDir).Any())
                {
                    Directory.Delete(tempDir);
                }
                _logger.LogDebug("Cleaned up temp file: {Path}", _localTempPath);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to cleanup temp file: {Path}", _localTempPath);
            }
        }
    }
}
