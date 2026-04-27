using MatchLogic.Application.Features.Export;
using MatchLogic.Application.Interfaces.Export;
using MatchLogic.Domain.Export;
using MatchLogic.Domain.Import;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using MatchLogic.Domain.Project;

namespace MatchLogic.Infrastructure.Export.Writers;

/// <summary>
/// CSV export writer with streaming support.
/// Features:
/// - 128KB buffered writes for performance
/// - Periodic flush every 10K rows
/// - Configurable delimiter, encoding, quotation via CSVConnectionConfig
/// - Proper escaping of special characters
/// </summary>
[HandlesExportWriter(DataSourceType.CSV)]
public class CsvExportDataWriter : BaseExportDataWriter
{
    private readonly CSVConnectionConfig _connectionConfig;
    // From CSVConnectionConfig properties (shared with reader)
    private readonly string _delimiter;
    private readonly char _quoteChar;
    private readonly Encoding _encoding;
    private readonly bool _includeHeader;

    // From Parameters (export-specific)
    private readonly string _dateFormat;
    private readonly string _decimalSeparator;
    private readonly bool _useQuotation;
    private readonly string _newLine;

    private StreamWriter? _writer;
    private const int BufferSize = 131072; // 128KB buffer

    public override string Name => "CSV Writer";
    public override DataSourceType Type => DataSourceType.CSV;

    public CsvExportDataWriter(ConnectionConfig connectionConfig, ILogger logger)
        : base(logger, 10000)
    {
        _connectionConfig = connectionConfig as CSVConnectionConfig
            ?? throw new ArgumentException("Invalid configuration type for CsvExportDataWriter", nameof(connectionConfig));

        var p = _connectionConfig.Parameters;

        // From config properties (same as reader uses)
        _delimiter = string.IsNullOrEmpty(_connectionConfig.Delimiter) ? "," : _connectionConfig.Delimiter;
        _quoteChar = _connectionConfig.QuoteChar ?? '"';
        _encoding = _connectionConfig.Encoding ?? Encoding.UTF8;
        _includeHeader = _connectionConfig.HasHeaders;

        // From Parameters (export-specific)
        _dateFormat = p.GetString(CsvExportKeys.DateFormat, CsvExportKeys.Defaults.DateFormat);
        _decimalSeparator = p.GetString(CsvExportKeys.DecimalSeparator, CsvExportKeys.Defaults.DecimalSeparator);
        _useQuotation = p.GetBool(CsvExportKeys.UseQuotation, CsvExportKeys.Defaults.UseQuotation);
        _newLine = p.GetString(CsvExportKeys.NewLine, CsvExportKeys.Defaults.NewLine);

    }

    #region IExportDataWriter Implementation

    public override async Task InitializeAsync(ExportSchema schema, CancellationToken ct = default)
    {
        await base.InitializeAsync(schema, ct);

        try
        {
            // Ensure directory exists
            var directory = Path.GetDirectoryName(_connectionConfig.FilePath);
            if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
                Directory.CreateDirectory(directory);

            // Open file with buffering
            _writer = new StreamWriter(_connectionConfig.FilePath, false, _encoding, BufferSize);

            // Write header row if configured
            if (_connectionConfig.HasHeaders)
            {
                var headerLine = string.Join(
                    _delimiter,
                    schema.ColumnNames.Select(h => CsvEscape(h)));
                await _writer.WriteLineAsync(headerLine);
            }

            _logger.LogInformation("CSV file created: {Path}", _connectionConfig.FilePath);
        }
        catch (Exception ex)
        {
            AddError($"Failed to initialize CSV writer: {ex.Message}");
            throw;
        }
    }

    public override async Task WriteBatchAsync(
        IReadOnlyList<IDictionary<string, object>> batch,
        CancellationToken ct = default)
    {
        await base.WriteBatchAsync(batch, ct);

        if (_writer == null || _schema == null)
            throw new InvalidOperationException("Writer not initialized");

        try
        {
            var sb = new StringBuilder(4096);

            foreach (var row in batch)
            {
                ct.ThrowIfCancellationRequested();

                sb.Clear();
                bool first = true;

                foreach (var col in _schema.ColumnNames)
                {
                    if (!first) sb.Append(_delimiter);
                    first = false;

                    var value = row.TryGetValue(col, out var v) ? v : null;
                    sb.Append(CsvEscape(FormatValue(value)));
                }

                await _writer.WriteLineAsync(sb.ToString());
                _rowsWritten++;
            }

            // Periodic flush for large exports
            if (_rowsWritten % 10_000 == 0)
            {
                await _writer.FlushAsync();
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            AddError($"Error writing batch at row {_rowsWritten}: {ex.Message}");
            throw;
        }
    }

    public override async Task<ExportWriteResult> FinalizeAsync(CancellationToken ct = default)
    {
        try
        {
            if (_writer != null)
            {
                await _writer.FlushAsync();
                await _writer.DisposeAsync();
                _writer = null;
            }

            var result = await base.FinalizeAsync(ct);
            result.FilePath = _connectionConfig.FilePath;

            // Log file size
            if (File.Exists(_connectionConfig.FilePath))
            {
                var fileInfo = new FileInfo(_connectionConfig.FilePath);
                _logger.LogInformation(
                    "CSV export completed: {Rows} rows, {Size:N0} bytes",
                    _rowsWritten,
                    fileInfo.Length);
            }

            return result;
        }
        catch (Exception ex)
        {
            AddError($"Error finalizing CSV: {ex.Message}");
            return ExportWriteResult.Failed(_errors);
        }
    }

    #endregion

    #region Legacy ExportAsync Support

    protected override async Task InitializeExportAsync(ExportContext context, CancellationToken cancellationToken)
    {
        // Ensure directory exists
        var directory = Path.GetDirectoryName(_connectionConfig.FilePath);
        if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
            Directory.CreateDirectory(directory);

        _writer = new StreamWriter(_connectionConfig.FilePath, false, _encoding, BufferSize);

        // Write header row
        if (_connectionConfig.HasHeaders)
        {
            var headerLine = string.Join(
                _delimiter,
                context.OrderedColumnNames.Select(h => CsvEscape(h)));
            await _writer.WriteLineAsync(headerLine);
        }
    }

    protected override async Task WriteBatchAsync(
        List<IDictionary<string, object>> batch,
        ExportContext context,
        CancellationToken cancellationToken)
    {
        if (_writer == null)
            throw new InvalidOperationException("Writer not initialized");

        foreach (var row in batch)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var line = string.Join(_delimiter, context.OrderedColumnNames.Select(col =>
                CsvEscape(row.TryGetValue(col, out var v) ? FormatValue(v) : string.Empty)));

            await _writer.WriteLineAsync(line);
        }

        // Periodic flush
        if (batch.Count > 0 && _rowsWritten % 10_000 < batch.Count)
        {
            await _writer.FlushAsync();
        }
    }

    protected override async Task FinalizeExportAsync(ExportContext context, int totalRows, CancellationToken cancellationToken)
    {
        if (_writer != null)
        {
            await _writer.FlushAsync();
            await _writer.DisposeAsync();
            _writer = null;
        }
    }

    #endregion

    #region Helpers

    private string CsvEscape(string? value)
    {
        if (string.IsNullOrEmpty(value))
            return string.Empty;

        bool mustQuote = value.Contains(_delimiter) ||
                         value.Contains('\n') ||
                         value.Contains('\r') ||
                         value.Contains(_quoteChar);

        if (mustQuote)
        {
            var escaped = value.Replace(_quoteChar.ToString(), new string(_quoteChar, 2));
            return $"{_quoteChar}{escaped}{_quoteChar}";
        }

        return value;
    }

    private string FormatValue(object? value)
    {
        return value switch
        {
            null => string.Empty,
            DateTime dt => dt.ToString(_dateFormat),  // Uses _dateFormat from Parameters
            DateTimeOffset dto => dto.ToString(_dateFormat),
            double d => d.ToString("G").Replace(".", _decimalSeparator),  // Uses _decimalSeparator
            decimal dec => dec.ToString("G").Replace(".", _decimalSeparator),
            float f => f.ToString("G").Replace(".", _decimalSeparator),
            bool b => b ? "true" : "false",
            Guid g => g.ToString(),
            byte[] bytes => Convert.ToBase64String(bytes),
            _ => value.ToString() ?? string.Empty
        };
    }

    public override void Dispose()
    {
        if (!_disposed)
        {
            _writer?.Dispose();
            _writer = null;
        }
        base.Dispose();
    }

    #endregion
}