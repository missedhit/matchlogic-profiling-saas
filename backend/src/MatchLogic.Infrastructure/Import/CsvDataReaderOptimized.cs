using CsvHelper;
using CsvHelper.Configuration;
using MatchLogic.Application.Features.DataMatching.Comparators;
using MatchLogic.Application.Features.Import;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using MatchLogic.Infrastructure.Common;
using Microsoft.Extensions.Logging;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Import;

[HandlesConnectionConfig(typeof(CSVConnectionConfig))]
public class CsvDataReaderOptimized : BaseReaderStrategy
{
    private CSVConnectionConfig _config;
    private string _filePath => _config.FilePath;

    private StreamReader _streamReader;
    private CsvReader _csvReader;
    private const int BatchSize = 1000;
    private const int BufferSize = 131072; // 128KB buffer for optimal performance

    private readonly bool _hasHeaders = true;
    private readonly string _delimiter;
    private readonly char _quoteCharacter;
    private readonly char _commentCharacter;
    private readonly Encoding _encoding = Encoding.UTF8;

    private bool _isFirstBatchRead = false;
    private bool _headerRead;
    private bool _rowCountRead;
    private long _sameNameColumnsCount = 0;

    // Performance optimizations
    private readonly ArrayPool<string> _stringArrayPool = ArrayPool<string>.Shared;
    private readonly ArrayPool<object> _objectArrayPool = ArrayPool<object>.Shared;
    private int _headerLength;
    private string[] _cachedHeaders;

    public override string Name => Path.GetFileName(_filePath);
    public override long RowCount
    {
        get
        {
            long totalRows = GetTotalRowCount();
            return _hasHeaders && totalRows > 0 ? totalRows - 1 : totalRows;
        }
    }

    public override long DuplicateHeaderCount
    {
        get
        {
            if (_headers == null || !_headerRead)
            {
                GetHeaders();
            }
            return _sameNameColumnsCount;
        }
    }

    

    public CsvDataReaderOptimized(ConnectionConfig config,ILogger logger) : base(config, logger)
    {
        _config = config as CSVConnectionConfig ?? throw new ArgumentException("Invalid configuration type for CSVDataReader");

        _delimiter = _config.Delimiter ?? "," ;
        _hasHeaders = _config.HasHeaders;
        _encoding = _config.Encoding ?? Encoding.UTF8;
        _quoteCharacter = _config.QuoteChar ?? '"';
        _commentCharacter = _config.CommentChar ?? '#';
    }
 
    private long GetTotalRowCount()
    {
        if (_rowCount.HasValue)
            return _rowCount.Value;

        // Use memory-mapped file for very large files or buffered reading for smaller ones
        var fileInfo = new FileInfo(_filePath);

        // For files larger than 100MB, use a more efficient counting method
        if (fileInfo.Length > 100 * 1024 * 1024)
        {
            return CountRowsEfficient();
        }

        using var fileStream = new FileStream(_filePath, FileMode.Open, FileAccess.Read, FileShare.Read, BufferSize, FileOptions.SequentialScan);
        using var reader = new StreamReader(fileStream, _encoding, detectEncodingFromByteOrderMarks: false, BufferSize);

        int count = 0;
        while (reader.ReadLine() != null)
        {
            count++;
        }

        _rowCount = count;
        _rowCountRead = true;
        return count;
    }

    private long CountRowsEfficient()
    {
        const int bufferSize = 65536;
        var buffer = ArrayPool<byte>.Shared.Rent(bufferSize);

        try
        {
            using var fileStream = new FileStream(_filePath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize, FileOptions.SequentialScan);

            long count = 0;
            int bytesRead;

            while ((bytesRead = fileStream.Read(buffer, 0, bufferSize)) > 0)
            {
                var span = buffer.AsSpan(0, bytesRead);
                for (int i = 0; i < span.Length; i++)
                {
                    if (span[i] == '\n')
                        count++;
                }
            }

            _rowCount = count;
            return count;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private void Initialize()
    {
        if (_isInitialized)
            return;

        try
        {
            var config = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                HasHeaderRecord = _hasHeaders,
                MissingFieldFound = null,
                BadDataFound = null,
                BufferSize = BufferSize,
                Encoding = _encoding,
                Quote = _quoteCharacter,
                Comment = _commentCharacter,
                Delimiter = _delimiter,
                AllowComments = true,
                TrimOptions = TrimOptions.Trim,
                CacheFields = true, // Enable field caching for better performance
            };

            config.ReadingExceptionOccurred = ex =>
            {
                _logger.LogError(ex.Exception, "Error reading CSV file at row {Row} in file: {FilePath}", ex.Exception, _filePath);
                ErrorMessage.Add($"Error reading line {ex.Record}: {ex.Exception.Message}");
                return true;
            };
            config.BadDataFound = context =>
            {
                _logger.LogWarning("Bad data found at line {CurrentIndex} : {Row} in file: {FilePath}. Error: {Error}", context.Context?.Reader?.CurrentIndex, context.RawRecord, _filePath, context.Field);
                ErrorMessage.Add($"Bad data found at line position {context.Context?.Reader?.CurrentIndex} : {context.RawRecord}: {context.Field}");
            };
            /*config.HeaderValidated = (HeaderValidatedArgs context) =>
            {
                if (context.InvalidHeaders == null || context.InvalidHeaders.Length == 0)
                {
                    _logger.LogWarning("Invalid headers is empty or null at row {Row} in file: {FilePath}", context.Context.Parser?.RawRecord, _filePath);
                    ErrorMessage.Add($"Invalid headers is empty or null at row {context.Context.Parser?.RawRecord} in file: {_filePath}");
                }
                else
                { = context.InvalidHeaders
                        .Where(header => !string.IsNullOrWhiteSpace(header))
                        .ToArray();
                    _logger.LogWarning("Invalid headers {InvalidHeaders} at line {Line} in file: {FilePath}", string.Join<>(',', context.InvalidHeaders), context.Context.Parser?.RawRecord, _filePath);
                    ErrorMessage.Add($"Invalid headers at line {context.Context.Parser?.RawRecord}");
                }
            };*/
            config.MissingFieldFound = context =>
            {
                if (context.HeaderNames == null || context.HeaderNames.Length == 0)
                {
                    //AddErrorMessage("Missing fields at line is empty or null at row position at {}" + context.Context.Parser?.RawRecord + " in file: " + _filePath);
                    _logger.LogWarning("Missing fields {HeaderNames} at line {Line} position {Position} in file: {FilePath}", string.Join(',', context.HeaderNames), context.Context.Parser?.RawRecord, context.Index, _filePath);
                    ErrorMessage.Add($"Missing fields are empty or null at line {context.Context.Parser?.RawRecord} position {context.Index}");
                }
                else
                {
                    _logger.LogWarning("Missing fields {HeaderNames} at line {Line} position {Position} in file: {FilePath}", string.Join(',', context.HeaderNames), context.Context.Parser?.RawRecord, context.Index, _filePath);
                    ErrorMessage.Add($"Missing fields {string.Join(',', context.HeaderNames)} at line {context.Context.Parser?.RawRecord} position {context.Index}");
                }
            };

            var fileStream = new FileStream(_filePath, FileMode.Open, FileAccess.Read, FileShare.Read, BufferSize, FileOptions.SequentialScan);
            _streamReader = new StreamReader(fileStream, _encoding, detectEncodingFromByteOrderMarks: false, BufferSize);
            _csvReader = new CsvReader(_streamReader, config);

            _isInitialized = true;
            _logger.LogInformation("CSV reader initialized for file: {FilePath}", _filePath);
        }
        catch (Exception ex)
        {

            _logger.LogError(ex, "Failed to initialize CSV reader for file: {FilePath}", _filePath);
            ErrorMessage.Add($"Error reading CSV from file: {_filePath}: {ex.Message}");
            throw;
        }
    }

    private void HeaderValidationHandler(HeaderValidatedArgs args)
    {
        if (args.InvalidHeaders == null || args.InvalidHeaders.Length == 0)
        {
            _logger.LogWarning("CSV header is empty or null at row {Row} in file: {FilePath}", args.Context.Parser?.RawRecord, _filePath);
            ErrorMessage.Add($"CSV header is empty or null at row {args.Context.Parser?.RawRecord} in file: {_filePath}");
        }

        /*        _logger.LogWarning("CSV header is empty or null at row {Row} in file: {FilePath}", context.Row, _filePath);
                //(header, index, context)
                if (args.InvalidHeaders == null || args.InvalidHeaders.Count == 0)
                    return;
                // Handle invalid headers

                args.InvalidHeaders?.ForEach(header =>
                {
                    if (string.IsNullOrWhiteSpace(header))
                    {
                        _logger.LogWarning("CSV header is empty or null at row {Row} in file: {FilePath}", args.Context.Row, _filePath);
                        ErrorMessage.Add($"Empty header found at row {args.Context.Row}");
                    }
                    else
                    {
                        _logger.LogWarning("Invalid header '{Header}' at row {Row} in file: {FilePath}", header, args.Context.Row, _filePath);
                        ErrorMessage.Add($"Invalid header '{header}' at row {args.Context.Row}");
                    }
                });*/
    }
    public override IEnumerable<string> GetHeaders()
    {
        if (_headers != null)
            return _headers;

        Initialize();

        try
        {
            if (_hasHeaders)
            {
                _csvReader.Read();

                if (_csvReader.ColumnCount > 0)
                {
                    _csvReader.ReadHeader();
                    var headerRecord = _csvReader.HeaderRecord;
                    _headerLength = headerRecord.Length;

                    // Use array pool for temporary processing
                    var tempHeaders = _stringArrayPool.Rent(_headerLength);

                    try
                    {
                        var sameColumns = new Dictionary<string, int>(_headerLength, StringComparer.OrdinalIgnoreCase);

                        for (int i = 0; i < _headerLength; i++)
                        {
                            var headerValue = headerRecord[i];
                            if (string.IsNullOrWhiteSpace(headerValue))
                            {
                                tempHeaders[i] = $"Column{i + 1}";
                            }
                            else
                            {
                                tempHeaders[i] = ColumnMapperHelper.HandleDuplicateHeaders(headerValue, ref sameColumns);
                            }
                        }

                        _sameNameColumnsCount = sameColumns.Values.Sum();

                        // Create final headers array
                        _headers = new string[_headerLength];
                        Array.Copy(tempHeaders, _headers, _headerLength);
                        _cachedHeaders = _headers; // Cache for fast access
                    }
                    finally
                    {
                        _stringArrayPool.Return(tempHeaders);
                    }
                }
                else
                {
                    _headers = Array.Empty<string>();
                    _cachedHeaders = _headers;
                }
            }
            else
            {
                if (_csvReader.Read())
                {
                    _headerLength = _csvReader.Parser.Count;
                    _headers = new string[_headerLength];

                    for (int i = 0; i < _headerLength; i++)
                    {
                        _headers[i] = $"Column{i + 1}";
                    }
                    _cachedHeaders = _headers;
                }
                else
                {
                    _headers = Array.Empty<string>();
                    _cachedHeaders = _headers;
                }
            }

            _headerRead = true;
            _logger.LogDebug("CSV headers read: {Headers}", string.Join(", ", _headers));
        }
        catch (Exception ex)
        {
            ErrorMessage.Add($"Error reading CSV headers from file: {_filePath}: {ex.Message}");
            _logger.LogError(ex, "Failed to read CSV headers from file: {FilePath}", _filePath);
            throw;
        }
        finally
        {
            CloseReader();
        }

        return _headers;
    }

    protected override async Task<IEnumerable<IDictionary<string, object>>> ReadBatchAsync(CancellationToken cancellationToken)
    {
        return await ReadDataAsync(BatchSize, cancellationToken);
    }

    public override async Task<IEnumerable<IDictionary<string, object>>> ReadPreviewBatchAsync(DataImportOptions options, IColumnFilter columnFilter, CancellationToken cancellationToken)
    {
        return await ReadDataAsync(options.PreviewLimit, cancellationToken, options.ColumnMappings, columnFilter, true);
    }

    private async Task<IEnumerable<IDictionary<string, object>>> ReadDataAsync(int limit, CancellationToken cancellationToken, Dictionary<string, ColumnMapping>? columnMappings = null, IColumnFilter? columnFilter = null, bool cleanupReader = false)
    {
        //Initialize();
        GetHeaders();

        // Pre-allocate batch with exact capacity
        var batch = new List<IDictionary<string, object>>(limit);

        try
        {
            if (!_hasHeaders && !_isFirstBatchRead)//Reset reader for no header files
            {
                CloseReader();
                Initialize();
                _isFirstBatchRead = true;
            }
            else if(!_isInitialized)
            {
                Initialize();
                _csvReader.Read(); // skip the First Row of Header
            } 

            int rowCount = 0;
            var fieldsBuffer = _objectArrayPool.Rent(_headerLength);

            while (rowCount < limit && !cancellationToken.IsCancellationRequested)
            {
                try
                {
                    // Add async yield for cooperative cancellation
                    if (rowCount % 100 == 0)
                    {
                        await Task.Yield();
                        cancellationToken.ThrowIfCancellationRequested();
                    }

                    if (!_csvReader.Read())
                        break;

                    // Create row dictionary with pre-allocated capacity
                    var row = new Dictionary<string, object>(_headerLength, StringComparer.Ordinal);

                    // Batch field reading for better performance
                    for (int j = 0; j < _headerLength; j++)
                    {
                        fieldsBuffer[j] = _csvReader.GetField(j) ?? string.Empty;
                    }

                    // Build row from buffered fields
                    for (int j = 0; j < _headerLength; j++)
                    {
                        row[_cachedHeaders[j]] = fieldsBuffer[j];
                    }

                    if (columnMappings != null && columnFilter != null)
                    {
                        var filteredRow = columnFilter.FilterColumns(row, columnMappings);
                        batch.Add(filteredRow);
                    }
                    else
                    {
                        batch.Add(row);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error reading line number {RowNumber} Row : {RawRecord} from CSV file: {FilePath}", rowCount + 1, _csvReader.Parser.RawRecord, _filePath);
                    ErrorMessage.Add($"Error reading line {rowCount + 1}: {ex.Message} => {_csvReader.Parser.RawRecord}");
                }
                rowCount++;
            }
            _objectArrayPool.Return(fieldsBuffer);
        }
        catch (Exception ex)
        {

            _logger.LogError(ex, "Error reading CSV batch from file: {FilePath}", _filePath);
            ErrorMessage.Add($"Error reading CSV from file: {_filePath}: {ex.Message}");
            throw;
        }
        finally
        {
            if (cleanupReader)
                Dispose();
        }

        return batch;
    }

    public override Task<bool> TestConnectionAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            using var fileStream = new FileStream(_filePath, FileMode.Open, FileAccess.Read, FileShare.Read, BufferSize, FileOptions.SequentialScan);
            using var _streamReader = new StreamReader(fileStream, _encoding, detectEncodingFromByteOrderMarks: false, BufferSize);
            using var _csvReader = new CsvReader(_streamReader, CultureInfo.InvariantCulture);
            return Task.FromResult(_csvReader != null);

        }
        catch (Exception)
        {

            return Task.FromResult(false);
        }
    }
    
    public override Task<TableSchema> GetTableSchema(string tableName)
    {
        return Task.FromResult(new TableSchema
        {
            Columns = GetColumnsFromTable(GetHeaders().ToList())
        });
    }
    public override Task<List<TableInfo>> GetAvailableTables()
    {
        var tableName = Path.GetFileNameWithoutExtension(_filePath);

        var tableInfo = new TableInfo
        {
            Schema = null, // CSV files don't have schemas
            Name = tableName,
            Type = "TABLE",
            Columns = null // Or populate columns if you want to read headers here
        };

        return Task.FromResult(new List<TableInfo> { tableInfo });
    }
    private List<ColumnInfo> GetColumnsFromTable(List<string> headers)
    {
        var columns = new List<ColumnInfo>();

        foreach (string column in headers)
        {
            columns.Add(new ColumnInfo
            {
                Name = column,
                //DataType = InferColumnType(table, column.Ordinal),
                IsNullable = true
            });
        }
        return columns;
    }
    public override void Dispose()
    {
        CloseReader();
        _logger.LogInformation("Disposed CSV reader for file: {FilePath}", _filePath);
    }

    private void CloseReader()
    {
        _csvReader?.Dispose();
        _streamReader?.Dispose();
        _isInitialized = false;
        _headerRead = false;
        _isFirstBatchRead = false;
        //_csvReader = null;
    }
}

