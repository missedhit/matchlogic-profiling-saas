using MatchLogic.Application.Features.Import;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using MatchLogic.Infrastructure.Common;
using ExcelDataReader;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Import;
[HandlesConnectionConfig(typeof(ExcelConnectionConfig))]
public class ExcelDataReader : BaseReaderStrategy
{
    private FileStream _fileStream;
    private IExcelDataReader _excelReader;
    private int _sheetIndex;
    private const int BatchSize = 1000;
    private bool _headerRead;
    private bool _rowCountRead;
    private bool? _isHeaderDetected; // Will be set automatically during header reading
    private long _sameNameColumnsCount = 0;

    private Dictionary<string, string> _headersDict = new();

    private string _filePath => _config.FilePath;

    private readonly ExcelConnectionConfig _config;
    public ExcelDataReader(ConnectionConfig config, ILogger logger) : base(config, logger)
    {
        _config = config as ExcelConnectionConfig ?? throw new ArgumentException("Invalid configuration for ExcelDataReader", nameof(config));
        _sheetIndex = _config.SheetIndex ?? 0; // Default to first sheet if not specified
        SheetName = _config.SheetName ?? string.Empty; // Default to empty if not specified
    }

    private bool _isFirstBatchRead = false;
    public string SheetName { get; set; }

    public override string Name => Path.GetFileName(_filePath);
    public override long RowCount
    {
        get
        {

            // First make sure we've read the headers to detect if we have header row
            EnsureHeadersRead();
            // Get total rows from Excel
            long totalRows = GetTotalRowCount();

            // Subtract header row if detected
            return _isHeaderDetected == true && totalRows > 0 ? totalRows - 1 : totalRows;
        }
    }

    public override long DuplicateHeaderCount
    {
        get
        {
            // First make sure we've read the headers to detect if we have header row
            EnsureHeadersRead();
            return _sameNameColumnsCount;
        }
    }

    // Helper to get the raw row count from Excel
    private long GetTotalRowCount()
    {
        if (_rowCount == null)
        {
            // if Reader is not initialized, initialize it
            if (_excelReader == null)
            {
                _isInitialized = false;
                Initialize();
                //if(_isHeaderDetected==true)
                //    _excelReader?.Read();

            }



            try
            {
                if (!_rowCountRead)
                {
                    var rowCount = _excelReader?.RowCount;
                    _rowCount = rowCount;
                    _rowCountRead = true;
                }
                else
                {
                    _logger.LogWarning("Failed to read row count - sheet may be empty");
                    _rowCount = 0;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to read Excel row count from file: {FilePath}", _filePath);
                ErrorMessage.Add($"Error reading Excel Row Count from file: {_filePath}: {ex.Message}");
                throw;
            }
        }

        return _rowCount.GetValueOrDefault(0);
    }

    // Helper method to ensure headers are read
    private void EnsureHeadersRead()
    {
        if (_headers == null || !_headerRead)
        {
            GetHeaders();
        }
    }

    private void Initialize()
    {
        if (_isInitialized)
            return;
        try
        {
            Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
            OpenReader();

            // If sheet name was provided, we need to find its index
            if (_sheetIndex == -1 && !string.IsNullOrEmpty(SheetName))
            {
                _sheetIndex = FindSheetIndexByName(SheetName);
                if (_sheetIndex == -1)
                {
                    _logger.LogWarning("Sheet '{SheetName}' not found. Falling back to first sheet.", SheetName);
                    _sheetIndex = 0;
                }

                // After finding sheet, reopen reader to get back to beginning
                ReopenReader();
            }

            // Navigate to the correct sheet
            NavigateToSheet(_sheetIndex);

            _isInitialized = true;
            _logger.LogInformation("Excel reader initialized for file: {FilePath}, sheet index: {SheetIndex}",
                _filePath, _sheetIndex);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to initialize Excel reader for file: {FilePath}", _filePath);
            ErrorMessage.Add($"Error reading Excel from file: {_filePath}: {ex.Message}");
            CloseReader();
            throw;
        }
    }

    private int FindSheetIndexByName(string sheetName)
    {
        int index = 0;
        do
        {
            if (_excelReader.Name?.Equals(sheetName, StringComparison.OrdinalIgnoreCase) == true)
            {
                return index;
            }
            index++;
        } while (_excelReader.NextResult());
        return -1;
    }

    private void NavigateToSheet(int index)
    {
        // Navigate to desired sheet
        for (int i = 0; i < index; i++)
        {
            if (!_excelReader.NextResult())
            {
                _logger.LogWarning("Sheet index {SheetIndex} out of range. Using last available sheet.", index);
                break;
            }
        }
    }

    public override IEnumerable<string> GetHeaders()
    {
        if (_headers != null)
            return _headers;

        Initialize();
        try
        {
            if (!_headerRead && _excelReader.Read())
            {
                var headerCount = _excelReader.FieldCount;
                _headers = new string[headerCount];
                bool hasCustomHeaders = false;
                Dictionary<string, int> sameColumns = new(StringComparer.OrdinalIgnoreCase);

                for (int i = 0; i < headerCount; i++)
                {
                    var headerValue = _excelReader.GetString(i);
                    if (string.IsNullOrWhiteSpace(headerValue))
                    {
                        // If we find empty header cells, we generate a custom column name
                        _headers[i] = $"Column{i + 1}";
                        hasCustomHeaders = true;
                    }
                    else
                    {
                        _headers[i] = ColumnMapperHelper.HandleDuplicateHeaders(headerValue, ref sameColumns);
                    }
                }
                // If we have duplicate headers, we need to count them
                _sameNameColumnsCount = sameColumns.Values.Sum(value => value);
                // Auto-detect if this is a real header row
                // If we had to generate any column names, it's likely not a true header row
                _isHeaderDetected = !hasCustomHeaders;

                _headerRead = true;
                _logger.LogDebug("Excel headers read: {Headers}, IsHeaderDetected: {IsHeaderDetected}",
                    string.Join(", ", _headers), _isHeaderDetected);
            }
            else
            {
                _logger.LogWarning("Failed to read headers - sheet may be empty");
                _headers = Array.Empty<string>();
                _isHeaderDetected = false;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to read Excel headers from file: {FilePath}", _filePath);
            ErrorMessage.Add($"Error reading Excel Headers from file: {_filePath}: {ex.Message}");
            throw;
        }
        finally
        {
            CloseReader();
        }

        return _headers;
    }
    private void EnsureDataTypes()
    {
        // Ensure headers are read and available.
        if (_headers == null || _headers.Length == 0)
        {
            GetHeaders();
        }

        if (_headers == null || _headers.Length == 0)
            return;

        // If we've already inferred types for all headers, nothing to do.
        bool allPresent = true;
        foreach (var header in _headers)
        {
            if (!_headersDict.ContainsKey(header))
            {
                allPresent = false;
                break;
            }
        }
        if (allPresent)
            return;

        try
        {
            // Ensure the reader is open and positioned at sheet start.
            Initialize();

            if (_excelReader == null)
            {
                _isInitialized = false;
                Initialize();
            }
            // Skip header row if the header was detected (we need the first DATA row).
            bool gotDataRow = false;
            int columns = _headers.Length;

            // Move to first data row:
            // If header detected -> first Read() will be header, so call Read() twice to get data row.
            // If header not detected -> first Read() will be data row, so call Read() once.
            if (_isHeaderDetected == true)
            {

                // Skip header
                if (!_excelReader.Read())
                {
                    // No rows at all
                    gotDataRow = false;
                }
                else
                {
                    // Try to read first data row
                    if (_excelReader.Read())
                        gotDataRow = true;
                }
            }
            else
            {
                // No header present, first read should be data
                if (_excelReader.Read())
                    gotDataRow = true;
            }

            if (gotDataRow)
            {
                for (int i = 0; i < columns; i++)
                {
                    Type dataType;
                    try
                    {
                        var value = _excelReader.GetValue(i);
                        if (value != null && value != DBNull.Value)
                            dataType = value.GetType();
                        else
                            dataType = _excelReader.GetFieldType(i) ?? typeof(string);
                    }
                    catch
                    {
                        dataType = typeof(string);
                    }

                    // Use indexer (adds or updates)
                    _headersDict[_headers[i]] = dataType.FullName;
                }
            }
            else
            {
                // No data rows - default all to string
                for (int i = 0; i < columns; i++)
                {
                    _headersDict[_headers[i]] = typeof(string).FullName;
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to infer Excel column data types for file: {FilePath}", _filePath);
            // On error, ensure at least string defaults
            foreach (var header in _headers)
            {
                if (!_headersDict.ContainsKey(header))
                    _headersDict[header] = typeof(string).FullName;
            }

            // Do not rethrow here; leave error recorded via logger
        }
        finally
        {
            // Reset the reader so other operations start from the beginning.
            try
            {
                ReopenReader();
            }
            catch
            {
                // If reopening fails, ensure reader is closed.
                CloseReader();
            }
        }
    }
    protected override Task<IEnumerable<IDictionary<string, object>>> ReadBatchAsync(CancellationToken cancellationToken)
    {
        return ReadDataAsyc(limit: BatchSize, cancellationToken: cancellationToken);
    }

    public override Task<IEnumerable<IDictionary<string, object>>> ReadPreviewBatchAsync(DataImportOptions options, IColumnFilter columnFilter, CancellationToken cancellationToken)
    {
        return ReadDataAsyc(options.PreviewLimit, cancellationToken, options.ColumnMappings, columnFilter, true);
    }

    private Task<IEnumerable<IDictionary<string, object>>> ReadDataAsyc(int limit, CancellationToken cancellationToken, Dictionary<string, ColumnMapping>? columnMappings = null, IColumnFilter? columnFilter = null, bool CleanupReader = false)
    {
        Initialize();
        // Create a batch of the specified size
        var batch = new List<IDictionary<string, object>>(limit);
        try
        {
            GetHeaders(); // Ensure headers are read
            if (_isHeaderDetected == true && !_isFirstBatchRead)//Reset reader for no header files
            {
                if (_excelReader == null)
                {
                    _isInitialized = false;
                    Initialize();
                }
                _excelReader?.Read();// Skip header row if detected
                _isFirstBatchRead = true;
            }

            int rowCount = 0;
            while (rowCount < limit && !cancellationToken.IsCancellationRequested && _excelReader.Read())
            {
                var row = new Dictionary<string, object>(_headers.Length); // Pre-allocate capacity for better performance
                for (int j = 0; j < _headers.Length; j++)
                {
                    row[_headers[j]] = _excelReader.GetValue(j);
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
                rowCount++;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error reading Excel preview batch from file: {FilePath}", _filePath);
            ErrorMessage.Add($"Error reading Excel Rows from file: {_filePath}: {ex.Message}");
            throw;
        }
        finally
        {
            if (CleanupReader)
                Dispose(); // Close the reader after preview
        }

        return Task.FromResult<IEnumerable<IDictionary<string, object>>>(batch);
    }

    public override void Dispose()
    {
        CloseReader();
        _logger.LogInformation("Disposed Excel reader for file: {FilePath}", _filePath);
    }

    public List<string> GetTables()
    {

        Initialize(); // Ensure the reader is initialized
        var sheetNames = new List<string>();
        try
        {
            do
            {
                var sheetName = _excelReader.Name;
                if (!string.IsNullOrEmpty(sheetName))
                {
                    sheetNames.Add(sheetName);
                }
            } while (_excelReader.NextResult());
        }
        catch (Exception ex)
        {

            _logger.LogError(ex, "Error retrieving sheet names from file: {FilePath}", _filePath);
            ErrorMessage.Add($"Error retrieving sheet names from file: {_filePath}: {ex.Message}");
            throw;
        }
        finally
        {
            CloseReader();
        }

        return sheetNames;
    }


    private void OpenReader()
    {

        try
        {
            CloseReader(); // Close any existing reader first
            _fileStream = File.Open(_filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            _excelReader = ExcelReaderFactory.CreateReader(_fileStream);
            _headerRead = false;
            _rowCountRead = false;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to initialize Excel reader for file: {FilePath}", _filePath);
            ErrorMessage.Add($"Error reading Excel from file: {_filePath}: {ex.Message}");
            throw;
        }
    }

    private void CloseReader()
    {
        if (_excelReader != null)
        {
            _excelReader.Dispose();
            _excelReader = null;
        }

        if (_fileStream != null)
        {
            _fileStream.Dispose();
            _fileStream = null;
        }
        //_isInitialized = false;
    }

    private void ReopenReader()
    {
        CloseReader();
        OpenReader();
    }

    public override Task<bool> TestConnectionAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
            using var stream = File.Open(_filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
            using var reader = ExcelReaderFactory.CreateReader(stream);
            return Task.FromResult(reader != null);
        }
        catch
        {
            return Task.FromResult(false);
        }
    }

    public override Task<List<TableInfo>> GetAvailableTables()
    {
        var result = new List<TableInfo>();
        foreach (var table in GetTables())
        {
            result.Add(new TableInfo
            {
                Schema = "Excel",
                Name = table,
                Type = "Worksheet"
            });
        }
        return Task.FromResult(result);
    }
    public override Task<TableSchema> GetTableSchema(string tableName)
    {
        try
        {
            var arr = tableName.Split('.');
            tableName = arr[arr.Length - 1];
            // Reset Sheet Defaults 
            _sheetIndex = -1;
            SheetName = tableName;
            _isInitialized = false;
            _headers = null;
            Initialize();

            // Ensure headers are read for the correct sheet
            var headers = GetHeaders();

            var tableSchema = new TableSchema
            {
                Columns = GetColumnsFromTable(headers.ToList())
            };

            return Task.FromResult(tableSchema);
        }
        catch (Exception)
        {

            //throw;
            var tableSchema = new TableSchema
            {
                Columns = GetColumnsFromTable(new List<string>())
            };
            return Task.FromResult(tableSchema);
        }
        finally
        {
            CloseReader();
        }
    }

    private List<ColumnInfo> GetColumnsFromTable(List<string> headers)
    {
        EnsureDataTypes();
        var columns = new List<ColumnInfo>();
        int index = 0;
        foreach (string column in headers)
        {
            columns.Add(new ColumnInfo
            {
                Name = column,
                //DataType = InferColumnType(table, column.Ordinal),
                Ordinal = index++,
                DataType = _headersDict[column],
                IsNullable = true
            });
        }
        return columns;
    }


}

