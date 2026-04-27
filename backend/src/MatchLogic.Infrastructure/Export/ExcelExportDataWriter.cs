using MatchLogic.Application.Features.Export;
using MatchLogic.Application.Interfaces.Export;
using MatchLogic.Domain.Export;
using MatchLogic.Domain.Import;
using Microsoft.Extensions.Logging;
using NPOI.SS.UserModel;
using NPOI.XSSF.Streaming;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Export.Writers;

/// <summary>
/// Excel export writer using NPOI SXSSFWorkbook for streaming.
/// Features:
/// - Streaming mode (keeps only 100 rows in memory)
/// - Multi-sheet support for large datasets
/// - Type-aware cell formatting
/// - Bold header row with optional freeze/filter
/// Settings read from ConnectionConfig.Parameters using ExcelExportKeys.
/// </summary>
[HandlesExportWriter(DataSourceType.Excel)]
public class ExcelExportDataWriter : BaseExportDataWriter
{
    private readonly ExcelConnectionConfig _connectionConfig;

    // Settings from Parameters
    private readonly string _sheetName;
    private readonly int _numericFormatId;
    private readonly int _dateTimeFormatId;
    private readonly int _floatingPointFormatId;
    private readonly int _maxRowsPerSheet;
    private readonly bool _createMultipleSheets;
    private readonly bool _autoSizeColumns;
    private readonly bool _freezeHeaderRow;
    private readonly bool _applyAutoFilter;

    private SXSSFWorkbook? _workbook;
    private ISheet? _currentSheet;
    private ICellStyle? _headerStyle;
    private ICellStyle? _dateStyle;
    private ICellStyle? _numberStyle;
    private ICellStyle? _integerStyle;
    private int _currentRowIndex;
    private int _sheetNumber;

    public override string Name => "Excel Writer";
    public override DataSourceType Type => DataSourceType.Excel;

    public ExcelExportDataWriter(ConnectionConfig connectionConfig, ILogger logger)
        : base(logger, 1000)
    {
        _connectionConfig = connectionConfig as ExcelConnectionConfig
            ?? throw new ArgumentException("Invalid configuration type for ExcelExportDataWriter", nameof(connectionConfig));

        var p = _connectionConfig.Parameters;

        // From config property
        _sheetName = !string.IsNullOrEmpty(_connectionConfig.SheetName)
            ? _connectionConfig.SheetName
            : "Export";

        // From Parameters (export-specific)
        _numericFormatId = p.GetInt(ExcelExportKeys.NumericFormatId, ExcelExportKeys.Defaults.NumericFormatId);
        _dateTimeFormatId = p.GetInt(ExcelExportKeys.DateTimeFormatId, ExcelExportKeys.Defaults.DateTimeFormatId);
        _floatingPointFormatId = p.GetInt(ExcelExportKeys.FloatingPointFormatId, ExcelExportKeys.Defaults.FloatingPointFormatId);
        _maxRowsPerSheet = p.GetInt(ExcelExportKeys.MaxRowsPerSheet, ExcelExportKeys.Defaults.MaxRowsPerSheet);
        _createMultipleSheets = p.GetBool(ExcelExportKeys.CreateMultipleSheets, ExcelExportKeys.Defaults.CreateMultipleSheets);
        _autoSizeColumns = p.GetBool(ExcelExportKeys.AutoSizeColumns, ExcelExportKeys.Defaults.AutoSizeColumns);
        _freezeHeaderRow = p.GetBool(ExcelExportKeys.FreezeHeaderRow, ExcelExportKeys.Defaults.FreezeHeaderRow);
        _applyAutoFilter = p.GetBool(ExcelExportKeys.ApplyAutoFilter, ExcelExportKeys.Defaults.ApplyAutoFilter);
    }

    #region IExportDataWriter Implementation

    public override async Task InitializeAsync(ExportSchema schema, CancellationToken ct = default)
    {
        await base.InitializeAsync(schema, ct);

        try
        {
            var directory = Path.GetDirectoryName(_connectionConfig.FilePath);
            if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
                Directory.CreateDirectory(directory);

            _workbook = new SXSSFWorkbook(100);
            CreateCellStyles();
            CreateNewSheet();

            _logger.LogInformation("Excel workbook created: {Path}", _connectionConfig.FilePath);
        }
        catch (Exception ex)
        {
            AddError($"Failed to initialize Excel writer: {ex.Message}");
            throw;
        }
    }

    public override async Task WriteBatchAsync(
        IReadOnlyList<IDictionary<string, object>> batch,
        CancellationToken ct = default)
    {
        await base.WriteBatchAsync(batch, ct);

        if (_workbook == null || _currentSheet == null || _schema == null)
            throw new InvalidOperationException("Writer not initialized");

        try
        {
            foreach (var rowData in batch)
            {
                ct.ThrowIfCancellationRequested();

                if (_currentRowIndex > _maxRowsPerSheet)
                {
                    if (_createMultipleSheets)
                    {
                        CreateNewSheet();
                    }
                    else
                    {
                        _logger.LogWarning("Excel row limit reached at {Rows}. Stopping export.", _rowsWritten);
                        break;
                    }
                }

                var row = _currentSheet.CreateRow(_currentRowIndex++);

                for (int i = 0; i < _schema.Columns.Count; i++)
                {
                    var col = _schema.Columns[i];
                    var value = rowData.TryGetValue(col.Name, out var v) ? v : null;
                    SetCellValue(row.CreateCell(i), value, col.DataType);
                }

                _rowsWritten++;
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
            if (_workbook != null)
            {
                if (_autoSizeColumns && _schema != null)
                {
                    _logger.LogInformation("Auto-sizing columns...");
                    for (int s = 0; s < _workbook.NumberOfSheets; s++)
                    {
                        var sheet = _workbook.GetSheetAt(s);
                        for (int i = 0; i < _schema.Columns.Count; i++)
                        {
                            sheet.AutoSizeColumn(i);
                        }
                    }
                }

                using var fileStream = new FileStream(_connectionConfig.FilePath, FileMode.Create, FileAccess.Write);
                _workbook.Write(fileStream, leaveOpen: false);

                _workbook.Dispose();
                _workbook = null;
            }

            var result = await base.FinalizeAsync(ct);
            result.FilePath = _connectionConfig.FilePath;

            if (File.Exists(_connectionConfig.FilePath))
            {
                var fileInfo = new FileInfo(_connectionConfig.FilePath);
                _logger.LogInformation(
                    "Excel export completed: {Rows} rows, {Sheets} sheets, {Size:N0} bytes",
                    _rowsWritten, _sheetNumber, fileInfo.Length);
            }

            return result;
        }
        catch (Exception ex)
        {
            AddError($"Error finalizing Excel: {ex.Message}");
            return ExportWriteResult.Failed(_errors);
        }
    }

    #endregion

    #region Legacy ExportAsync Support

    protected override async Task InitializeExportAsync(ExportContext context, CancellationToken cancellationToken)
    {
        var directory = Path.GetDirectoryName(_connectionConfig.FilePath);
        if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
            Directory.CreateDirectory(directory);

        _workbook = new SXSSFWorkbook(100);
        CreateCellStyles();

        _sheetNumber++;
        _currentSheet = _workbook.CreateSheet(_sheetName);
        _currentRowIndex = 0;

        var headerRow = _currentSheet.CreateRow(_currentRowIndex++);
        for (int i = 0; i < context.OrderedColumnNames.Count; i++)
        {
            var cell = headerRow.CreateCell(i);
            cell.SetCellValue(context.OrderedColumnNames[i]);
            cell.CellStyle = _headerStyle;
        }

        if (_freezeHeaderRow)
            _currentSheet.CreateFreezePane(0, 1);

        await Task.CompletedTask;
    }

    protected override async Task WriteBatchAsync(
        List<IDictionary<string, object>> batch,
        ExportContext context,
        CancellationToken cancellationToken)
    {
        if (_workbook == null || _currentSheet == null)
            throw new InvalidOperationException("Writer not initialized");

        foreach (var rowData in batch)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (_currentRowIndex > _maxRowsPerSheet && _createMultipleSheets)
            {
                _sheetNumber++;
                _currentSheet = _workbook.CreateSheet($"{_sheetName}_{_sheetNumber}");
                _currentRowIndex = 0;

                var headerRow = _currentSheet.CreateRow(_currentRowIndex++);
                for (int i = 0; i < context.OrderedColumnNames.Count; i++)
                {
                    var cell = headerRow.CreateCell(i);
                    cell.SetCellValue(context.OrderedColumnNames[i]);
                    cell.CellStyle = _headerStyle;
                }
            }

            var row = _currentSheet.CreateRow(_currentRowIndex++);
            for (int i = 0; i < context.OrderedColumnNames.Count; i++)
            {
                var colName = context.OrderedColumnNames[i];
                var value = rowData.TryGetValue(colName, out var v) ? v : null;
                SetCellValue(row.CreateCell(i), value, null);
            }
        }

        await Task.CompletedTask;
    }

    protected override async Task FinalizeExportAsync(ExportContext context, int totalRows, CancellationToken cancellationToken)
    {
        if (_workbook != null)
        {
            using var fileStream = new FileStream(_connectionConfig.FilePath, FileMode.Create, FileAccess.Write);
            _workbook.Write(fileStream, leaveOpen: false);
            _workbook.Dispose();
            _workbook = null;
        }

        await Task.CompletedTask;
    }

    #endregion

    #region Helpers

    private void CreateCellStyles()
    {
        _headerStyle = _workbook!.CreateCellStyle();
        var headerFont = _workbook.CreateFont();
        headerFont.IsBold = true;
        _headerStyle.SetFont(headerFont);

        _dateStyle = _workbook.CreateCellStyle();
        var dateFormat = _workbook.CreateDataFormat();
        _dateStyle.DataFormat = dateFormat.GetFormat(GetDateTimeFormat());

        _numberStyle = _workbook.CreateCellStyle();
        var numberFormat = _workbook.CreateDataFormat();
        _numberStyle.DataFormat = numberFormat.GetFormat(GetFloatingPointFormat());

        _integerStyle = _workbook.CreateCellStyle();
        _integerStyle.DataFormat = numberFormat.GetFormat(GetNumericFormat());
    }

    private string GetDateTimeFormat()
    {
        return _dateTimeFormatId switch
        {
            1 => "m/dd/yyyy",
            2 => "m/d/yyyy h:mm",
            3 => "d-mmm-yy",
            4 => "dddd, mmmm dd, yyyy",
            5 => "m/dd/yy",
            6 => "mm/dd/yy",
            7 => "dd-mmm-yy",
            8 => "mmmm dd, yyyy",
            9 => "m/dd/yy h:mm AM/PM",
            10 => "m/dd/yy h:mm",
            11 => "m/dd/yyyy",
            _ => "yyyy-mm-dd hh:mm:ss"
        };
    }

    private string GetNumericFormat()
    {
        return _numericFormatId switch
        {
            12 => "0.00E+00",
            13 => "0.00",
            14 => "0.000",
            15 => "0",
            16 => "#,##0.00",
            17 => "0.00000E+00",
            _ => "0"
        };
    }

    private string GetFloatingPointFormat()
    {
        return _floatingPointFormatId switch
        {
            12 => "0.00E+00",
            13 => "0.00",
            14 => "0.000",
            15 => "0",
            16 => "#,##0.00",
            17 => "0.00000E+00",
            _ => "#,##0.00"
        };
    }

    private void CreateNewSheet()
    {
        _sheetNumber++;
        var sheetName = _sheetNumber == 1 ? _sheetName : $"{_sheetName}_{_sheetNumber}";

        _currentSheet = _workbook!.CreateSheet(sheetName);
        _currentRowIndex = 0;

        if (_schema != null)
        {
            var headerRow = _currentSheet.CreateRow(_currentRowIndex++);
            for (int i = 0; i < _schema.Columns.Count; i++)
            {
                var cell = headerRow.CreateCell(i);
                cell.SetCellValue(_schema.Columns[i].Name);
                cell.CellStyle = _headerStyle;
            }

            if (_freezeHeaderRow)
                _currentSheet.CreateFreezePane(0, 1);

            if (_applyAutoFilter)
                _currentSheet.SetAutoFilter(new NPOI.SS.Util.CellRangeAddress(0, 0, 0, _schema.Columns.Count - 1));
        }

        _logger.LogDebug("Created Excel sheet: {SheetName}", sheetName);
    }

    private void SetCellValue(ICell cell, object? value, string? dataType)
    {
        if (value == null || value == DBNull.Value)
        {
            cell.SetCellType(CellType.Blank);
            return;
        }

        switch (value)
        {
            case DateTime dt:
                cell.SetCellValue(dt);
                cell.CellStyle = _dateStyle;
                break;
            case DateTimeOffset dto:
                cell.SetCellValue(dto.DateTime);
                cell.CellStyle = _dateStyle;
                break;
            case double d:
                cell.SetCellValue(d);
                cell.CellStyle = _numberStyle;
                break;
            case decimal dec:
                cell.SetCellValue((double)dec);
                cell.CellStyle = _numberStyle;
                break;
            case float f:
                cell.SetCellValue(f);
                cell.CellStyle = _numberStyle;
                break;
            case int i:
                cell.SetCellValue(i);
                cell.CellStyle = _integerStyle;
                break;
            case long l:
                cell.SetCellValue(l);
                cell.CellStyle = _integerStyle;
                break;
            case bool b:
                cell.SetCellValue(b);
                break;
            case Guid g:
                cell.SetCellValue(g.ToString());
                break;
            case byte[] bytes:
                cell.SetCellValue(Convert.ToBase64String(bytes));
                break;
            default:
                cell.SetCellValue(value.ToString());
                break;
        }
    }

    public override void Dispose()
    {
        if (!_disposed)
        {
            _workbook?.Dispose();
            _workbook = null;
        }
        base.Dispose();
    }

    #endregion
}