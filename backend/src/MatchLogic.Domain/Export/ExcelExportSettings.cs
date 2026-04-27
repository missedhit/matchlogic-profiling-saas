namespace MatchLogic.Domain.Export;

/// <summary>
/// Parameter keys for Excel export settings.
/// SheetName is in ExcelConnectionConfig.
/// These are EXPORT-SPECIFIC settings stored in Parameters dictionary.
/// </summary>
public static class ExcelExportKeys
{
    public const string NumericFormatId = "Excel.NumericFormatId";
    public const string DateTimeFormatId = "Excel.DateTimeFormatId";
    public const string FloatingPointFormatId = "Excel.FloatingPointFormatId";
    public const string MaxRowsPerSheet = "Excel.MaxRowsPerSheet";
    public const string CreateMultipleSheets = "Excel.CreateMultipleSheets";
    public const string AutoSizeColumns = "Excel.AutoSizeColumns";
    public const string FreezeHeaderRow = "Excel.FreezeHeaderRow";
    public const string ApplyAutoFilter = "Excel.ApplyAutoFilter";

    public static class Defaults
    {
        public const int NumericFormatId = 0;
        public const int DateTimeFormatId = 1;
        public const int FloatingPointFormatId = 0;
        public const int MaxRowsPerSheet = 1_048_575;
        public const bool CreateMultipleSheets = true;
        public const bool AutoSizeColumns = false;
        public const bool FreezeHeaderRow = true;
        public const bool ApplyAutoFilter = false;
    }
}