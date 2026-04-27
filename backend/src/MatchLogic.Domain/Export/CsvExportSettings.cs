namespace MatchLogic.Domain.Export;

/// <summary>
/// Parameter keys for CSV export settings.
/// Connection properties (Delimiter, HasHeaders, etc.) are in CSVConnectionConfig.
/// These are EXPORT-SPECIFIC settings stored in Parameters dictionary.
/// </summary>
public static class CsvExportKeys
{
    // Format settings (export-specific)
    public const string DateFormat = "Csv.DateFormat";
    public const string DecimalSeparator = "Csv.DecimalSeparator";
    public const string UseQuotation = "Csv.UseQuotation";
    public const string NewLine = "Csv.NewLine";

    // Defaults
    public static class Defaults
    {
        public const string DateFormat = "yyyy-MM-dd HH:mm:ss";
        public const string DecimalSeparator = ".";
        public const bool UseQuotation = false;
        public const string NewLine = "\r\n";
    }
}