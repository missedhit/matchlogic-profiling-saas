using System.Linq;

namespace MatchLogic.Api.Common;

public static class ApiConstants // Avoided existing name "Constants" to prevent confusion with other constants class in the project
{
    public static readonly string[] ExcelExtensions = { ".xlsx", ".xls" };
    public static readonly string[] CsvExtensions = { ".csv" };
    // Add More Future File Extensions here if needed
    public static readonly string[] AllowedExtensions = ExcelExtensions.Concat(CsvExtensions).ToArray();
    public static readonly string[] AllowedMimeTypes =
    {
        "text/csv",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/vnd.ms-excel"
    };
    public static class RegexFieldLength
    {
        public static int NameMaxLength = 200;

        public static int RegexMaxLength = 500;
    }
}
