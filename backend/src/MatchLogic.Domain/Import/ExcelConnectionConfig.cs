using MatchLogic.Domain.Project;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace MatchLogic.Domain.Import;

public class ExcelConnectionConfig : ConnectionConfig, IFileConnectionInfo
{
    public Guid FileId { get; set; }
    public string FilePath { get; set; }
    public string? SheetName { get; set; }
    public int? SheetIndex { get; set; }
    //public bool HasHeaders { get; set; }
    private const string FileIdKey = "FileId";
    private const string FilePathKey = "FilePath";
    private const string SheetNameKey = "TableName";
    private const string SheetIndexKey = "SheetIndex";
    //private const string HasHeaderKey = "HasHeaders";
    private readonly string[] allowedExtensions = { ".xlsx", ".xls" };
    public override bool CanCreateFromArgs(DataSourceType Type)
        => Type == DataSourceType.Excel;
    public override ConnectionConfig CreateFromArgs(DataSourceType Type, Dictionary<string, string> args, DataSourceConfiguration? sourceConfiguration = null)
    {
        if (Type != DataSourceType.Excel)
        {
            throw new ArgumentException("Invalid data source type for ExcelConnectionConfig");
        }
        Parameters = args;
        SourceConfig = sourceConfiguration;
        if (!args.ContainsKey(FilePathKey) || string.IsNullOrEmpty(args[FilePathKey]))
        {
            throw new ArgumentException("FilePath is required for Excel connection");
        }
        //if (args.ContainsKey(SheetNameKey) && args.ContainsKey(SheetIndexKey))
        //{
        //    throw new ArgumentException("Specify either SheetName or SheetIndex, not both.");
        //}

        FileId = args.TryGetValue(FileIdKey, out string? File_Id) && Guid.TryParse(File_Id, out var fileId) ? fileId : Guid.Empty;
        FilePath = args.ContainsKey(FilePathKey) && !string.IsNullOrEmpty(args[FilePathKey]) ? args[FilePathKey] : string.Empty;
        SheetName = SourceConfig?.TableOrSheet != null ? SourceConfig.TableOrSheet : args.TryGetValue(SheetNameKey, out var sheetName) ? sheetName : null;
        SheetIndex = args.TryGetValue(SheetIndexKey, out var sheetIndexStr) && int.TryParse(sheetIndexStr, out var sheetIndex) ? sheetIndex : -1;

/*        var config = new ExcelConnectionConfig
        {
            FileId = args.TryGetValue(FileIdKey, out string? FileId) && Guid.TryParse(FileId, out var fileId) ? fileId : Guid.Empty,
            FilePath = args.ContainsKey(FilePathKey) && !string.IsNullOrEmpty(args[FilePathKey]) ? args[FilePathKey] : string.Empty,
            SheetName = args.TryGetValue(SheetNameKey, out var sheetName) ? sheetName : null,
            SheetIndex = args.TryGetValue(SheetIndexKey, out var sheetIndexStr) && int.TryParse(sheetIndexStr, out var sheetIndex) ? sheetIndex : null,
            //HasHeaders = args.ContainsKey(HasHeaderKey) && bool.TryParse(args[HasHeaderKey], out var hasHeader) && hasHeader
        };*/
        return this;
    }
    public override bool ValidateConnection()
    {
        if (!base.ValidateConnection() || !ValidateParameter(FilePathKey))
            return false;
        return File.Exists(Parameters[FilePathKey]) && ValidateFileExtension(Parameters[FilePathKey], allowedExtensions);
    }


    public bool ValidateFileExtension(string filePath, params string[] allowedExtensions)
    {
        var extension = Path.GetExtension(filePath).ToLower();
        return allowedExtensions.Contains(extension);
    }
}


