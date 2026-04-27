using MatchLogic.Domain.Project;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace MatchLogic.Domain.Import;
#region Config Concrete Classes
public class CSVConnectionConfig : ConnectionConfig, IFileConnectionInfo
{
    public Guid FileId { get; set; }
    public string FilePath { get; set; }
    public string Delimiter { get; set; } = ",";
    public bool HasHeaders { get; set; } = true;
    public char? QuoteChar { get; set; } = '"';
    public char? CommentChar { get; set; } = '#';

    public System.Text.Encoding Encoding = System.Text.Encoding.UTF8;

    private const string FileIdKey = "FileId";
    private const string FilePathKey = "FilePath";
    private const string DelimiterKey = "Delimiter";
    private const string QuoteKey = "Quote";
    private const string CommentKey = "Comment";
    private const string EncodingKey = "Encoding";
    private const string HasHeaderKey = "HasHeaders";

    private readonly string[] allowedExtensions = { ".csv" };



    public override ConnectionConfig CreateFromArgs(DataSourceType Type, Dictionary<string, string> args, DataSourceConfiguration? sourceConfiguration = null)
    {
        if (Type != DataSourceType.CSV)
        {
            throw new ArgumentException("Invalid data source type for CSVConnectionConfig");
        }
        Parameters = args;
        SourceConfig = sourceConfiguration;
        if (!args.ContainsKey(FilePathKey) || string.IsNullOrEmpty(args[FilePathKey]))
        {
            throw new ArgumentException("FilePath is required for CSV connection");
        }

        args.TryGetValue(DelimiterKey, out var delimiter);
        if (args.TryGetValue(EncodingKey, out var encodingName))
        {
            // Supported Encoding Types
            Encoding = encodingName.ToUpperInvariant() switch
            {
                "ASCII" => System.Text.Encoding.ASCII,
                "BIGENDIANUNICODE" => System.Text.Encoding.BigEndianUnicode,
                "DEFAULT" => System.Text.Encoding.Default,
                "UNICODE" => System.Text.Encoding.Unicode,
                "UTF7" => System.Text.Encoding.UTF7,
                "UTF8" => System.Text.Encoding.UTF8,
                "UTF32" => System.Text.Encoding.UTF32,
                _ => System.Text.Encoding.UTF8 // Fallback to UTF8 for unknown encodings
            };
        }
        if (args.TryGetValue(QuoteKey, out var quoteValue))
        {
            QuoteChar = quoteValue.Length > 0 ? quoteValue[0] : '"'; // Default to double quote if not specified
        }
        if (args.TryGetValue(CommentKey, out var commentValue))
        {
            CommentChar = commentValue.Length > 0 ? commentValue[0] : '#'; // Default to double quote if not specified
        }

        FileId = args.TryGetValue(FileIdKey, out string? File_Id) && Guid.TryParse(File_Id, out var fileId) ? fileId : Guid.Empty;
        FilePath = args.ContainsKey(FilePathKey) && !string.IsNullOrEmpty(args[FilePathKey]) ? args[FilePathKey] : string.Empty;
        Delimiter = args.TryGetValue(DelimiterKey, out string? value) ? value : ",";
        HasHeaders = args.ContainsKey(HasHeaderKey) && bool.TryParse(args[HasHeaderKey], out var hasHeader) ? hasHeader : true;
 
        /*var config = new CSVConnectionConfig
        {
            FileId = args.TryGetValue(FileIdKey, out string? FileId) && Guid.TryParse(FileId, out var fileId) ? fileId : Guid.Empty,
            FilePath = args.ContainsKey(FilePathKey) && !string.IsNullOrEmpty(args[FilePathKey]) ? args[FilePathKey] : string.Empty,
            Delimiter = args.TryGetValue(DelimiterKey, out string? value) ? value : ",",
            HasHeaders = args.ContainsKey(HasHeaderKey) && bool.TryParse(args[HasHeaderKey], out var hasHeader)? hasHeader : true,
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

    public override bool CanCreateFromArgs(DataSourceType Type)
    {
        return Type == DataSourceType.CSV;
    }
    /*protected bool GetHasHeaders()
{
return Parameters.TryGetValue(HasHeaderKey, out var hasHeaders)
   && bool.TryParse(hasHeaders, out var useHeaders)
   && useHeaders;
}*/



}

#endregion


