using MatchLogic.Application.Interfaces.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Common;
public class SHA256RecordHasher : IRecordHasher
{
    private readonly JsonSerializerOptions _jsonOptions; 
    private readonly string MetaDataField = "_metadata";
    private readonly string HashField = "Hash";

    public SHA256RecordHasher()
    {
        _jsonOptions = new()
        {
            WriteIndented = false,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };
    }

    public string ComputeHash(IDictionary<string, object> record)
    {
        using SHA256 sha256 = SHA256.Create();
        var orderedRecord = new SortedDictionary<string, object>(record);
        var jsonString = JsonSerializer.Serialize(orderedRecord, _jsonOptions);
        var hashBytes = sha256.ComputeHash(Encoding.UTF8.GetBytes(jsonString));
        return Convert.ToBase64String(hashBytes);
    }

    public string ComputeGroupHash(List<IDictionary<string, object>> records)
    {
        if(records.Any(r => !r.ContainsKey(MetaDataField)))
        {
            return string.Empty;
        }

        var recordHashes = records
            .Select(r => ((r[MetaDataField] as Dictionary<string, object>)?[HashField]?.ToString())!)
            .OrderBy(h => h);

        using var sha256 = System.Security.Cryptography.SHA256.Create();
        var combinedBytes = Encoding.UTF8.GetBytes(string.Join("", recordHashes));
        var hashBytes = sha256.ComputeHash(combinedBytes);
        return Convert.ToBase64String(hashBytes);
    }

}
