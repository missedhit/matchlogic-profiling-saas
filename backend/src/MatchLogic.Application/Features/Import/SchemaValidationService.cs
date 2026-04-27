using MatchLogic.Application.Interfaces.Import;
using MatchLogic.Domain.Project;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using RegexEx = System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.Import;

public class SchemaValidationService : ISchemaValidationService
{
    public string ComputeSignature(IEnumerable<string> headers, SchemaPolicy policy)
    {
        var normalised = headers.Select(NormaliseHeader).Where(h => h.Length > 0).ToList();

        var payload = policy == SchemaPolicy.StrictExactMatch
            ? string.Join("|", normalised)
            : string.Join("|", normalised.OrderBy(x => x, StringComparer.Ordinal));

        using var sha = SHA256.Create();
        var hash = sha.ComputeHash(Encoding.UTF8.GetBytes(payload));
        return Convert.ToHexString(hash);
    }

    public void ValidateHeadersAgainstDataSource(DataSource dataSource, IEnumerable<string> headers)
    {
        var headerList = headers?.ToList() ?? new List<string>();
        if (headerList.Count == 0)
            throw new InvalidOperationException("Refresh rejected: no headers found in uploaded file.");

        // Required columns from mappings must exist (prevents “garbage but similar signature” scenarios)
        EnsureRequiredColumnsPresent(dataSource, headerList);

        var newSig = ComputeSignature(headerList, dataSource.SchemaPolicy);

        // Legacy datasource: first time set
        if (string.IsNullOrWhiteSpace(dataSource.SchemaSignature))
        {
            dataSource.SchemaSignature = newSig;
            return;
        }

        if (dataSource.SchemaPolicy == SchemaPolicy.ReorderInsensitive_NameSensitive ||
            dataSource.SchemaPolicy == SchemaPolicy.StrictExactMatch)
        {
            if (!string.Equals(dataSource.SchemaSignature, newSig, StringComparison.Ordinal))
                throw new InvalidOperationException("Refresh rejected: schema mismatch (column names differ).");
        }

        // AllowAdditiveColumns: rely on required-columns check (and optionally subset logic later)
    }

    private static void EnsureRequiredColumnsPresent(DataSource ds, List<string> headers)
    {
        var mappings = ds.Configuration?.ColumnMappings;
        if (mappings == null || mappings.Count == 0)
            return;

        var headerSet = new HashSet<string>(headers.Select(NormaliseHeader), StringComparer.OrdinalIgnoreCase);

        var required = mappings
            .Where(kvp => kvp.Value?.Include == true)
            .Select(kvp => kvp.Key)                 // keys are source columns in your current model
            .Where(k => !string.IsNullOrWhiteSpace(k))
            .Select(NormaliseHeader)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        var missing = required.Where(r => !headerSet.Contains(r)).ToList();
        if (missing.Count > 0)
            throw new InvalidOperationException($"Refresh rejected: missing required columns: {string.Join(", ", missing)}");
    }

    private static string NormaliseHeader(string s)
    {
        if (string.IsNullOrWhiteSpace(s)) return "";
        var t = s.Trim();
        
        t = RegexEx.Regex.Replace(t, @"\s+", " ");
        return t.ToLowerInvariant();
    }
}
