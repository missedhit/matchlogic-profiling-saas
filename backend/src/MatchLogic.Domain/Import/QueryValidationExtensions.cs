using System.Text.RegularExpressions;

namespace MatchLogic.Domain.Import;

public static class QueryValidationExtensions
{

    /// <summary>
    /// Validates a custom SQL query to ensure it only contains SELECT statements and does not allow dangerous operations.
    /// Prevents SQL injection by excluding queries containing CREATE, ALTER, DROP, BACKUP, INSERT, UPDATE, DELETE, EXEC, or other non-SELECT commands.
    /// </summary>
    /// <param name="dbConnectionInfo">The database connection info object.</param>
    /// <returns>True if the query is a safe SELECT statement; otherwise, false.</returns>
    public static bool IsValidSelectQuery(this IDBConnectionInfo dbConnectionInfo)
    {
        var query = dbConnectionInfo?.Query;
        if (string.IsNullOrWhiteSpace(query))
            return false;

        // Remove leading/trailing whitespace and normalize case
        var trimmedQuery = query.Trim();

        // Only allow queries that start with SELECT (optionally with comments/whitespace before)
        // Disallow any semicolon-separated statements (to prevent stacked queries)
        // Disallow keywords for DDL/DML/administrative commands
        var forbiddenKeywords = new[]
        {
            "CREATE", "ALTER", "DROP", "BACKUP", "INSERT", "UPDATE", "DELETE", "EXEC", "MERGE", "TRUNCATE", "GRANT", "REVOKE", "CALL", "REPLACE"
        };

        // Check for forbidden keywords (case-insensitive, word boundaries)
        foreach (var keyword in forbiddenKeywords)
        {
            if (System.Text.RegularExpressions.Regex.IsMatch(trimmedQuery, $@"\b{keyword}\b", RegexOptions.IgnoreCase))
                return false;
        }

        // Allow only SELECT as the first non-comment statement
        // Remove SQL comments (single-line and multi-line)
        var noComments = System.Text.RegularExpressions.Regex.Replace(trimmedQuery, @"(--[^\r\n]*|/\*.*?\*/)", "", RegexOptions.Singleline);

        // Check if the first non-whitespace word is SELECT
        var match = System.Text.RegularExpressions.Regex.Match(noComments, @"^\s*SELECT\b", RegexOptions.IgnoreCase);
        if (!match.Success)
            return false;

        // Disallow multiple statements separated by semicolons
        if (noComments.Trim().Split(';').Length > 1)
            return false;

        return true;
    }
}



