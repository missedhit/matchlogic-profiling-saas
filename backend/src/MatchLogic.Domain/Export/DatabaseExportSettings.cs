namespace MatchLogic.Domain.Export;

/// <summary>
/// Parameter keys for database export settings.
/// </summary>
public static class DatabaseExportKeys
{
    public const string TableName = "Db.TableName";
    public const string SchemaName = "Db.SchemaName";
    public const string UseBulkCopy = "Db.UseBulkCopy";
    public const string BatchSize = "Db.BatchSize";
    public const string TruncateExisting = "Db.TruncateExisting";
    public const string DropAndRecreate = "Db.DropAndRecreate";
    public const string CommandTimeout = "Db.CommandTimeout";
    public const string CreateIndexes = "Db.CreateIndexes";
    public const string IndexColumns = "Db.IndexColumns";
    public const string UseTransaction = "Db.UseTransaction";

    public static class Defaults
    {
        public const string SchemaName = "dbo";
        public const bool UseBulkCopy = true;
        public const int BatchSize = 5000;
        public const bool TruncateExisting = false;
        public const bool DropAndRecreate = false;
        public const int CommandTimeout = 300;
        public const bool CreateIndexes = false;
        public const bool UseTransaction = false;
    }
}