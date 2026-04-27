using MatchLogic.Domain.Export;
using NpgsqlTypes;
using System;

namespace MatchLogic.Infrastructure.Export.Helpers;

/// <summary>
/// Centralized type mapping for database exports.
/// SINGLE SOURCE OF TRUTH for all type decisions, DDL generation, and value conversion.
/// </summary>
public static class ExportTypeMapper
{
    #region SQL Server

    /// <summary>
    /// Get SQL Server DDL type definition.
    /// </summary>
    public static string ToSqlServerType(this ExportColumnInfo col)
    {
        var type = NormalizeDataType(col.DataType);

        return type switch
        {
            "int" or "integer" => "INT",
            "bigint" => "BIGINT",
            "smallint" => "SMALLINT",
            "tinyint" => "TINYINT",
            "bit" or "boolean" or "bool" => "BIT",
            "decimal" or "numeric" => col.Precision.HasValue
                ? $"DECIMAL({col.Precision},{col.Scale ?? 0})"
                : "DECIMAL(18,4)",
            "float" or "double" => "FLOAT",
            "real" => "REAL",
            "money" => "MONEY",
            "smallmoney" => "SMALLMONEY",
            "date" => "DATE",
            "time" => "TIME",
            "datetime" => "DATETIME",
            "datetime2" => "DATETIME2",
            "datetimeoffset" => "DATETIMEOFFSET",
            "timestamp" => "DATETIME2",
            "smalldatetime" => "SMALLDATETIME",
            "uniqueidentifier" or "guid" => "UNIQUEIDENTIFIER",
            "varbinary" => col.Length.HasValue && col.Length > 0 && col.Length <= 8000
                ? $"VARBINARY({col.Length})"
                : "VARBINARY(MAX)",
            "binary" => col.Length.HasValue && col.Length > 0 && col.Length <= 8000
                ? $"BINARY({col.Length})"
                : "VARBINARY(MAX)",
            "image" => "VARBINARY(MAX)",
            "nvarchar" => col.Length.HasValue && col.Length > 0 && col.Length <= 4000
                ? $"NVARCHAR({col.Length})"
                : "NVARCHAR(MAX)",
            "nchar" => col.Length.HasValue && col.Length > 0 && col.Length <= 4000
                ? $"NCHAR({col.Length})"
                : "NVARCHAR(MAX)",
            "varchar" => col.Length.HasValue && col.Length > 0 && col.Length <= 8000
                ? $"VARCHAR({col.Length})"
                : "VARCHAR(MAX)",
            "char" => col.Length.HasValue && col.Length > 0 && col.Length <= 8000
                ? $"CHAR({col.Length})"
                : "VARCHAR(MAX)",
            "text" => "VARCHAR(MAX)",
            "ntext" => "NVARCHAR(MAX)",
            "xml" => "XML",
            _ => "NVARCHAR(MAX)"
        };
    }

    public static object? ConvertForSqlServer(object? value, string? dataType)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        var type = NormalizeDataType(dataType);

        try
        {
            return type switch
            {
                "int" or "integer" => Convert.ToInt32(value),
                "bigint" => Convert.ToInt64(value),
                "smallint" => Convert.ToInt16(value),
                "tinyint" => Convert.ToByte(value),
                "bit" or "boolean" or "bool" => ConvertToBoolean(value),
                "decimal" or "numeric" or "money" or "smallmoney" => Convert.ToDecimal(value),
                "float" or "double" => Convert.ToDouble(value),
                "real" => Convert.ToSingle(value),
                "date" or "datetime" or "datetime2" or "smalldatetime" or "timestamp" => ConvertToDateTime(value),
                "datetimeoffset" => ConvertToDateTimeOffset(value),
                "time" => ConvertToTimeSpan(value),
                "uniqueidentifier" or "guid" => ConvertToGuid(value),
                "varbinary" or "binary" or "image" => ConvertToByteArray(value),
                _ => value.ToString()
            };
        }
        catch
        {
            return value.ToString();
        }
    }

    #endregion

    #region MySQL

    /// <summary>
    /// Get MySQL DDL type definition.
    /// </summary>
    public static string ToMySqlType(this ExportColumnInfo col)
    {
        var type = NormalizeDataType(col.DataType);

        return type switch
        {
            "int" or "integer" => "INT",
            "bigint" => "BIGINT",
            "smallint" => "SMALLINT",
            "tinyint" => "TINYINT",
            "bit" or "boolean" or "bool" => "TINYINT(1)",
            "decimal" or "numeric" => col.Precision.HasValue
                ? $"DECIMAL({col.Precision},{col.Scale ?? 0})"
                : "DECIMAL(18,4)",
            "float" => "FLOAT",
            "double" or "real" => "DOUBLE",
            "money" or "smallmoney" => "DECIMAL(19,4)",
            "date" => "DATE",
            "time" => "TIME",
            "datetime" or "datetime2" or "smalldatetime" => "DATETIME",
            "datetimeoffset" => "DATETIME",
            "timestamp" => "TIMESTAMP",
            "uniqueidentifier" or "guid" => "CHAR(36)",
            "varbinary" or "binary" or "image" => col.Length.HasValue && col.Length > 0 && col.Length <= 65535
                ? $"VARBINARY({col.Length})"
                : "LONGBLOB",
            "nvarchar" or "varchar" or "text" => col.Length.HasValue && col.Length > 0 && col.Length <= 16383
                ? $"VARCHAR({col.Length})"
                : "TEXT",
            "nchar" or "char" => col.Length.HasValue && col.Length > 0 && col.Length <= 255
                ? $"CHAR({col.Length})"
                : "TEXT",
            "ntext" => "LONGTEXT",
            "xml" => "LONGTEXT",
            _ => "TEXT"
        };
    }

    public static object? ConvertForMySql(object? value, string? dataType)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        var type = NormalizeDataType(dataType);

        try
        {
            return type switch
            {
                "int" or "integer" => Convert.ToInt32(value),
                "bigint" => Convert.ToInt64(value),
                "smallint" => Convert.ToInt16(value),
                "tinyint" => Convert.ToByte(value),
                "bit" or "boolean" or "bool" => ConvertToBoolean(value) ? 1 : 0,
                "decimal" or "numeric" or "money" => Convert.ToDecimal(value),
                "float" => Convert.ToSingle(value),
                "double" or "real" => Convert.ToDouble(value),
                "date" or "datetime" or "datetime2" or "timestamp" => ConvertToDateTime(value),
                "uniqueidentifier" or "guid" => ConvertToGuid(value).ToString(),
                "varbinary" or "binary" => ConvertToByteArray(value),
                _ => value.ToString()
            };
        }
        catch
        {
            return value.ToString();
        }
    }

    #endregion

    #region PostgreSQL

    /// <summary>
    /// Get PostgreSQL DDL type definition.
    /// </summary>
    public static string ToPostgresType(this ExportColumnInfo col)
    {
        var type = NormalizeDataType(col.DataType);

        return type switch
        {
            "int" or "integer" => "INTEGER",
            "bigint" => "BIGINT",
            "smallint" or "tinyint" => "SMALLINT",
            "bit" or "boolean" or "bool" => "BOOLEAN",
            "decimal" or "numeric" or "money" or "smallmoney" => col.Precision.HasValue
                ? $"NUMERIC({col.Precision},{col.Scale ?? 0})"
                : "NUMERIC(18,4)",
            "float" or "real" => "REAL",           // CRITICAL: Must match ToNpgsqlDbType
            "double" => "DOUBLE PRECISION",        // CRITICAL: Must match ToNpgsqlDbType
            "date" => "DATE",
            "time" => "TIME",
            "datetime" or "datetime2" or "smalldatetime" or "timestamp" => "TIMESTAMP",
            "datetimeoffset" => "TIMESTAMPTZ",
            "uniqueidentifier" or "guid" => "UUID",
            "varbinary" or "binary" or "image" => "BYTEA",
            "nvarchar" or "varchar" or "text" or "ntext" => col.Length.HasValue && col.Length > 0
                ? $"VARCHAR({col.Length})"
                : "TEXT",
            "nchar" or "char" => col.Length.HasValue && col.Length > 0
                ? $"CHAR({col.Length})"
                : "TEXT",
            "xml" => "XML",
            _ => "TEXT"
        };
    }

    /// <summary>
    /// Maps ExportColumnInfo to NpgsqlDbType for Binary COPY.
    /// CRITICAL: Must align with ToPostgresType() to avoid "22P03: incorrect binary data format".
    /// </summary>
    public static NpgsqlDbType ToNpgsqlDbType(this ExportColumnInfo col)
    {
        var type = NormalizeDataType(col.DataType);

        return type switch
        {
            "int" or "integer" => NpgsqlDbType.Integer,
            "bigint" => NpgsqlDbType.Bigint,
            "smallint" or "tinyint" => NpgsqlDbType.Smallint,
            "bit" or "boolean" or "bool" => NpgsqlDbType.Boolean,
            "decimal" or "numeric" or "money" or "smallmoney" => NpgsqlDbType.Numeric,
            "float" or "real" => NpgsqlDbType.Real,      // REAL (4-byte)
            "double" => NpgsqlDbType.Double,             // DOUBLE PRECISION (8-byte)
            "date" => NpgsqlDbType.Date,
            "time" => NpgsqlDbType.Time,
            "datetime" or "datetime2" or "smalldatetime" or "timestamp" => NpgsqlDbType.Timestamp,
            "datetimeoffset" => NpgsqlDbType.TimestampTz,
            "uniqueidentifier" or "guid" => NpgsqlDbType.Uuid,
            "varbinary" or "binary" or "image" => NpgsqlDbType.Bytea,
            _ => NpgsqlDbType.Text
        };
    }

    public static object? ConvertForPostgres(object? value, NpgsqlDbType npgType)
    {
        if (value == null || value == DBNull.Value)
            return null;

        try
        {
            return npgType switch
            {
                NpgsqlDbType.Integer => Convert.ToInt32(value),
                NpgsqlDbType.Bigint => Convert.ToInt64(value),
                NpgsqlDbType.Smallint => Convert.ToInt16(value),
                NpgsqlDbType.Boolean => ConvertToBoolean(value),
                NpgsqlDbType.Numeric => Convert.ToDecimal(value),
                NpgsqlDbType.Real => Convert.ToSingle(value),
                NpgsqlDbType.Double => Convert.ToDouble(value),
                NpgsqlDbType.Date => DateOnly.FromDateTime(ConvertToDateTime(value)),
                NpgsqlDbType.Time => ConvertToTimeSpan(value),
                NpgsqlDbType.Timestamp => ConvertToDateTime(value),
                NpgsqlDbType.TimestampTz => ConvertToDateTimeOffset(value),
                NpgsqlDbType.Uuid => ConvertToGuid(value),
                NpgsqlDbType.Bytea => ConvertToByteArray(value),
                _ => value.ToString() ?? string.Empty
            };
        }
        catch
        {
            return value.ToString() ?? string.Empty;
        }
    }

    #endregion

    #region CLR Types

    /// <summary>
    /// Get CLR type for DataTable column creation.
    /// </summary>
    public static Type GetClrType(this ExportColumnInfo col)
    {
        var type = NormalizeDataType(col.DataType);

        return type switch
        {
            "int" or "integer" => typeof(int),
            "bigint" => typeof(long),
            "smallint" => typeof(short),
            "tinyint" => typeof(byte),
            "bit" or "boolean" or "bool" => typeof(bool),
            "decimal" or "numeric" or "money" or "smallmoney" => typeof(decimal),
            "float" or "double" => typeof(double),
            "real" => typeof(float),
            "date" or "datetime" or "datetime2" or "smalldatetime" or "timestamp" => typeof(DateTime),
            "datetimeoffset" => typeof(DateTimeOffset),
            "time" => typeof(TimeSpan),
            "uniqueidentifier" or "guid" => typeof(Guid),
            "varbinary" or "binary" or "image" => typeof(byte[]),
            _ => typeof(string)
        };
    }

    #endregion

    #region Shared Helpers

    private static bool ConvertToBoolean(object value) => value switch
    {
        bool b => b,
        int i => i != 0,
        long l => l != 0,
        byte by => by != 0,
        string s => bool.TryParse(s, out var sb) ? sb :
                    s == "1" || s.Equals("true", StringComparison.OrdinalIgnoreCase),
        _ => Convert.ToBoolean(value)
    };

    private static DateTime ConvertToDateTime(object value) => value switch
    {
        DateTime dt => dt,
        DateTimeOffset dto => dto.DateTime,
        DateOnly d => d.ToDateTime(TimeOnly.MinValue),
        string s => DateTime.TryParse(s, out var d) ? d : DateTime.MinValue,
        long ticks => new DateTime(ticks),
        _ => Convert.ToDateTime(value)
    };

    private static DateTimeOffset ConvertToDateTimeOffset(object value) => value switch
    {
        DateTimeOffset dto => dto,
        DateTime dt => new DateTimeOffset(dt),
        string s => DateTimeOffset.TryParse(s, out var d) ? d : DateTimeOffset.MinValue,
        _ => new DateTimeOffset(Convert.ToDateTime(value))
    };

    private static TimeSpan ConvertToTimeSpan(object value) => value switch
    {
        TimeSpan ts => ts,
        DateTime dt => dt.TimeOfDay,
        TimeOnly t => t.ToTimeSpan(),
        string s => TimeSpan.TryParse(s, out var t) ? t : TimeSpan.Zero,
        long ticks => new TimeSpan(ticks),
        _ => TimeSpan.Zero
    };

    private static Guid ConvertToGuid(object value) => value switch
    {
        Guid g => g,
        string s => Guid.TryParse(s, out var g) ? g : Guid.Empty,
        byte[] bytes when bytes.Length == 16 => new Guid(bytes),
        _ => Guid.Empty
    };

    private static byte[] ConvertToByteArray(object value) => value switch
    {
        byte[] bytes => bytes,
        string s when !string.IsNullOrEmpty(s) => Convert.FromBase64String(s),
        _ => Array.Empty<byte>()
    };

    private static string NormalizeDataType(string? dataType)
    {
        if (string.IsNullOrWhiteSpace(dataType))
            return "nvarchar";

        return dataType.Trim().ToLowerInvariant();
    }

    #endregion
}