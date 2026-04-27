using MatchLogic.Domain.Entities;
using System;

namespace MatchLogic.Application.Common;

/// <summary>
/// Metadata-only export type resolver that works with domain FieldMappingEx inputs.
/// - Uses only metadata: FieldMappingEx.DataType (string) and FieldMappingEx.Length (int?).
/// - Returns a FieldMappingEx that represents the chosen export column (keeps the model shape).
/// - When possible returns a copy of the chosen input column (so caller can tell "a" or "b" by DataSourceName/DataSourceId).
/// - When a new combined/synthesized export type is required, returns a new FieldMappingEx with DataSourceName set to "{a.DataSourceName}|{b.DataSourceName}".
/// - Always checks for null Length before using it and treats null/<=0 as "unbounded/unknown".
/// - Conservative: if unsure, chooses a string (varchar) with unbounded length to avoid data loss.
/// </summary>
public static class ExportTypeResolver
{
    /// <summary>
    /// Decide which column (a or b) to use as the export column (or produce a synthesized export column)
    /// such that no data is lost based on metadata only.
    /// </summary>
    /// <param name="a">First column metadata (FieldMappingEx)</param>
    /// <param name="b">Second column metadata (FieldMappingEx)</param>
    /// <returns>FieldMappingEx representing export column. DataSourceName/DataSourceId indicate which original column was selected when applicable.</returns>
    public static FieldMappingEx DetermineLosslessExportField(
        FieldMappingEx a,
        FieldMappingEx b)
    {
        if (a is null) throw new ArgumentNullException(nameof(a));
        if (b is null) throw new ArgumentNullException(nameof(b));

        var famA = MapFamily(a.DataType);
        var famB = MapFamily(b.DataType);

        // Same family -> promote within family and prefer the column that already matches the promoted type/size.
        if (famA == famB)
        {
            return PromoteWithinFamilyAndChooseSource(famA, a, b);
        }

        // Mixed families -> apply conservative rules (prefer non-destructive choice, else string)
        // Any + String -> choose string (prefer the string column if it has enough length)
        if (famA == SemanticFamily.String || famB == SemanticFamily.String)
        {
            return ChooseStringExport(a, b);
        }

        // DateTime + non-DateTime -> string
        if (famA == SemanticFamily.DateTime || famB == SemanticFamily.DateTime)
        {
            return ChooseStringExport(a, b);
        }

        // Boolean + anything -> string (safe)
        if (famA == SemanticFamily.Boolean || famB == SemanticFamily.Boolean)
        {
            return ChooseStringExport(a, b);
        }

        // Binary + non-binary -> string (safe). If both binary, promote binary.
        if (famA == SemanticFamily.Binary || famB == SemanticFamily.Binary)
        {
            if (famA == SemanticFamily.Binary && famB == SemanticFamily.Binary)
                return PromoteBinaryAndChoose(a, b);

            return ChooseStringExport(a, b);
        }

        // Integer + Decimal -> decimal
        if (IsEither(famA, famB, SemanticFamily.Integer, SemanticFamily.Decimal))
        {
            return SynthesizeDecimalExport(a, b);
        }

        // Integer + Floating -> decimal (more conservative than float)
        if (IsEither(famA, famB, SemanticFamily.Integer, SemanticFamily.Floating))
        {
            return SynthesizeDecimalExport(a, b);
        }

        // Floating + Decimal -> decimal
        if (IsEither(famA, famB, SemanticFamily.Floating, SemanticFamily.Decimal))
        {
            return SynthesizeDecimalExport(a, b);
        }

        // Default: conservative string
        return ChooseStringExport(a, b);
    }

    #region Family promotions (choose source where appropriate)

    private static FieldMappingEx PromoteWithinFamilyAndChooseSource(
        SemanticFamily family,
        FieldMappingEx a,
        FieldMappingEx b)
    {
        switch (family)
        {
            case SemanticFamily.Integer:
                return PromoteIntegerIntegerAndChoose(a, b);

            case SemanticFamily.Decimal:
                // No precision/scale metadata available: synthesize decimal export
                return SynthesizeDecimalExport(a, b);

            case SemanticFamily.Floating:
                // Prefer double; no precision metadata - synthesize double
                return SynthesizeTypedExport("double", null, a, b);

            case SemanticFamily.DateTime:
                // prefer timestamp-like; else datetime; if both date-only pick date
                if (IsTimestampLike(a.DataType) || IsTimestampLike(b.DataType))
                    return SynthesizeTypedExport("timestamp", null, a, b);

                if (IsDateOnly(a.DataType) && IsDateOnly(b.DataType))
                    return SynthesizeTypedExport("date", null, a, b);

                // choose the datetime-like column if present
                if (IsDateLike(a.DataType) && !IsDateLike(b.DataType)) return CloneForExport(a, "datetime");
                if (IsDateLike(b.DataType) && !IsDateLike(a.DataType)) return CloneForExport(b, "datetime");

                return SynthesizeTypedExport("datetime", null, a, b);

            case SemanticFamily.String:
                return PromoteStringStringAndChoose(a, b);

            case SemanticFamily.Binary:
                return PromoteBinaryAndChoose(a, b);

            case SemanticFamily.Boolean:
                // both boolean -> choose either (prefer a)
                return CloneForExport(a, "bit");

            default:
                return ChooseStringExport(a, b);
        }
    }

    private static FieldMappingEx PromoteIntegerIntegerAndChoose(
        FieldMappingEx a,
        FieldMappingEx b)
    {
        // Determine sizes from DataType names where possible (bigint/int/smallint/tinyint)
        var sizeA = MapIntegerSizeFromName(a.DataType);
        var sizeB = MapIntegerSizeFromName(b.DataType);

        // If either is bigint -> choose that one as export (preserves range)
        if (sizeA == "bigint" || sizeB == "bigint")
        {
            return sizeA == "bigint" ? CloneForExport(a, "bigint") : CloneForExport(b, "bigint");
        }

        // If either is int -> choose int
        if (sizeA == "int" || sizeB == "int")
        {
            return sizeA == "int" ? CloneForExport(a, "int") : CloneForExport(b, "int");
        }

        // both smallint -> smallint
        if (sizeA == "smallint" && sizeB == "smallint")
        {
            return CloneForExport(a, "smallint");
        }

        // fallback to bigint conservative
        return SynthesizeTypedExport("bigint", null, a, b);
    }

    private static FieldMappingEx PromoteStringStringAndChoose(
        FieldMappingEx a,
        FieldMappingEx b)
    {
        var lenA = NormalizeLength(a.Length);
        var lenB = NormalizeLength(b.Length);

        // If either is unbounded/unknown -> prefer that string column (preserve)
        if (!lenA.HasValue || !lenB.HasValue)
        {
            // Prefer nvarchar if either looks unicode
            var chosen = (IsNVarCharLike(a.DataType) || !lenB.HasValue) ? a : b;
            var exportType = IsNVarCharLike(a.DataType) || IsNVarCharLike(b.DataType) ? "nvarchar" : "varchar";
            return CloneForExport(chosen, exportType, null);
        }

        // Both bounded -> choose the one with larger length but set length to max(lengths)
        if (lenA.Value >= lenB.Value)
        {
            var chosen = CloneForExport(a, IsNVarCharLike(a.DataType) || IsNVarCharLike(b.DataType) ? "nvarchar" : "varchar", Math.Max(lenA.Value, lenB.Value));
            return chosen;
        }
        else
        {
            var chosen = CloneForExport(b, IsNVarCharLike(a.DataType) || IsNVarCharLike(b.DataType) ? "nvarchar" : "varchar", Math.Max(lenA.Value, lenB.Value));
            return chosen;
        }
    }

    private static FieldMappingEx PromoteBinaryAndChoose(
        FieldMappingEx a,
        FieldMappingEx b)
    {
        var lenA = NormalizeLength(a.Length);
        var lenB = NormalizeLength(b.Length);

        if (!lenA.HasValue || !lenB.HasValue)
        {
            // at least one unbounded -> choose that one
            var chosen = !lenA.HasValue ? a : b;
            return CloneForExport(chosen, "varbinary", null);
        }

        // both bounded -> choose larger and set Length to max
        if (lenA.Value >= lenB.Value) return CloneForExport(a, "varbinary", Math.Max(lenA.Value, lenB.Value));
        return CloneForExport(b, "varbinary", Math.Max(lenA.Value, lenB.Value));
    }

    #endregion

    #region Synthesized outputs (when no single source suffices)

    private static FieldMappingEx SynthesizeDecimalExport(
        FieldMappingEx a,
        FieldMappingEx b)
    {
        // We do not have precision/scale metadata on FieldMappingEx.
        // Create a synthesized decimal export column to avoid any loss.
        return SynthesizeTypedExport("decimal", null, a, b);
    }

    /// <summary>
    /// Create a synthesized export column with chosen DataType and optional Length.
    /// DataSourceName is combined to indicate derived-from-both.
    /// </summary>
    private static FieldMappingEx SynthesizeTypedExport(
        string exportDataType,
        int? length,
        FieldMappingEx a,
        FieldMappingEx b)
    {
        return new FieldMappingEx
        {
            FieldName = a.FieldName ?? b.FieldName,
            DataSourceId = Guid.Empty,
            DataSourceName = $"{a.DataSourceName}|{b.DataSourceName}",
            DataType = exportDataType,
            Length = length,
            FieldIndex = -1,
            Mapped = false
        };
    }

    #endregion

    #region Helpers - cloning / choose string / normalization

    private static FieldMappingEx ChooseStringExport(
        FieldMappingEx a,
        FieldMappingEx b)
    {
        // Prefer a string column if either column is already string (and has sufficient length).
        var lenA = NormalizeLength(a.Length);
        var lenB = NormalizeLength(b.Length);

        // If either is string family, choose string promotion accordingly
        if (MapFamily(a.DataType) == SemanticFamily.String && MapFamily(b.DataType) == SemanticFamily.String)
        {
            return PromoteStringStringAndChoose(a, b);
        }

        // If one is string and the other isn't, pick the string column as template and increase its length if needed.
        if (MapFamily(a.DataType) == SemanticFamily.String)
        {
            if (!lenA.HasValue || !lenB.HasValue)
                return CloneForExport(a, IsNVarCharLike(a.DataType) ? "nvarchar" : "varchar", null);

            var l = Math.Max(lenA.Value, lenB.Value);
            return CloneForExport(a, IsNVarCharLike(a.DataType) ? "nvarchar" : "varchar", l);
        }

        if (MapFamily(b.DataType) == SemanticFamily.String)
        {
            if (!lenA.HasValue || !lenB.HasValue)
                return CloneForExport(b, IsNVarCharLike(b.DataType) ? "nvarchar" : "varchar", null);

            var l = Math.Max(lenA.Value, lenB.Value);
            return CloneForExport(b, IsNVarCharLike(b.DataType) ? "nvarchar" : "varchar", l);
        }

        // Neither is string -> synthesize varchar with max/unbounded length to be safe
        return SynthesizeTypedExport("varchar", null, a, b);
    }

    /// <summary>
    /// Create a copy of the chosen source column and set DataType/Length for export.
    /// Using a copy ensures caller can inspect DataSourceName/DataSourceId to know which original was selected.
    /// </summary>
    private static FieldMappingEx CloneForExport(
        FieldMappingEx source,
        string exportDataType,
        int? length = null)
    {
        if (source is null) throw new ArgumentNullException(nameof(source));

        return new FieldMappingEx
        {
            FieldName = source.FieldName,
            DataSourceId = source.DataSourceId,
            DataSourceName = source.DataSourceName,
            DataType = exportDataType,
            Length = length,
            FieldIndex = source.FieldIndex,
            Mapped = source.Mapped
        };
    }

    #endregion

    #region Type-family mapping helpers

    private enum SemanticFamily
    {
        Unknown,
        Integer,
        Floating,
        Decimal,
        DateTime,
        Boolean,
        String,
        Binary
    }

    private static SemanticFamily MapFamily(string raw)
    {
        if (string.IsNullOrWhiteSpace(raw)) return SemanticFamily.Unknown;
        var t = raw.Trim().ToLowerInvariant();

        // Integer family
        if (t.Contains("tinyint") || t.Contains("smallint") || t == "int" || t.Contains("integer") || t.Contains("bigint") || t.EndsWith("int"))
            return SemanticFamily.Integer;

        // Decimal / numeric
        if (t.Contains("decimal") || t.Contains("numeric") || t == "money" || t == "smallmoney")
            return SemanticFamily.Decimal;

        // Floating
        if (t == "float" || t == "real" || t == "double" || t.Contains("double"))
            return SemanticFamily.Floating;

        // Boolean
        if (t == "bit" || t == "boolean" || t == "bool")
            return SemanticFamily.Boolean;

        // Date/time
        if (t.Contains("date") || t.Contains("time") || t.Contains("timestamp") || t.Contains("datetime"))
            return SemanticFamily.DateTime;

        // Binary
        if (t.Contains("binary") || t.Contains("varbinary") || t == "image" || t == "blob")
            return SemanticFamily.Binary;

        // String/text/char types
        if (t.Contains("char") || t.Contains("text") || t.Contains("nchar") || t.Contains("nvarchar") || t.Contains("varchar") || t.Contains("ntext"))
            return SemanticFamily.String;

        return SemanticFamily.Unknown;
    }

    private static bool IsTimestampLike(string raw)
    {
        if (string.IsNullOrWhiteSpace(raw)) return false;
        var t = raw.Trim().ToLowerInvariant();
        return t.Contains("timestamp") || t.Contains("datetime2") || t.Contains("datetimeoffset");
    }

    private static bool IsDateOnly(string raw)
    {
        if (string.IsNullOrWhiteSpace(raw)) return false;
        var t = raw.Trim().ToLowerInvariant();
        return t == "date";
    }

    private static bool IsDateLike(string raw)
    {
        if (string.IsNullOrWhiteSpace(raw)) return false;
        var t = raw.Trim().ToLowerInvariant();
        return t.Contains("date") || t.Contains("time") || t.Contains("datetime");
    }

    private static bool IsNVarCharLike(string raw)
    {
        if (string.IsNullOrWhiteSpace(raw)) return false;
        var t = raw.Trim().ToLowerInvariant();
        return t.Contains("nvar") || t.Contains("nvarchar") || t.Contains("nchar") || t.Contains("ntext");
    }

    private static int? NormalizeLength(int? length)
    {
        if (!length.HasValue) return null;
        if (length.Value <= 0) return null; // treat non-positive as unbounded/unknown
        return length.Value;
    }

    private static bool IsEither(SemanticFamily a, SemanticFamily b, SemanticFamily x, SemanticFamily y)
        => (a == x && b == y) || (a == y && b == x);

    private static string MapIntegerSizeFromName(string raw)
    {
        if (string.IsNullOrWhiteSpace(raw)) return "bigint";
        var t = raw.Trim().ToLowerInvariant();
        if (t.Contains("bigint")) return "bigint";
        if (t.Contains("int") && !t.Contains("tinyint") && !t.Contains("smallint")) return "int";
        if (t.Contains("smallint")) return "smallint";
        if (t.Contains("tinyint")) return "tinyint";
        return "bigint";
    }

    #endregion
}