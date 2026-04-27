using System;

namespace MatchLogic.Domain.MergeAndSurvivorship
{
    /// <summary>
    /// Defines the operations available for field overwriting
    /// Maps to OverwriteFieldRules.Rules enum from legacy code
    /// </summary>
    public enum OverwriteOperation : byte
    {
        /// <summary>
        /// Select the longest value from the group
        /// </summary>
        Longest = 0,

        /// <summary>
        /// Select the shortest value from the group
        /// </summary>
        Shortest = 1,

        /// <summary>
        /// Select the maximum numeric value from the group
        /// </summary>
        Max = 2,

        /// <summary>
        /// Select the minimum numeric value from the group
        /// </summary>
        Min = 3,

        /// <summary>
        /// Select the most frequently occurring value from the group
        /// </summary>
        MostPopular = 4,

        /// <summary>
        /// Select the value from the master record
        /// </summary>
        FromMaster = 5,

        /// <summary>
        /// Select the value from the record with the best match score
        /// </summary>
        FromBestRecord = 6,

        /// <summary>
        /// Merge all distinct values into one field (comma-separated)
        /// </summary>
        MergeAllValues = 7
    }

    /// <summary>
    /// Extension methods for OverwriteOperation enum
    /// </summary>
    public static class OverwriteOperationExtensions
    {
        public static string ToDisplayString(this OverwriteOperation operation)
        {
            return operation switch
            {
                OverwriteOperation.Longest => "Longest",
                OverwriteOperation.Shortest => "Shortest",
                OverwriteOperation.Max => "Max",
                OverwriteOperation.Min => "Min",
                OverwriteOperation.MostPopular => "Most Popular",
                OverwriteOperation.FromMaster => "From Master",
                OverwriteOperation.FromBestRecord => "From The Best Record",
                OverwriteOperation.MergeAllValues => "Merge All Values In One",
                _ => operation.ToString()
            };
        }

        public static OverwriteOperation FromString(string operationString)
        {
            return operationString?.Trim() switch
            {
                "Longest" => OverwriteOperation.Longest,
                "Shortest" => OverwriteOperation.Shortest,
                "Max" => OverwriteOperation.Max,
                "Min" => OverwriteOperation.Min,
                "Most Popular" => OverwriteOperation.MostPopular,
                "From Master" => OverwriteOperation.FromMaster,
                "From The Best Record" => OverwriteOperation.FromBestRecord,
                "Merge All Values In One" => OverwriteOperation.MergeAllValues,
                _ => OverwriteOperation.Longest
            };
        }
    }
}
