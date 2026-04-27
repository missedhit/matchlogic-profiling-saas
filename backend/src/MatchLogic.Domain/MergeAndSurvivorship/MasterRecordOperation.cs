namespace MatchLogic.Domain.MergeAndSurvivorship
{
    /// <summary>
    /// Operations available for master record determination
    /// </summary>
    public enum MasterRecordOperation : byte
    {
        /// <summary>
        /// Select record with longest value for the field
        /// </summary>
        Longest = 0,

        /// <summary>
        /// Select record with shortest value for the field
        /// </summary>
        Shortest = 1,

        /// <summary>
        /// Select record with maximum numeric value for the field
        /// </summary>
        Max = 2,

        /// <summary>
        /// Select record with minimum numeric value for the field
        /// </summary>
        Min = 3,

        /// <summary>
        /// Select record with most frequently occurring value (mode)
        /// </summary>
        MostPopular = 4,

        /// <summary>
        /// Prefer records from a specific data source
        /// </summary>
        PreferDataSource = 5,

        /// <summary>
        /// Select first record with non-null/non-empty value
        /// </summary>
        FirstNonNull = 6,

        /// <summary>
        /// Select record with most recent timestamp
        /// </summary>
        MostRecent = 7,

        /// <summary>
        /// User-defined custom logic
        /// </summary>
        Custom = 99
    }
}
