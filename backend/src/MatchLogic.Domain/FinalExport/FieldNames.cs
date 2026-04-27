namespace MatchLogic.Domain.FinalExport;

/// <summary>
/// System field names as stored in group records (lowercase as per actual data)
/// </summary>
public static class RecordSystemFieldNames
{
    public const string IsMasterRecord = "_isMasterRecord";
    public const string MasterRecordUpdated = "_masterRecordUpdated";
    public const string Selected = "_selected";
    public const string SelectedUpdated = "_selectedUpdated";
    public const string NotDuplicate = "_notDuplicate";
    public const string NotDuplicateUpdated = "_notDuplicateUpdated";
    public const string Metadata = "_metadata";
    public const string GroupMatchScores = "_group_match_scores";
    public const string GroupMatchDetails = "_group_match_details";
    public const string GroupDegree = "_group_degree";
    public const string GroupAvgScore = "_group_avg_score";
}

/// <summary>
/// Export output column names
/// </summary>
public static class ExportFieldNames
{
    public const string GroupId = "GroupId";
    public const string DataSource = "DataSource";
    public const string DataSourceName = "DataSourceName";
    public const string Record = "Record";
    public const string Master = "Master";
    public const string Selected = "Selected";
    public const string NotDuplicate = "NotDuplicate";
    public const string MdHits = "MDs";
    public const string IsDuplicate = "IsDuplicate";
}

public static class MetadataFieldNames
{
    public const string RowNumber = "RowNumber";
    public const string DataSourceId = "DataSourceId";
}