namespace MatchLogic.Domain.Entities.Common;

public static class RecordSystemFieldNames
{
    public static readonly string IsMasterRecord = "_isMasterRecord";
    public static readonly string IsMasterRecord_DefaultChanged = "_masterRecordUpdated";

    public static readonly string Selected = "_selected";
    public static readonly string Selected_DefaultChanged = "_selectedUpdated";

    public static readonly string NotDuplicate = "_notDuplicate";
    public static readonly string NotDuplicate_DefaultChanged = "_notDuplicateUpdated";
}
