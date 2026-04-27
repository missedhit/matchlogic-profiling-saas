using System;

namespace MatchLogic.Domain.MergeAndSurvivorship
{
    /// <summary>
    /// Defines conditions for when to apply field overwriting
    /// Maps to OverwriteFieldRules.OverwriteConditions enum from legacy code
    /// </summary>
    public enum OverwriteCondition : byte
    {
        /// <summary>
        /// No condition - always overwrite
        /// </summary>
        NoCondition = 0,

        /// <summary>
        /// Overwrite only if the target field contains an empty value
        /// </summary>
        FieldIsEmpty = 1,

        /// <summary>
        /// Overwrite only if the source field (to overwrite FROM) is empty
        /// </summary>
        OverwriteByEmpty = 2,

        /// <summary>
        /// Overwrite only if the record is selected
        /// </summary>
        RecordIsSelected = 3,

        /// <summary>
        /// Overwrite only if the record is flagged as not duplicate
        /// </summary>
        RecordIsNotDuplicate = 4,

        /// <summary>
        /// Overwrite only if the record is flagged as master
        /// </summary>
        RecordIsMaster = 5
    }

    /// <summary>
    /// Extension methods for OverwriteCondition enum
    /// </summary>
    public static class OverwriteConditionExtensions
    {
        public static string ToDisplayString(this OverwriteCondition condition)
        {
            return condition switch
            {
                OverwriteCondition.NoCondition => "No Condition",
                OverwriteCondition.FieldIsEmpty => "Field Contains Empty Value",
                OverwriteCondition.OverwriteByEmpty => "Field to Overwrite FROM IS Empty",
                OverwriteCondition.RecordIsSelected => "Record is Selected",
                OverwriteCondition.RecordIsNotDuplicate => "Record is Flagged as Not Duplicate",
                OverwriteCondition.RecordIsMaster => "Record is Flagged as Master",
                _ => condition.ToString()
            };
        }

        public static OverwriteCondition FromString(string conditionString)
        {
            return conditionString?.Trim() switch
            {
                "No Condition" => OverwriteCondition.NoCondition,
                "Field Contains Empty Value" => OverwriteCondition.FieldIsEmpty,
                "Field to Overwrite FROM IS Empty" => OverwriteCondition.OverwriteByEmpty,
                "Record is Selected" => OverwriteCondition.RecordIsSelected,
                "Record is Flagged as Not Duplicate" => OverwriteCondition.RecordIsNotDuplicate,
                "Record is Flagged as Master" => OverwriteCondition.RecordIsMaster,
                _ => OverwriteCondition.NoCondition
            };
        }
    }
}
