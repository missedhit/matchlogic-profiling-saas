using MatchLogic.Domain.Entities.Common;
using System;
using System.Collections.Generic;

namespace MatchLogic.Domain.MergeAndSurvivorship
{
    /// <summary>
    /// Represents a single rule for master record determination
    /// </summary>
    public class MasterRecordRule : IEntity
    {
        public Guid RuleSetId { get; set; }
        public int Order { get; set; }
        public string LogicalFieldName { get; set; }
        public MasterRecordOperation Operation { get; set; }
        public Guid? PreferredDataSourceId { get; set; }
        public bool IsActive { get; set; }
        public Dictionary<string, object> OperationConfig { get; set; }

        /// <summary>
        /// Selected data sources for "Take data from" checkbox filtering.
        /// When populated, only records from these data sources are considered.
        /// </summary>
        public List<Guid> SelectedDataSourceIds { get; set; } = new();

        public MasterRecordRule()
        {
            IsActive = true;
            OperationConfig = new Dictionary<string, object>();
        }

        public MasterRecordRule(
            Guid ruleSetId, 
            int order, 
            string logicalFieldName, 
            MasterRecordOperation operation) : this()
        {
            RuleSetId = ruleSetId;
            Order = order;
            LogicalFieldName = logicalFieldName;
            Operation = operation;
        }

        /// <summary>
        /// Validates the rule configuration
        /// </summary>
        public bool IsValid(out List<string> errors)
        {
            errors = new List<string>();

            if (RuleSetId == Guid.Empty)
                errors.Add("RuleSetId is required");

            if (Order < 0)
                errors.Add("Order must be non-negative");

            if (string.IsNullOrWhiteSpace(LogicalFieldName))
                errors.Add("LogicalFieldName is required");

            // Validate operation-specific requirements
            if (Operation == MasterRecordOperation.PreferDataSource && 
                PreferredDataSourceId == null)
            {
                errors.Add("PreferredDataSourceId is required for PreferDataSource operation");
            }

            if (Operation == MasterRecordOperation.MostRecent)
            {
                // MostRecent expects the field to contain date/time values
                // This validation could be enhanced based on field metadata
            }

            return errors.Count == 0;
        }

        /// <summary>
        /// Gets a configuration value
        /// </summary>
        public T GetConfigValue<T>(string key, T defaultValue = default)
        {
            if (OperationConfig != null && OperationConfig.TryGetValue(key, out var value))
            {
                try
                {
                    return (T)Convert.ChangeType(value, typeof(T));
                }
                catch
                {
                    return defaultValue;
                }
            }
            return defaultValue;
        }

        /// <summary>
        /// Sets a configuration value
        /// </summary>
        public void SetConfigValue(string key, object value)
        {
            if (OperationConfig == null)
                OperationConfig = new Dictionary<string, object>();
            
            OperationConfig[key] = value;
        }
    }
}
