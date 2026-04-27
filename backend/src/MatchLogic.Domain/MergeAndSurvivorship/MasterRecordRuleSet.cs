using MatchLogic.Domain.Entities.Common;
using System;
using System.Collections.Generic;
using System.Linq;

namespace MatchLogic.Domain.MergeAndSurvivorship
{
    /// <summary>
    /// Represents a collection of rules for determining the master record in a group
    /// </summary>
    public class MasterRecordRuleSet : AuditableEntity
    {      
        public Guid ProjectId { get; set; }
        public bool IsActive { get; set; }
        public List<MasterRecordRule> Rules { get; set; }

        public MasterRecordRuleSet()
        {
            Rules = new List<MasterRecordRule>();
            IsActive = true;
        }

        public MasterRecordRuleSet(Guid projectId) : this()
        {
            ProjectId = projectId;
        }

        /// <summary>
        /// Validates the rule set configuration
        /// </summary>
        public bool IsValid(out List<string> errors)
        {
            errors = new List<string>();

            if (ProjectId == Guid.Empty)
                errors.Add("ProjectId is required");

            if (Rules == null || Rules.Count == 0)
                errors.Add("At least one rule is required");

            // Check for duplicate order values
            var orders = new HashSet<int>();
            foreach (var rule in Rules ?? new List<MasterRecordRule>())
            {
                if (!orders.Add(rule.Order))
                    errors.Add($"Duplicate rule order: {rule.Order}");

                if (!rule.IsValid(out var ruleErrors))
                    errors.AddRange(ruleErrors);
            }

            return errors.Count == 0;
        }

        /// <summary>
        /// Gets active rules sorted by order
        /// </summary>
        public List<MasterRecordRule> GetActiveRulesSorted()
        {
            return Rules?
                .Where(r => r.IsActive)
                .OrderBy(r => r.Order)
                .ToList() ?? new List<MasterRecordRule>();
        }
    }
}
