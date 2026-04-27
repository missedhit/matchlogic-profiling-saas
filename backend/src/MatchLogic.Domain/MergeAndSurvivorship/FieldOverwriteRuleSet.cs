using MatchLogic.Domain.Entities.Common;
using System;
using System.Collections.Generic;
using System.Linq;

namespace MatchLogic.Domain.MergeAndSurvivorship
{
    /// <summary>
    /// Represents a collection of rules for field overwriting
    /// </summary>
    public class FieldOverwriteRuleSet : AuditableEntity
    {
        public Guid ProjectId { get; set; }

        public string Name { get; set; }

        public string Description { get; set; }

        public bool IsActive { get; set; }

        public List<FieldOverwriteRule> Rules { get; set; }

        public FieldOverwriteRuleSet()
        {
            Rules = new List<FieldOverwriteRule>();
            IsActive = true;
        }

        public FieldOverwriteRuleSet(Guid projectId) : this()
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
            {
                errors.Add("At least one rule is required");
                return errors.Count == 0;
            }

            // Check for duplicate order values
            var orders = new HashSet<int>();
            foreach (var rule in Rules)
            {
                if (!orders.Add(rule.Order))
                    errors.Add($"Duplicate rule order: {rule.Order}");

                if (!rule.IsValid(out var ruleErrors))
                    errors.AddRange(ruleErrors.Select(e => $"Rule '{rule.LogicalFieldName}': {e}"));
            }

            return errors.Count == 0;
        }

        /// <summary>
        /// Gets active rules sorted by order
        /// </summary>
        public List<FieldOverwriteRule> GetActiveRulesSorted()
        {
            return Rules?
                .Where(r => r.IsActive)
                .OrderBy(r => r.Order)
                .ToList() ?? new List<FieldOverwriteRule>();
        }

        /// <summary>
        /// Adds a rule to the rule set
        /// </summary>
        public void AddRule(FieldOverwriteRule rule)
        {
            if (Rules == null)
                Rules = new List<FieldOverwriteRule>();

            rule.RuleSetId = this.Id;
            Rules.Add(rule);
            // Audit fields set by GenericRepository
        }

        /// <summary>
        /// Removes a rule from the rule set
        /// </summary>
        public bool RemoveRule(Guid ruleId)
        {
            var rule = Rules?.FirstOrDefault(r => r.Id == ruleId);
            if (rule != null)
            {
                Rules.Remove(rule);
                // Audit fields set by GenericRepository
                return true;
            }
            return false;
        }
    }
}
