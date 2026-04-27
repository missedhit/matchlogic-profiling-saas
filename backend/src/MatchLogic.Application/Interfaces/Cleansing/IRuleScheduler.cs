using MatchLogic.Domain.CleansingAndStandaradization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.Cleansing
{
    /// <summary>
    /// Interface for rule scheduler that determines execution order of rules
    /// </summary>
    public interface IRuleScheduler
    {
        /// <summary>
        /// Gets a list of rules to apply to a record in the correct order
        /// </summary>
        List<TransformationRule> GetRulesToApply(Record record);

        /// <summary>
        /// Optimizes the rule execution order based on record schema
        /// </summary>
        void OptimizeForSchema(IEnumerable<string> columnNames);

        /// <summary>
        /// Creates a rule execution plan for a specific set of columns
        /// </summary>
        List<TransformationRule> CreateExecutionPlan(IEnumerable<string> columnNames);
    }

}
