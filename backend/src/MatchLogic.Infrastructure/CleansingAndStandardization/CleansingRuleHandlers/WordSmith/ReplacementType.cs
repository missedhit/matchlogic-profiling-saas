using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.CleansingAndStandardization.CleansingRuleHandlers.WordSmith
{
    /// <summary>
    /// Represents different types of replacement logic.
    /// </summary>
    public enum ReplacementType
    {
        /// <summary>
        /// Original values are replaced with new values.
        /// </summary>
        Full = 0,
        /// <summary>
        /// Only new column contains replacements.
        /// </summary>
        Flag = 1
    }
}
