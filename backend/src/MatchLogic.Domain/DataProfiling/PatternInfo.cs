using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Domain.DataProfiling
{
    public class PatternInfo
    {
        /// <summary>
        /// Name of the pattern
        /// </summary>
        public string Pattern { get; set; }

        /// <summary>
        /// Description of the pattern
        /// </summary>
        public string Description { get; set; }

        /// <summary>
        /// Number of values matching this pattern
        /// </summary>
        public long Count { get; set; }

        /// <summary>
        /// Percentage of values matching this pattern (0-100)
        /// </summary>
        public double MatchPercentage { get; set; }
    }
}
