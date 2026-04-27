using MatchLogic.Domain.CleansingAndStandaradization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.CleansingAndStandardization.CleansingRuleHandlers.RegexRule
{
    /// <summary>
    /// Interface for regex processing strategies
    /// </summary>
    public interface IRegexProcessingStrategy
    {
        /// <summary>
        /// Process a regex match and update the record
        /// </summary>
        void ProcessMatch(Record record, Match match);
    }

    /// <summary>
    /// Basic regex processing that simply extracts matched groups
    /// </summary>
    public class BasicRegexProcessingStrategy : IRegexProcessingStrategy
    {        
        private readonly List<string> _outputColumns;

        public BasicRegexProcessingStrategy(List<string> outputColumns)
        {
            _outputColumns = outputColumns;
        }

        public void ProcessMatch(Record record, Match match)
        {
            if (_outputColumns == null || _outputColumns.Count == 0)
                return;

            // Map each output column directly to the group with the same name
            for (int i = 0; i < _outputColumns.Count; i++)
            {
                var outputColumn = _outputColumns[i];
                record.AddColumn(outputColumn, GetGroupValue(match, outputColumn, i + 1));
            }
        }
        // Helper method to get a group value based on name or position
        private string GetGroupValue(Match match, string groupName, int fallbackIndex)
        {
            // Try to get the group value directly by name
            var group = match.Groups[groupName];
            if (group.Success)
            {
                return group.Value;
            }
            // Fall back to position-based mapping if group by name doesn't exist
            else if (fallbackIndex < match.Groups.Count)
            {
                return match.Groups[fallbackIndex].Value;
            }
            return null;
        }
    }

    /// <summary>
    /// Advanced regex processing with custom column mapping and formatting
    /// </summary>
    public class AdvancedRegexProcessingStrategy : IRegexProcessingStrategy
    {
        private readonly List<string> _outputColumns;
        private readonly Dictionary<int, int> _columnMapping;
        private readonly Dictionary<int, string> _namedGroupMapping;
        private readonly string _outputFormat;

        public AdvancedRegexProcessingStrategy(
            List<string> outputColumns,
            Dictionary<int, int> columnMapping,
            Dictionary<int, string> namedGroupMapping,
            string outputFormat)
        {
            _outputColumns = outputColumns;
            _columnMapping = columnMapping ?? new Dictionary<int, int>();
            _namedGroupMapping = namedGroupMapping ?? new Dictionary<int, string>();
            _outputFormat = outputFormat;
        }

        public void ProcessMatch(Record record, Match match)
        {
            if (_outputColumns == null || _outputColumns.Count == 0)
                return;

            // Check if we have an output format - prioritize this over individual mappings
            if (!string.IsNullOrEmpty(_outputFormat))
            {
                // Apply the format to get a properly formatted value
                string formattedValue = ApplyOutputFormat(match);

                // If we only have one output column, apply the formatted value to it
                if (_outputColumns.Count == 1)
                {
                    record.AddColumn(_outputColumns[0], formattedValue);
                    return;
                }
            }

            // If no format specified or we have multiple output columns, proceed with individual mappings
            for (int i = 0; i < _outputColumns.Count; i++)
            {
                record.AddColumn(_outputColumns[i], GetGroupValue(match, i));
            }
        }

        // Helper method to get a group value based on mappings
        private string GetGroupValue(Match match, int outputIndex)
        {
            // Check if we have a named group mapping for this output
            if (_namedGroupMapping != null && _namedGroupMapping.TryGetValue(outputIndex, out var groupName))
            {
                var group = match.Groups[groupName];
                if (group.Success)
                {
                    return group.Value;
                }
            }
            // Check if we have a column mapping for this output
            else if (_columnMapping != null && _columnMapping.TryGetValue(outputIndex, out int groupIndex))
            {
                if (groupIndex < match.Groups.Count)
                {
                    return match.Groups[groupIndex].Value;
                }
            }
            // Default to sequential mapping
            else if (outputIndex + 1 < match.Groups.Count)
            {
                return match.Groups[outputIndex + 1].Value;
            }

            return null;
        }
        private string ApplyOutputFormat(Match match)
        {
            if (string.IsNullOrEmpty(_outputFormat))
                return null;

            var builder = new StringBuilder(_outputFormat);
            for (int i = 0; i < match.Groups.Count; i++)
            {
                builder.Replace($"${i}", match.Groups[i].Value);
            }
            return builder.ToString();
        }
    }
}
