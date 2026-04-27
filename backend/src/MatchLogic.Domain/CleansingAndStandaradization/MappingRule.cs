using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Domain.CleansingAndStandaradization
{
    /// <summary>
    /// Types of mapping operations that can be performed
    /// </summary>
    public enum MappingOperationType : byte
    {
        /// <summary>
        /// Analyze text using WordSmith
        /// </summary>
        WordSmith = 1,

        /// <summary>
        /// Extract data using regular expressions
        /// </summary>
        RegexPattern = 2,

        /// <summary>
        /// Parse an address into components
        /// </summary>
        AddressParser = 3,

        /// <summary>
        /// Parse a full name into components
        /// </summary>
        FullNameParser = 4,

        /// <summary>
        /// Extract first name from a full name
        /// </summary>
        FirstNameExtractor = 5,

        /// <summary>
        /// Extract last name from a full name
        /// </summary>
        LastNameExtractor = 6,

        /// <summary>
        /// Parse an email address
        /// </summary>
        EmailParser = 7,

        /// <summary>
        /// Parse a phone number
        /// </summary>
        PhoneNumberParser = 8,
        /// <summary>
        /// Zip code
        /// </summary>
        Zip = 9,

        MergeFields
    }

    /// <summary>
    /// Represents a rule for mapping data from one column to multiple output columns
    /// </summary>
    public class MappingRule
    {
        /// <summary>
        /// Unique identifier for this rule
        /// </summary>
        public Guid Id { get; set; } = Guid.NewGuid();

        /// <summary>
        /// Type of mapping operation to perform
        /// </summary>
        public MappingOperationType OperationType { get; set; }

        /// <summary>
        /// Source column to map from
        /// </summary>
        public List<string> SourceColumn { get; set; }

        /// <summary>
        /// Configuration for the mapping operation
        /// </summary>
        public Dictionary<string, string> MappingConfig { get; set; } = new Dictionary<string, string>();

        /// <summary>
        /// List of output column names
        /// </summary>
        public List<string> OutputColumns { get; set; } = new List<string>();
        
        /// Creates a new instance of the MappingRule class
        /// </summary>
        public MappingRule()
        {
        }

        /// <summary>
        /// Creates a clone of this MappingRule
        /// </summary>
        public MappingRule Clone()
        {
            var clone = new MappingRule
            {
                Id = Guid.NewGuid(), // Generate a new ID for the clone
                OperationType = OperationType,
                SourceColumn = SourceColumn,                
                
            };

            // Clone the mapping configuration
            foreach (var kvp in MappingConfig)
            {
                clone.MappingConfig[kvp.Key] = kvp.Value;
            }

            // Clone the output columns
            clone.OutputColumns.AddRange(OutputColumns);

            return clone;
        }

        /// <summary>
        /// Returns a string representation of this MappingRule
        /// </summary>
        public override string ToString()
        {
            return $"MappingRule: {OperationType} on {SourceColumn} (ID: {Id})";
        }
    }
}
