using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Domain.DataProfiling
{
    public class ColumnProfile
    {
        public string FieldName { get; set; }
        public string Type { get; set; }
        public int Length { get; set; }
        public string Pattern { get; set; }

        // Count statistics
        public long Total { get; set; }
        public long Valid { get; set; }
        public long Invalid { get; set; }
        public long Filled { get; set; }
        public long Null { get; set; }
        public long Distinct { get; set; }

        // Character counts
        public long Numbers { get; set; }
        public long NumbersOnly { get; set; }
        public long Letters { get; set; }
        public long LettersOnly { get; set; }
        public long NumbersAndLetters { get; set; }
        public long Punctuation { get; set; }
        public long LeadingSpaces { get; set; }
        public long NonPrintableCharacters { get; set; }

        // Min/Max/Mean/Median/Mode
        public string Min { get; set; }
        public string Max { get; set; }
        public string Mean { get; set; }
        public string Median { get; set; }
        public string Mode { get; set; }
        public string Extreme { get; set; }

        // Sample values for UI display
        public List<string> SampleValues { get; set; } = new();

        // Value distribution for categorical fields
        public Dictionary<string, long> ValueDistribution { get; set; }

        public List<PatternInfo> Patterns { get; set; }

        // Store document IDs for each type of row reference
        public Dictionary<ProfileCharacteristic, Guid> CharacteristicRowDocumentIds { get; set; } = new();
        public Dictionary<string, Guid> PatternMatchRowDocumentIds { get; set; } = new();
        public Dictionary<string, Guid> ValueRowDocumentIds { get; set; } = new();
    }

}
