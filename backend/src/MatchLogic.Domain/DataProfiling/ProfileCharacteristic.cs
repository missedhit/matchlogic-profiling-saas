using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Domain.DataProfiling;

public enum ProfileCharacteristic : byte
{
    // Basic value characteristics
    Null = 1,
    Empty = 2,
    Valid = 3,
    Invalid = 4,
    Filled = 5,
    Total = 6,

    // Data types
    Text = 10,
    Numeric = 11,
    DateTime = 12,
    Boolean = 13,

    // Character characteristics
    LettersOnly = 20,
    NumbersOnly = 21,
    Alphanumeric = 22,
    WithPunctuation = 23,
    WithSpecialChars = 24,
    WithLeadingSpaces = 25,
    WithNonPrintable = 26,
    Letters = 27,
    Numbers = 28,

    // Statistical characteristics
    Minimum = 30,
    Maximum = 31,
    DistinctValue = 32,
    Duplicate = 33,

    // Pattern matches
    PatternMatch = 40,
    DictionaryMatch = 41,
    UnclassifiedPattern = 42
}
