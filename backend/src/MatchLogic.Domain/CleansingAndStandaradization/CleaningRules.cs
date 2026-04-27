using MatchLogic.Domain.Entities.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Domain.CleansingAndStandaradization;
public class CleaningRules : IEntity
{    
    public Guid ProjectId { get; set; }
    public Guid ProjectRunId { get; set; }
    public Guid DataSourceId { get; set; }

    // will contain Cleaning Rules against each 
    public  List<CleaningRule> Rules { get; set; } = new();
}

public class CleaningRule : IEntity
{
    /// <summary>
    /// Creates a new instance of the CleaningRule class
    /// </summary>
    public CleaningRule()
    {
    }

    /// <summary>
    /// Creates a new instance of the CleaningRule class with the specified column and rule type
    /// </summary>
    public CleaningRule(string columnName, CleaningRuleType ruleType)
    {
        ColumnName = columnName;
        RuleType = ruleType;
    }    
    public string ColumnName { get; set; }
    public CleaningRuleType RuleType { get; set; }
    public Dictionary<string, string> Arguments { get; set; } = new();
    
    /// <summary>
    /// Creates a clone of this CleaningRule
    /// </summary>
    public CleaningRule Clone()
    {
        var clone = new CleaningRule
        {
            Id = Guid.NewGuid(), // Generate a new ID for the clone
            ColumnName = ColumnName,
            RuleType = RuleType,            
        };

        // Clone the arguments
        foreach (var kvp in Arguments)
        {
            clone.Arguments[kvp.Key] = kvp.Value;
        }

        return clone;
    }

    /// <summary>
    /// Returns a string representation of this CleaningRule
    /// </summary>
    public override string ToString()
    {
        return $"CleaningRule: {RuleType} on {ColumnName} (ID: {Id})";
    }
}

public class ProperCaseOptions : IEntity
{
    public string Delimiters { get; set; } = " .-'";

    /// <summary>
    /// When FALSE: Check exceptions with case-insensitive matching
    /// When TRUE: Skip exception checking entirely
    /// </summary>
    public bool IgnoreCaseOnExceptions { get; set; } = false;

    public List<string> Exceptions { get; set; } = new List<string>();

    public ActionOnException ActionOnException { get; set; } = ActionOnException.ConvertToUpper;

    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public DateTime UpdatedAt { get; set; } = DateTime.UtcNow;

    // Future: public string UserId { get; set; }
}

public enum ActionOnException
{
    ConvertToUpper = 0,
    ConvertToLower = 1,
    LeaveCaseAsItIs = 2
}

public enum CleaningRuleType : byte
{
    Replace = 1,
    Remove = 2,
    Trim = 3,
    UpperCase = 4,
    LowerCase = 5,
    ReverseCase = 6,
    ProperCase = 7,
    RemoveSpecialCharacters = 8,
    RemoveNumbers = 9,
    RemoveWhiteSpace = 10,
    RemoveExtraWhiteSpace = 11,
    RemoveLeadingWhiteSpace = 12,
    RemoveTrailingWhiteSpace = 13,
    RemoveNonAlphaNumeric = 14,
    RemoveNonAlpha = 15,
    RemoveNonNumeric = 16,
    RemoveNonWhiteSpace = 17,
    RemoveNonPrintable = 18,
    ReplaceZerosWithOs = 19,
    ReplaceOsWithZeros = 20,
    RemoveLetters = 21,
    ReplacementForNonPrintableCharacters = 22,
    ReplacementForEmptyValues = 23,
}
