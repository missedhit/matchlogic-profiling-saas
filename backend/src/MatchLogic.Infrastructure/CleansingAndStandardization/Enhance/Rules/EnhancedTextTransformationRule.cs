using MatchLogic.Domain.CleansingAndStandaradization;
using MatchLogic.Infrastructure.CleansingAndStandardization.CleansingRuleHandlers;
using MatchLogic.Infrastructure.CleansingAndStandardization.Parser.AddressParser;
using MatchLogic.Parsers;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.CleansingAndStandardization.Enhance.Rules;
/// <summary>
/// Enhanced text transformation rule with proper input/output tracking
/// </summary>
public class EnhancedTextTransformationRule : EnhancedTransformationRule
{
    private readonly string _columnName;
    private readonly CleaningRuleType _ruleType;
    private readonly Dictionary<string, string> _arguments;
    private readonly IEnumerable<Guid> _dependencies;

    // ProperCase support
    private readonly ProperCaseOptions _properCaseOptions;
    private readonly AbbreviationParser _abbreviationParser;    
    protected String NonPrintableCharacters = GetNonPrintableCharacters();
    public override IEnumerable<string> InputColumns => new[] { _columnName };
    public override IEnumerable<string> OutputColumns => new[] { _columnName };
    public override IEnumerable<string> AffectedColumns => new[] { _columnName };
    public override IEnumerable<string> ModifiedColumns => new[] { _columnName };
    public override IEnumerable<Guid> DependsOn => _dependencies;
    public override int Priority => GetPriorityForRuleType(_ruleType);

    public EnhancedTextTransformationRule(
        string columnName,
        CleaningRuleType ruleType,
        Dictionary<string, string> arguments = null,        
        ProperCaseOptions properCaseOptions = null,
        AbbreviationParser abbreviationParser = null,
        IEnumerable<Guid> dependencies = null)
    {
        _columnName = columnName;
        _ruleType = ruleType;
        _arguments = arguments ?? new Dictionary<string, string>();
        _properCaseOptions = properCaseOptions ?? new ProperCaseOptions();
        _abbreviationParser = abbreviationParser;
        _dependencies = dependencies ?? Enumerable.Empty<Guid>();        
    }    

    public override void Apply(Record record)
    {
        if (!record.HasColumn(_columnName))
            return;

        var column = record[_columnName];


        var value = column.Value != null ? column.Value.ToString() : string.Empty;
        var transformed = TransformText(value);

        column.Value = transformed;
        column.AppliedTransformations.Add(_ruleType.ToString());
        column.TryRecoverOriginalType();
    }

    private string TransformText(string input)
    {
        if (string.IsNullOrEmpty(input) && _ruleType != CleaningRuleType.ReplacementForEmptyValues)
            return input;

        if (string.IsNullOrEmpty(input))
            return input;

        switch (_ruleType)
        {
            case CleaningRuleType.UpperCase:
                return input.ToUpper();

            case CleaningRuleType.LowerCase:
                return input.ToLower();

            case CleaningRuleType.ReverseCase:
                return ReverseCase(input);

            case CleaningRuleType.ProperCase:
                return ApplyProperCase(input);

            case CleaningRuleType.Trim:
                return input.Trim();

            case CleaningRuleType.RemoveLeadingWhiteSpace:
                return input.TrimStart();

            case CleaningRuleType.RemoveTrailingWhiteSpace:
                return input.TrimEnd();

            case CleaningRuleType.RemoveWhiteSpace:
                return input.Replace(" ", "");

            case CleaningRuleType.RemoveNumbers:
                return new string(input.Where(c => !char.IsDigit(c)).ToArray());

            case CleaningRuleType.RemoveLetters:
                return new string(input.Where(c => !char.IsLetter(c)).ToArray());

            case CleaningRuleType.RemoveSpecialCharacters:
                return Regex.Replace(input, @"[^\w\s]", "");

            case CleaningRuleType.RemoveNonAlphaNumeric:
                return Regex.Replace(input, @"[^\w]", "");

            case CleaningRuleType.RemoveNonAlpha:
                return Regex.Replace(input, @"[^a-zA-Z]", "");

            case CleaningRuleType.RemoveNonNumeric:
                return Regex.Replace(input, @"[^\d]", "");

            case CleaningRuleType.RemoveNonWhiteSpace:
                return Regex.Replace(input, @"[^\s]", "");

            case CleaningRuleType.RemoveNonPrintable:
                return RemoveNonPrintableCharacters(input);

            case CleaningRuleType.ReplacementForNonPrintableCharacters:
                if (_arguments.TryGetValue("replacement", out var replacementForNon))
                {
                    return ReplaceNonPrintableCharacters(input, replacementForNon);
                }
                return input;

            case CleaningRuleType.ReplacementForEmptyValues:
                if (_arguments.TryGetValue("replacement", out var replacementForEmpty) && string.IsNullOrEmpty(input))
                {
                    return replacementForEmpty;
                }
                return input;

            case CleaningRuleType.Replace:
                if (_arguments.TryGetValue("replacements", out var replacementsJson))
                {
                    return ReplaceMultipleCharacters(input, replacementsJson);
                }
                return input;

            case CleaningRuleType.Remove:
                if (_arguments.TryGetValue("pattern", out var removePattern))
                {
                    return Regex.Replace(input, removePattern, "");
                }
                return input;

            case CleaningRuleType.ReplaceOsWithZeros:
                return input.Replace("O", "0", true, System.Globalization.CultureInfo.CurrentCulture);

            case CleaningRuleType.ReplaceZerosWithOs:
                return input.Replace('0', 'O');

            default:
                return input;
        }
    }

    private string ReplaceMultipleCharacters(string input, string replacementsJson)
    {
        try
        {
            var replacements = JsonSerializer.Deserialize<List<CharacterReplacementItem>>(replacementsJson);
            if (replacements == null || !replacements.Any())
                return input;

            var result = input;

            foreach (var replacement in replacements)
            {
                if (string.IsNullOrEmpty(replacement.Pattern))
                    continue;

                if (replacement.CaseInsensitive)
                {
                    // Case-insensitive replacement using IndexOf
                    result = ReplaceCaseInsensitive(result, replacement.Pattern, replacement.Replacement ?? string.Empty);
                }
                else
                {
                    // Case-sensitive replacement
                    result = result.Replace(replacement.Pattern, replacement.Replacement ?? string.Empty);
                }
            }

            return result;
        }
        catch (JsonException)
        {
            // If JSON parsing fails, return original input
            return input;
        }
    }
    /// <summary>
    /// Performs case-insensitive string replacement
    /// </summary>
    private string ReplaceCaseInsensitive(string input, string pattern, string replacement)
    {
        var result = new StringBuilder(input);
        int start = 0;
        var workingString = input;

        while ((start = workingString.IndexOf(pattern, start, StringComparison.OrdinalIgnoreCase)) != -1)
        {
            result.Remove(start, pattern.Length);
            result.Insert(start, replacement);
            workingString = result.ToString();
            start += replacement.Length;
        }

        return result.ToString();
    }

    private string ReplaceNonPrintableCharacters(string s, string replacement)
    {
        StringBuilder result = new StringBuilder(s);
        foreach (char c in NonPrintableCharacters)
        {
            string nonPrintableCh = c + string.Empty;
            result.Replace(nonPrintableCh, replacement);
        }

        return result.ToString();
    }
    public static string GetNonPrintableCharacters()
    {
        string str = string.Empty;

        for (Int32 index = 0; index < UInt16.MaxValue; ++index)
        {
            char c = (char)index;

            if (char.IsWhiteSpace(c) && c != 32)
            {
                str += c.ToString();
            }
        }

        return str;
    }    

    private static string ReverseCase(string s)
    {
        StringBuilder result = new StringBuilder();

        foreach (Char ch in s)
        {
            if (Char.IsUpper(ch))
            {
                result.Append(Char.ToLower(ch));
            }
            else if (Char.IsLower(ch))
            {
                result.Append(Char.ToUpper(ch));
            }
            else
            {
                result.Append(ch);
            }
        }

        return result.ToString();
    }
    private string ApplyProperCase(string input)
    {
        if (_properCaseOptions == null)
        {
            return System.Globalization.CultureInfo.CurrentCulture.TextInfo.ToTitleCase(input.ToLower());
        }

        bool useProperCase = GetBoolArg("use_proper_case", true);
        bool toUpperAfterMc = GetBoolArg("to_upper_after_mc", false);
        bool toUpperAfterO = GetBoolArg("to_upper_after_o", false);
        bool toUpperDeLa = GetBoolArg("to_upper_de_la", false);
        bool leaveAbbreviations = GetBoolArg("leave_abbreviations", false);

        // Step 1: Apply AbbreviationParser (handles base proper case, exceptions, abbreviations)
        var delimiters = _properCaseOptions.Delimiters.ToCharArray();
        string result = _abbreviationParser.Transform(
            input,
            _properCaseOptions,
            leaveAbbreviations,
            delimiters);

        // Step 2: Apply IrishSpanishNames (only if useProperCase AND any option selected)
        bool isAnyOptionSelected = toUpperAfterMc || toUpperAfterO || toUpperDeLa;
        if (useProperCase && isAnyOptionSelected)
        {
            result = TransformIrishSpanishNames(result, toUpperAfterMc, toUpperAfterO, toUpperDeLa);
        }

        return result;
    }
    private string TransformIrishSpanishNames(string input, bool toUpperAfterMc, bool toUpperAfterO, bool toUpperDeLa)
    {
        if (string.IsNullOrEmpty(input))
            return input;

        int len = input.Length;
        var newChars = new char[len];
        int currentCharIndex = 0;

        for (int i = 0; i < len; i++)
        {
            char c = input[i];

            if (c == 'O' && toUpperAfterO && i < len - 2 && input[i + 1] == '\'')
            {
                newChars[currentCharIndex++] = 'O';
                newChars[currentCharIndex++] = '\'';
                i += 2;
                newChars[currentCharIndex++] = char.ToUpper(input[i]);
            }
            else if (c == 'M' && toUpperAfterMc && i < len - 2 && input[i + 1] == 'c')
            {
                newChars[currentCharIndex++] = 'M';
                newChars[currentCharIndex++] = 'c';
                i += 2;
                newChars[currentCharIndex++] = char.ToUpper(input[i]);
            }
            else if (c == 'D' && toUpperDeLa && i < len - 3 && input[i + 1] == 'e' && input[i + 2] == ' ')
            {
                // Only lowercase the 'D', let loop continue to process 'e' and ' ' normally
                newChars[currentCharIndex++] = 'd';
            }
            else
            {
                newChars[currentCharIndex++] = c;
            }
        }

        return new string(newChars, 0, currentCharIndex);
    }

    private bool GetBoolArg(string key, bool defaultValue)
    {
        if (_arguments.TryGetValue(key, out var value))
        {
            if (bool.TryParse(value, out var boolValue))
                return boolValue;
        }
        return defaultValue;
    }    
    
    private static string RemoveNonPrintableCharacters(string input)
    {
        var result = new StringBuilder(input.Length);
        foreach (char c in input)
        {
            if (c >= 32 && c != 127) // 127 is DEL
            {
                result.Append(c);
            }
        }
        return result.ToString();
    }

    private static int GetPriorityForRuleType(CleaningRuleType ruleType)
    {
        // Lower numbers execute first
        switch (ruleType)
        {
            case CleaningRuleType.RemoveNonPrintable:
                return 10; // Non-printable chars should be removed first

            case CleaningRuleType.Trim:
            case CleaningRuleType.RemoveLeadingWhiteSpace:
            case CleaningRuleType.RemoveTrailingWhiteSpace:
                return 20; // Whitespace removal should happen early

            case CleaningRuleType.Remove:
            case CleaningRuleType.Replace:
                return 50; // Custom removals and replacements in the middle

            case CleaningRuleType.ReplaceOsWithZeros:
            case CleaningRuleType.ReplaceZerosWithOs:
                return 60;

            case CleaningRuleType.UpperCase:
            case CleaningRuleType.LowerCase:            
            case CleaningRuleType.ProperCase:
                return 80; // Case changes happen later

            default:
                return 100; // Default priority
        }
    }
}

// <summary>
/// Enhanced copy field rule
/// </summary>
public class EnhancedCopyFieldRule : EnhancedTransformationRule
{
    private readonly string _sourceColumn;
    private readonly string _targetColumn;
    private readonly IEnumerable<Guid> _dependencies;

    public override IEnumerable<string> InputColumns => new[] { _sourceColumn };
    public override IEnumerable<string> OutputColumns => new[] { _targetColumn };
    public override IEnumerable<string> AffectedColumns => new[] { _sourceColumn };
    public override IEnumerable<Guid> DependsOn => _dependencies;
    public override int Priority => 1;
    public override bool CreatesNewColumns => true;

    public EnhancedCopyFieldRule(
        string sourceColumn,
        string targetColumn,
        IEnumerable<Guid> dependencies = null)
    {
        _sourceColumn = sourceColumn;
        _targetColumn = targetColumn;
        _dependencies = dependencies ?? Enumerable.Empty<Guid>();
    }

    public override void Apply(Record record)
    {
        if (!record.HasColumn(_sourceColumn))
            return;

        var sourceColumn = record[_sourceColumn];
        if (sourceColumn == null)
            return;

        var newColumn = new ColumnValue(_targetColumn, sourceColumn.Value)
        {
            OriginalType = sourceColumn.OriginalType
        };

        newColumn.AppliedTransformations.AddRange(sourceColumn.AppliedTransformations);
        newColumn.AppliedTransformations.Add("Copy");

        record.AddColumn(newColumn);
    }
}