using MatchLogic.Parsers;
using MatchLogic.Domain.CleansingAndStandaradization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace MatchLogic.Infrastructure.CleansingAndStandardization.CleansingRuleHandlers;

/// <summary>
/// Represents a single character replacement rule
/// </summary>
public class CharacterReplacementItem
{
    public string Pattern { get; set; }
    public string Replacement { get; set; }
    public bool CaseInsensitive { get; set; }
}
/// <summary>
/// Rule for performing text transformations on a single column
/// </summary>
public class TextTransformationRule : TransformationRule
{
    private readonly string _columnName;
    private readonly CleaningRuleType _ruleType;
    private readonly Dictionary<string, string> _arguments;
    private readonly IEnumerable<Guid> _dependencies;
    protected String NonPrintableCharacters = GetNonPrintableCharacters();
    private readonly ProperCaseOptions _properCaseOptions;
    private readonly AbbreviationParser _abbreviationParser;

    /// <summary>
    /// Gets the columns affected by this transformation rule
    /// </summary>
    public override IEnumerable<string> AffectedColumns => new[] { _columnName };

    /// <summary>
    /// Gets the IDs of rules that this rule depends on
    /// </summary>
    public override IEnumerable<Guid> DependsOn => _dependencies;

    /// <summary>
    /// Gets the priority of this rule
    /// </summary>
    public override int Priority => GetPriorityForRuleType(_ruleType);

    public override IEnumerable<string> OutputColumns => new[] { _columnName };

    /// <summary>
    /// Creates a new text transformation rule
    /// </summary>
    public TextTransformationRule(
        string columnName,
        CleaningRuleType ruleType,
        Dictionary<string, string> arguments,
        ProperCaseOptions properCaseOptions,
        AbbreviationParser abbreviationParser,
        IEnumerable<Guid> dependencies = null)
    {
        _columnName = columnName;
        _ruleType = ruleType;
        _properCaseOptions = properCaseOptions;
        _abbreviationParser = abbreviationParser;
        _arguments = arguments ?? new Dictionary<string, string>();
        _dependencies = dependencies ?? Enumerable.Empty<Guid>();
    }

    /// <summary>
    /// Applies this transformation rule to a record
    /// </summary>
    public override void Apply(Record record)
    {
        if (!record.HasColumn(_columnName))
            return;

        var column = record[_columnName];
        //if (column.Value == null)
        //    return;

        var value = column.Value as string;
        var transformed = TransformText(value);

        column.Value = transformed;
        column.AppliedTransformations.Add(_ruleType.ToString());
        column.TryRecoverOriginalType();
    }

    private string TransformText(string input)
    {
        if (string.IsNullOrEmpty(input) && _ruleType != CleaningRuleType.ReplacementForEmptyValues)
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
                    return RemoveChars(input, removePattern);
                }
                return input;

            case CleaningRuleType.ReplaceOsWithZeros:
                return input.Replace('O', '0');

            case CleaningRuleType.ReplaceZerosWithOs:
                return input.Replace('0', 'O');

            default:
                return input;
        }
    }
    private String RemoveChars(String s, String charsToRemove, StringComparison stringComparison = StringComparison.InvariantCultureIgnoreCase)
    {
        for (Int32 i = 0; i < charsToRemove.Length; i++)
        {
            //char ch = charsToRemove[i];
            String ch = charsToRemove.Substring(i, 1);
            Int32 pos = s.IndexOf(ch, stringComparison);

            while (pos >= 0)
            {
                s = s.Remove(pos, 1);
                pos = s.IndexOf(ch, StringComparison.Ordinal);
            }
        }

        return s;
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

    /// <summary>
    /// Performs multiple character replacements based on a JSON-serialized array of replacement rules
    /// </summary>
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
    private static int GetPriorityForRuleType(CleaningRuleType ruleType)
    {
        // Lower numbers execute first
        switch (ruleType)
        {
            case CleaningRuleType.RemoveNonPrintable:
                return 10; // Non-printable chars should be removed first

            case CleaningRuleType.ReplacementForNonPrintableCharacters:
                return 15;
            case CleaningRuleType.ReplacementForEmptyValues:
                return 20; // Non-printable chars should be removed first

            case CleaningRuleType.Trim:
            case CleaningRuleType.RemoveLeadingWhiteSpace:
            case CleaningRuleType.RemoveTrailingWhiteSpace:
                return 25; // Whitespace removal should happen early

            case CleaningRuleType.Remove:
                return 30; // Custom removals and replacements in the middle
            case CleaningRuleType.Replace:
                return 35; // Custom removals and replacements in the middle



            case CleaningRuleType.RemoveLetters:
                return 40;

            case CleaningRuleType.RemoveNumbers:
                return 45;

            case CleaningRuleType.ReplaceOsWithZeros:
            case CleaningRuleType.ReplaceZerosWithOs:
                return 50;

            case CleaningRuleType.UpperCase:
            case CleaningRuleType.LowerCase:
            case CleaningRuleType.ReverseCase:
            case CleaningRuleType.ProperCase:
                return 55; // Case changes happen later

            default:
                return 100; // Default priority
        }
    }
}
