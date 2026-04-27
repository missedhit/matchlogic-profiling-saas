using MatchLogic.Application.Features.CleansingAndStandardization.DTOs;
using MatchLogic.Domain.CleansingAndStandaradization;
using MatchLogic.Infrastructure.CleansingAndStandardization.CleansingRuleHandlers.WordSmith;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static MatchLogic.Infrastructure.CleansingAndStandardization.CleansingRuleHandlers.WordSmith.WordSmithRule;

namespace MatchLogic.Infrastructure.CleansingAndStandardization.Enhance.Rules;
/// <summary>
/// Enhanced rule for performing complex word replacements using WordSmith functionality
/// </summary>
public class EnhancedWordSmithTransformationRule : EnhancedTransformationRule
{
    private readonly string _sourceColumn;
    private readonly Dictionary<string, ReplacingStep> _replacementsDictionary;
    private readonly Dictionary<string, ReplacingStep> _newColumnsDictionary;
    private readonly string _customSeparators;
    private readonly int _maxWordCount;
    private readonly ReplacementType _replacementType;
    private readonly IEnumerable<Guid> _dependencies;
    private readonly bool _includeFullText;
    private readonly ILogger _logger;

    // Cache of output column names for quick access
    private readonly List<string> _outputColumns = new List<string>();

    /// <summary>
    /// Columns that this rule reads from (inputs)
    /// </summary>
    public override IEnumerable<string> InputColumns => new[] { _sourceColumn };

    /// <summary>
    /// Columns that this rule writes to (outputs)
    /// </summary>
    public override IEnumerable<string> OutputColumns => _outputColumns;

    /// <summary>
    /// Columns that this rule modifies in-place
    /// </summary>
    public override IEnumerable<string> ModifiedColumns =>
        _replacementType == ReplacementType.Full ? new[] { _sourceColumn } : Enumerable.Empty<string>();

    /// <summary>
    /// Gets the columns affected by this transformation rule
    /// </summary>
    public override IEnumerable<string> AffectedColumns => InputColumns.Concat(OutputColumns);

    /// <summary>
    /// Gets the IDs of rules that this rule depends on
    /// </summary>
    public override IEnumerable<Guid> DependsOn => _dependencies;

    /// <summary>
    /// Gets the priority of this rule (WordSmith runs after basic text cleaning)
    /// </summary>
    public override int Priority { get; set; } = 200;

    /// <summary>
    /// Whether this rule creates new columns
    /// </summary>
    public override bool CreatesNewColumns => _outputColumns.Any();

    /// <summary>
    /// Creates a new enhanced WordSmith rule
    /// </summary>
    public EnhancedWordSmithTransformationRule(
        string sourceColumn,
        Dictionary<string, ReplacingStep> replacementsDictionary,
        Dictionary<string, ReplacingStep> newColumnsDictionary,
        ILogger logger,
        string customSeparators = " \t\r\n,.;:!?\"'()[]{}<>-_/\\|+=*&^%$#@~`",
        int maxWordCount = 3,
        ReplacementType replacementType = ReplacementType.Full,
        bool includeFullText = true,
        string dictionaryPath = "",
        IEnumerable<Guid> dependencies = null)
    {
        _sourceColumn = sourceColumn ?? throw new ArgumentNullException(nameof(sourceColumn));
        _replacementsDictionary = replacementsDictionary ?? new Dictionary<string, ReplacingStep>(StringComparer.OrdinalIgnoreCase);
        _newColumnsDictionary = newColumnsDictionary ?? new Dictionary<string, ReplacingStep>(StringComparer.OrdinalIgnoreCase);
        _customSeparators = customSeparators;
        _maxWordCount = maxWordCount;
        _replacementType = replacementType;
        _includeFullText = includeFullText;
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _dependencies = dependencies ?? Enumerable.Empty<Guid>();

        // Extract all unique output column names
        foreach (var step in _newColumnsDictionary.Values)
        {
            if (!string.IsNullOrEmpty(step.NewColumnName) && !_outputColumns.Contains(step.NewColumnName))
            {
                _outputColumns.Add(step.NewColumnName);
            }
        }
    }

    /// <summary>
    /// Applies this transformation rule to a record
    /// </summary>
    public override void Apply(Record record)
    {
        try
        {
            if (!record.HasColumn(_sourceColumn))
            {
                _logger.LogWarning("Source column {SourceColumn} not found in record", _sourceColumn);
                AddEmptyOutputColumns(record);
                return;
            }

            var column = record[_sourceColumn];
            if (column?.Value == null)
            {
                _logger.LogDebug("Source column {SourceColumn} has null value", _sourceColumn);
                AddEmptyOutputColumns(record);
                return;
            }

            var input = column.Value.ToString();
            if (string.IsNullOrEmpty(input))
            {
                _logger.LogDebug("Source column {SourceColumn} has empty value", _sourceColumn);
                AddEmptyOutputColumns(record);
                return;
            }

            // Apply replacements to the main column
            if (_replacementType == ReplacementType.Full)
            {
                var output = ReplaceWords(input);
                column.Value = output;
                column.AppliedTransformations.Add("WordSmith");
            }

            // Create additional columns
            ExtractToNewColumns(record, input);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error applying WordSmith transformation rule to column {SourceColumn}", _sourceColumn);

            // Ensure output columns are created even on error
            AddEmptyOutputColumns(record);
        }
    }

    /// <summary>
    /// Adds empty values for all output columns that haven't been set
    /// </summary>
    private void AddEmptyOutputColumns(Record record)
    {
        foreach (var columnName in _outputColumns)
        {
            if (!record.HasColumn(columnName))
            {
                record.AddColumn(columnName, string.Empty);
            }
        }
    }

    /// <summary>
    /// Extracts values to new columns based on the configuration
    /// </summary>
    private void ExtractToNewColumns(Record record, string input)
    {
        const string garbage = " *&^&%^%$GHF%$$^KJ7784554 "; // Same marker as original to avoid collisions

        var stringLower = input.ToLower();
        var whatIsLeft = stringLower;
        var separators = _customSeparators.ToCharArray();

        // Get sorted replacing steps for new columns (reversed order as in original)
        var replacingStepsList = GetSortedReplacingSteps(_newColumnsDictionary, input);

        // Invert the order of the list with replacements as in the original implementation
        var inverted = new List<ReplacingStep>();
        for (int i = replacingStepsList.Count - 1; i >= 0; i--)
        {
            inverted.Add(replacingStepsList[i]);
        }
        replacingStepsList = inverted;

        // Process each replacement step
        foreach (var step in replacingStepsList)
        {
            try
            {
                string word = step.Words;
                string replacement = step.Replacement;
                string newColumnName = step.NewColumnName;

                int startIndex = IndexOfWholeWord(input.ToLower(), word.ToLower(), separators);

                if (startIndex != -1)
                {
                    bool isFormatted = stringLower.Contains(word.ToLower());

                    if (isFormatted)
                    {
                        if (!string.IsNullOrEmpty(newColumnName))
                        {
                            var valueToAdd = string.IsNullOrEmpty(replacement) ? word : replacement;
                            AddOutputValue(record, newColumnName, valueToAdd);
                        }

                        whatIsLeft = whatIsLeft.Replace(word, garbage);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error processing replacement step for word {Word}", step.Words);
            }
        }

        // Add empty values for any output columns that weren't set
        AddEmptyOutputColumns(record);
    }

    /// <summary>
    /// Adds an output value to the record for a specific column
    /// </summary>
    private void AddOutputValue(Record record, string columnName, string value)
    {
        if (!string.IsNullOrEmpty(columnName))
        {
            record.AddColumn(columnName, value ?? string.Empty);
        }
    }

    /// <summary>
    /// Replaces words in the input string according to the replacement rules
    /// </summary>
    private string ReplaceWords(string input)
    {
        string output = input;
        char[] separators = _customSeparators.ToCharArray();

        // Get sorted replacing steps for main column
        List<ReplacingStep> replacingStepsList = GetSortedReplacingSteps(_replacementsDictionary, output);
        List<Limits> limits = new List<Limits>();

        foreach (var step in replacingStepsList)
        {
            if (_replacementType == ReplacementType.Flag && !string.IsNullOrEmpty(step.NewColumnName))
            {
                continue; // Skip if we're in Flag mode and this is for a new column
            }

            string word = step.Words;

            if (!string.IsNullOrEmpty(step.Replacement) || step.ToDelete)
            {
                int startIndex = IndexOfWholeWord(output.ToLower(), word.ToLower(), separators);

                if (startIndex == -1)
                {
                    continue;
                }

                // Check for overlapping replacements
                Limits newLimits = new Limits { Start = startIndex, Length = step.Replacement?.Length ?? 0 };

                if (newLimits.IsCrossing(limits))
                {
                    continue;
                }

                int length = word.Length;
                output = output.Remove(startIndex, length);

                if (!step.ToDelete && !string.IsNullOrEmpty(step.Replacement))
                {
                    output = output.Insert(startIndex, step.Replacement);
                    limits.Add(newLimits);
                }
                // Clean up extra space if the word is in the middle
                else if (startIndex > 0 && output.Length > startIndex
                         && output[startIndex - 1] == ' ' && output[startIndex] == ' ')
                {
                    output = output.Remove(startIndex - 1, 1);
                }
            }
        }

        return output;
    }

    /// <summary>
    /// Gets sorted replacing steps based on priority and word length
    /// </summary>
    private List<ReplacingStep> GetSortedReplacingSteps(Dictionary<string, ReplacingStep> dictionary, string text)
    {
        List<ReplacingStep> result = new List<ReplacingStep>();
        char[] separators = _customSeparators.ToCharArray();

        // Get all words and combinations of words from the input text
        string[] combinedWords = GetCombinedWords(text, _maxWordCount, ref separators, _includeFullText);

        for (int i = 0; i < combinedWords.Length; i++)
        {
            string word = combinedWords[i];
            string lowerWord = word.ToLower();

            if (dictionary.TryGetValue(lowerWord, out ReplacingStep step))
            {
                result.Add(step);
            }
        }

        // Sort by priority and then by word length (descending) as in original
        result.Sort(new ReplacingStepPriorityComparer());

        return result;
    }

    /// <summary>
    /// Gets all words and combinations of words from a text using the original algorithm
    /// </summary>
    private static string[] GetCombinedWords(string text, int maxWordCount, ref char[] separators, bool includeFullText)
    {
        if (separators == null || separators.Length == 0)
        {
            return new string[] { text };
        }

        string[] words = text.Split(separators, StringSplitOptions.RemoveEmptyEntries);

        if (words.Length <= 1)
        {
            return words;
        }

        int finalLength = words.Length;
        for (int collocationLength = 2; collocationLength <= maxWordCount; ++collocationLength)
        {
            if (collocationLength <= words.Length)
            {
                finalLength += words.Length - collocationLength + 1;
            }
        }

        if (includeFullText && words.Length > maxWordCount)
        {
            ++finalLength;
        }

        string[] combinedWords = new string[finalLength];

        if (includeFullText)
        {
            combinedWords[finalLength - 1] = text;
        }

        Array.Copy(words, combinedWords, words.Length);
        int emptyIndex = words.Length;

        for (int collocationLength = 2; collocationLength <= maxWordCount; ++collocationLength)
        {
            for (int firstWord = 0; firstWord <= words.Length - collocationLength; ++firstWord)
            {
                StringBuilder collocation = new StringBuilder();

                for (int wordInCollocation = 0; wordInCollocation < collocationLength; ++wordInCollocation)
                {
                    collocation.Append(words[firstWord + wordInCollocation]);

                    if (wordInCollocation < collocationLength - 1)
                    {
                        collocation.Append(" ");
                    }
                }

                combinedWords[emptyIndex] = collocation.ToString();
                ++emptyIndex;
            }
        }

        return combinedWords;
    }

    /// <summary>
    /// Find 'whole' word using the original algorithm
    /// </summary>
    private static int IndexOfWholeWord(string target, string value, char[] separators)
    {
        int index;

        for (int startIndex = 0;
            startIndex < target.Length && (index = target.IndexOf(value, startIndex)) != -1;
            startIndex = index + 1)
        {
            bool correctStart = true;

            if (index > 0)
            {
                correctStart = separators.Contains(target[index - 1]);
            }

            bool correctFinish = true;

            if (index + value.Length < target.Length)
            {
                correctFinish = separators.Contains(target[index + value.Length]);
            }

            if (correctStart & correctFinish)
            {
                return index;
            }
        }

        return -1;
    }

    /// <summary>
    /// Returns a string representation of this rule
    /// </summary>
    public override string ToString()
    {
        return $"EnhancedWordSmithTransformationRule: {_sourceColumn} -> {_outputColumns.Count} outputs (ID: {Id})";
    }

    public override bool CanApply(Record record)
    {
        // Check if at least one source column exists
        return InputColumns.Any(column => record.HasColumn(column));
    }
}