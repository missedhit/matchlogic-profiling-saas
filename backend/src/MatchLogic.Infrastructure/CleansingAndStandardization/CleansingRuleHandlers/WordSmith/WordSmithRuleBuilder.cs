using MatchLogic.Application.Features.CleansingAndStandardization.DTOs;
using MatchLogic.Application.Interfaces.Cleansing;
using MatchLogic.Infrastructure.CleansingAndStandardization.Enhance.Rules;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.CleansingAndStandardization.CleansingRuleHandlers.WordSmith;

/// <summary>
/// Builder for creating WordSmith rules
/// </summary>
public class WordSmithRuleBuilder
{
    private readonly string _sourceColumn;
    private readonly ILogger _logger;
    private readonly WordSmithDictionaryLoader _dictionaryLoader;
    private readonly IWordSmithDictionaryService _dictionaryService;

    private Dictionary<string, ReplacingStep> _replacementsDictionary;
    private Dictionary<string, ReplacingStep> _newColumnsDictionary;
    private string _customSeparators = " \t\r\n,.;:!?\"'()[]{}<>-_/\\|+=*&^%$#@~`";
    private int _maxWordCount = 3;
    private ReplacementType _replacementType = ReplacementType.Full;
    private bool _includeFullText = true;
    private string _dictionaryPath;
    private List<Guid> _dependencies = new List<Guid>();

    /// <summary>
    /// Creates a new WordSmith rule builder
    /// </summary>
    /// <param name="sourceColumn">Column to apply transformations to</param>
    /// <param name="logger">Logger</param>
    public WordSmithRuleBuilder(string sourceColumn, ILogger logger, WordSmithDictionaryLoader dictionaryLoader,
        IWordSmithDictionaryService dictionaryService)
    {
        _sourceColumn = sourceColumn ?? throw new ArgumentNullException(nameof(sourceColumn));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        _dictionaryLoader = dictionaryLoader;
        _dictionaryService = dictionaryService;

        // Initialize empty dictionaries
        _replacementsDictionary = new Dictionary<string, ReplacingStep>(StringComparer.OrdinalIgnoreCase);
        _newColumnsDictionary = new Dictionary<string, ReplacingStep>(StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Sets the dictionary file to load replacements from
    /// </summary>
    /// <param name="filePath">Path to the dictionary file</param>
    /// <param name="encoding">Encoding to use (null for default Unicode)</param>
    /// <param name="firstLineContainsHeaders">Whether the first line contains headers</param>
    /// <returns>This builder for method chaining</returns>
    public WordSmithRuleBuilder WithDictionaryFile(
        string filePath,
        Encoding encoding = null,
        bool firstLineContainsHeaders = true)
    {
        if (string.IsNullOrEmpty(filePath))
            throw new ArgumentNullException(nameof(filePath));

        if (!File.Exists(filePath))
            throw new FileNotFoundException($"Dictionary file not found: {filePath}");

        _dictionaryPath = filePath;

        // Load dictionaries
        var result = _dictionaryLoader.LoadDictionary(filePath, encoding, firstLineContainsHeaders);

        // Log any errors
        if (result.ErrorMessages.Count > 0)
        {
            _logger.LogWarning($"Encountered {result.ErrorMessages.Count} errors loading dictionary {filePath}");
            foreach (var error in result.ErrorMessages.Take(5)) // Log first 5 errors
            {
                _logger.LogWarning(error);
            }

            if (result.ErrorMessages.Count > 5)
            {
                _logger.LogWarning($"... and {result.ErrorMessages.Count - 5} more errors");
            }
        }

        _replacementsDictionary = result.ReplacementsDictionary;
        _newColumnsDictionary = result.NewColumnsDictionary;

        return this;
    }

    /// <summary>
    /// Sets the custom separators for word splitting
    /// </summary>
    public WordSmithRuleBuilder WithSeparators(string separators)
    {
        if (string.IsNullOrEmpty(separators))
            throw new ArgumentNullException(nameof(separators));

        _customSeparators = separators;
        return this;
    }

    /// <summary>
    /// Sets the maximum number of words to combine
    /// </summary>
    public WordSmithRuleBuilder WithMaxWordCount(int maxWordCount)
    {
        if (maxWordCount < 0)
            throw new ArgumentOutOfRangeException(nameof(maxWordCount), "Max word count must be at least greater than 0");

        _maxWordCount = maxWordCount;
        return this;
    }

    /// <summary>
    /// Sets the replacement type
    /// </summary>
    public WordSmithRuleBuilder WithReplacementType(ReplacementType replacementType)
    {
        _replacementType = replacementType;
        return this;
    }

    /// <summary>
    /// Sets whether to include the full text as a word
    /// </summary>
    public WordSmithRuleBuilder WithIncludeFullText(bool includeFullText)
    {
        _includeFullText = includeFullText;
        return this;
    }

    /// <summary>
    /// Adds a dependency on another rule
    /// </summary>
    public WordSmithRuleBuilder WithDependency(Guid dependencyId)
    {
        if (dependencyId != Guid.Empty && !_dependencies.Contains(dependencyId))
        {
            _dependencies.Add(dependencyId);
        }

        return this;
    }

    /// <summary>
    /// Adds a replacement to the dictionary
    /// </summary>
    public WordSmithRuleBuilder AddReplacement(string words, string replacement, int priority = 5)
    {
        if (string.IsNullOrEmpty(words))
            throw new ArgumentNullException(nameof(words));

        string key = words.ToLower();

        var step = new ReplacingStep
        {
            Words = words,
            Replacement = replacement ?? string.Empty,
            Priority = priority
        };

        _replacementsDictionary[key] = step;

        return this;
    }

    /// <summary>
    /// Adds a word to delete
    /// </summary>
    public WordSmithRuleBuilder AddDeletion(string words, int priority = 5)
    {
        if (string.IsNullOrEmpty(words))
            throw new ArgumentNullException(nameof(words));

        string key = words.ToLower();

        var step = new ReplacingStep
        {
            Words = words,
            ToDelete = true,
            Priority = priority
        };

        _replacementsDictionary[key] = step;

        return this;
    }

    /// <summary>
    /// Adds a new column extraction
    /// </summary>
    public WordSmithRuleBuilder AddNewColumn(string words, string newColumnName, string replacement = null, int priority = 5)
    {
        if (string.IsNullOrEmpty(words))
            throw new ArgumentNullException(nameof(words));

        if (string.IsNullOrEmpty(newColumnName))
            throw new ArgumentNullException(nameof(newColumnName));

        string key = words.ToLower();

        var step = new ReplacingStep
        {
            Words = words,
            NewColumnName = newColumnName,
            Replacement = replacement ?? string.Empty,
            Priority = priority
        };

        _replacementsDictionary[key] = step;
        _newColumnsDictionary[key] = step;

        return this;
    }

    /// <summary>
    /// Clears all replacements
    /// </summary>
    public WordSmithRuleBuilder ClearReplacements()
    {
        _replacementsDictionary.Clear();
        _newColumnsDictionary.Clear();
        return this;
    }

    public async Task<WordSmithRuleBuilder> WithDictionaryIdAsync(Guid dictionaryId)
    {
        if (_dictionaryService == null)
        {
            throw new InvalidOperationException("Dictionary service is not available. Cannot load dictionary by ID.");
        }

        _logger.LogInformation("Loading dictionary with ID {DictionaryId}", dictionaryId);

        // Get dictionary metadata from service
        var dictionary = await _dictionaryService.GetDictionaryAsync(dictionaryId);
        if (dictionary == null)
        {
            throw new KeyNotFoundException($"Dictionary with ID {dictionaryId} not found");
        }

        _dictionaryPath = dictionary.OriginalFileName;


        // Load dictionary using existing loader
        //var result = _dictionaryLoader.LoadDictionary(
        //    _dictionaryPath,
        //    Encoding.Unicode, // Could be stored in dictionary metadata
        //    true); // Could be stored in dictionary metadata

        var (replacements, newColumns) = await _dictionaryService.BuildDictionariesAsync(dictionaryId);

        _replacementsDictionary = replacements;
        _newColumnsDictionary = newColumns;

        _logger.LogInformation(
              "Successfully loaded dictionary {DictionaryName} with {RuleCount} rules from database",
              dictionary.Name,
              _replacementsDictionary.Count);


        return this;
    }
    public WordSmithRuleBuilder WithDictionaryId(Guid dictionaryId)
    {
        return WithDictionaryIdAsync(dictionaryId).GetAwaiter().GetResult();
    }
    /// <summary>
    /// Creates the WordSmith rule
    /// </summary>
    public WordSmithRule Build()
    {
        return new WordSmithRule(
            _sourceColumn,
            new Dictionary<string, ReplacingStep>(_replacementsDictionary, StringComparer.OrdinalIgnoreCase),
            new Dictionary<string, ReplacingStep>(_newColumnsDictionary, StringComparer.OrdinalIgnoreCase),
            _customSeparators,
            _maxWordCount,
            _replacementType,
            _includeFullText,
            _dictionaryPath,
            _dependencies);
    }
}


public class EnhancedWordSmithRuleBuilder
{
    private readonly string _sourceColumn;
    private readonly ILogger _logger;
    private readonly WordSmithDictionaryLoader _dictionaryLoader;
    private readonly IWordSmithDictionaryService _dictionaryService;

    private Dictionary<string, ReplacingStep> _replacementsDictionary;
    private Dictionary<string, ReplacingStep> _newColumnsDictionary;
    private string _customSeparators = " \t\r\n,.;:!?\"'()[]{}<>-_/\\|+=*&^%$#@~`";
    private int _maxWordCount = 3;
    private ReplacementType _replacementType = ReplacementType.Full;
    private bool _includeFullText = true;
    private string _dictionaryPath;
    private List<Guid> _dependencies = new List<Guid>();

    /// <summary>
    /// Creates a new WordSmith rule builder
    /// </summary>
    /// <param name="sourceColumn">Column to apply transformations to</param>
    /// <param name="logger">Logger</param>
    public EnhancedWordSmithRuleBuilder(string sourceColumn, ILogger logger
        , WordSmithDictionaryLoader dictionaryLoader
        , IWordSmithDictionaryService dictionaryService)
    {
        _sourceColumn = sourceColumn ?? throw new ArgumentNullException(nameof(sourceColumn));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        _dictionaryLoader = dictionaryLoader;
        _dictionaryService = dictionaryService;

        // Initialize empty dictionaries
        _replacementsDictionary = new Dictionary<string, ReplacingStep>(StringComparer.OrdinalIgnoreCase);
        _newColumnsDictionary = new Dictionary<string, ReplacingStep>(StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Sets the dictionary file to load replacements from
    /// </summary>
    /// <param name="filePath">Path to the dictionary file</param>
    /// <param name="encoding">Encoding to use (null for default Unicode)</param>
    /// <param name="firstLineContainsHeaders">Whether the first line contains headers</param>
    /// <returns>This builder for method chaining</returns>
    public EnhancedWordSmithRuleBuilder WithDictionaryFile(
        string filePath,
        Encoding encoding = null,
        bool firstLineContainsHeaders = true)
    {
        if (string.IsNullOrEmpty(filePath))
            throw new ArgumentNullException(nameof(filePath));

        if (!File.Exists(filePath))
            throw new FileNotFoundException($"Dictionary file not found: {filePath}");

        _dictionaryPath = filePath;

        // Load dictionaries
        var result = _dictionaryLoader.LoadDictionary(filePath, encoding, firstLineContainsHeaders);

        // Log any errors
        if (result.ErrorMessages.Count > 0)
        {
            _logger.LogWarning($"Encountered {result.ErrorMessages.Count} errors loading dictionary {filePath}");
            foreach (var error in result.ErrorMessages.Take(5)) // Log first 5 errors
            {
                _logger.LogWarning(error);
            }

            if (result.ErrorMessages.Count > 5)
            {
                _logger.LogWarning($"... and {result.ErrorMessages.Count - 5} more errors");
            }
        }

        _replacementsDictionary = result.ReplacementsDictionary;
        _newColumnsDictionary = result.NewColumnsDictionary;

        return this;
    }

    /// <summary>
    /// Sets the custom separators for word splitting
    /// </summary>
    public EnhancedWordSmithRuleBuilder WithSeparators(string separators)
    {
        if (string.IsNullOrEmpty(separators))
            throw new ArgumentNullException(nameof(separators));

        _customSeparators = separators;
        return this;
    }

    /// <summary>
    /// Sets the maximum number of words to combine
    /// </summary>
    public EnhancedWordSmithRuleBuilder WithMaxWordCount(int maxWordCount)
    {
        if (maxWordCount < 0)
            throw new ArgumentOutOfRangeException(nameof(maxWordCount), "Max word count must be at least greater than 0");

        _maxWordCount = maxWordCount;
        return this;
    }

    /// <summary>
    /// Sets the replacement type
    /// </summary>
    public EnhancedWordSmithRuleBuilder WithReplacementType(ReplacementType replacementType)
    {
        _replacementType = replacementType;
        return this;
    }

    /// <summary>
    /// Sets whether to include the full text as a word
    /// </summary>
    public EnhancedWordSmithRuleBuilder WithIncludeFullText(bool includeFullText)
    {
        _includeFullText = includeFullText;
        return this;
    }

    /// <summary>
    /// Adds a dependency on another rule
    /// </summary>
    public EnhancedWordSmithRuleBuilder WithDependency(Guid dependencyId)
    {
        if (dependencyId != Guid.Empty && !_dependencies.Contains(dependencyId))
        {
            _dependencies.Add(dependencyId);
        }

        return this;
    }

    /// <summary>
    /// Adds a replacement to the dictionary
    /// </summary>
    public EnhancedWordSmithRuleBuilder AddReplacement(string words, string replacement, int priority = 5)
    {
        if (string.IsNullOrEmpty(words))
            throw new ArgumentNullException(nameof(words));

        string key = words.ToLower();

        var step = new ReplacingStep
        {
            Words = words,
            Replacement = replacement ?? string.Empty,
            Priority = priority
        };

        _replacementsDictionary[key] = step;

        return this;
    }

    /// <summary>
    /// Adds a word to delete
    /// </summary>
    public EnhancedWordSmithRuleBuilder AddDeletion(string words, int priority = 5)
    {
        if (string.IsNullOrEmpty(words))
            throw new ArgumentNullException(nameof(words));

        string key = words.ToLower();

        var step = new ReplacingStep
        {
            Words = words,
            ToDelete = true,
            Priority = priority
        };

        _replacementsDictionary[key] = step;

        return this;
    }

    /// <summary>
    /// Adds a new column extraction
    /// </summary>
    public EnhancedWordSmithRuleBuilder AddNewColumn(string words, string newColumnName, string replacement = null, int priority = 5)
    {
        if (string.IsNullOrEmpty(words))
            throw new ArgumentNullException(nameof(words));

        if (string.IsNullOrEmpty(newColumnName))
            throw new ArgumentNullException(nameof(newColumnName));

        string key = words.ToLower();

        var step = new ReplacingStep
        {
            Words = words,
            NewColumnName = newColumnName,
            Replacement = replacement ?? string.Empty,
            Priority = priority
        };

        _replacementsDictionary[key] = step;
        _newColumnsDictionary[key] = step;

        return this;
    }

    /// <summary>
    /// Clears all replacements
    /// </summary>
    public EnhancedWordSmithRuleBuilder ClearReplacements()
    {
        _replacementsDictionary.Clear();
        _newColumnsDictionary.Clear();
        return this;
    }

    /// <summary>
    /// Sets the dictionary by ID (NEW METHOD)
    /// </summary>
    /// <param name="dictionaryId">The ID of the dictionary in the database</param>
    /// <returns>This builder for method chaining</returns>
    public async Task<EnhancedWordSmithRuleBuilder> WithDictionaryIdAsync(Guid dictionaryId)
    {
        if (_dictionaryService == null)
        {
            throw new InvalidOperationException("Dictionary service is not available. Cannot load dictionary by ID.");
        }

        _logger.LogInformation("Loading dictionary with ID {DictionaryId}", dictionaryId);

        // Get dictionary metadata from service
        var dictionary = await _dictionaryService.GetDictionaryAsync(dictionaryId);
        if (dictionary == null)
        {
            throw new KeyNotFoundException($"Dictionary with ID {dictionaryId} not found");
        }

        _dictionaryPath = dictionary.OriginalFileName;


        // Load dictionary using existing loader
        //var result = _dictionaryLoader.LoadDictionary(
        //    _dictionaryPath,
        //    Encoding.Unicode, // Could be stored in dictionary metadata
        //    true); // Could be stored in dictionary metadata

        var (replacements, newColumns) = await _dictionaryService.BuildDictionariesAsync(dictionaryId);

        _replacementsDictionary = replacements;
        _newColumnsDictionary = newColumns;

        _logger.LogInformation(
              "Successfully loaded dictionary {DictionaryName} with {RuleCount} rules from database",
              dictionary.Name,
              _replacementsDictionary.Count);


        return this;
    }

    /// <summary>
    /// Sets the dictionary by ID synchronously (for non-async contexts)
    /// </summary>
    public EnhancedWordSmithRuleBuilder WithDictionaryId(Guid dictionaryId)
    {
        return WithDictionaryIdAsync(dictionaryId).GetAwaiter().GetResult();
    }
    /// <summary>
    /// Creates the WordSmith rule
    /// </summary>
    public EnhancedWordSmithTransformationRule Build()
    {
        return new EnhancedWordSmithTransformationRule(
            _sourceColumn,
            new Dictionary<string, ReplacingStep>(_replacementsDictionary, StringComparer.OrdinalIgnoreCase),
            new Dictionary<string, ReplacingStep>(_newColumnsDictionary, StringComparer.OrdinalIgnoreCase),
            _logger,
            _customSeparators,
            _maxWordCount,
            _replacementType,
            _includeFullText,
            _dictionaryPath,
            _dependencies);
    }
}