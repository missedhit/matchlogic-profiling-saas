using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using MatchLogic.Data.QualityII;
using MatchLogic.Application.Common;
using MatchLogic.Infrastructure.CleansingAndStandardization.Parser.GenericParser;
using Microsoft.Extensions.Logging;

namespace MatchLogic.Parsers;

public class FullNameParseResult
{
    public string Prefix { get; set; }
    public string FirstName { get; set; }
    public string MiddleName { get; set; }
    public string LastName { get; set; }
    public string Suffix { get; set; }

    public FullNameParseResult()
    {
        Prefix = string.Empty;
        FirstName = string.Empty;
        MiddleName = string.Empty;
        LastName = string.Empty;
        Suffix = string.Empty;
    }
}
public class FullNameParser : GenericParser
{
    #region Enums

    public enum RecognizedTypes
    {
        FirstName,
        LastName,
        MiddleName,
        Prefix,
        Suffix,
        SingleLetter,
        And,
        UndefinedLetters
    }

    /// <summary>
    /// this is very similar to RecognizedTypes, the difference is that PatternIds are one level higher and can be identified by many combinations of RecognizedTypes
    /// e.g. pattern PostalBox is recognized as combination of:
    /// PoBoxPrefix, Numeric or
    /// PoBoxPrefix, AlphaNumeric or
    /// RuralRoutePrefix, Numeric or
    /// RuralRoutePrefix, Numeric, Box, AlphaNumeric
    /// </summary>
    public enum PatternIds
    {
        FirstNamePattern,
        LastNamePattern,
        MiddleNamePattern,
        SalutationPattern,
        PrefixPattern,
        SuffixPattern,
        UndefinedPattern
    }

    #endregion

    #region Fields

    #region simple dictionaries

    public static DictionaryCategory PrefixesDictionary = new DictionaryCategory((Int32) RecognizedTypes.Prefix);
    public static DictionaryCategory SuffixesDictionary = new DictionaryCategory((Int32) RecognizedTypes.Suffix);
    private static DictionaryCategory andsDictionary = new DictionaryCategory((Int32) RecognizedTypes.And);

    #endregion

    #region patternGroups

    PatternsGroup lastNamesPatternsGroup = new PatternsGroup((Int32) PatternIds.LastNamePattern
#if DEBUG
        , "Last Name"
#endif
    );

    PatternsGroup firstNamesPatternsGroup = new PatternsGroup((Int32) PatternIds.FirstNamePattern
#if DEBUG
        , "First Name"
#endif
    );

    PatternsGroup prefixesPatternsGroup = new PatternsGroup((Int32) PatternIds.PrefixPattern
#if DEBUG
        , "Prefixes"
#endif
    );

    PatternsGroup suffixesPatternsGroup = new PatternsGroup((Int32) PatternIds.SuffixPattern
#if DEBUG
        , "Suffixes"
#endif
    );

    PatternsGroup middleNamePatternsGroup = new PatternsGroup((Int32) PatternIds.MiddleNamePattern
#if DEBUG
        , "Middle Name"
#endif
    );

    #endregion

    #region misc

    private PredefinedStringTypes predefinedStringTypes;

    #endregion

    #endregion

    #region Constructors

    public FullNameParser() :
        this(new PredefinedStringTypes())
    {
    }

    public FullNameParser(PredefinedStringTypes predefinedStringTypes)
        : base()
    {
        this.predefinedStringTypes = predefinedStringTypes;
        //this.predefinedStringTypes.StringDataTypeDeterminator =
        //    new StringDataTypeDeterminator(this.predefinedStringTypes);
        this.predefinedStringTypes.LoadNameDictionaries();
        initDictionaries();
    }

    #endregion

    #region Properties and Fields

    private ChoosenPattern firstName;

    public String FirstName
    {
        get
        {
            String result = "";
            if (firstName != null)
            {
                result = firstName.Text;
            }

            return result;
        }
    }

    private ChoosenPattern lastName;

    public String LastName
    {
        get
        {
            String result = "";
            if (lastName != null)
            {
                result = lastName.Text;
            }

            return result;
        }
    }

    private ChoosenPattern middleName;

    public String MiddleName
    {
        get
        {
            String result = "";
            if (middleName != null)
            {
                result = middleName.Text;
            }

            return result;
        }
    }

    private ChoosenPattern prefix;

    public String Prefix
    {
        get
        {
            String result = "";
            if (prefix != null)
            {
                if (prefix.Text != null)
                {
                    result = prefix.Text.Trim();
                }
            }

            return result;
        }
    }

    private ChoosenPattern suffix;

    public String Suffix
    {
        get
        {
            String result = "";
            if (suffix != null)
            {
                if (suffix.Text != null)
                {
                    result = suffix.Text.Replace(" ", "");
                }
            }

            return result;
        }
    }

    #endregion

    #region Methods

    #region initialization

    private void initDictionaries()
    {
        initPrefixesDictionary();
        initSuffixesDictionary();
        initAndsDictionary();
    }

    #endregion

    #region overridden methods

    protected override void initPatterns()
    {
        firstNamesPatternsGroup.Add(new Pattern(new List<Int32> {(Int32) RecognizedTypes.SingleLetter},
            firstNamesPatternsGroup, typeof(RecognizedTypes)));
        firstNamesPatternsGroup.Add(new Pattern(new List<Int32> {(Int32) RecognizedTypes.FirstName},
            firstNamesPatternsGroup, typeof(RecognizedTypes)));
        firstNamesPatternsGroup.Add(new Pattern(
            new List<Int32>
            {
                (Int32) RecognizedTypes.FirstName,
                (Int32) RecognizedTypes.And,
                (Int32) RecognizedTypes.FirstName
            }, firstNamesPatternsGroup, typeof(RecognizedTypes)));
        firstNamesPatternsGroup.Add(new Pattern(
            new List<Int32>
            {
                (Int32) RecognizedTypes.UndefinedLetters,
                (Int32) RecognizedTypes.And,
                (Int32) RecognizedTypes.FirstName
            }, firstNamesPatternsGroup, typeof(RecognizedTypes)));
        firstNamesPatternsGroup.Add(new Pattern(
            new List<Int32>
            {
                (Int32) RecognizedTypes.UndefinedLetters,
                (Int32) RecognizedTypes.And,
                (Int32) RecognizedTypes.UndefinedLetters
            }, firstNamesPatternsGroup, typeof(RecognizedTypes)));
        firstNamesPatternsGroup.Add(new Pattern(
            new List<Int32>
            {
                (Int32) RecognizedTypes.UndefinedLetters,
                (Int32) RecognizedTypes.And,
                (Int32) RecognizedTypes.UndefinedLetters
            }, firstNamesPatternsGroup, typeof(RecognizedTypes)));
        firstNamesPatternsGroup.Add(new Pattern(new List<Int32> {(Int32) RecognizedTypes.UndefinedLetters},
            firstNamesPatternsGroup, typeof(RecognizedTypes)));
        patternsGroupList.Add(firstNamesPatternsGroup);

        lastNamesPatternsGroup.Add(new Pattern(new List<Int32> {(Int32) RecognizedTypes.FirstName},
            lastNamesPatternsGroup, typeof(RecognizedTypes)));
        lastNamesPatternsGroup.Add(new Pattern(new List<Int32> {(Int32) RecognizedTypes.UndefinedLetters},
            lastNamesPatternsGroup, typeof(RecognizedTypes)));
        patternsGroupList.Add(lastNamesPatternsGroup);

        middleNamePatternsGroup.Add(new Pattern(new List<Int32> {(Int32) RecognizedTypes.SingleLetter},
            middleNamePatternsGroup, typeof(RecognizedTypes)));
        middleNamePatternsGroup.Add(new Pattern(new List<Int32> {(Int32) RecognizedTypes.FirstName},
            middleNamePatternsGroup, typeof(RecognizedTypes)));
        middleNamePatternsGroup.Add(new Pattern(new List<Int32> {(Int32) RecognizedTypes.UndefinedLetters},
            middleNamePatternsGroup, typeof(RecognizedTypes)));
        patternsGroupList.Add(middleNamePatternsGroup);

        prefixesPatternsGroup.Add(new Pattern(new List<Int32> {(Int32) RecognizedTypes.Prefix},
            prefixesPatternsGroup, typeof(RecognizedTypes)));
        prefixesPatternsGroup.Add(new Pattern(
            new List<Int32>
            {
                (Int32) RecognizedTypes.Prefix,
                (Int32) RecognizedTypes.And,
                (Int32) RecognizedTypes.Prefix
            }, prefixesPatternsGroup, typeof(RecognizedTypes)));
        patternsGroupList.Add(prefixesPatternsGroup);

        suffixesPatternsGroup.Add(new Pattern(new List<Int32> {(Int32) RecognizedTypes.Suffix},
            suffixesPatternsGroup, typeof(RecognizedTypes)));
        patternsGroupList.Add(suffixesPatternsGroup);

    }

    public override Boolean isSeparatorChar(Char ch)
    {
        Boolean result = (ch == ','); // dot is used in suffixes like "M.D." ...
        return result;
    }

    private Boolean isLetterOrDash(String s)
    {
        Boolean result = true;
        for (Int32 i = 0; i < s.Length; i++)
        {
            Char ch = s[i];
            if ((ch == '\'') || (ch == '-'))
            {
                continue;
            }
            else if (!Char.IsLetter(ch))
            {
                result = false;
                break;
            }
        }

        return result;
    }

    protected override Dictionary<Int32, Boolean> findCategories(String s)
    {
        Dictionary<Int32, Boolean> result = new Dictionary<Int32, Boolean>();
        if (PrefixesDictionary.IsMember(s))
        {
            result.Add(PrefixesDictionary.CategoryId, false);
        }
        else if (SuffixesDictionary.IsMember(s))
        {
            result.Add(SuffixesDictionary.CategoryId, false);
        }
        else if (andsDictionary.IsMember(s))
        {
            result.Add(andsDictionary.CategoryId, false);
        }
        else if (isLetterOrDash(s))
        {
            if (s.Length == 1)
            {
                if (isLetter(s))
                {
                    result.Add((Int32) RecognizedTypes.SingleLetter, false);
                }
            }
            else
            {
                result.Add((Int32) RecognizedTypes.UndefinedLetters, false);
            }
        }

        tryFindTypeInOtherDictionaries(s, result);
        return result;
    }

    private void tryFindTypeInOtherDictionaries(String s, Dictionary<Int32, Boolean> result)
    {
        List<String> typesFromOtherDictionaries = predefinedStringTypes.FindTypeNames(s);
        for (Int32 i = 0; i < typesFromOtherDictionaries.Count; i++)
        {
            String typeName = typesFromOtherDictionaries[i];
            if (typeName == PredefinedStringTypes.FirstNameDictionaryDesription)
            {
                if (!result.ContainsKey((Int32) RecognizedTypes.FirstName))
                {
                    result.Add((Int32) RecognizedTypes.FirstName, false);
                }
            }
            else if (typeName == PredefinedStringTypes.LastNameDictionaryDesription)
            {
                if (!result.ContainsKey((Int32) RecognizedTypes.LastName))
                {
                    result.Add((Int32) RecognizedTypes.LastName, false);
                }
            }
        }

        if (s.Contains("-")) // very special case when string contains dash, e.g.:
        {
            if (s.IndexOf("-") > 0)
            {
                String[] subStrings = s.Split(new String[] {"-"}, StringSplitOptions.RemoveEmptyEntries);
                for (Int32 i = 0; i < subStrings.Length; i++)
                {
                    tryFindTypeInOtherDictionaries(subStrings[i], result);
                }
            }
        }
    }

    public override void Parse(String inputString, Int32 maxWordsToParse = 5)
    {
        ResetValues();
        ChoosenPatternList = new ChoosenPatternList(inputWords, (Int32) PatternIds.UndefinedPattern);
        base.Parse(inputString, maxWordsToParse);
        prefix = getMatchedValue(prefixesPatternsGroup, 0, Int32.MaxValue, PreferedOccurrence.First,
            PreferedWidth.Max);
        suffix = getMatchedValue(suffixesPatternsGroup, 2, Int32.MaxValue, PreferedOccurrence.Last,
            PreferedWidth.Min);
        //      int minFirstNamePosition = (prefix != null) ? prefix.LastIndex + 1 : 0;
        Int32 minFirstNamePosition = 0; // e.g. "M.D." is defined as prefix and comes after the name...
        firstName = getMatchedValue(firstNamesPatternsGroup, minFirstNamePosition, Int32.MaxValue,
            PreferedOccurrence.First, PreferedWidth.Max);
        Int32 firstNameLastPosition = (firstName != null) ? firstName.LastIndex + 1 : 0;
        Int32 minLastNamePosition = Math.Max(minFirstNamePosition + 1, firstNameLastPosition);
        lastName = getMatchedValue(lastNamesPatternsGroup, minLastNamePosition, Int32.MaxValue,
            PreferedOccurrence.Last, PreferedWidth.Max);
        Int32 lastNameLastPosition = (lastName != null) ? lastName.LastIndex : Int32.MaxValue;
        //suffix = getMatchedValue(suffixesPatternsGroup, 2, int.MaxValue, PreferedOccurrence.Last, PreferedWidth.Min);
        reorganizePartsIfNeededParts();
        if ((firstName != null) && (lastName != null) && (middleName == null))
        {
            middleName = getMatchedValue(middleNamePatternsGroup, minFirstNamePosition, lastNameLastPosition,
                PreferedOccurrence.First, PreferedWidth.Max);
        }

        tryToDetermineUnrecognizedParts();
        if ((lastName == null) && (firstName != null))
        {
            lastName = firstName;
            firstName = null;
        }

        if ((lastName != null) && (suffix != null))
        {
            if (lastName.StartIndex > suffix.StartIndex)
            {
                // swap them...
                ChoosenPattern tmp = lastName;
                lastName = suffix;
                suffix = tmp;
            }
        }
    }

    private void reorganizePartsIfNeededParts()
    {
        if ((suffix != null) && (middleName == null))
        {
            Dictionary<Int32, Boolean> inOtherDictionaries = new Dictionary<Int32, Boolean>();
            tryFindTypeInOtherDictionaries(suffix.Text, inOtherDictionaries);
            if (inOtherDictionaries.Count > 0) // suffix is found in last or first names...
            {
                middleName = lastName;
                lastName = suffix;
                Int32 removedCount = 0;
                ChoosenPatternList.RemovePermanently(ref suffix, ref removedCount);
            }
        }
    }

    public void ResetValues()
    {
        prefix = null;
        firstName = null;
        middleName = null;
        lastName = null;
        suffix = null;
    }

    protected override void tryToDetermineUnrecognizedParts()
    {
        while (true)
        {
            Boolean newDeterminedPatterns = false;
            for (Int32 i = 0; i < ChoosenPatternList.Count; i++)
            {
                ChoosenPattern choosenPattern = ChoosenPatternList[i];
                newDeterminedPatterns |= determineFirstName(choosenPattern);
            }

            if (!newDeterminedPatterns)
            {
                break;
            }
        }
    }

    #endregion

    #region initialization

    private void initPrefixesDictionary()
    {
        lock (PrefixesDictionary)
        {
            if (PrefixesDictionary.Count == 0)
            {
                PrefixesDictionary.Add("dr.");
                PrefixesDictionary.Add("dr");
                PrefixesDictionary.Add("mr.");
                PrefixesDictionary.Add("mr");
                PrefixesDictionary.Add("ms.");
                PrefixesDictionary.Add("ms");
                PrefixesDictionary.Add("seńor");
                PrefixesDictionary.Add("mrs.");
                PrefixesDictionary.Add("mrs");
                PrefixesDictionary.Add("madam");
                PrefixesDictionary.Add("professor");
                PrefixesDictionary.Add("md");
                PrefixesDictionary.Add("md.");
                PrefixesDictionary.Add("m.d.");
                PrefixesDictionary.Add("miss");
                PrefixesDictionary.Add("honorable");
            }
        }
    }

    public static String GetGender(String prefix)
    {
        String result = Constants.GenderConstants.GenderUndefined;
        String lowerPrefix = prefix.ToLower();
        if ((lowerPrefix == "mr") || (lowerPrefix == "mr.") || (lowerPrefix == "seńor"))
        {
            result = Constants.GenderConstants.GenderMale;
        }

        if ((lowerPrefix == "ms") || (lowerPrefix == "mrs") || (lowerPrefix == "ms.") || (lowerPrefix == "mrs.") ||
            (lowerPrefix == "madam"))
        {
            result = Constants.GenderConstants.GenderFemale;
        }

        return result;
    }

    private void initSuffixesDictionary()
    {
        lock (SuffixesDictionary)
        {
            if (SuffixesDictionary.Count == 0)
            {
                SuffixesDictionary.Add("jr.");
                SuffixesDictionary.Add("jr");
                SuffixesDictionary.Add("sr");
                SuffixesDictionary.Add("sr.");
                SuffixesDictionary.Add("ii");
                SuffixesDictionary.Add("iii");
                SuffixesDictionary.Add("iv");
                SuffixesDictionary.Add("v");
                SuffixesDictionary.Add("vi");
                SuffixesDictionary.Add("phd");
                SuffixesDictionary.Add("phd.");
                SuffixesDictionary.Add("ph.d.");
            }
        }
    }

    private void initAndsDictionary()
    {
        lock (andsDictionary)
        {
            if (andsDictionary.Count == 0)
            {
                andsDictionary.Add("and");
                andsDictionary.Add("&");
            }
        }
    }

    #endregion

    #region misc

    private Boolean determineFirstName(ChoosenPattern choosenPattern)
    {
        Boolean result = false;
        if (firstName == null)
        {
            if (choosenPattern.PatternsGroup.PatternId == (Int32) PatternIds.UndefinedPattern)
            {
                if (choosenPattern.StartIndex == 0)
                {
                    firstName = choosenPattern;
                }
                else if (choosenPattern.StartIndex == 1)
                {
                    if (choosenPattern.PreviousPatternId == (Int32) PatternIds.PrefixPattern)
                    {
                        choosenPattern.PatternsGroup.PatternId = (Int32) PatternIds.FirstNamePattern;
                        firstName = choosenPattern;
                        result = true;
                    }
                }
            }
        }

        return result;
    }

    #endregion

    #endregion
}

public class FullNameParserOptimized
{
    private readonly ILogger<FullNameParserOptimized> _logger;

    private readonly PredefinedStringTypes _sharedPredefinedTypes;
    // Pool of parser instances for thread-safe parallel processing
    private readonly ConcurrentBag<FullNameParser> _parserPool;
    private readonly int _maxPoolSize;

    public FullNameParserOptimized(PredefinedStringTypes sharedPredefinedTypes, 
        ILogger<FullNameParserOptimized> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _sharedPredefinedTypes = sharedPredefinedTypes ?? throw new ArgumentNullException(nameof(sharedPredefinedTypes));

        // Create a pool of parsers (2x CPU cores for optimal throughput)
        _maxPoolSize = Environment.ProcessorCount * 2;
        _parserPool = new ConcurrentBag<MatchLogic.Parsers.FullNameParser>();

        // Pre-populate the pool with parser instances
        // Static dictionaries in FullNameParser are thread-safe (use locks)
        for (int i = 0; i < _maxPoolSize; i++)
        {
            _parserPool.Add(CreateParser());
        }

        _logger.LogInformation(
            "Initialized FullNameParser pool with {PoolSize} instances",
            _maxPoolSize);
    }

    /// <summary>
    /// Parses a full name string into its components (thread-safe via pooling)
    /// </summary>
    public FullNameParseResult Parse(string fullName)
    {
        if (string.IsNullOrWhiteSpace(fullName))
        {
            return new FullNameParseResult();
        }

        // Get a parser from the pool (or create a new one if pool is empty)
        if (!_parserPool.TryTake(out var parser))
        {
            // Pool exhausted - create temporary parser
            parser = CreateParser();
            _logger.LogDebug("Parser pool exhausted, created temporary instance");
        }

        try
        {
            // Parse the name (max 10 words to handle complex names)
            parser.Parse(fullName.Trim(), maxWordsToParse: 10);

            // Extract results from parser properties
            var result = new FullNameParseResult
            {
                Prefix = parser.Prefix ?? string.Empty,
                FirstName = parser.FirstName ?? string.Empty,
                MiddleName = parser.MiddleName ?? string.Empty,
                LastName = parser.LastName ?? string.Empty,
                Suffix = parser.Suffix ?? string.Empty
            };

            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error parsing full name: '{FullName}'", fullName);
            return new FullNameParseResult();
        }
        finally
        {
            // Return parser to pool (if pool isn't at max capacity)
            if (_parserPool.Count < _maxPoolSize)
            {
                // Reset the parser state before returning to pool
                parser.ResetValues();
                _parserPool.Add(parser);
            }
            // If pool is full, let the temporary parser be garbage collected
        }
    }

    /// <summary>
    /// Determines gender from a prefix (thread-safe, static method)
    /// </summary>
    public string GetGenderFromPrefix(string prefix)
    {
        if (string.IsNullOrWhiteSpace(prefix))
            return Constants.GenderConstants.GenderUndefined;

        // Use the static method from legacy FullNameParser
        return MatchLogic.Parsers.FullNameParser.GetGender(prefix);
    }

    /// <summary>
    /// Creates a new parser instance
    /// </summary>
    private MatchLogic.Parsers.FullNameParser CreateParser()
    {
        // Create with PredefinedStringTypes which loads name dictionaries
        return new MatchLogic.Parsers.FullNameParser(
            _sharedPredefinedTypes);
    }
}