using System.Collections.Generic;
using System;
using System.Linq;
using MatchLogic.Domain.CleansingAndStandaradization;


namespace MatchLogic.Api.Handlers.Cleansing.Get;

public class CleansingOperatorHelper
{
    public static readonly Dictionary<OperationType, string> OperationTypeOverrides = new()
    {
        { OperationType.Standard, "Standard" },
        { OperationType.Mapping, "Mapping" }
    };

    public static readonly Dictionary<MappingOperationType, string> MappingOperationTypeOverrides = new()
    {
        { MappingOperationType.WordSmith, "Word Smith" },
        { MappingOperationType.AddressParser, "Address Parser" },
        { MappingOperationType.RegexPattern, "Regex Pattern" },
        { MappingOperationType.FirstNameExtractor, "First Name Extractor" },
        { MappingOperationType.FullNameParser, "Full Name Parser" },
        { MappingOperationType.Zip, "Zip" },
        { MappingOperationType.MergeFields, "Merge Fields" },
    };

    public static readonly Dictionary<CleaningRuleType, string> CleaningRuleTypeOverrides = new()
    {
        { CleaningRuleType.Replace, "Characters To Replace" },
        { CleaningRuleType.Remove, "Remove" },
        { CleaningRuleType.Trim, "Trim" },
        { CleaningRuleType.UpperCase, "Upper Case" },
        { CleaningRuleType.LowerCase, "Lower Case" },
        { CleaningRuleType.ReverseCase, "Reverse Case" },
        { CleaningRuleType.ProperCase, "Proper Case" },
        { CleaningRuleType.RemoveSpecialCharacters, "Remove Special Characters" },
        { CleaningRuleType.RemoveNumbers, "Remove Numbers" },
        { CleaningRuleType.RemoveWhiteSpace, "Remove White Space" },
        { CleaningRuleType.RemoveLeadingWhiteSpace, "Remove Leading WhiteSpace" },
        { CleaningRuleType.RemoveTrailingWhiteSpace, "Remove Trailing WhiteSpace" },
        { CleaningRuleType.RemoveNonAlphaNumeric, "Remove Special Characters" },
        { CleaningRuleType.RemoveNonNumeric, "Remove Letters" },
        { CleaningRuleType.RemoveNonPrintable, "Remove non-printable characters" },
        { CleaningRuleType.ReplaceZerosWithOs, "Replace zeros with Os" },
        { CleaningRuleType.ReplaceOsWithZeros, "Replace Os with zeros" },
        { CleaningRuleType.RemoveLetters, "Remove Letters" },
        { CleaningRuleType.ReplacementForNonPrintableCharacters, "Replacement For NonPrintable Characters" },
        { CleaningRuleType.ReplacementForEmptyValues , "Replacement For Empty Values" },
    };

    /// <summary>
    /// Gets parameters for cleaning rule types
    /// </summary>
    public static Dictionary<CleaningRuleType, List<OperationParameter>> GetCleaningRuleParameters()
    {
        return new Dictionary<CleaningRuleType, List<OperationParameter>>
        {
            // Most operations don't need parameters
            [CleaningRuleType.Replace] = new List<OperationParameter>()
            {
                new OperationParameter()
                {
                    Label = "Replacement Rules",
                    Name = "replacements",
                    Required = true,
                    Type = "array", // Special type for complex UI
                    DefaultValue = "[]",
                    // Additional metadata for the UI
                    Description = "Array of replacement rules with pattern, replacement, and caseInsensitive properties"
                }
            },
            [CleaningRuleType.Remove] = new List<OperationParameter>()
            {
                new OperationParameter()
                {
                    Label ="Pattern",
                    Name = "pattern",
                    Required = true,
                    DefaultValue = "",
                    Type = "string",
                }
            },
            [CleaningRuleType.Trim] = new List<OperationParameter>(),
            [CleaningRuleType.ReverseCase] = new List<OperationParameter>(),
            [CleaningRuleType.UpperCase] = new List<OperationParameter>(),
            [CleaningRuleType.LowerCase] = new List<OperationParameter>(),
            [CleaningRuleType.RemoveSpecialCharacters] = new List<OperationParameter>(),
            [CleaningRuleType.RemoveLeadingWhiteSpace] = new List<OperationParameter>(),
            [CleaningRuleType.RemoveTrailingWhiteSpace] = new List<OperationParameter>(),
            [CleaningRuleType.RemoveNonAlphaNumeric] = new List<OperationParameter>(),
            [CleaningRuleType.RemoveNonNumeric] = new List<OperationParameter>(),
            [CleaningRuleType.RemoveNonPrintable] = new List<OperationParameter>(),
            [CleaningRuleType.ReplaceZerosWithOs] = new List<OperationParameter>(),
            [CleaningRuleType.ReplaceOsWithZeros] = new List<OperationParameter>(),
            [CleaningRuleType.RemoveLetters] = new List<OperationParameter>(),
            [CleaningRuleType.ReplacementForNonPrintableCharacters] = new List<OperationParameter>()
            {
                new OperationParameter()
                {
                    Label ="Replacement",
                    Name = "replacement",
                    Required = true,
                    DefaultValue = "",
                    Type = "string",
                }
            },
            [CleaningRuleType.ReplacementForEmptyValues] = new List<OperationParameter>()
            {
                new OperationParameter()
                {
                    Label ="Replacement",
                    Name = "replacement",
                    Required = true,
                    DefaultValue = "",
                    Type = "string",
                }
            },
            [CleaningRuleType.ProperCase] = new List<OperationParameter>()
            {
                new OperationParameter()
                {
                    Label ="Use Proper Case",
                    Name = "use_proper_case",
                    Required = true,
                    DefaultValue = "",
                    Type = "string",
                },
                new OperationParameter()
                {
                    Label ="Mcdonald = McDonald",
                    Name = "to_upper_after_mc",
                    Required = true,
                    DefaultValue = "",
                    Type = "string",
                },
                new OperationParameter()
                {
                    Label ="O'relly = O'Relly",
                    Name = "to_upper_after_o",
                    Required = true,
                    DefaultValue = "",
                    Type = "string",
                },
                new OperationParameter()
                {
                    Label ="Oscar De La Hoya = Oscar de la Hoya",
                    Name = "to_upper_de_la",
                    Required = true,
                    DefaultValue = "",
                    Type = "string",
                },
                new OperationParameter()
                {
                    Label ="Preserve Abbreviations",
                    Name = "leave_abbreviations",
                    Required = true,
                    DefaultValue = "",
                    Type = "string",
                },
            },
        };
    }

    /// <summary>
    /// Gets parameters for mapping operation types
    /// </summary>
    public static Dictionary<MappingOperationType, List<OperationParameter>> GetMappingOperationParameters()
    {
        return new Dictionary<MappingOperationType, List<OperationParameter>>
        {
            // Concatenate parameters
            [MappingOperationType.WordSmith] = new List<OperationParameter>
                {
                    new OperationParameter
                    {
                        Name = "dictionaryId",
                        Label = "Dictionary",
                        Type = "string",
                        Required = true,
                        DefaultValue = " "
                    },
                    new OperationParameter
                    {
                        Name = "separators",
                        Label = "Separators",
                        Type = "string",
                        Required = true,
                        DefaultValue = " ,;"
                    },
                    new OperationParameter
                    {
                        Name = "maxWordCount",
                        Label = "Max Word Count",
                        Type = "int",
                        Required = false,
                        DefaultValue = "3"
                    },
                    new OperationParameter
                    {
                        Name = "flagMode",
                        Label = "Flag Mode",
                        Type = "boolean",
                        Required = false,
                        DefaultValue = "false"
                    },
                },

            // Split parameters
            [MappingOperationType.AddressParser] = new List<OperationParameter>(),

            // Replace parameters
            [MappingOperationType.RegexPattern] = new List<OperationParameter>
                {
                    new OperationParameter
                    {
                        Name = "pattern",
                        Label = "Pattern",
                        Type = "string",
                        Required = true
                    },
                    new OperationParameter
                    {
                        Name = "useAdvancedFunctionality",
                        Label = "Use Advanced Functionality",
                        Type = "boolean",
                        Required = false
                    },
                    new OperationParameter
                    {
                        Name = "outputFormat",
                        Label = "Output Format",
                        Type = "string",
                        Required = false
                    }
                },
            [MappingOperationType.FirstNameExtractor] = new List<OperationParameter>(),
            [MappingOperationType.FullNameParser] = new List<OperationParameter>(),
            [MappingOperationType.Zip] = new List<OperationParameter>(),
            // Replace parameters
            [MappingOperationType.MergeFields] = new List<OperationParameter>
                {
                    new OperationParameter
                    {
                        Name = "delimiter",
                        Label = "Delimiter",
                        Type = "string",
                        Required = true,
                        DefaultValue = ","
                    },
                    new OperationParameter
                    {
                        Name = "onlyFirstNotEmptyCount",
                        Label = "Merge only first not empty values",
                        Type = "int",
                        Required = true,
                        DefaultValue = "0",
                    },
                    new OperationParameter
                    {
                        Name = "skipEmpty",
                        Label = "Skip Empty",
                        Type = "boolean",
                        Required = false
                    },
                    new OperationParameter
                    {
                        Name = "trimValues",
                        Label = "Trim Values",
                        Type = "boolean",
                        Required = false
                    },
                },
        };
    }

    /// <summary>
    /// Gets mapping requirements for mapping operations
    /// </summary>
    public static Dictionary<MappingOperationType, MappingRequirements> GetMappingRequirements()
    {
        return new Dictionary<MappingOperationType, MappingRequirements>
        {
            [MappingOperationType.WordSmith] = new MappingRequirements
            {
                RequiresSourceColumns = true,
                RequiresOutputColumns = false
            },
            [MappingOperationType.AddressParser] = new MappingRequirements
            {
                RequiresSourceColumns = true,
                RequiresOutputColumns = false
            },
            [MappingOperationType.RegexPattern] = new MappingRequirements
            {
                RequiresSourceColumns = true,
                RequiresOutputColumns = false
            },
            [MappingOperationType.FirstNameExtractor] = new MappingRequirements
            {
                RequiresSourceColumns = true,
                RequiresOutputColumns = false
            },
            [MappingOperationType.FullNameParser] = new MappingRequirements
            {
                RequiresSourceColumns = true,
                RequiresOutputColumns = false
            },
            [MappingOperationType.Zip] = new MappingRequirements
            {
                RequiresSourceColumns = true,
                RequiresOutputColumns = false
            },
            [MappingOperationType.MergeFields] = new MappingRequirements
            {
                RequiresSourceColumns = true,
                RequiresOutputColumns = true
            },
        };
    }


    public static List<KeyValuePair<int, string>> GetEnumKeyValueList<TEnum>(
    Func<TEnum, string> displayNameSelector = null) where TEnum : Enum
    {
        return Enum.GetValues(typeof(TEnum))
                   .Cast<TEnum>()
                   .Select(e => new KeyValuePair<int, string>(
                       Convert.ToInt32(e),
                       displayNameSelector?.Invoke(e) ?? e.ToString()
                   ))
                   .ToList();
    }


}


