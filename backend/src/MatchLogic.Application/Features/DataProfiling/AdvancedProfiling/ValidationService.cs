using MatchLogic.Application.Interfaces.DataProfiling;
using MatchLogic.Domain.DataProfiling;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataProfiling.AdvancedProfiling
{
    public class ValidationService : IValidationRuleService
    {
        private readonly ILogger<ValidationService> _logger;
        private readonly Dictionary<string, Func<AdvancedColumnProfile, bool>> _ruleSelectors;
        private readonly Dictionary<string, Func<object, ValidationRule, bool, string>> _validators;
        private readonly ReaderWriterLockSlim _validationLock = new(LockRecursionPolicy.SupportsRecursion);

        public ValidationService(ILogger<ValidationService> logger)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));

            // Define rule selectors based on column properties
            _ruleSelectors = new Dictionary<string, Func<AdvancedColumnProfile, bool>>
            {
                ["NotNull"] = profile => profile.Null > 0,
                ["MinLength"] = profile => profile.Type == "String" && profile.Length > 0,
                ["MaxLength"] = profile => profile.Type == "String" && profile.Length > 0,
                ["ValidEmail"] = profile => profile.Type == "String" &&
                                         (profile.PossibleSemanticTypes?.Any(t => t.Type == "EmailAddress") == true ||
                                          profile.Pattern == "Email"),
                ["ValidURL"] = profile => profile.Type == "String" &&
                                       (profile.PossibleSemanticTypes?.Any(t => t.Type == "URL") == true ||
                                        profile.Pattern == "URL"),
                ["ValidPhoneNumber"] = profile => profile.Type == "String" &&
                                              (profile.PossibleSemanticTypes?.Any(t => t.Type == "PhoneNumber") == true ||
                                               profile.Pattern == "PhoneUS"),
                ["ValidZipCode"] = profile => profile.Type == "String" &&
                                         (profile.PossibleSemanticTypes?.Any(t => t.Type == "ZipCode") == true ||
                                          profile.Pattern == "ZipCodeUS"),
                ["NumericRange"] = profile => profile.Type == "Integer" || profile.Type == "Decimal",
                ["DateRange"] = profile => profile.Type == "DateTime",
                ["NoOutliers"] = profile => (profile.Type == "Integer" || profile.Type == "Decimal") &&
                                        profile.Outliers?.Count > 0,
                ["PatternMatch"] = profile => profile.Type == "String" && !string.IsNullOrEmpty(profile.Pattern) &&
                                          profile.Pattern != "Unclassified",
                ["UniqueValues"] = profile => profile.Distinct < profile.Filled,
                ["NoLeadingSpaces"] = profile => profile.LeadingSpaces > 0,
                ["NoPunctuation"] = profile => profile.Type == "String" && profile.Punctuation > 0 &&
                                           profile.Pattern != "Email" && profile.Pattern != "URL"
            };

            // Define validators for each rule type
            _validators = new Dictionary<string, Func<object, ValidationRule, bool, string>>
            {
                ["NotNull"] = (value, rule, caseSensitive) =>
                    value != null ? null : "Value cannot be null",

                ["NotEmpty"] = (value, rule, caseSensitive) =>
                    value != null && value.ToString() != string.Empty ? null : "Value cannot be empty",

                ["MinLength"] = (value, rule, caseSensitive) => {
                    if (value == null) return "Value cannot be null";

                    string strValue = value.ToString();
                    if (GetRuleParamAsInt(rule, "MinLength", out int minLength) && strValue.Length < minLength)
                        return $"Length must be at least {minLength} characters";

                    return null;
                },

                ["MaxLength"] = (value, rule, caseSensitive) => {
                    if (value == null) return null; // Null values are handled by NotNull rule

                    string strValue = value.ToString();
                    if (GetRuleParamAsInt(rule, "MaxLength", out int maxLength) && strValue.Length > maxLength)
                        return $"Length must not exceed {maxLength} characters";

                    return null;
                },

                ["ValidEmail"] = (value, rule, caseSensitive) => {
                    if (value == null) return null; // Null values are handled by NotNull rule

                    string strValue = value.ToString();
                    if (string.IsNullOrEmpty(strValue)) return null; // Empty values handled by NotEmpty rule

                    // Simple regex for email validation
                    if (!System.Text.RegularExpressions.Regex.IsMatch(strValue, @"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"))
                        return "Invalid email address format";

                    return null;
                },

                ["ValidURL"] = (value, rule, caseSensitive) => {
                    if (value == null) return null;

                    string strValue = value.ToString();
                    if (string.IsNullOrEmpty(strValue)) return null;

                    // Simple regex for URL validation
                    if (!System.Text.RegularExpressions.Regex.IsMatch(strValue, @"^(https?:\/\/)?(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)$"))
                        return "Invalid URL format";

                    return null;
                },

                ["ValidPhoneNumber"] = (value, rule, caseSensitive) => {
                    if (value == null) return null;

                    string strValue = value.ToString();
                    if (string.IsNullOrEmpty(strValue)) return null;

                    // Phone number validation (US format)
                    if (!System.Text.RegularExpressions.Regex.IsMatch(strValue, @"^\(?([0-9]{3})\)?[-. ]?([0-9]{3})[-. ]?([0-9]{4})$"))
                        return "Invalid phone number format";

                    return null;
                },

                ["ValidZipCode"] = (value, rule, caseSensitive) => {
                    if (value == null) return null;

                    string strValue = value.ToString();
                    if (string.IsNullOrEmpty(strValue)) return null;

                    // US Zip code validation
                    if (!System.Text.RegularExpressions.Regex.IsMatch(strValue, @"^\d{5}(-\d{4})?$"))
                        return "Invalid ZIP code format";

                    return null;
                },

                ["NumericRange"] = (value, rule, caseSensitive) => {
                    if (value == null) return null;

                    if (!TryConvertToDouble(value, out double numValue))
                        return "Value is not a valid number";

                    if (GetRuleParamAsDouble(rule, "Min", out double min) && numValue < min)
                        return $"Value must be at least {min}";

                    if (GetRuleParamAsDouble(rule, "Max", out double max) && numValue > max)
                        return $"Value must not exceed {max}";

                    return null;
                },

                ["DateRange"] = (value, rule, caseSensitive) => {
                    if (value == null) return null;

                    DateTime date;
                    if (value is DateTime dt)
                    {
                        date = dt;
                    }
                    else if (!DateTime.TryParse(value.ToString(), out date))
                    {
                        return "Value is not a valid date";
                    }

                    if (GetRuleParamAsDateTime(rule, "MinDate", out DateTime minDate) && date < minDate)
                        return $"Date must be on or after {minDate:d}";

                    if (GetRuleParamAsDateTime(rule, "MaxDate", out DateTime maxDate) && date > maxDate)
                        return $"Date must be on or before {maxDate:d}";

                    return null;
                },

                ["NoOutliers"] = (value, rule, caseSensitive) => {
                    if (value == null) return null;

                    if (!TryConvertToDouble(value, out double numValue))
                        return null; // Not applicable for non-numeric values

                    if (GetRuleParamAsDouble(rule, "LowerBound", out double lowerBound) &&
                        GetRuleParamAsDouble(rule, "UpperBound", out double upperBound))
                    {
                        if (numValue < lowerBound || numValue > upperBound)
                            return $"Value is an outlier (outside range {lowerBound} to {upperBound})";
                    }

                    return null;
                },

                ["PatternMatch"] = (value, rule, caseSensitive) => {
                    if (value == null) return null;

                    string strValue = value.ToString();
                    if (string.IsNullOrEmpty(strValue)) return null;

                    if (GetRuleParamAsString(rule, "Pattern", out string pattern))
                    {
                        try
                        {
                            // Check if it's a named pattern first
                            switch (pattern)
                            {
                                case "Email":
                                    if (!System.Text.RegularExpressions.Regex.IsMatch(strValue, @"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"))
                                        return "Not a valid email format";
                                    break;

                                case "URL":
                                    if (!System.Text.RegularExpressions.Regex.IsMatch(strValue, @"^(https?:\/\/)?(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)$"))
                                        return "Not a valid URL format";
                                    break;

                                case "PhoneUS":
                                    if (!System.Text.RegularExpressions.Regex.IsMatch(strValue, @"^\(?([0-9]{3})\)?[-. ]?([0-9]{3})[-. ]?([0-9]{4})$"))
                                        return "Not a valid phone number format";
                                    break;

                                case "ZipCodeUS":
                                    if (!System.Text.RegularExpressions.Regex.IsMatch(strValue, @"^\d{5}(-\d{4})?$"))
                                        return "Not a valid ZIP code format";
                                    break;

                                default:
                                    // Try to interpret as a regex pattern
                                    try
                                    {
                                        if (!System.Text.RegularExpressions.Regex.IsMatch(strValue, pattern))
                                            return $"Value does not match pattern: {pattern}";
                                    }
                                    catch (ArgumentException)
                                    {
                                        // Handle invalid regex pattern
                                        return "Invalid pattern specified in rule";
                                    }
                                    break;
                            }
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning(ex, "Error in pattern matching for rule {RuleName}", rule.RuleName);
                            return "Invalid pattern matching rule";
                        }
                    }

                    return null;
                },

                ["UniqueValues"] = (value, rule, caseSensitive) => {
                    // This rule is more complex and requires dataset context
                    // Not implemented here as it requires knowledge of all values in the dataset
                    // Should be handled separately in a specialized validation method
                    return null;
                },

                ["NoLeadingSpaces"] = (value, rule, caseSensitive) => {
                    if (value == null) return null;

                    string strValue = value.ToString();
                    if (string.IsNullOrEmpty(strValue)) return null;

                    if (strValue.Length > 0 && char.IsWhiteSpace(strValue[0]))
                        return "Value should not have leading whitespace";

                    return null;
                },

                ["NoPunctuation"] = (value, rule, caseSensitive) => {
                    if (value == null) return null;

                    string strValue = value.ToString();
                    if (string.IsNullOrEmpty(strValue)) return null;

                    if (strValue.Any(char.IsPunctuation))
                        return "Value should not contain punctuation";

                    return null;
                }
            };
        }

        /// <summary>
        /// Gets applicable validation rules for a column
        /// </summary>
        public async Task<List<ValidationRule>> GetApplicableRulesAsync(AdvancedColumnProfile columnProfile)
        {
            if (columnProfile == null)
                throw new ArgumentNullException(nameof(columnProfile));

            var rules = new List<ValidationRule>();

            try
            {
                // Select rules based on column properties
                foreach (var selector in _ruleSelectors)
                {
                    if (selector.Value(columnProfile))
                    {
                        ValidationRule rule = CreateRule(selector.Key, columnProfile);
                        if (rule != null)
                        {
                            rules.Add(rule);
                        }
                    }
                }

                // Add properties to rules for proper validation
                foreach (var rule in rules)
                {
                    SetRuleParameters(rule, columnProfile);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting applicable rules for column {ColumnName}", columnProfile.FieldName);
            }

            return rules;
        }

        /// <summary>
        /// Validates values against rules
        /// </summary>
        public async Task<List<ValidationViolation>> ValidateAsync(
            IEnumerable<object> values,
            IEnumerable<ValidationRule> rules)
        {
            var violations = new List<ValidationViolation>();
            var valuesList = values?.ToList() ?? new List<object>();
            var rulesList = rules?.ToList() ?? new List<ValidationRule>();

            if (rulesList.Count == 0 || valuesList.Count == 0)
            {
                return violations;
            }

            try
            {
                // Process each rule
                foreach (var rule in rulesList)
                {
                    bool caseSensitive = GetRuleParamAsBool(rule, "CaseSensitive", false);

                    if (_validators.TryGetValue(rule.RuleName, out var validator))
                    {
                        int passCount = 0;
                        int failCount = 0;
                        var violationExamples = new List<string>();

                        // Validate each value against this rule
                        foreach (var value in valuesList)
                        {
                            string validationMessage = validator(value, rule, caseSensitive);

                            if (validationMessage != null)
                            {
                                failCount++;

                                // Store violation example (up to 5 per rule)
                                if (violationExamples.Count < 5)
                                {
                                    violationExamples.Add(value?.ToString() ?? "null");
                                }
                            }
                            else
                            {
                                passCount++;
                            }
                        }

                        // Update rule statistics
                        rule.PassCount = passCount;
                        rule.FailCount = failCount;

                        // Create violation if any failures occurred
                        if (failCount > 0)
                        {
                            violations.Add(new ValidationViolation
                            {
                                RuleName = rule.RuleName,
                                Message = rule.Description,
                                Examples = violationExamples.Select(v => new RowReference { Value = v }).ToList()
                            });
                        }
                    }
                    else
                    {
                        _logger.LogWarning("No validator found for rule {RuleName}", rule.RuleName);
                    }
                }

                // Handle special case for UniqueValues rule which needs all values
                HandleUniqueValuesValidation(valuesList, rulesList, violations);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error validating data against rules");
            }

            return violations;
        }

        /// <summary>
        /// Special handling for unique values validation that requires all values
        /// </summary>
        private void HandleUniqueValuesValidation(
            List<object> values,
            List<ValidationRule> rules,
            List<ValidationViolation> violations)
        {
            var uniqueRule = rules.FirstOrDefault(r => r.RuleName == "UniqueValues");
            if (uniqueRule == null)
                return;

            var duplicates = new Dictionary<string, List<object>>();
            var valueCounts = new Dictionary<string, int>();

            // Count occurrences of each value
            foreach (var value in values)
            {
                if (value == null)
                    continue;

                string key = value.ToString();

                if (!valueCounts.ContainsKey(key))
                {
                    valueCounts[key] = 0;
                    duplicates[key] = new List<object>();
                }

                valueCounts[key]++;

                // Keep track of duplicate values
                if (valueCounts[key] > 1 && duplicates[key].Count < 5)
                {
                    duplicates[key].Add(value);
                }
            }

            // Find duplicates
            var duplicateValues = valueCounts
                .Where(kv => kv.Value > 1)
                .Select(kv => kv.Key)
                .ToList();

            int passCount = values.Count - duplicateValues.Count;
            int failCount = duplicateValues.Count;

            // Update rule statistics
            uniqueRule.PassCount = passCount;
            uniqueRule.FailCount = failCount;

            // Add violation if duplicates found
            if (duplicateValues.Any())
            {
                var examples = duplicateValues
                    .Take(5)
                    .Select(v => new RowReference { Value = v })
                    .ToList();

                violations.Add(new ValidationViolation
                {
                    RuleName = "UniqueValues",
                    Message = uniqueRule.Description,
                    Examples = examples
                });
            }
        }

        /// <summary>
        /// Creates a rule object based on the rule type and column profile
        /// </summary>
        private ValidationRule CreateRule(string ruleType, AdvancedColumnProfile profile)
        {
            switch (ruleType)
            {
                case "NotNull":
                    return new ValidationRule
                    {
                        RuleName = "NotNull",
                        Description = "Value should not be null",
                        Parameters = new Dictionary<string, string>()
                    };

                case "NotEmpty":
                    return new ValidationRule
                    {
                        RuleName = "NotEmpty",
                        Description = "Value should not be empty",
                        Parameters = new Dictionary<string, string>()
                    };

                case "MinLength":
                    int minLength = Math.Max(1, (int)(profile?.Length ?? 1));
                    return new ValidationRule
                    {
                        RuleName = "MinLength",
                        Description = $"Minimum length: {minLength}",
                        Parameters = new Dictionary<string, string>
                        {
                            ["MinLength"] = minLength.ToString()
                        }
                    };

                case "MaxLength":
                    int maxLength = (int)(profile.Length);
                    return new ValidationRule
                    {
                        RuleName = "MaxLength",
                        Description = $"Maximum length: {maxLength}",
                        Parameters = new Dictionary<string, string>
                        {
                            ["MaxLength"] = maxLength.ToString()
                        }
                    };

                case "ValidEmail":
                    return new ValidationRule
                    {
                        RuleName = "ValidEmail",
                        Description = "Must be a valid email address",
                        Parameters = new Dictionary<string, string>()
                    };

                case "ValidURL":
                    return new ValidationRule
                    {
                        RuleName = "ValidURL",
                        Description = "Must be a valid URL",
                        Parameters = new Dictionary<string, string>()
                    };

                case "ValidPhoneNumber":
                    return new ValidationRule
                    {
                        RuleName = "ValidPhoneNumber",
                        Description = "Must be a valid phone number",
                        Parameters = new Dictionary<string, string>()
                    };

                case "ValidZipCode":
                    return new ValidationRule
                    {
                        RuleName = "ValidZipCode",
                        Description = "Must be a valid ZIP code",
                        Parameters = new Dictionary<string, string>()
                    };

                case "NumericRange":
                    double? min = null;
                    double? max = null;

                    if (!string.IsNullOrEmpty(profile.Min) && double.TryParse(profile.Min, out double minValue))
                    {
                        min = minValue;
                    }

                    if (!string.IsNullOrEmpty(profile.Max) && double.TryParse(profile.Max, out double maxValue))
                    {
                        max = maxValue;
                    }

                    if (min != null || max != null)
                    {
                        string description = "Value must be ";
                        var parameters = new Dictionary<string, string>();

                        if (min != null && max != null)
                        {
                            description += $"between {min} and {max}";
                            parameters["Min"] = min.ToString();
                            parameters["Max"] = max.ToString();
                        }
                        else if (min != null)
                        {
                            description += $"greater than or equal to {min}";
                            parameters["Min"] = min.ToString();
                        }
                        else
                        {
                            description += $"less than or equal to {max}";
                            parameters["Max"] = max.ToString();
                        }

                        return new ValidationRule
                        {
                            RuleName = "NumericRange",
                            Description = description,
                            Parameters = parameters
                        };
                    }
                    break;

                case "DateRange":
                    DateTime? minDate = null;
                    DateTime? maxDate = null;

                    // Try to determine date ranges from profile data
                    if (!string.IsNullOrEmpty(profile.Min) && DateTime.TryParse(profile.Min, out DateTime minDateValue))
                    {
                        minDate = minDateValue;
                    }

                    if (!string.IsNullOrEmpty(profile.Max) && DateTime.TryParse(profile.Max, out DateTime maxDateValue))
                    {
                        maxDate = maxDateValue;
                    }

                    if (minDate != null || maxDate != null)
                    {
                        string description = "Date must be ";
                        var parameters = new Dictionary<string, string>();

                        if (minDate != null && maxDate != null)
                        {
                            description += $"between {minDate:d} and {maxDate:d}";
                            parameters["MinDate"] = minDate.Value.ToString("o");
                            parameters["MaxDate"] = maxDate.Value.ToString("o");
                        }
                        else if (minDate != null)
                        {
                            description += $"on or after {minDate:d}";
                            parameters["MinDate"] = minDate.Value.ToString("o");
                        }
                        else
                        {
                            description += $"on or before {maxDate:d}";
                            parameters["MaxDate"] = maxDate.Value.ToString("o");
                        }

                        return new ValidationRule
                        {
                            RuleName = "DateRange",
                            Description = description,
                            Parameters = parameters
                        };
                    }
                    break;

                case "NoOutliers":
                    if (profile.Outliers?.Count > 0)
                    {
                        // Calculate outlier bounds if available
                        double lowerBound = 0;
                        double upperBound = 0;

                        if (double.TryParse(profile.Outliers.First().Value, out double lower) &&
                            double.TryParse(profile.Outliers.Last().Value, out double upper))
                        {
                            lowerBound = lower;
                            upperBound = upper;

                            return new ValidationRule
                            {
                                RuleName = "NoOutliers",
                                Description = $"Value should not be an outlier (outside range {lowerBound} to {upperBound})",
                                Parameters = new Dictionary<string, string>
                                {
                                    ["LowerBound"] = lowerBound.ToString(),
                                    ["UpperBound"] = upperBound.ToString()
                                }
                            };
                        }
                    }
                    break;

                case "PatternMatch":
                    if (!string.IsNullOrEmpty(profile.Pattern) && profile.Pattern != "Unclassified")
                    {
                        return new ValidationRule
                        {
                            RuleName = "PatternMatch",
                            Description = $"Must match pattern: {profile.Pattern}",
                            Parameters = new Dictionary<string, string>
                            {
                                ["Pattern"] = profile.Pattern
                            }
                        };
                    }
                    break;

                case "UniqueValues":
                    if (profile.Distinct < profile.Filled)
                    {
                        return new ValidationRule
                        {
                            RuleName = "UniqueValues",
                            Description = "Values should be unique",
                            Parameters = new Dictionary<string, string>()
                        };
                    }
                    break;

                case "NoLeadingSpaces":
                    if (profile.LeadingSpaces > 0)
                    {
                        return new ValidationRule
                        {
                            RuleName = "NoLeadingSpaces",
                            Description = "Values should not have leading spaces",
                            Parameters = new Dictionary<string, string>()
                        };
                    }
                    break;

                case "NoPunctuation":
                    if (profile.Punctuation > 0)
                    {
                        return new ValidationRule
                        {
                            RuleName = "NoPunctuation",
                            Description = "Values should not contain punctuation",
                            Parameters = new Dictionary<string, string>()
                        };
                    }
                    break;
            }

            return null;
        }

        /// <summary>
        /// Set rule parameters based on column profile
        /// </summary>
        private void SetRuleParameters(ValidationRule rule, AdvancedColumnProfile profile)
        {
            // Initialize parameters dictionary if needed
            if (rule.Parameters == null)
            {
                rule.Parameters = new Dictionary<string, string>();
            }

            // Add common parameters
            rule.Parameters["ColumnName"] = profile.FieldName;

            // Add rule-specific parameters
            switch (rule.RuleName)
            {
                case "MinLength":
                    if (!rule.Parameters.ContainsKey("MinLength"))
                    {
                        int minLength = Math.Max(1, (int)(profile?.Length ?? 1));
                        rule.Parameters["MinLength"] = minLength.ToString();
                    }
                    break;

                case "MaxLength":
                    if (!rule.Parameters.ContainsKey("MaxLength"))
                    {
                        int maxLength = (int)(profile?.Length ?? profile.Length);
                        rule.Parameters["MaxLength"] = maxLength.ToString();
                    }
                    break;

                case "NumericRange":
                    if (!string.IsNullOrEmpty(profile.Min) && double.TryParse(profile.Min, out double minValue) &&
                        !rule.Parameters.ContainsKey("Min"))
                    {
                        rule.Parameters["Min"] = minValue.ToString();
                    }

                    if (!string.IsNullOrEmpty(profile.Max) && double.TryParse(profile.Max, out double maxValue) &&
                        !rule.Parameters.ContainsKey("Max"))
                    {
                        rule.Parameters["Max"] = maxValue.ToString();
                    }
                    break;

                case "NoOutliers":
                    if (!rule.Parameters.ContainsKey("LowerBound") &&
                        !string.IsNullOrEmpty(profile.Outliers.First().Value) &&
                        double.TryParse(profile.Outliers.Last().Value, out double lowerBound))
                    {
                        rule.Parameters["LowerBound"] = lowerBound.ToString();
                    }

                    if (!rule.Parameters.ContainsKey("UpperBound") &&
                        !string.IsNullOrEmpty(profile.Outliers.Last().Value) &&
                        double.TryParse(profile.Outliers.Last().Value, out double upperBound))
                    {
                        rule.Parameters["UpperBound"] = upperBound.ToString();
                    }
                    break;
            }
        }

        #region Helper Methods

        private bool GetRuleParamAsString(ValidationRule rule, string paramName, out string value)
        {
            value = null;

            // First try to get directly from Parameters dictionary
            if (rule.Parameters != null && rule.Parameters.TryGetValue(paramName, out value))
            {
                return true;
            }

            // Fall back to parsing from description
            string pattern = $"{paramName}: ([^;]+)";
            var match = System.Text.RegularExpressions.Regex.Match(rule.Description, pattern);

            if (match.Success)
            {
                value = match.Groups[1].Value;
                return true;
            }

            // Check special cases based on rule name and description
            if (rule.RuleName == "PatternMatch" && rule.Description.StartsWith("Must match pattern:"))
            {
                value = rule.Description.Substring("Must match pattern:".Length).Trim();
                return true;
            }

            return false;
        }

        private bool GetRuleParamAsInt(ValidationRule rule, string paramName, out int value)
        {
            value = 0;

            // First try to get directly from Parameters dictionary
            if (rule.Parameters != null && rule.Parameters.TryGetValue(paramName, out string strValue) &&
                int.TryParse(strValue, out value))
            {
                return true;
            }

            // Fall back to parsing from description
            if (GetRuleParamAsString(rule, paramName, out strValue))
            {
                return int.TryParse(strValue, out value);
            }

            // Special cases based on rule description
            if (paramName == "MinLength" && rule.Description.StartsWith("Minimum length:"))
            {
                string lenStr = rule.Description.Substring("Minimum length:".Length).Trim();
                return int.TryParse(lenStr, out value);
            }
            else if (paramName == "MaxLength" && rule.Description.StartsWith("Maximum length:"))
            {
                string lenStr = rule.Description.Substring("Maximum length:".Length).Trim();
                return int.TryParse(lenStr, out value);
            }

            return false;
        }

        private bool GetRuleParamAsDouble(ValidationRule rule, string paramName, out double value)
        {
            value = 0;

            // First try to get directly from Parameters dictionary
            if (rule.Parameters != null && rule.Parameters.TryGetValue(paramName, out string strValue) &&
                double.TryParse(strValue, out value))
            {
                return true;
            }

            // Fall back to parsing from description
            if (GetRuleParamAsString(rule, paramName, out strValue))
            {
                return double.TryParse(strValue, out value);
            }

            // Parse from description for numeric range
            if (rule.RuleName == "NumericRange")
            {
                if (paramName == "Min" && rule.Description.Contains("greater than or equal to"))
                {
                    var match = System.Text.RegularExpressions.Regex.Match(rule.Description, @"greater than or equal to\s+([\d.-]+)");
                    if (match.Success)
                    {
                        return double.TryParse(match.Groups[1].Value, out value);
                    }
                }
                else if (paramName == "Min" && rule.Description.Contains("between"))
                {
                    var match = System.Text.RegularExpressions.Regex.Match(rule.Description, @"between\s+([\d.-]+)\s+and");
                    if (match.Success)
                    {
                        return double.TryParse(match.Groups[1].Value, out value);
                    }
                }
                else if (paramName == "Max" && rule.Description.Contains("less than or equal to"))
                {
                    var match = System.Text.RegularExpressions.Regex.Match(rule.Description, @"less than or equal to\s+([\d.-]+)");
                    if (match.Success)
                    {
                        return double.TryParse(match.Groups[1].Value, out value);
                    }
                }
                else if (paramName == "Max" && rule.Description.Contains("between"))
                {
                    var match = System.Text.RegularExpressions.Regex.Match(rule.Description, @"and\s+([\d.-]+)");
                    if (match.Success)
                    {
                        return double.TryParse(match.Groups[1].Value, out value);
                    }
                }
            }

            return false;
        }

        private bool GetRuleParamAsDateTime(ValidationRule rule, string paramName, out DateTime value)
        {
            value = DateTime.MinValue;

            // First try to get directly from Parameters dictionary
            if (rule.Parameters != null && rule.Parameters.TryGetValue(paramName, out string strValue) &&
                DateTime.TryParse(strValue, out value))
            {
                return true;
            }

            // Fall back to parsing from description
            if (GetRuleParamAsString(rule, paramName, out strValue))
            {
                return DateTime.TryParse(strValue, out value);
            }

            // Parse from description for date range
            if (rule.RuleName == "DateRange")
            {
                if (paramName == "MinDate" && rule.Description.Contains("on or after"))
                {
                    var match = System.Text.RegularExpressions.Regex.Match(rule.Description, @"on or after\s+(.+)");
                    if (match.Success)
                    {
                        return DateTime.TryParse(match.Groups[1].Value, out value);
                    }
                }
                else if (paramName == "MinDate" && rule.Description.Contains("between"))
                {
                    var match = System.Text.RegularExpressions.Regex.Match(rule.Description, @"between\s+(.+)\s+and");
                    if (match.Success)
                    {
                        return DateTime.TryParse(match.Groups[1].Value, out value);
                    }
                }
                else if (paramName == "MaxDate" && rule.Description.Contains("on or before"))
                {
                    var match = System.Text.RegularExpressions.Regex.Match(rule.Description, @"on or before\s+(.+)");
                    if (match.Success)
                    {
                        return DateTime.TryParse(match.Groups[1].Value, out value);
                    }
                }
                else if (paramName == "MaxDate" && rule.Description.Contains("between"))
                {
                    var match = System.Text.RegularExpressions.Regex.Match(rule.Description, @"and\s+(.+)");
                    if (match.Success)
                    {
                        return DateTime.TryParse(match.Groups[1].Value, out value);
                    }
                }
            }

            return false;
        }

        private bool GetRuleParamAsBool(ValidationRule rule, string paramName, bool defaultValue)
        {
            // First try to get directly from Parameters dictionary
            if (rule.Parameters != null && rule.Parameters.TryGetValue(paramName, out string strValue))
            {
                if (bool.TryParse(strValue, out bool value))
                {
                    return value;
                }
            }

            // Fall back to parsing from description
            if (GetRuleParamAsString(rule, paramName, out strValue))
            {
                if (bool.TryParse(strValue, out bool value))
                {
                    return value;
                }
            }

            return defaultValue;
        }

        /// <summary>
        /// Try to convert various value types to a double
        /// </summary>
        private bool TryConvertToDouble(object value, out double result)
        {
            result = 0;

            if (value == null)
                return false;

            if (value is double d)
            {
                result = d;
                return true;
            }

            if (value is int i)
            {
                result = i;
                return true;
            }

            if (value is long l)
            {
                result = l;
                return true;
            }

            if (value is decimal m)
            {
                result = (double)m;
                return true;
            }

            if (value is float f)
            {
                result = f;
                return true;
            }

            if (value is byte b)
            {
                result = b;
                return true;
            }

            if (value is sbyte sb)
            {
                result = sb;
                return true;
            }

            if (value is short s)
            {
                result = s;
                return true;
            }

            if (value is ushort us)
            {
                result = us;
                return true;
            }

            if (value is uint ui)
            {
                result = ui;
                return true;
            }

            if (value is ulong ul && ul <= long.MaxValue)
            {
                result = ul;
                return true;
            }

            if (value is bool bl)
            {
                result = bl ? 1 : 0;
                return true;
            }

            // Try parsing string value
            return double.TryParse(value.ToString(), out result);
        }

        #endregion
    }
}
