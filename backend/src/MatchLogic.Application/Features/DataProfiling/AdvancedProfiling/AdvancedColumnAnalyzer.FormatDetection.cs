using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataProfiling.AdvancedProfiling
{
    public partial class AdvancedColumnAnalyzer
    {
        /// <summary>
        /// Detect common format patterns in strings
        /// </summary>
        private void DetectFormat(string value)
        {
            if (string.IsNullOrEmpty(value))
                return;

            // Try date formats
            if (DateTime.TryParse(value, out _))
            {
                // Detect common date formats
                if (value.Contains("-"))
                {
                    if (value.Length == 10 && value[4] == '-' && value[7] == '-')
                    {
                        IncrementFormatPattern("yyyy-MM-dd");
                    }
                }
                else if (value.Contains("/"))
                {
                    if (value.Length == 10 && value[2] == '/' && value[5] == '/')
                    {
                        IncrementFormatPattern("MM/dd/yyyy");
                    }
                    else if (value.Length == 10 && value[4] == '/' && value[7] == '/')
                    {
                        IncrementFormatPattern("yyyy/MM/dd");
                    }
                }
                return;
            }

            // Number formats
            if (double.TryParse(value, out _))
            {
                if (value.Contains("."))
                {
                    IncrementFormatPattern("Decimal");

                    // Check for currency format
                    if (value.StartsWith("$") || value.StartsWith("€") || value.StartsWith("£"))
                    {
                        IncrementFormatPattern("Currency");
                    }
                }
                else
                {
                    IncrementFormatPattern("Integer");
                }
                return;
            }

            // Phone number format
            if (value.Length >= 10 && value.Count(c => char.IsDigit(c)) >= 10)
            {
                if (value.Contains("-") || value.Contains("(") || value.Contains(")") || value.Contains(" "))
                {
                    IncrementFormatPattern("PhoneNumber");
                    return;
                }
            }

            // Email format
            if (value.Contains("@") && value.Contains("."))
            {
                IncrementFormatPattern("Email");
                return;
            }

            // URL format
            if (value.StartsWith("http://") || value.StartsWith("https://") || value.StartsWith("www."))
            {
                IncrementFormatPattern("URL");
                return;
            }

            // IP address format
            if (value.Count(c => c == '.') == 3 && value.Split('.').All(part => int.TryParse(part, out var num) && num >= 0 && num <= 255))
            {
                IncrementFormatPattern("IPAddress");
                return;
            }

            // GUID format
            if (Guid.TryParse(value, out _))
            {
                IncrementFormatPattern("GUID");
                return;
            }

            // Default format
            IncrementFormatPattern("Text");
        }

        /// <summary>
        /// Increment the counter for a detected format pattern
        /// </summary>
        private void IncrementFormatPattern(string format)
        {
            _formatPatterns.AddOrUpdate(format, 1, (_, count) => count + 1);
        }

        /// <summary>
        /// Get examples for a detected format
        /// </summary>
        public List<string> GetFormatExamples(string format)
        {
            // Get examples from value rows with this format
            var examples = new List<string>();

            try
            {
                // Look for values matching the format
                foreach (var entry in _valueRows.Take(50))
                {
                    string value = entry.Key;

                    bool isMatch = false;

                    switch (format)
                    {
                        case "yyyy-MM-dd":
                            isMatch = value.Length == 10 && value[4] == '-' && value[7] == '-' &&
                                     DateTime.TryParse(value, out _);
                            break;
                        case "MM/dd/yyyy":
                            isMatch = value.Length == 10 && value[2] == '/' && value[5] == '/' &&
                                     DateTime.TryParse(value, out _);
                            break;
                        case "yyyy/MM/dd":
                            isMatch = value.Length == 10 && value[4] == '/' && value[7] == '/' &&
                                     DateTime.TryParse(value, out _);
                            break;
                        case "Decimal":
                            isMatch = value.Contains(".") && double.TryParse(value, out _);
                            break;
                        case "Integer":
                            isMatch = !value.Contains(".") && long.TryParse(value, out _);
                            break;
                        case "Currency":
                            isMatch = (value.StartsWith("$") || value.StartsWith("€") || value.StartsWith("£")) &&
                                     value.Contains(".") &&
                                     (value.Length > 1 && double.TryParse(value.Substring(1), out _));
                            break;
                        case "PhoneNumber":
                            isMatch = value.Length >= 10 && value.Count(c => char.IsDigit(c)) >= 10 &&
                                     (value.Contains("-") || value.Contains("(") || value.Contains(")") || value.Contains(" "));
                            break;
                        case "Email":
                            isMatch = value.Contains("@") && value.Contains(".");
                            break;
                        case "URL":
                            isMatch = value.StartsWith("http://") || value.StartsWith("https://") || value.StartsWith("www.");
                            break;
                        case "IPAddress":
                            isMatch = value.Count(c => c == '.') == 3 &&
                                     value.Split('.').All(part => int.TryParse(part, out var num) && num >= 0 && num <= 255);
                            break;
                        case "GUID":
                            isMatch = Guid.TryParse(value, out _);
                            break;
                    }

                    if (isMatch && !examples.Contains(value))
                    {
                        examples.Add(value);

                        if (examples.Count >= 5)
                            break;
                    }
                }
            }
            catch
            {
                // Fallback if any error occurs
                return new List<string>();
            }

            return examples;
        }
    }
}
