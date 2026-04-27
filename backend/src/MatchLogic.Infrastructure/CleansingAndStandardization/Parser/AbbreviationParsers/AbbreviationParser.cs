using MatchLogic.Domain.CleansingAndStandaradization;
using MatchLogic.Infrastructure;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace MatchLogic.Parsers;

public class AbbreviationParser
{
    private Dictionary<string, string> _abbreviationDict;
    public AbbreviationParser()
    {
        _abbreviationDict = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        Load();
    }

    private void Load()
    {
        _abbreviationDict.Clear();
        AddFileContentToAbbreviationDictionary("MatchLogic.Infrastructure.Resources.Abbreviations.xml");
        AddFileContentToAbbreviationDictionary("MatchLogic.Infrastructure.Resources.AdditionalAbbreviations.xml");
    }

    private void AddFileContentToAbbreviationDictionary(string fileName)
    {
        DataTable table = new DataTable("Abbreviations");
        if (ResourceHelper.LoadTableFromResourcesXml(table, fileName))
        {
            for (int i = 0; i < table.Rows.Count; i++)
            {
                String abbreviation = (String)table.Rows[i][0];
                String key = abbreviation.ToUpper();

                if (!_abbreviationDict.ContainsKey(key))
                {
                    _abbreviationDict.Add(key, abbreviation);
                }
            }
        }
        else
        {
            // resource is absent
            throw new ArgumentException($"Can't find resource'{fileName}'");
        }
    }

    public string Transform(String inputString, ProperCaseOptions options, Boolean leaveAbbreviations, char[] delimiters)
    {
        var actualDelimiters = delimiters ?? options.Delimiters.ToCharArray();

        String[] parts = inputString.Split(actualDelimiters, StringSplitOptions.RemoveEmptyEntries);

        Int32 startIndex = 0;
        Int32 index = -1;

        StringBuilder sb = new StringBuilder(inputString.Length);

        foreach (String part in parts)
        {
            String upperPart = part.ToUpper();
            String value;

            index = inputString.IndexOf(part, startIndex, StringComparison.CurrentCulture);

            if (index >= 0)
            {
                sb.Append(inputString.Substring(startIndex, index - startIndex));

                if (leaveAbbreviations && _abbreviationDict.TryGetValue(upperPart, out value))
                {
                    sb.Append(value);
                }
                else
                {
                    String transformedPart = part;

                    if (!options.IgnoreCaseOnExceptions)
                    {
                        if (options.Exceptions.Any(e => e.Equals(part, StringComparison.CurrentCultureIgnoreCase)))
                        {
                            switch (options.ActionOnException)
                            {
                                case ActionOnException.ConvertToLower:
                                    transformedPart = part.ToLower();
                                    break;
                                case ActionOnException.ConvertToUpper:
                                    transformedPart = part.ToUpper();
                                    break;
                                case ActionOnException.LeaveCaseAsItIs:
                                    transformedPart = part;
                                    break;
                            }
                        }
                        else
                        {
                            transformedPart = ProperCase(transformedPart);
                        }
                    }
                    else
                    {
                        transformedPart = ProperCase(transformedPart);
                    }

                    sb.Append(transformedPart);
                }

                startIndex = index + part.Length;
            }
        }

        if (startIndex < inputString.Length)
        {
            sb.Append(inputString.Substring(startIndex, inputString.Length - startIndex));
        }

        return sb.ToString();
    }
    private string ProperCase(string input)
    {
        System.Globalization.TextInfo text = System.Globalization.CultureInfo.CurrentCulture.TextInfo;
        return text.ToTitleCase(input.ToLower());
    }
}   
