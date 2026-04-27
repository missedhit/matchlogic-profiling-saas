using System;
using System.Collections.Generic;

namespace MatchLogic.Domain.Export;

/// <summary>
/// Extension methods for reading typed values from Parameters dictionary.
/// </summary>
public static class ParameterExtensions
{
    public static string GetString(this Dictionary<string, string>? parameters, string key, string defaultValue)
    {
        if (parameters == null)
            return defaultValue;

        return parameters.TryGetValue(key, out var value) && !string.IsNullOrEmpty(value)
            ? value
            : defaultValue;
    }

    public static int GetInt(this Dictionary<string, string>? parameters, string key, int defaultValue)
    {
        if (parameters == null)
            return defaultValue;

        return parameters.TryGetValue(key, out var value) && int.TryParse(value, out var result)
            ? result
            : defaultValue;
    }

    public static bool GetBool(this Dictionary<string, string>? parameters, string key, bool defaultValue)
    {
        if (parameters == null)
            return defaultValue;

        return parameters.TryGetValue(key, out var value) && bool.TryParse(value, out var result)
            ? result
            : defaultValue;
    }

    public static char GetChar(this Dictionary<string, string>? parameters, string key, char defaultValue)
    {
        if (parameters == null)
            return defaultValue;

        return parameters.TryGetValue(key, out var value) && value.Length > 0
            ? value[0]
            : defaultValue;
    }

    public static T GetEnum<T>(this Dictionary<string, string>? parameters, string key, T defaultValue) where T : struct, Enum
    {
        if (parameters == null)
            return defaultValue;

        return parameters.TryGetValue(key, out var value) && Enum.TryParse<T>(value, true, out var result)
            ? result
            : defaultValue;
    }

    public static double GetDouble(this Dictionary<string, string>? parameters, string key, double defaultValue)
    {
        if (parameters == null)
            return defaultValue;

        return parameters.TryGetValue(key, out var value) && double.TryParse(value, out var result)
            ? result
            : defaultValue;
    }
}