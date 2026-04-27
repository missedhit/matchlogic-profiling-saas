using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Data.QualityII;

/// <summary>
/// Defines which dictionaries are used in profiling.
/// </summary>
public class ProfilerDictionaryOptions
{
    /// <summary>
    /// pairs:  Name Of Dictionary - Enabled
    /// </summary>
    private readonly List<KeyValuePair<String,Boolean>> Items = new List<KeyValuePair<String, Boolean>>();

    /// <summary>
    /// Shows if a dictionary with the specified name enabled.
    /// </summary>
    /// <param name="dictionaryName">Name of the dictionary.</param>
    /// <returns></returns>
    public Boolean IsEnabled(String dictionaryName)
    {
        foreach (var item in Items)
        {
            if (item.Key.Equals(dictionaryName))
            {
                return item.Value;
            }
        }
        return true;
    }

    /// <summary>
    /// Adds an entry to the list of dictionaries.
    /// </summary>
    /// <param name="dictionaryName">The name of the dictionary.</param>
    /// <param name="isEnabled">Shows if the dictionary is enabled.</param>
    public void Add(String dictionaryName, Boolean isEnabled)
    {
        Items.Add(new KeyValuePair<String, Boolean>(dictionaryName, isEnabled));
    }
}
