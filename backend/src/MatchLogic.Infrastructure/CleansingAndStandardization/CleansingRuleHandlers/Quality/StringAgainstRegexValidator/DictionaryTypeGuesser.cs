using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Data.QualityII;

/// <summary>
/// Provides the ability to determine whether a string value belongs to a set of predefined dictionaries.
/// </summary>
public class DictionaryTypeGuesser : StringTypeGuesser
{
    #region Properties and Fields

    private PredefinedStringTypes DictionaryStringTypes { get; }

    /// <summary>
    /// Indicates whether we want to split the string value into separate words to process or not.
    /// </summary>
    public Boolean AllowTextSplit { get; }
    // example: "Barn Barnes llc" is a company name because it contains a whole word "llc" - AllowTextSplit should be true
    // "New York" - AllowTextSplit should be false because the whole text needs to match a city name

    #endregion

    #region Constructors

    /// <summary>
    /// Constructs an instance of the DictionaryTypeGuesser class.
    /// </summary>
    /// <param name="dictionaryDescription">The description of the dictionary.</param>
    /// <param name="predefinedStringTypes">The list of predefined dictionaries.</param>
    /// <param name="allowTextSplit">Indicates whether we want to split the string value into separate words to process or not.</param>
    public DictionaryTypeGuesser(String dictionaryDescription, PredefinedStringTypes predefinedStringTypes,
        Boolean allowTextSplit)
    {
        _protectedDescription = dictionaryDescription;

        DictionaryStringTypes = predefinedStringTypes;
        AllowTextSplit = allowTextSplit;
    }

    #endregion

    #region Methods
    
    /// <summary>
    /// Validates the string specified against the list of dictionaries.
    /// </summary>
    /// <param name="stringToTest">A string data to test.</param>
    /// <returns></returns>
    public override Boolean Validate(String stringToTest) => DictionaryStringTypes.FindTypes(stringToTest).Count > 0;

    #endregion
}
