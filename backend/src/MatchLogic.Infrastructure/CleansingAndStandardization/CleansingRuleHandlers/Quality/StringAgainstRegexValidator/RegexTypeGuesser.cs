using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace MatchLogic.Data.QualityII;

/// <summary>
/// Provides the ability to test a string value against a regular expression.
/// </summary>
public class RegexTypeGuesser : StringTypeGuesser
{
    #region Enums

    /// <summary>
    /// Represents the categories of regular expressions used.
    /// </summary>
    public enum Types
    {
        Email,
        Url,
        DateTime,
        Number,
        Name,
        SocialSecurity,
        Address,
        Zip,
        Phone,
        Misc,
        Unclassified,
        UserDefined,
        Null
    }

    #endregion

    #region Constructors

    /// <summary>
    /// Constructs an instance of the RegexTypeGuesser class.
    /// </summary>
    /// <param name="pattern">The regular expression to use.</param>
    /// <param name="patternDescription">The description of the regular expression.</param>
    /// <param name="patternType">The type of the regular expression.</param>
    public RegexTypeGuesser(String pattern, String patternDescription, Types patternType)
    {
        _protectedDescription = patternDescription;
        IsUserDefined = false;
        Type = patternType;

        RegexPattern = pattern;
        ValidatingRegex = new Regex(RegexPattern, RegexOptions.Compiled);
    }

    /// <summary>
    /// Constructs an instance of the RegexTypeGuesser class.
    /// </summary>
    /// <param name="patternType">The type of the regular expression.</param>
    public RegexTypeGuesser(Types patternType) : this(String.Empty, String.Empty, patternType)
    {
        IsUserDefined = true;
    }
    #endregion

    #region Fields and properties

    private readonly Regex ValidatingRegex;

    /// <summary>
    /// The regular expression used in the type guesser.
    /// </summary>
    public String RegexPattern { get; } = String.Empty;

    /// <summary>
    /// The category of the regular expression used in the type guesser.
    /// </summary>
    public Types Type { get; }

    /// <summary>
    /// Indicates if the pattern is defined by a user.
    /// </summary>
    public readonly Boolean IsUserDefined; // when set to false, pattern is reverted to its original value

    //TODO: add XML comments
    public String[] GroupNames { get; } = null;
    //TODO: add XML comments
    public String WordSmithFullFileName { get; set; } = String.Empty;
    //TODO: add XML comments
    public String WordSmithFileName => System.IO.Path.GetFileNameWithoutExtension(WordSmithFullFileName);

    #endregion

    #region Methods

    /// <summary>
    /// Validates the string specified against the regular expression.
    /// </summary>
    /// <param name="stringToTest">A string data to test.</param>
    /// <returns></returns>
    public override Boolean Validate(String stringToTest) => ValidatingRegex.Match(stringToTest).Success;

    #endregion
}
