using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;

namespace MatchLogic.Data.QualityII;

/// <summary>
/// Provides common functionality for string type guessers.
/// </summary>
public abstract class StringTypeGuesser : IEquatable<StringTypeGuesser>
{
    #region Properties and Fields

    /// <summary>
    /// The text that describes the string type guesser (protected backing field).
    /// </summary>
    protected String _protectedDescription;

    /// <summary>
    /// The text that describes the string type guesser.
    /// </summary>
    public String Description
    {
        get { return _protectedDescription; }
        set { _protectedDescription = value; }
    }

    #endregion

    #region Methods

    /// <summary>
    /// Checks if the test value has matches among the dictionaries and regular expressions involved. Returns true if there is at least one match.
    /// </summary>
    /// <param name="testString">A string value to test against the list of dictionaries and regular expressions.</param>
    /// <returns></returns>
    public abstract Boolean Validate(String testString);

    #endregion

    #region Overridden methods

    /// <summary>
    /// Returns the hash code for the instance of the class.
    /// </summary>
    public override Int32 GetHashCode()
    {
        return Description.GetHashCode();
    }

    #endregion

    #region IEquatable<> implementation

    /// <summary>
    /// Determines whether the instance of the StringTypeGuesser class is equal to another instance of the class.
    /// </summary>
    /// <param name="other">Instance to check for equality.</param>
    /// <returns></returns>
    public Boolean Equals(StringTypeGuesser other)
    {
        return Description.Equals(other.Description, StringComparison.InvariantCultureIgnoreCase);
    }

    #endregion
}


