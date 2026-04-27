using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.CleansingAndStandardization.Parser.GenericParser
{
    internal class AbbreviationCategory : GenericCategory
    {
        #region Fields

        public Dictionary<String, String> Dictionary = new Dictionary<String, String>();

        #endregion

        #region Constructors

        public AbbreviationCategory(Int32 categoryId)
            : base(categoryId)
        {
        }

        #endregion

        #region Properties and Fields

        public Int32 Count
        {
            get { return Dictionary.Count; }
        }

        #endregion

        #region Methods

        #region overriddedn methods

        public override Boolean IsMember(String testString)
        {
            String tmp = GetValue(testString);
            Boolean result = (!String.IsNullOrEmpty(tmp));
            return result;
        }

        #endregion

        public void AddPair(String key, String value, Boolean toAddKeyOnly = true)
        {
            key = key.Trim().ToLower();
            if (!Dictionary.ContainsKey(key))
            {
                String trimmedValue = value.Trim();
                Dictionary.Add(key, trimmedValue);

                /*
                 for example: if we already added a key value pair: "ALABAMA", "AL", we want to add the pair "AL", "AL", to simplify the process
                 if we find word "ALABAMA" we will find a value "AL" and also if we test for a key "AL" we will also find the value "AL"
                 in other words using the same dictionary we determine is the word in the dictionary and get its abbreviation.
                 this method create a pairs like "AL", "AL" so we don't need to hardcode every pair
                */

                if (toAddKeyOnly)
                {
                    String trimmedValueLower = trimmedValue.ToLower();
                    if (!Dictionary.ContainsKey(trimmedValueLower))
                    {
                        Dictionary.Add(trimmedValueLower, trimmedValue);
                    }

                    String trimmedValueLowerWithPoint =
                        trimmedValueLower + "."; // abbreviations are sometimes with point at end
                    if (!Dictionary.ContainsKey(trimmedValueLowerWithPoint))
                    {
                        Dictionary.Add(trimmedValueLowerWithPoint, trimmedValue);
                    }
                }
            }
        }

        public String GetValue(String key)
        {
            String result = null;
            key = key.Trim().ToLower();
            if (Dictionary.ContainsKey(key))
            {
                result = Dictionary[key];
            }

            return result;
        }

        #endregion
    }
}
