using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.CleansingAndStandardization.Parser.GenericParser
{
    public class DictionaryCategory : GenericCategory
    {
        #region Fields

        public Dictionary<String, Boolean> Dictionary = new Dictionary<String, Boolean>();

        #endregion

        #region Constructors

        public DictionaryCategory(Int32 categoryId)
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

        #region overridden methods

        public override Boolean IsMember(String testString)
        {
            testString = testString.ToLower();
            Boolean result = Dictionary.ContainsKey(testString);
            return result;
        }

        #endregion

        public void Add(String key)
        {
            key = key.Trim().ToLower();
            Dictionary.Add(key, true);
        }

        #endregion
    }
}
