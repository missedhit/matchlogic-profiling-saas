using MatchLogic.Infrastructure.CleansingAndStandardization.Parser.GenericParser;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.CleansingAndStandardization.Parser.AddressParser
{
    internal class StateCategory : AbbreviationCategory
    {
        #region Fields

        /// <summary>
        /// key is an abbreviation of state and value is a country
        /// </summary>
        private Dictionary<String, String> internallStateCountryDictionary = new Dictionary<String, String>();

        #endregion

        #region Constructors

        public StateCategory(Int32 categoryId)
            : base(categoryId)
        {
        }

        #endregion

        #region Methods

        public void AddPairPlusCountry(String key, String value, String country)
        {
            AddPair(key, value);
            String tmpCountry;
            String stateAbbreviation = value;
            if (!internallStateCountryDictionary.TryGetValue(stateAbbreviation, out tmpCountry))
            {
                internallStateCountryDictionary.Add(stateAbbreviation, country);
            }
        }

        public String TryGetCountry(String stateAbbreviation)
        {
            String result;
            internallStateCountryDictionary.TryGetValue(stateAbbreviation, out result);
            return result;
        }

        #endregion
    }
}
