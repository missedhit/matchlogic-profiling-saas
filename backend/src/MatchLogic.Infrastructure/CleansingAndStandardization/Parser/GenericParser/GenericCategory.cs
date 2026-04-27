using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.CleansingAndStandardization.Parser.GenericParser
{
    public abstract class GenericCategory
    {
        #region Constructors

        public GenericCategory(Int32 categoryId)
        {
            CategoryId = categoryId;
        }

        #endregion

        #region Methods

        /// <summary>
        /// if testString belongs to category then returns true
        /// </summary>
        /// <param name="testString"></param>
        /// <returns></returns>
        public abstract Boolean IsMember(String testString);

        #endregion

        #region Properties and Fields

        public readonly Int32 CategoryId;

        #endregion
    }
}
